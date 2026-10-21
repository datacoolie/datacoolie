package org.datacoolie.iceberg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.actions.RewritePositionDeleteFiles;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/**
 * Typed native maintenance actions for a table already bound by {@link GuardedTableOperations}.
 *
 * <p>This helper never loads a table by name. Every action receives the same bound table and
 * refreshes that table through its guarded operations before invoking the native action. Spark
 * action defaults are retained; callers can select a procedure without this helper forcing options
 * such as {@code rewrite-all}.
 */
public final class NativeMaintenance {
  /** ABI identifier checked by the Python runtime verifier. */
  public static final String HELPER_ABI = GuardedTableOperations.HELPER_ABI;
  /** SHA-256 of every canonical Java helper source used for this build. */
  public static final String SOURCE_FINGERPRINT = GuardedTableOperations.SOURCE_FINGERPRINT;
  /** SHA-256 of the pinned Iceberg Spark runtime JAR used for this build. */
  public static final String RUNTIME_JAR_SHA256 = GuardedTableOperations.RUNTIME_JAR_SHA256;

  public static final String REWRITE_DATA_FILES = "rewrite_data_files";
  public static final String REWRITE_POSITION_DELETE_FILES = "rewrite_position_delete_files";
  public static final String REWRITE_MANIFESTS = "rewrite_manifests";
  public static final String EXPIRE_SNAPSHOTS = "expire_snapshots";
  public static final String REMOVE_ORPHAN_FILES = "remove_orphan_files";

  private NativeMaintenance() {}

  /**
   * Execute one native action on the supplied guarded table and return observed action metrics.
   *
   * @param table a table returned by {@link GuardedTableOperations#bind(Table, String)}
   * @param spark the borrowed Spark session used by {@link SparkActions}
   * @param action one of the five supported action names
   * @param cutoff epoch milliseconds used by snapshot expiration and orphan-file discovery
   * @return an immutable map containing only values observed from the native action
   * @throws ActionFailure when the action or a delete callback fails after partial work
   */
  public static Map<String, Object> execute(
      Table table, SparkSession spark, String action, long cutoff) {
    Table guarded = requireGuarded(table);
    Objects.requireNonNull(spark, "spark session");
    requireAction(action);
    // This refresh is intentionally before SparkActions obtains any action state.
    guarded.refresh();
    switch (action) {
      case REWRITE_DATA_FILES:
        return rewriteDataFiles(guarded, spark, action);
      case REWRITE_POSITION_DELETE_FILES:
        return rewritePositionDeleteFiles(guarded, spark, action);
      case REWRITE_MANIFESTS:
        return rewriteManifests(guarded, spark, action);
      case EXPIRE_SNAPSHOTS:
        return expireSnapshots(guarded, spark, action, cutoff);
      case REMOVE_ORPHAN_FILES:
        return removeOrphanFiles(guarded, spark, action, cutoff);
      default:
        // requireAction above keeps this branch unreachable if the action list changes.
        throw new IllegalArgumentException("Unsupported native maintenance action: " + action);
    }
  }

  /**
   * Return history from the same guarded table after one guarded refresh.
   *
   * <p>Entries are returned newest first. A missing snapshot summary is omitted from that entry;
   * no synthetic summary or timestamp is invented.
   */
  public static List<Map<String, Object>> history(Table table, int limit) {
    return history(table, limit, null, null);
  }

  /** Return bounded history from the same guarded table with inclusive millisecond filters. */
  public static List<Map<String, Object>> history(
      Table table, int limit, Long startTimeMillis, Long endTimeMillis) {
    if (limit < 0) {
      throw new IllegalArgumentException("History limit must be non-negative");
    }
    Table guarded = requireGuarded(table);
    guarded.refresh();
    if (limit == 0) {
      return Collections.emptyList();
    }
    List<HistoryEntry> entries = new ArrayList<>(guarded.history());
    entries.sort(
        Comparator.comparingLong(HistoryEntry::timestampMillis)
            .reversed()
            .thenComparing(Comparator.comparingLong(HistoryEntry::snapshotId).reversed()));
    List<Map<String, Object>> result = new ArrayList<>();
    for (HistoryEntry entry : entries) {
      long timestamp = entry.timestampMillis();
      if (startTimeMillis != null && timestamp < startTimeMillis) {
        continue;
      }
      if (endTimeMillis != null && timestamp > endTimeMillis) {
        continue;
      }
      Map<String, Object> item = new LinkedHashMap<>();
      item.put("snapshot_id", entry.snapshotId());
      item.put("timestamp_ms", timestamp);
      Snapshot snapshot = guarded.snapshot(entry.snapshotId());
      if (snapshot != null) {
        Long parentId = snapshot.parentId();
        if (parentId != null) {
          item.put("parent_id", parentId);
        }
        String operation = snapshot.operation();
        if (operation != null) {
          item.put("operation", operation);
        }
        Map<String, String> summary = snapshot.summary();
        if (summary != null) {
          item.put("summary", Collections.unmodifiableMap(new LinkedHashMap<>(summary)));
        }
      }
      result.add(Collections.unmodifiableMap(item));
      if (result.size() == limit) {
        break;
      }
    }
    return Collections.unmodifiableList(result);
  }

  private static Map<String, Object> rewriteDataFiles(
      Table table, SparkSession spark, String action) {
    Map<String, Object> detail = baseDetail(action);
    try {
      RewriteDataFiles.Result result = SparkActions.get(spark).rewriteDataFiles(table).execute();
      long rewrittenFiles = result.rewrittenDataFilesCount();
      long addedFiles = result.addedDataFilesCount();
      long rewrittenBytes = result.rewrittenBytesCount();
      long removedDeleteFiles = result.removedDeleteFilesCount();
      long failedFiles = result.failedDataFilesCount();
      detail.put("rewritten_data_files", rewrittenFiles);
      detail.put("added_data_files", addedFiles);
      detail.put("rewritten_bytes", rewrittenBytes);
      detail.put("removed_delete_files", removedDeleteFiles);
      detail.put("failed_data_files", failedFiles);
      // Rewritten data files are the replaced input files; delete-file cleanup remains a
      // separately reported native count and is not substituted for the primary effect.
      putMetrics(detail, addedFiles, rewrittenFiles, null, rewrittenBytes);
      if (failedFiles > 0) {
        throw failure(
            action,
            detail,
            new IllegalStateException(
                "Native data-file rewrite reported failed data files: " + failedFiles));
      }
      return succeeded(detail, "Native data-file rewrite completed");
    } catch (ActionFailure failure) {
      throw failure;
    } catch (Exception error) {
      throw failure(action, detail, error);
    }
  }

  private static Map<String, Object> rewritePositionDeleteFiles(
      Table table, SparkSession spark, String action) {
    Map<String, Object> detail = baseDetail(action);
    try {
      RewritePositionDeleteFiles.Result result =
          SparkActions.get(spark).rewritePositionDeletes(table).execute();
      long rewrittenFiles = result.rewrittenDeleteFilesCount();
      long addedFiles = result.addedDeleteFilesCount();
      long rewrittenBytes = result.rewrittenBytesCount();
      long addedBytes = result.addedBytesCount();
      detail.put("rewritten_delete_files", rewrittenFiles);
      detail.put("added_delete_files", addedFiles);
      detail.put("rewritten_bytes", rewrittenBytes);
      detail.put("added_bytes", addedBytes);
      putMetrics(detail, addedFiles, rewrittenFiles, addedBytes, rewrittenBytes);
      return succeeded(detail, "Native position-delete rewrite completed");
    } catch (ActionFailure failure) {
      throw failure;
    } catch (Exception error) {
      throw failure(action, detail, error);
    }
  }

  private static Map<String, Object> rewriteManifests(
      Table table, SparkSession spark, String action) {
    Map<String, Object> detail = baseDetail(action);
    try {
      RewriteManifests.Result result = SparkActions.get(spark).rewriteManifests(table).execute();
      long rewrittenManifests = count(result.rewrittenManifests());
      long addedManifests = count(result.addedManifests());
      detail.put("rewritten_manifests", rewrittenManifests);
      detail.put("added_manifests", addedManifests);
      // Iceberg exposes manifest objects/counts here, but no direct byte reclamation metric.
      putMetrics(detail, addedManifests, rewrittenManifests, null, null);
      return succeeded(detail, "Native manifest rewrite completed");
    } catch (ActionFailure failure) {
      throw failure;
    } catch (Exception error) {
      throw failure(action, detail, error);
    }
  }

  private static Map<String, Object> expireSnapshots(
      Table table, SparkSession spark, String action, long cutoff) {
    CleanupCallbacks callbacks = new CleanupCallbacks(table.io());
    Map<String, Object> detail = baseDetail(action);
    detail.put("pre_snapshot_id", snapshotId(table));
    try {
      ExpireSnapshots.Result result =
          SparkActions.get(spark)
              .expireSnapshots(table)
              .expireOlderThan(cutoff)
              .deleteWith(callbacks)
              .execute();
      detail.put("deleted_data_files", result.deletedDataFilesCount());
      detail.put("deleted_equality_delete_files", result.deletedEqualityDeleteFilesCount());
      detail.put("deleted_position_delete_files", result.deletedPositionDeleteFilesCount());
      detail.put("deleted_manifests", result.deletedManifestsCount());
      detail.put("deleted_manifest_lists", result.deletedManifestListsCount());
      detail.put("deleted_statistics_files", result.deletedStatisticsFilesCount());
      // Re-read through the guard so the post-action incarnation is observed.
      String postSnapshotDiagnostic = null;
      try {
        table.refresh();
        detail.put("post_snapshot_id", snapshotId(table));
      } catch (Exception diagnostic) {
        // The native action has already acknowledged its metadata/deletion effects. A failed
        // observation must not turn those effects into a retryable action failure.
        postSnapshotDiagnostic = diagnosticReason(diagnostic);
        detail.put("post_snapshot_diagnostic", postSnapshotDiagnostic);
      }
      addCallbackDetail(detail, callbacks);
      // Expiration removes callbacks' files when reclamation is enabled, but
      // it never adds files.  Keep the addition dimension unmeasured rather
      // than turning that semantic fact into an observed zero; callback
      // success count remains the measured removal count (including zero).
      putMetrics(detail, null, (long) callbacks.successes().size(), null, null);
      if (!callbacks.failures.isEmpty()) {
        throw failure(
            action,
            detail,
            new IllegalStateException("One or more native delete callbacks failed"));
      }
      String reason = "Native snapshot expiration completed";
      if (postSnapshotDiagnostic != null) {
        reason += "; post-action snapshot observation unavailable";
      }
      return succeeded(detail, reason);
    } catch (ActionFailure failure) {
      throw failure;
    } catch (Exception error) {
      addCallbackDetail(detail, callbacks);
      if (!detail.containsKey("metrics")) {
        putMetrics(detail, null, (long) callbacks.successes().size(), null, null);
      }
      throw failure(action, detail, error);
    }
  }

  private static Map<String, Object> removeOrphanFiles(
      Table table, SparkSession spark, String action, long cutoff) {
    CleanupCallbacks callbacks = new CleanupCallbacks(table.io());
    Map<String, Object> detail = baseDetail(action);
    try {
      DeleteOrphanFiles.Result result =
          SparkActions.get(spark)
              .deleteOrphanFiles(table)
              .olderThan(cutoff)
              .usePrefixListing(true)
              .deleteWith(callbacks)
              .execute();
      detail.put("orphan_candidate_count", count(result.orphanFileLocations()));
      addCallbackDetail(detail, callbacks);
      // Orphan cleanup has no added-file observation.  Callback successes are
      // the only measured removal evidence available at this boundary.
      putMetrics(detail, null, (long) callbacks.successes().size(), null, null);
      if (!callbacks.failures.isEmpty()) {
        throw failure(
            action,
            detail,
            new IllegalStateException("One or more native delete callbacks failed"));
      }
      return succeeded(detail, "Native orphan-file removal completed");
    } catch (ActionFailure failure) {
      throw failure;
    } catch (Exception error) {
      addCallbackDetail(detail, callbacks);
      if (!detail.containsKey("metrics")) {
        putMetrics(detail, null, (long) callbacks.successes().size(), null, null);
      }
      throw failure(action, detail, error);
    }
  }

  private static Table requireGuarded(Table table) {
    if (!(table instanceof BaseTable)) {
      throw new IllegalArgumentException("Native maintenance requires a native guarded BaseTable");
    }
    BaseTable base = (BaseTable) table;
    if (!(base.operations() instanceof GuardedTableOperations)) {
      throw new IllegalArgumentException("Native maintenance requires a UUID-guarded table");
    }
    return table;
  }

  private static void requireAction(String action) {
    if (!REWRITE_DATA_FILES.equals(action)
        && !REWRITE_POSITION_DELETE_FILES.equals(action)
        && !REWRITE_MANIFESTS.equals(action)
        && !EXPIRE_SNAPSHOTS.equals(action)
        && !REMOVE_ORPHAN_FILES.equals(action)) {
      throw new IllegalArgumentException("Unsupported native maintenance action: " + action);
    }
  }

  private static Map<String, Object> baseDetail(String action) {
    Map<String, Object> detail = new LinkedHashMap<>();
    detail.put("action", action);
    return detail;
  }

  private static void putMetrics(
      Map<String, Object> detail,
      Long filesAdded,
      Long filesRemoved,
      Long bytesAdded,
      Long bytesRemoved) {
    Map<String, Object> metrics = new LinkedHashMap<>();
    metrics.put("files_added", filesAdded);
    metrics.put("files_removed", filesRemoved);
    metrics.put("bytes_added", bytesAdded);
    metrics.put("bytes_removed", bytesRemoved);
    detail.put("metrics", Collections.unmodifiableMap(metrics));
  }

  private static Map<String, Object> succeeded(Map<String, Object> detail, String reason) {
    detail.put("status", "succeeded");
    detail.put("reason", reason);
    return immutable(detail);
  }

  private static String diagnosticReason(Exception error) {
    String message = error.getMessage();
    return error.getClass().getName() + (message == null ? "" : ": " + message);
  }

  private static Long snapshotId(Table table) {
    Snapshot snapshot = table.currentSnapshot();
    return snapshot == null ? null : snapshot.snapshotId();
  }

  private static long count(Iterable<?> values) {
    long count = 0;
    for (Object ignored : values) {
      count++;
    }
    return count;
  }

  private static Map<String, Object> immutable(Map<String, Object> detail) {
    return Collections.unmodifiableMap(new LinkedHashMap<>(detail));
  }

  private static ActionFailure failure(
      String action, Map<String, Object> partial, Throwable cause) {
    Map<String, Object> detail = new LinkedHashMap<>(partial);
    detail.put("action", action);
    detail.put("status", "failed");
    detail.put("reason", String.valueOf(cause.getMessage()));
    detail.put("failure_type", cause.getClass().getName());
    detail.put("failure_message", String.valueOf(cause.getMessage()));
    return new ActionFailure("Native Iceberg action failed: " + action, immutable(detail), cause);
  }

  private static void addCallbackDetail(Map<String, Object> detail, CleanupCallbacks callbacks) {
    List<String> successes = callbacks.successes();
    List<Map<String, Object>> failures = callbacks.failures();
    detail.put("delete_callback_success_count", successes.size());
    detail.put("confirmed_deleted_count", successes.size());
    detail.put("delete_callback_successes", successes);
    detail.put("delete_callback_error_count", failures.size());
    detail.put("delete_callback_errors", failures);
  }

  /** A public failure carrying the action's observed partial result and callback causes. */
  public static final class ActionFailure extends RuntimeException {
    private final Map<String, Object> detail;

    public ActionFailure(String message, Map<String, Object> detail, Throwable cause) {
      super(message, cause);
      this.detail = Objects.requireNonNull(detail, "detail");
    }

    /** Return the immutable partial result, including observed callback successes and causes. */
    public Map<String, Object> detail() {
      return detail;
    }
  }

  private static final class CleanupCallbacks implements Consumer<String> {
    private final FileIO io;
    private final ConcurrentLinkedQueue<String> successPaths = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<DeleteFailure> failures = new ConcurrentLinkedQueue<>();

    private CleanupCallbacks(FileIO io) {
      this.io = Objects.requireNonNull(io, "table FileIO");
    }

    @Override
    public void accept(String path) {
      try {
        io.deleteFile(path);
        successPaths.add(path);
      } catch (Exception error) {
        failures.add(new DeleteFailure(path, error));
        // Native Spark actions suppress callback failures internally. Rethrowing here prevents
        // the native result from counting this path as deleted while preserving the queued cause.
        throw new CallbackFailure(path, error);
      }
    }

    private List<String> successes() {
      return Collections.unmodifiableList(new ArrayList<>(successPaths));
    }

    private List<Map<String, Object>> failures() {
      List<Map<String, Object>> detail = new ArrayList<>();
      for (DeleteFailure failure : failures) {
        detail.add(failure.asMap());
      }
      return Collections.unmodifiableList(detail);
    }
  }

  private static final class CallbackFailure extends RuntimeException {
    private CallbackFailure(String path, Exception cause) {
      super("Native delete callback failed for " + path, cause);
    }
  }

  private static final class DeleteFailure {
    private final String path;
    private final Throwable cause;

    private DeleteFailure(String path, Throwable cause) {
      this.path = path;
      this.cause = cause;
    }

    private Map<String, Object> asMap() {
      Map<String, Object> detail = new LinkedHashMap<>();
      detail.put("path", path);
      detail.put("cause_type", cause.getClass().getName());
      detail.put("cause", String.valueOf(cause.getMessage()));
      return Collections.unmodifiableMap(detail);
    }
  }
}

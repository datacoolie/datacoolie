package org.datacoolie.qualification;

import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.SparkSession;

/** Test-only deterministic DDL/failure injection at real native boundaries. */
public final class ReplacementProbe implements TableOperations {
  private final TableOperations delegate;
  private final SparkSession spark;
  private final String name;
  private final String trigger;
  private final boolean reusedRoot;
  private final java.util.Map<String, String> originalFiles;
  private java.util.Map<String, String> filesBeforeReplace;
  private java.util.Map<String, String> filesAfterReplace;
  private boolean fired;
  private int refreshes;
  private int commits;
  private String replacementMetadata;
  private String replacementFilesBefore;
  private final List<String> deletions = new ArrayList<>();

  public ReplacementProbe(Table table, SparkSession spark, String name, String trigger) {
    this.delegate = ((BaseTable) table).operations();
    this.spark = spark;
    this.name = name;
    this.reusedRoot = trigger.startsWith("reused_");
    this.trigger = reusedRoot ? trigger.substring(7) : trigger;
    this.originalFiles = fileHashes(table);
  }

  public Table guarded() {
    return new BaseTable(new GuardedTableOperations(this, delegate.current().uuid()), name);
  }
  public Table nativeTable() { return new BaseTable(this, name); }

  public int refreshes() { return refreshes; }
  public int commits() { return commits; }
  public String replacementMetadata() { return replacementMetadata; }
  public int deletions() { return deletions.size(); }
  public String replacementFilesBefore() { return replacementFilesBefore; }

  public String replacementLocation() {
    try {
      return org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, name).location();
    } catch (Exception e) { throw new IllegalStateException(e); }
  }

  public String replacementFileHashes() {
    try {
      Table replacement = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, name);
      return fileHashes(replacement).toString();
    } catch (Exception e) { throw new IllegalStateException(e); }
  }

  private static java.util.Map<String, String> fileHashes(Table replacement) {
    try {
      org.apache.iceberg.io.SupportsPrefixOperations io = (org.apache.iceberg.io.SupportsPrefixOperations) replacement.io();
      java.util.TreeMap<String, String> hashes = new java.util.TreeMap<>();
      for (org.apache.iceberg.io.FileInfo file : io.listPrefix(replacement.location() + "/")) {
        java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
        try (java.io.InputStream input = io.newInputFile(file.location()).newStream()) {
          byte[] buffer = new byte[8192];
          int count;
          while ((count = input.read(buffer)) != -1) digest.update(buffer, 0, count);
        }
        StringBuilder hex = new StringBuilder();
        for (byte value : digest.digest()) hex.append(String.format("%02x", value & 0xff));
        hashes.put(file.location(), hex.toString());
      }
      return hashes;
    } catch (Exception e) { throw new IllegalStateException(e); }
  }

  public void replace() {
    filesBeforeReplace = fileHashes(new BaseTable(delegate, name));
    String freshLocation = delegate.current().location();
    if (!reusedRoot) freshLocation += "_replacement_" + java.util.UUID.randomUUID();
    spark.sql("DROP TABLE " + name);
    spark.sql("CREATE TABLE " + name + " (id BIGINT) USING iceberg LOCATION '" + freshLocation + "'");
    spark.sql("INSERT INTO " + name + " VALUES (901), (902)");
    try {
      Table replacement = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, name);
      replacementMetadata = ((BaseTable) replacement).operations().current().metadataFileLocation();
      replacementFilesBefore = replacementFileHashes();
      filesAfterReplace = fileHashes(replacement);
    } catch (Exception e) {
      throw new IllegalStateException("Could not load synthetic replacement", e);
    }
  }

  public int verifyReusedRootOwnership() {
    try {
      java.util.Map<String, String> after = fileHashes(
          org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, name));
      if (!filesAfterReplace.keySet().containsAll(after.keySet())) {
        throw new AssertionError("Unexpected objects created after replacement");
      }
      int oldStagedDeleted = 0;
      for (java.util.Map.Entry<String, String> file : filesAfterReplace.entrySet()) {
        if (!after.containsKey(file.getKey())) {
          if (!filesBeforeReplace.containsKey(file.getKey()) || originalFiles.containsKey(file.getKey())) {
            throw new AssertionError("Deleted replacement-owned or old committed file: " + file.getKey());
          }
          oldStagedDeleted++;
        } else if (!file.getValue().equals(after.get(file.getKey()))) {
          throw new AssertionError("Object contents changed: " + file.getKey());
        }
      }
      return oldStagedDeleted;
    } catch (Exception e) { throw new IllegalStateException(e); }
  }

  private void inject(String boundary) {
    if (!fired && trigger.equals(boundary)) {
      fired = true;
      replace();
      if (trigger.equals("retry")) {
        throw new CommitFailedException("Injected conflict before retry refresh");
      }
    }
  }

  @Override
  public TableMetadata current() { return delegate.current(); }
  @Override
  public TableMetadata refresh() { refreshes++; inject("refresh"); return delegate.refresh(); }
  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    commits++;
    if (!fired && trigger.equals("same_uuid_retry")) {
      fired = true;
      throw new CommitFailedException("Injected same-UUID conflict before native retry");
    }
    inject(trigger.equals("retry") ? "retry" : "commit");
    delegate.commit(base, metadata);
    inject("postcommit");
  }
  @Override
  public FileIO io() { return delegate.io(); }
  @Override
  public String metadataFileLocation(String filename) { return delegate.metadataFileLocation(filename); }
  @Override
  public LocationProvider locationProvider() { return delegate.locationProvider(); }
  @Override
  public TableOperations temp(TableMetadata metadata) { return delegate.temp(metadata); }

  public void verifyPathsAndLatch(Table table) {
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata before = ops.current();
    String metadataPath = ops.metadataFileLocation("guard-check.metadata.json");
    String dataPath = ops.locationProvider().newDataLocation("guard-check.parquet");
    TableOperations temp = ops.temp(before);
    replace();
    // Simulate a second holder refreshing the delegate without the guard.
    delegate.refresh();
    if (!metadataPath.equals(ops.metadataFileLocation("guard-check.metadata.json"))
        || !dataPath.equals(ops.locationProvider().newDataLocation("guard-check.parquet"))) {
      throw new AssertionError("Guarded paths adopted replacement metadata");
    }
    expectRejected(() -> ops.refresh());
    int refreshCount = refreshes;
    int commitCount = commits;
    expectRejected(() -> ops.current());
    expectRejected(() -> ops.refresh());
    expectRejected(() -> ops.commit(before, before));
    expectRejected(() -> ops.metadataFileLocation("guard-check.metadata.json"));
    expectRejected(() -> ops.locationProvider());
    expectRejected(() -> ops.temp(before));
    if (refreshes != refreshCount || commits != commitCount) {
      throw new AssertionError("Rejected guard invoked mutable delegate");
    }
    if (!before.uuid().equals(temp.current().uuid())) {
      throw new AssertionError("Immutable temporary operations adopted replacement");
    }
    if (!metadataPath.equals(temp.metadataFileLocation("guard-check.metadata.json"))
        || !dataPath.equals(temp.locationProvider().newDataLocation("guard-check.parquet"))) {
      throw new AssertionError("Immutable temporary paths changed");
    }
  }

  private static void expectRejected(Runnable call) {
    try { call.run(); }
    catch (org.apache.iceberg.exceptions.ValidationException expected) { return; }
    throw new AssertionError("Expected sticky UUID rejection");
  }

  public Object execute(Table table, String action, long cutoff) {
    SparkActions actions = SparkActions.get(spark);
    switch (action) {
      case "rewrite_data_files":
        return actions.rewriteDataFiles(table).option("rewrite-all", "true").execute();
      case "rewrite_position_delete_files":
        return actions.rewritePositionDeletes(table).option("rewrite-all", "true").execute();
      case "rewrite_manifests":
        return actions.rewriteManifests(table).execute();
      case "expire_snapshots":
        return actions.expireSnapshots(table).expireOlderThan(cutoff)
            .deleteWith(path -> { deletions.add(path); table.io().deleteFile(path); }).execute();
      case "remove_orphan_files":
        return actions.deleteOrphanFiles(table).olderThan(cutoff).usePrefixListing(true)
            .deleteWith(path -> { deletions.add(path); table.io().deleteFile(path); }).execute();
      default: throw new IllegalArgumentException("Unknown test action: " + action);
    }
  }
}

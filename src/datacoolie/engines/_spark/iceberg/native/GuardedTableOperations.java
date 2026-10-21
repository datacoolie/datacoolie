package org.datacoolie.iceberg;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

/**
 * A bounded incarnation guard for native Iceberg table operations.
 *
 * <p>The expected UUID is supplied by the caller when the table is prepared. It is deliberately
 * never recaptured from the table supplied to {@link #bind(Table, String)}. A changed UUID latches
 * this wrapper into a rejected state, so later reads, path lookups, commits, retries, and temporary
 * operations cannot adopt a replacement table.
 */
public final class GuardedTableOperations implements TableOperations {
  private static final String BUILD_RESOURCE =
      "META-INF/datacoolie-iceberg-guard.properties";
  private static final Properties BUILD = loadBuildProperties();

  /** ABI identifier checked by the Python runtime verifier. */
  public static final String HELPER_ABI = required("helper_abi");
  /** SHA-256 of all canonical Java helper sources used to build this artifact. */
  public static final String SOURCE_FINGERPRINT = required("source_fingerprint");
  /** SHA-256 of the pinned Iceberg Spark runtime JAR used for compilation. */
  public static final String RUNTIME_JAR_SHA256 = required("runtime_jar_sha256");

  private final TableOperations delegate;
  private final String expectedUuid;
  private TableMetadata current;
  private boolean rejected;

  private GuardedTableOperations(TableOperations delegate, String expectedUuid) {
    this.delegate = Objects.requireNonNull(delegate, "table operations");
    this.expectedUuid = Objects.requireNonNull(expectedUuid, "expected UUID");
    this.current = checked(delegate.current());
  }

  /**
   * Bind a native Iceberg table to the UUID selected during preparation.
   *
   * <p>The UUID argument is authoritative. This method intentionally does not obtain a UUID from
   * {@code table}; execution must retain the value captured before the native action starts.
   */
  public static Table bind(Table table, String expectedUuid) {
    if (!(table instanceof BaseTable)) {
      throw new IllegalArgumentException("Qualification requires a native BaseTable");
    }
    BaseTable base = (BaseTable) table;
    return new BaseTable(new GuardedTableOperations(base.operations(), expectedUuid), table.name());
  }

  private static Properties loadBuildProperties() {
    Properties properties = new Properties();
    try (InputStream input = GuardedTableOperations.class.getClassLoader()
        .getResourceAsStream(BUILD_RESOURCE)) {
      if (input == null) {
        throw new IllegalStateException("Missing native guard build resource: " + BUILD_RESOURCE);
      }
      properties.load(input);
      return properties;
    } catch (IOException error) {
      throw new IllegalStateException("Could not read native guard build resource", error);
    }
  }

  private static String required(String key) {
    String value = BUILD.getProperty(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalStateException("Missing native guard build property: " + key);
    }
    return value;
  }

  private TableMetadata checked(TableMetadata metadata) {
    if (rejected || metadata == null || !expectedUuid.equals(metadata.uuid())) {
      rejected = true;
      throw new ValidationException("Prepared table UUID no longer matches the native target");
    }
    return metadata;
  }

  @Override
  public synchronized TableMetadata current() {
    return checked(current);
  }

  @Override
  public synchronized TableMetadata refresh() {
    checked(current);
    current = checked(delegate.refresh());
    return current;
  }

  @Override
  public synchronized void commit(TableMetadata base, TableMetadata metadata) {
    checked(current);
    checked(base);
    checked(metadata);
    // Native REST commits derive AssertTableUUID from this checked base.
    delegate.commit(base, metadata);
    current = checked(delegate.current());
  }

  @Override
  public FileIO io() {
    return delegate.io();
  }

  @Override
  public EncryptionManager encryption() {
    return delegate.encryption();
  }

  @Override
  public synchronized String metadataFileLocation(String filename) {
    checked(current);
    return delegate.temp(current).metadataFileLocation(filename);
  }

  @Override
  public synchronized LocationProvider locationProvider() {
    checked(current);
    return delegate.temp(current).locationProvider();
  }

  @Override
  public synchronized TableOperations temp(TableMetadata metadata) {
    checked(metadata);
    return new GuardedTableOperations(delegate.temp(metadata), expectedUuid);
  }

  @Override
  public long newSnapshotId() {
    return delegate.newSnapshotId();
  }

  @Override
  public boolean requireStrictCleanup() {
    return delegate.requireStrictCleanup();
  }
}

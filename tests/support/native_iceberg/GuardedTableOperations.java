package org.datacoolie.qualification;

import java.util.Objects;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.encryption.EncryptionManager;

/** Experimental T1 guard, not a shipped maintenance integration. */
public final class GuardedTableOperations implements TableOperations {
  private final TableOperations delegate;
  private final String expectedUuid;
  private TableMetadata current;
  private boolean rejected;

  public GuardedTableOperations(TableOperations delegate, String expectedUuid) {
    this.delegate = Objects.requireNonNull(delegate, "table operations");
    this.expectedUuid = Objects.requireNonNull(expectedUuid, "expected UUID");
    this.current = checked(delegate.current());
  }

  public static Table bind(Table table) {
    if (!(table instanceof BaseTable)) {
      throw new IllegalArgumentException("Qualification requires a native BaseTable");
    }
    BaseTable base = (BaseTable) table;
    return new BaseTable(new GuardedTableOperations(base.operations(), table.uuid().toString()), table.name());
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
  public FileIO io() { return delegate.io(); }

  @Override
  public EncryptionManager encryption() { return delegate.encryption(); }

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
  public long newSnapshotId() { return delegate.newSnapshotId(); }

  @Override
  public boolean requireStrictCleanup() { return delegate.requireStrictCleanup(); }
}

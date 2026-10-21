package org.datacoolie.qualification;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.LocationProvider;

/** Test-only callback failure using real native action planning and commit. */
public final class FailureProbe implements TableOperations {
  private final TableOperations delegate;
  private final String name;
  private final boolean diagnosticOnly;
  private int refreshes;

  public FailureProbe(Table table) {
    this(table, false);
  }
  public FailureProbe(Table table, boolean diagnosticOnly) {
    delegate = ((BaseTable) table).operations();
    name = table.name();
    this.diagnosticOnly = diagnosticOnly;
  }
  public Table nativeTable() { return new BaseTable(this, name); }
  public TableMetadata current() { return delegate.current(); }
  public TableMetadata refresh() {
    refreshes++;
    if (diagnosticOnly && refreshes >= 4) {
      throw new IllegalStateException("Injected post-action observation failure");
    }
    return delegate.refresh();
  }
  public void commit(TableMetadata base, TableMetadata metadata) { delegate.commit(base, metadata); }
  public String metadataFileLocation(String file) { return delegate.metadataFileLocation(file); }
  public LocationProvider locationProvider() { return delegate.locationProvider(); }
  public TableOperations temp(TableMetadata metadata) { return delegate.temp(metadata); }
  public FileIO io() { return diagnosticOnly ? delegate.io() : new FailingIO(delegate.io()); }
  private static final class FailingIO implements FileIO {
    private final FileIO original;
    private FailingIO(FileIO original) { this.original = original; }
    public InputFile newInputFile(String path) { return original.newInputFile(path); }
    public OutputFile newOutputFile(String path) { return original.newOutputFile(path); }
    public void deleteFile(String path) {
      throw new IllegalStateException("Injected callback failure for case-owned object");
    }
  }
}

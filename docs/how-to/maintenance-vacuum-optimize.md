# Maintenance (vacuum / optimize)

**Prerequisites** · Existing Delta or Iceberg destinations.
**End state** · Periodic `OPTIMIZE` / `VACUUM` runs safely in parallel without racing fan-in topologies.

## Invoke

```python
result = driver.run_maintenance(
    connection=["bronze", "silver"],  # optional – filter by connection name
    do_compact=True,                  # run OPTIMIZE (default: True)
    do_cleanup=True,                  # run VACUUM  (default: True)
)
```

## Deduplication

When multiple dataflows write to the **same physical destination** (fan-in),
DataCoolie deduplicates before dispatching maintenance. Only the winning
dataflow emits a maintenance log row; covered dataflows are implicitly
covered.

This prevents concurrent `OPTIMIZE` calls from racing on the same table — a
common source of commit failures in Delta.

## Retention

`DataCoolieRunConfig.retention_hours` controls `VACUUM` retention (default:
`DEFAULT_RETENTION_HOURS` = 168 hours / 7 days). Pass it when constructing
the driver:

```python
from datacoolie import DataCoolieDriver, DataCoolieRunConfig

driver = DataCoolieDriver(config=DataCoolieRunConfig(retention_hours=72))
```

## CLI

```powershell
python usecase-sim/runner/maintenance.py `
    --connection local_bronze `
    --retention-hours 72
# Omit compact or cleanup steps individually:
#   --no-compact   skip OPTIMIZE
#   --no-cleanup   skip VACUUM
```

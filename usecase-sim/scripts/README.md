# Simulator scripts

`bootstrap/` owns environment setup, shared input generation, metadata seeding
and explicit reset commands. `features/<topic>/` keeps each feature's fixture
preparation beside its output validators. `runner/` owns execution and dispatch.

Run scripts directly, for example:

```powershell
python usecase-sim/scripts/bootstrap/generate_data.py --targets local
python usecase-sim/scripts/features/schema_types/prepare_datatype_qualification.py --formats parquet
python usecase-sim/runner/run_scenario.py --scenario local_polars_datatype_qualification
```

`runner/runtime_paths.py` is the owner of simulator roots. `_common.py` provides the
shared sample dataset, service defaults and lazy service helpers. Feature
scripts must resolve paths from those roots and put data, logs, watermarks and
dependency caches in their respective `.runtime` areas. Relative CLI paths
use the `datacoolie/` product root.

Every local Driver uses `.runtime/logs` as its framework log base, creating
`execution_logs/` and `system_logs/` directly below it. Feature subdirectories
under `logs/` hold setup/validation receipts and reports. They must not be passed
as framework log roots. Runtime log validators use job and session IDs to select
the current invocation in the shared streams.

When adding a feature, add its preparation/validation scripts to the matching
topic, point a named scenario at them, and isolate engine output and watermark
roots when the engines share an external service.

Behavior ownership, dedicated execution and unresolved gaps are catalogued in
[`cases/`](../cases/README.md). `audit_cases.py` inspects these routes read-only.

# Simulator runners

- `run_scenario.py`: named recipe/group dispatch, setup, final validation and receipts.
- `run.py`: normal ETL execution for selected engine/provider/platform.
- `maintenance.py`: compact/cleanup execution.
- `native_guard.py`: explicit pinned Spark Iceberg guard preparation before fixture setup.
- `run_perf_benchmark.py`: measurement suite execution.
- `qualified_sql_setup.py`: engine table registration for qualified SQL recipes.
- `_runner_utils.py`: shared runner assembly and replay helpers.
- `runtime_paths.py`: canonical data, logs, watermarks and dependency roots.
- `runtime_logs.py`: read-only framework record lookup by job/current session.
- `runtime_timing.py`: monotonic phase timing and additive console markers.
- `runtime_profile.py`: direct-run functional settings (opt-in) and conservative package selection.
- `admission.py`: resource locks and bounded scheduling; case catalogs own dependencies.
- `owned_process.py`: host/Docker descendant ownership and verified teardown.
- `runtime_workspace.py`: dispatcher-owned Linux workspace and verified ZIP export.

Import runtime helpers through `runner`. The workspace wrapper is a private
child entrypoint launched by the dispatcher; do not set its runtime-root override
manually. Ordinary framework logs remain under `.runtime/logs`.

## Direct metadata and selection

ETL and maintenance accept repeatable `--metadata-path` and optional exact
`--connections-path` / `--schema-hints-path` files. A single path keeps its scalar
behavior; multiple paths retain their order. Ordinary section-wrapped JSON goes
straight to FileProvider, which combines sections in memory. Explicit section
files replace their sections. Flat group JSON contains canonical flow definitions;
`connections.json` and `schema_hints.json` supply the complete shared sections.
Empty paths and config-file/directory conflicts fail before setup.

Scenario `metadata_path` accepts a string or ordered list and transports shared
section paths to host/Docker runners. Optimized dependency inspection for these
inputs uses a temporary FileProvider with full model validation and guaranteed
cleanup; unknown inputs keep conservative dependencies. Bootstrap and the JSON
metadata API also accept repeated source paths and shared section paths. Database
seeds require no JSON intermediate; explicit file exports go to
`.runtime/data/output/metadata_exports/` or `--output-dir` and cannot overwrite
authored sources.

Repeat `--dataflow` to select an exact ordered subset after full provider validation.
`--metadata-bindings` takes a JSON object of tokens and named row operations for
one invocation; name, stage and ID stay immutable. Focused validators keep their
projection in memory. `--metadata-document` accepts a generated document in memory.
`metadata_documents.py` reads explicit workflow selections for exports/API/database
seeding. Ordinary ETL and maintenance require no generated metadata file; see
[metadata layout](../metadata/file/README.md).

`error_messages` contains standalone negative steps. A qualified ref such as
`error_messages.local_polars_startup_failure` runs one step. A complete bundle runs
every step with its own services, setup, process and receipt; the aggregate reports
step results while profile qualification uses the individual receipts.
Bundle steps run exclusively in their declared order, including qualified leaf
selections. A failed probe still allows subsequent probes to run. Selecting the
bundle with `--all --execution-mode optimized` retains resource-based overlap
for ordinary independent scenarios.

## Optimized execution

An isolated service stack can supply an absolute regular Compose override file
through `DATACOOLIE_COMPOSE_OVERRIDE` and its `COMPOSE_PROJECT_NAME`. Both scenario
auto-start and platform bootstrap use the base file plus that override. Startup
uses local pinned images with `--no-build --pull never`; an invalid override or
project fails before service inspection/startup. The stack supervisor owns
container, volume and mount isolation.

Run the complete selection with `python usecase-sim/runner/run_scenario.py --all`;
the dispatcher defaults to optimized execution. Use
`--execution-mode sequential` for the serial control path. Direct functional
runners remain conservative when `DATACOOLIE_SIM_EXECUTION_MODE` is unset; set
it to `optimized` to opt into the functional profile. The dispatcher forwards
its selected mode to Docker, setup scripts and nested validator reruns.
Performance benchmarks retain their own profile.

The four guarded Spark REST maintenance/expiration recipes first supervise
`native_guard.py` in their executing Spark container (or host fallback). Missing
guard artifacts are built with the public native builder using existing pinned
Spark3.5.9/Iceberg1.10.1 JARs, then published without replacement under
`.runtime/dependencies/native-maintenance/<source-hash>-24classes/`.
The authored recipes use `{native_guard_manifest}` for this coordinate. The
dispatcher resolves that token once from the current native source and class
count, including every invocation/final validator argument. Changing source
selects a new immutable build; an explicitly supplied path stays pinned. The
executing helper independently checks the coordinate, so different source bytes
on the host and container still fail before seeding.
Publication uses an atomic rename where supported. Docker Desktop bind mounts
that reject this operation use exclusive directory reservation and write the
manifest last; incomplete publication fails verification and needs operator
inspection rather than automatic repair.
Existing artifacts require matching source, ABI, versions, class coverage,
embedded provenance and actual JAR hashes. Stale/incomplete artifacts fail before
namespace/table seeding; they are not overwritten. Missing pinned dependencies
also fail before seed, with no download or Spark session startup.
Each current receipt includes `prerequisite_execution`; the supervised command,
exit and output are in `<name>.prerequisite.process.json` and
`<name>.prerequisite.log`. A failed prerequisite also replaces the scenario's
combined console log with the current attempt ID and failure output.
`prerequisite_s` includes build/verification and belongs
to the scenario/suite total. JVM provenance and native effect/no-work validators
remain active during execution.

Read `scenarios/suite.timing.json`, `<name>.receipt.json` and invocation receipts
under `.runtime/logs`. Child engine/execution/shutdown timings are separate from
scenario setup/validation/wall timings; the latter include process supervision.
Cache preparation is a subset of engine creation, so these durations must not
be added as independent phases. Parallel scenario durations also overlap.
Standalone date/window receipts expose `validation_runtime_timing` and
`validation_runtime_profile` only from the current supervised attempt.
`log_lookup_s` excludes caller work but is nested inside some validation and
`driver_and_evidence_s` spans; `execution_s` measures the Driver call itself.
Focused case receipts also expose `runs[].timings`: metadata preparation,
Driver startup, execution, Driver close, evidence lookup and total invocation
wall. Metadata preparation includes binding/provenance; full provider hydration
belongs to Driver startup. Evidence lookup includes checks before and after the
session. These phases fit inside the invocation total; custom feature runners
that do not use the shared helper leave this map empty.

In optimized mode `runtime_logs.records` filters recognized foreign writer
filenames before reading JSON. Actual job/session values still decide matches;
names outside that layout are inspected and no parsed records persist between calls.
Its `full_scan=True` keyword retains a direct control for unusual layouts.

Process sidecars capture the actual supervised command, PID/token and container.
They are included only when their attempt ID matches the current receipt.
Interrupted or failed preflight attempts cannot retain a previous PASS receipt.

On Windows Docker, optimized `local_spark_replay_surface_window_safety` runs in
an owned Linux-local runtime tree. Its existing validator checks all data/state
and timestamp assertions there. After validation, one ZIP and a schema-2 manifest
are exported under `.runtime/logs/scenarios/native/<attempt-id>/`. The dispatcher
verifies the exact directory/member inventory and every file size/SHA-256 before
PASS. Export failure or timeout cannot publish passing evidence.

This optimized validator builds its tiny seed rows from a typed JSON literal on
the Spark JVM, keeping nullable BIGINT/BIGINT/STRING fields and the session's
partition count. Native Spark writes and all safety/retry assertions still run.
`fixture_dataframe_s` and `fixture_write_s` are subdivisions of
`fixture_setup_s`; do not add them to that total.

The current scenario receipt provides host `archive_path`, `manifest_path`, the
original Linux `runtime_root` and ZIP `member_prefix` (`runtime/`). Open the ZIP
to inspect native logs, data and watermarks; JSON paths remain original Linux
paths. Manifest `mtime_ns` records source timestamps; extraction does not promise
to reproduce nanosecond timestamps. No extracted host runtime directory is
implied. Other recipes and sequential mode keep their existing coordinates.

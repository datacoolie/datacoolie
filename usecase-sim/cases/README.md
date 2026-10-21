# Behavior catalog

Open `<group>.json` for expected behaviors, fixture/flow ownership, recipe routes,
preconditions, assertions and unresolved gaps. `inventory.json` declares standalone
helpers, generated metadata routes and intentional discovery exclusions.

From the `datacoolie/` product directory:

```powershell
python usecase-sim/scripts/audit_cases.py --list
python usecase-sim/scripts/audit_cases.py --group merge --json
python usecase-sim/runner/run_scenario.py --group merge --list
python usecase-sim/runner/run_scenario.py --group merge
```

Use the workspace root `.venv/Scripts/python.exe` when working in the parent
workspace. Inspection never runs setup/services. Execution of an unknown group
or a group with no dedicated recipe fails; it never falls back to broad baseline.

The primary groups are `read`, `write_basic`, `merge`, `scd2`, `schema_types`,
`transform`, `incremental`, `replay`, `maintenance`, `metadata_artifacts`,
`execution`, `logging` and `workflows`. `performance` is a separate measurement
suite, excluded from functional correctness totals. Dimensions belong to recipes
and required profiles rather than duplicated directory trees.

`behaviors` keeps stable requirement families. Each family's `checks` is the
behavior checklist: observable `acceptance`, activation `preconditions`, related
`case_ids`, exact `assertion_owners`, `coverage`, residual `gap` and `next_action`.
A missing check needs no fake fixture; an empty `case_ids` list is valid when
pytest already owns the behavior or the behavior is missing. Family coverage is
asserted only when all its checks are asserted.
`cases` reference authored fixtures and execution recipes without copying ETL flags.
Each authored `(fixture, flow)` has one primary owner; multiple recipes can reference
it as support. Each scenario has one owning case. `dedicated_scenarios` is the
explicit group execution route; broad bundles remain workflows.

Local/Docker/AWS-MinIO definitions live in `metadata/file/<group>.json`; each real-cloud
platform has one ordinary smoke JSON in `platforms/<platform>/`, owned by a
`workflows` case. Cloud smoke checks representative platform execution separately
from the full local feature matrix. Local workflow manifests reference the local
group documents under `scenarios/workflows/`. Shared connections and global hints
have one canonical file each. `cases` owns test intent and coverage, not another copy of ETL configuration.
Case IDs stay stable across file moves for dependency/history tracking; use
`flows[].fixture` for current locations. Shared documents and manifests have
explicit inventory reasons and their composition is audited.

Coverage is `missing`, `fixture_only`, `asserted`, `deferred` or `unsupported`.
Asserted means a goal-specific oracle exists, including sufficient external pytest
owners; it does not mean that a runtime profile passed. Execution is independently
`not_run`, `passed`, `failed` or `blocked`, derived from identified receipts.
Legacy receipts without revision/runtime/profile/command are not current qualification.
An assertion owner can be `path.py` or `path.py::Class::test_method`; paths are
product-relative. Audit resolves declared Python symbols through the AST;
contract paths must exist, and an optional contract symbol must resolve too.
Generated recipes declare preparation, expected flow IDs and engine/platform profiles.

Read each case's `gap` before execution. Broad baseline and platform demos mostly
remain smoke; services/cloud qualification needs its own prerequisites and evidence.
Do not run `--all` or shared reset/bootstrap commands merely to improve the summary.
Counts and statuses come from the catalog/audit, not a second hand-maintained table.
`--group` scopes checklist, profile and execution rows while checking integrity
of every catalog (`validation_scope: all_catalogs`). Authored dataflow names and
effective IDs are globally unique across the canonical flat group documents;
fixture/flow identity still identifies each case's primary owner.

The JSON report includes all `checks`, their `check_coverage` totals,
`functional_check_coverage` excluding performance, and separate
`measurement_check_coverage`. It includes a
derived `gap_ledger` for unresolved checks. `case_ledger` and
`case_actions` expose the separate fixture action queue. Actions are `none`,
`map_existing_owner`, `strengthen_oracle`, `add_focused_case`, `qualify_profile`
and `defer`. Review an existing pytest owner's actual assertions before promoting
a fixture; a related unit test can cover a check while that fixture stays smoke.
Profile qualification remains independent and requires matching execution evidence.
The shipped inventory sets `coverage_schema_version: 2` to require this checklist
and each case's action. Legacy temporary audit fixtures retain their older schema.


The focused native recipes use `local_polars_<group>_stateful` for `write_basic`,
`merge`, `scd2`, `schema_types` and `maintenance`. They allocate fresh roots and
exercise normal Driver calls. To select just the new schema or maintenance case,
use `--scenario`; their full groups also include existing service-dependent routes.
Local maintenance deletion qualification passed after the runtime explicitly
sets `dry_run=False`. The oracle checks obsolete-file deletion, protected retention
and missing-target skips; other engine/backend profiles remain unqualified.

The dispatcher writes final `.receipt.json` evidence beside legacy invocation
receipts after validation. Current evidence requires the source/catalog fingerprint,
profile, command, runtime and final status. Unverified Docker child runtimes remain
blocked; host runtime metadata cannot qualify them. Changing source, fixtures,
artifact SQL, recipes, tests or dependency declarations makes old evidence historical.


Catalog Python helpers live beside the JSON contracts:
`catalog.py` loads and validates ownership, `checklist.py` derives behavior and
fixture action ledgers, `discovery.py` inventories authored
files, `selection.py` orders dedicated recipes and dependencies, `receipts.py`
tracks runtime/revision evidence, and `report.py` produces summaries. These are
imported through `cases`; the command entrypoint is `scripts/audit_cases.py`.

Two additional focused recipes allocate fresh case-owned roots and run the normal
host-local Polars Driver with native Delta:

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_scd2_older_effective_stateful
python usecase-sim/runner/run_scenario.py --scenario local_polars_sql_root_precedence
```

The SCD2 sequence admits a late input using a newer file mtime while its effective
time is older. Its exact history oracle checks values, boundaries and current
flags, including the native append-all contract that permits two current rows for
the affected key. The SQL case gives the same `orders.sql` filename different
contents in explicit SQL and artifact roots; exact marker rows prove SQL-root
precedence. Multiple SQL roots still use prefix routing, not list-order priority.
These recipes qualify only their host-local profile; other engines and platforms
require separate receipts.

Replay claims distinguish exact monthly request endpoints, C1 no-save hash
preservation and source-authorized monotonic saving, C5 successful continuations
and rejection before offending dispatch. Post-write retry may expose the first
write; its oracle retains failed/succeeded history and reconciles final rows/state
without promising rollback. Folder discovery bounds and case-owned normalized
checkpoints are separate predicates. Global dependency validation also applies
when presentation is filtered with `--group`.

The supported-feature recipes also include four independent flat-file append
routes, composite business-key merge, explicit merge predicate and NTZ timezone
conversion:

```powershell
python usecase-sim/runner/run_scenario.py --scenario local_polars_flat_append_csv
python usecase-sim/runner/run_scenario.py --scenario local_polars_flat_append_json
python usecase-sim/runner/run_scenario.py --scenario local_polars_flat_append_jsonl
python usecase-sim/runner/run_scenario.py --scenario local_polars_flat_append_avro
python usecase-sim/runner/run_scenario.py --scenario local_polars_merge_composite_keys
python usecase-sim/runner/run_scenario.py --scenario local_polars_merge_custom_predicate
python usecase-sim/runner/run_scenario.py --scenario local_polars_ntz_timezone
```

Each route uses one authored primary flow across its state sequence and requires
successful expected-flow evidence in every current session. Flat append checks
exact Unicode/string rows, types, preservation of earlier file hashes and added
files; its receipt identifies the actual selected destination format. Repeating
the same untracked append input is not a deduplication contract. Merge checks
bootstrap, update/insert, untouched rows, tuple uniqueness and an executed stable
rerun. Timezone conversion interprets naive values in `Asia/Ho_Chi_Minh`, persists
microsecond UTC instants and preserves nulls and the aware control column.

Source-derived checks also expose unimplemented uppercase/map-null/mask variants,
persisted writer/schema-option effects, compound Iceberg evolution merge and live
authentication prerequisites. Existing lookback and folder-shard owners cover
their exact helper predicates; related baseline fixtures retain their own oracle
queue. Counts remain derived from the catalog and do not establish exhaustive
public-contract coverage. Host-local recipes require independent receipts for
other engines, services and cloud platforms.

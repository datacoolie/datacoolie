# Dataflow Orchestration

Read when selecting stages, authoring dependencies, sizing concurrency, or exposing job
parameters. The public [dataflows guide](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/),
[run-stage guide](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/), and
[orchestration reference](https://datacoolie.github.io/datacoolie/reference/concepts/orchestration/)
own field names, defaults, assignment, ordering, retry, and result semantics. This reference
keeps the agent decisions and evidence checks needed before a runner is authored.

Stage values are project-defined runtime selections. Use the user's metadata and dependency
contracts to identify upstream and downstream work; do not hardcode stage names, layer names,
their count, or their order. `runner-contract.md` owns parameter transport and
`operations-contract.md` owns replay/maintenance gates.

## Choose the smallest execution arrangement

| Situation | Agent decision |
|---|---|
| Independent dataflows in one invocation | Leave dependency metadata absent unless the project requires co-location or serialization. |
| Independent dataflows across invocations | Let the external scheduler launch the selected runner set and reconcile the complete set. |
| A consumer must wait for producers in one normal call | Use the framework's documented dependency grouping and ordering. |
| Several independent dependency chains | Keep each connected chain explicit; do not invent a cross-chain barrier. |
| Several dependent stages | Prefer separate stage invocations with completion, quality, and freshness checks between stages. |
| Dependencies across jobs or runner invocations | Use an external orchestration barrier; one invocation cannot wait for another. |

Do not add dependency metadata merely because there are many flows or stage names. Runtime does
not infer dependencies from source/destination tables, SQL, lineage, or file ordering. Do not copy
the public scheduling contract into a Skill reference; consult the version-compatible public page
when a scheduling or resource limit matters.

## Stage selection and completion gates

Prefer separate invocations for dependent upstream and downstream stages to isolate retries,
validation, and operational control. Independent flows within a stage can remain ungrouped, while
dependencies within that stage still need the framework-supported ordering when they consume output
from the current run.

The public [run-stage selection guide](https://datacoolie.github.io/datacoolie/guide/operations/run-stage/)
and [dataflow activation rules](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/#activation-and-selection)
define how stage filters and directly supplied dataflows are selected. Treat a list or comma
string as one selection; its textual order is not a stage barrier. A project-owned runner passes
the selected value through its host transport and one driver call.

Selection does not expand to include prerequisites. Before a dependent stage, confirm the required
upstream completion and the project's freshness/completeness contract. A consumer selected alone
may run against existing upstream data when the project allows it; otherwise it is a gate failure.

For stage-by-stage scale-out, the external orchestrator launches every upstream shard, waits for
all of them, checks failures and required quality evidence, then launches downstream shards. One
shard finishing is insufficient evidence for the whole stage. Preserve the same framework/metadata
snapshot and selection across a shard set, as required by the public orchestration reference.

## Selection and operation boundaries

- Use the documented `run(stage=...)` route when framework selection and assignment are needed;
  use a preloaded `dataflows=` list only when the project deliberately owns that selection check.
- Replay and maintenance have separate framework operations and do not inherit normal stage
  dependency ordering. Read `operations-contract.md` and the public operation guides before
  authoring those entrypoints.
- Normal project-owned runners perform one driver call; external orchestration owns repeated
  invocations, barriers, and downstream progression.

## Outcome-scoped verification

For author/build-only requests, validate the metadata, runner, and artifact without launching a
workload. For an explicitly requested safe runtime check, derive expected selected IDs, output, and
state from the matching public recipe/project criteria; inspect result status plus independent
output/state evidence. Treat legitimate empty shards, empty incremental reads, and explicit skips
according to the project contract, while reporting inactive/missing required flows as unmet gates.
Always report performed checks, skipped host/dependency checks, and unresolved questions.

## Evidence to verify when the runtime changes

When the framework itself changes, inspect the orchestration implementation and its characterization
tests under `src/datacoolie/orchestration/`, `src/datacoolie/core/`, and
`tests/unit/orchestration/`. Runner transport checks live under `tests/unit/docs/`; these are
repository verification paths, not generated-project imports. Reconcile any installed-version
behavior with the public orchestration reference before changing a runner.

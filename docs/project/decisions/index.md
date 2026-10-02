---
title: Architecture Decision Records — DataCoolie
description: "Architectural decisions (ADRs) for DataCoolie, including engine contracts, qualified Polars SQL relations, secrets, transformers, and watermarks."
---

# Architecture Decision Records

ADRs capture *why* a significant architectural decision was made. They use
the [MADR](https://adr.github.io/madr/) format (lite): Status, Context,
Decision, Consequences.

We only keep ADRs for **load-bearing decisions that affect plugin authors
or external consumers** — contracts that are expensive to change. Minor
naming or refactor decisions are not recorded here; they live in commit
history.

Until the project reaches 1.0, ADRs may be edited in place as the design
evolves. After 1.0, overturned decisions get a new ADR marked
"Supersedes N" rather than in-place edits.

An accepted decision explains the contract and rationale; it does not qualify
every allowed dependency or cloud runtime version. Follow each ADR's related
guide and source/test owner when applying it. For empirical claims, retain the
measurement date, workload, environment and evidence with the result; keep
historical measurements distinct from a later source-only review.

| # | Status | Title |
|---|---|---|
| [0009](0009-use-case-oriented-optional-dependencies.md) | Accepted | Use-case-oriented optional dependencies |
| [0008](0008-portable-databricks-platform-backends.md) | Accepted | Portable DatabricksPlatform native and SDK backends |
| [0006](0006-portable-fabric-platform-azure-backends.md) | Accepted | Portable FabricPlatform Azure backends |
| [0005](0005-polars-qualified-sql-relations.md) | Accepted | Qualified SQL relations in PolarsEngine |
| [0004](0004-raw-json-watermark-contract.md) | Accepted | Metadata provider returns raw JSON watermark text |
| [0003](0003-transformer-ordering-slots.md) | Accepted | Number-slot transformer ordering (5/10/18/20/30/35/60/70/80/84/85/90) |
| [0002](0002-secret-provider-resolver-split.md) | Accepted | Split secret **provider** from secret **resolver** |
| [0001](0001-engine-fmt-parameter.md) | Accepted | Engine `fmt=` parameter across format-aware methods |


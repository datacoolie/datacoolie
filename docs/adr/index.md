---
title: Architecture Decision Records — DataCoolie
description: "Recorded architectural decisions (ADRs) for DataCoolie: engine format dispatch, secret provider/resolver split, transformer ordering slots, and the raw-JSON watermark contract."
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

| # | Status | Title |
|---|---|---|
| [0001](0001-engine-fmt-parameter.md) | Accepted | Engine `fmt=` parameter across format-aware methods |
| [0002](0002-secret-provider-resolver-split.md) | Accepted | Split secret **provider** from secret **resolver** |
| [0003](0003-transformer-ordering-slots.md) | Accepted | Number-slot transformer ordering (10/20/30/70/80/90) |
| [0004](0004-raw-json-watermark-contract.md) | Accepted | Metadata provider returns raw JSON watermark text |


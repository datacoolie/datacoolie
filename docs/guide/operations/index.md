---
title: Runtime and operations | DataCoolie
description: Configure, execute, replay, maintain, observe, and troubleshoot DataCoolie runners in local and deployed environments.
---

# Runtime and operations

Use this section after a runner and metadata document exist. It follows the
runtime lifecycle: resolve configuration, execute a stage, recover or replay
work, then observe and maintain the destination.

## Lifecycle

| Concern | What it covers | Page |
|---|---|---|
| Configure | Runtime paths, provider injection, engine and platform settings | [Runtime configuration](runtime-configuration.md) |
| Execute | Run one explicit stage with a project-owned runner | [Run a stage](run-stage.md) |
| Recover | Replay a watermark window or backfill historical data | [Replay and backfill](replay-and-backfill.md) |
| Maintain | Vacuum, optimize, and perform destination maintenance | [Maintenance](maintenance.md) |
| Observe | Read system and execution logs and diagnose failures | [Logging](logging.md) · [Troubleshooting](troubleshooting.md) |

The CLI prepares and validates project artifacts; it does not replace the
project-owned runner. See the [Project and CLI guide](../cli/index.md) for the
build boundary and return contracts.

For host-specific setup, pair these pages with [Platforms and
deployment](../platforms/index.md).

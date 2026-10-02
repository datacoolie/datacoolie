---
title: DataCoolie user guide
description: Install, configure, run and operate DataCoolie projects with explicit metadata, runtime and deployment boundaries.
---

# User guide

The guide is organized around a successful project lifecycle:

1. install an engine and run the smallest local quickstart;
2. author metadata and choose a provider;
3. construct a project runner with explicit runtime paths;
4. run, replay or maintain a stage;
5. validate logs and state before handing a build to an external deployer.

## Choose a starting point

| Goal | Start here |
|---|---|
| First local pipeline | [Installation](getting-started/installation.md), then the [Polars quickstart](getting-started/quickstart-polars.md). |
| Author metadata | [Metadata guide](metadata/index.md). |
| Prepare, inspect, and build a project | [CLI preparation walkthrough](cli/quickstart.md), then the [CLI project workflow](cli/project.md). |
| Run an explicit stage | [Run a stage](operations/run-stage.md). |
| Use SQL files, replay or maintenance | [File metadata](providers/file.md), [replay](operations/replay-and-backfill.md), and [maintenance](operations/maintenance.md). |
| Operate a deployment | [Platform recipes and smoke project](platforms/index.md) and [logging/troubleshooting](operations/logging.md). |

## Project and runtime boundary

`datacoolie.yml` describes a project for the CLI. A runner is the executable
boundary: it chooses the engine and platform, constructs a Driver, supplies
providers and paths, and calls `run`, `replay` or maintenance operations. The
Driver does not read `datacoolie.yml` or a build manifest at runtime.

For exact precedence and ownership rules, use the
[runtime configuration guide](operations/runtime-configuration.md). For the complete CLI
contract, use the [CLI reference](cli/index.md).

## Find a task

- [Metadata authoring](metadata/index.md) — section wrappers, sources,
  destinations, transforms, load strategies and validation.
- [Providers](providers/index.md) — file, database and API metadata boundaries.
- [Run, replay and maintenance](operations/index.md) — explicit Driver lifecycle actions.
- [Platform recipes](platforms/index.md) — choose a host, prepare the smoke project and verify output.
- [Logging and troubleshooting](operations/logging.md) — runtime output and failure
  diagnosis.

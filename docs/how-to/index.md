---
title: How-to Guides — DataCoolie Task Recipes
description: Step-by-step guides for configuring metadata, authoring pipelines, deploying to Fabric, Databricks, and AWS Glue, and handling edge cases in DataCoolie.
---

# How-to guides

Task-oriented recipes. Each guide opens with **Prerequisites** + **End state**
so you can tell at a glance whether it fits your situation.

Most guides reference scenarios from the [`usecase-sim` testbed](https://github.com/datacoolie/datacoolie/tree/main/usecase-sim) — we
link to the scenarios rather than copy their contents so they stay
executable and in sync with the framework.

## New to DataCoolie? Start here

!!! tip "Metadata guide for new users"
    If you are new to DataCoolie and are not sure how to configure your first
    pipeline, the metadata guide walks through everything field by field —
    connections, sources, destinations, transforms, load strategies, and
    validation — in a logical beginner sequence.

    → **[Open the metadata guide](metadata-guide/index.md)**

    | Step | Page |
    |------|------|
    | 1 | [Build your first metadata file](metadata-guide/first-metadata-file.md) |
    | 2 | [Source patterns](metadata-guide/source-patterns.md) |
    | 3 | [Destination & load patterns](metadata-guide/destination-and-load-patterns.md) |
    | 4 | [Transform patterns](metadata-guide/transform-patterns.md) |
    | 5 | [Validation checklist](metadata-guide/validation-checklist.md) |

## After the guide, choose your next task

- The sample pipeline already works and I need my own input next: [Use your own data after the quickstart](../getting-started/use-your-own-data.md)
- I need a specific metadata storage backend: [File metadata](configure-file-metadata.md), [Database metadata](configure-database-metadata.md), or [API metadata](configure-api-metadata.md)
- I need runtime behavior recipes such as merge, partitioning, or maintenance: start in **Authoring pipelines** below
- I need platform deployment steps: jump to **Deploying** below

## Configuring metadata (by storage backend)

Once you understand the metadata shape, choose a storage backend:

- [File metadata](configure-file-metadata.md) — JSON as canonical, YAML/Excel generated.
- [Database metadata](configure-database-metadata.md) — SQLAlchemy schema, DDL, concurrency.
- [API metadata](configure-api-metadata.md) — bringing your own metadata service.

## Authoring pipelines

- [Run a stage](run-a-stage.md)
- [Merge & SCD2](merge-and-scd2.md)
- [Partitioning & column sanitization](partitioning-and-sanitization.md)
- [Maintenance (vacuum / optimize)](maintenance-vacuum-optimize.md)
- [Expected-failure scenarios](expected-failure-scenarios.md)

## Deploying

- [Fabric](deploy-to-fabric.md)
- [Databricks](deploy-to-databricks.md)
- [AWS Glue](deploy-to-aws-glue.md)

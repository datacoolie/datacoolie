# How-to guides

Task-oriented recipes. Each guide opens with **Prerequisites** + **End state**
so you can tell at a glance whether it fits your situation.

Most guides reference scenarios from the [`usecase-sim` testbed](https://github.com/datacoolie/datacoolie/tree/main/datacoolie/usecase-sim) — we
link to the scenarios rather than copy their contents so they stay
executable and in sync with the framework.

## Configuring metadata

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

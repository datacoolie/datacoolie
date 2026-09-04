---
title: Build a Multi-Cloud Medallion Pipeline with DataCoolie Skills
description: Watch DataCoolie Skills build and run a WWI Medallion pipeline locally and on AWS Glue, Microsoft Fabric, and Databricks.
video:
  name: Build a Medallion Data Pipeline with DataCoolie and AI
  description: An end-to-end DataCoolie walkthrough from SQL Server discovery and local Polars validation to AWS Glue, Microsoft Fabric, and Databricks.
  thumbnail_url: https://i.ytimg.com/vi/L9ejQf9tYAE/maxresdefault.jpg
  upload_date: "2026-09-04T02:16:22-07:00"
  duration: PT35M57S
  embed_url: https://www.youtube-nocookie.com/embed/L9ejQf9tYAE
  content_url: https://www.youtube.com/watch?v=L9ejQf9tYAE
  clips:
    - {name: Architecture and workflow, start_offset: 0, end_offset: 84, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=0s"}
    - {name: Setup, start_offset: 84, end_offset: 126, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=84s"}
    - {name: Build the project with Codex, start_offset: 126, end_offset: 229, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=126s"}
    - {name: Watermarks and Gold scope, start_offset: 229, end_offset: 287, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=229s"}
    - {name: Metadata and runners, start_offset: 287, end_offset: 402, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=287s"}
    - {name: DataCoolie Studio, start_offset: 402, end_offset: 657, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=402s"}
    - {name: Local execution, start_offset: 657, end_offset: 779, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=657s"}
    - {name: AWS design and deployment, start_offset: 779, end_offset: 1000, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=779s"}
    - {name: Run on AWS, start_offset: 1000, end_offset: 1204, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1000s"}
    - {name: AWS in Studio, start_offset: 1204, end_offset: 1275, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1204s"}
    - {name: Microsoft Fabric, start_offset: 1275, end_offset: 1543, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1275s"}
    - {name: Run on Fabric, start_offset: 1543, end_offset: 1708, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1543s"}
    - {name: Databricks, start_offset: 1708, end_offset: 1961, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1708s"}
    - {name: Databricks in Studio, start_offset: 1961, end_offset: 2072, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=1961s"}
    - {name: Conclusion, start_offset: 2072, end_offset: 2157, url: "https://www.youtube.com/watch?v=L9ejQf9tYAE&t=2072s"}
---

# Build a Multi-Cloud Medallion Pipeline with DataCoolie Skills

This 35-minute case study follows a Wide World Importers pipeline from source
discovery to local execution and three cloud targets. It demonstrates the
official [DataCoolie Skills workflow](../getting-started/ai-assisted-workflow.md)
with the package and Skills available when the recording was made.

The recorded project workspace is private and is not required for this
tutorial. Resource names and credentials are intentionally omitted; use your
own accounts, identities, and environment-specific configuration.

<div class="dc-video-embed">
  <iframe
    src="https://www.youtube-nocookie.com/embed/L9ejQf9tYAE"
    title="Build a Medallion Data Pipeline with DataCoolie and AI"
    loading="lazy"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
    referrerpolicy="strict-origin-when-cross-origin"
    allowfullscreen>
  </iframe>
</div>

**Choose a language:**
[English video](https://youtu.be/L9ejQf9tYAE) ·
[Video tiếng Việt](https://youtu.be/0lnxfS6WYew)

## What the demo proves

- Discover inspects the declared SQL Server source before architecture and
  metadata are finalized.
- Design records watermark, load, model, runtime, and recovery decisions and
  pauses for approval when the material architecture changes.
- Build produces canonical metadata, environment overlays, target-specific
  runners, immutable artifacts, and verification evidence.
- Provision and Release remain separate, explicitly authorized outcomes for
  each cloud environment.
- [DataCoolie Studio](../datacoolie-studio.md) reads the resulting project,
  metadata, lineage, sources, and operational evidence.

## Architecture demonstrated

```mermaid
flowchart TB
  M["Canonical pipeline metadata"] --> O["Environment overlays"]
  O --> L["Local runner<br/>Polars + Delta"]
  O --> A["AWS runners<br/>Polars external + Glue Spark"]
  O --> F["Fabric runners<br/>Polars external/native + Spark"]
  O --> D["Databricks runner<br/>Spark + Unity Catalog"]
  L --> X["Selected stage execution"]
  A --> X
  F --> X
  D --> X

  S[("SQL Server source")] --> B["Bronze<br/>raw business data"]
  B --> V["Silver<br/>validated replicas"]
  V --> G["Gold<br/>selected business models"]

  X -. runs selected stage .-> B
  X -. runs selected stage .-> V
  X -. runs selected stage .-> G
```

The dotted lines summarize target participation rather than a fixed scheduler.
Stages are selected by a runner; DataCoolie does not infer a cross-platform DAG.

## Environment matrix

| Environment | Source → Bronze | Bronze → Silver | Silver → Gold | Storage and catalog intent |
|---|---|---|---|---|
| Local | Local host, Polars | Local host, Polars | Local host, Polars | Local Parquet/Delta paths |
| AWS | On-premises host, Polars with external `AWSPlatform` | AWS Glue, Spark | AWS Glue, Spark | S3 for data/control paths; Glue databases for Silver and Gold |
| Fabric | On-premises host, Polars with external `FabricPlatform` | Fabric native Python, Polars | Fabric notebook, Spark | OneLake Bronze files; Lakehouse tables for Silver/Gold; separate ETL control Lakehouse |
| Databricks | Bronze files handed off to a Unity Catalog Volume | Databricks notebook/job, Spark | Databricks notebook/job, Spark | Catalog schemas for layers; dedicated Volume path for DataCoolie control state |

This is portability through shared intent, not identical runtime configuration.
Each environment supplies its own paths, catalogs, credentials, engine, and
runner while preserving the canonical dataflow model.

## Chapters

| Topic | English | Tiếng Việt |
|---|---:|---:|
| Architecture and workflow | [00:00](https://youtu.be/L9ejQf9tYAE?t=0) | [00:00](https://youtu.be/0lnxfS6WYew?t=0) |
| Setup | [01:24](https://youtu.be/L9ejQf9tYAE?t=84) | [01:24](https://youtu.be/0lnxfS6WYew?t=84) |
| Build the project with Codex | [02:06](https://youtu.be/L9ejQf9tYAE?t=126) | [02:06](https://youtu.be/0lnxfS6WYew?t=126) |
| Watermarks and Gold scope | [03:49](https://youtu.be/L9ejQf9tYAE?t=229) | [03:49](https://youtu.be/0lnxfS6WYew?t=229) |
| Metadata and runners | [04:47](https://youtu.be/L9ejQf9tYAE?t=287) | [04:47](https://youtu.be/0lnxfS6WYew?t=287) |
| DataCoolie Studio | [06:42](https://youtu.be/L9ejQf9tYAE?t=402) | [06:42](https://youtu.be/0lnxfS6WYew?t=402) |
| Local execution | [10:57](https://youtu.be/L9ejQf9tYAE?t=657) | [10:57](https://youtu.be/0lnxfS6WYew?t=657) |
| AWS design and deployment | [12:59](https://youtu.be/L9ejQf9tYAE?t=779) | [12:59](https://youtu.be/0lnxfS6WYew?t=779) |
| Run on AWS | [16:40](https://youtu.be/L9ejQf9tYAE?t=1000) | [16:40](https://youtu.be/0lnxfS6WYew?t=1000) |
| AWS in Studio | [20:04](https://youtu.be/L9ejQf9tYAE?t=1204) | [20:04](https://youtu.be/0lnxfS6WYew?t=1204) |
| Microsoft Fabric | [21:15](https://youtu.be/L9ejQf9tYAE?t=1275) | [21:15](https://youtu.be/0lnxfS6WYew?t=1275) |
| Run on Fabric | [25:43](https://youtu.be/L9ejQf9tYAE?t=1543) | [25:43](https://youtu.be/0lnxfS6WYew?t=1543) |
| Databricks | [28:28](https://youtu.be/L9ejQf9tYAE?t=1708) | [28:28](https://youtu.be/0lnxfS6WYew?t=1708) |
| Databricks in Studio | [32:41](https://youtu.be/L9ejQf9tYAE?t=1961) | [32:41](https://youtu.be/0lnxfS6WYew?t=1961) |
| Conclusion | [34:32](https://youtu.be/L9ejQf9tYAE?t=2072) | [34:32](https://youtu.be/0lnxfS6WYew?t=2072) |

## Continue with a target platform

- [Deploy to AWS Glue](../how-to/deploy-to-aws-glue.md)
- [Deploy to Microsoft Fabric](../how-to/deploy-to-fabric.md)
- [Deploy to Databricks](../how-to/deploy-to-databricks.md)
- [Inspect the project in DataCoolie Studio](../datacoolie-studio.md)

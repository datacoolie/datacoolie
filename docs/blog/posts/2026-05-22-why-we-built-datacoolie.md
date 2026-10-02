---
date: 2026-05-22
title: Why DataCoolie Uses Metadata-Driven Python ETL
slug: why-we-built-datacoolie
categories:
  - Architecture
authors:
  - datacoolie
description: Why we built a metadata-driven, AI-native ETL framework that separates pipeline intent from execution — and lets LLMs do the boilerplate.
---

# Why We Built DataCoolie

Data teams prototype pipelines locally, then rewrite the same logic for Spark and again for each cloud runtime. That duplicates ETL code and makes operational behavior — watermarks, schema hints, partitions, load strategies — drift across environments.

We built DataCoolie to solve this by separating **pipeline intent** from **execution details** — and by making that intent machine-readable so AI can author, validate, and evolve it alongside you.

<!-- more -->

## The Problem We Kept Hitting

Every time a data engineer moves a pipeline from local development to production, they face:

1. **Engine lock-in** — code written for Polars doesn't run on Spark (and vice versa). See [Engines](../../reference/concepts/engines.md).
2. **Platform coupling** — file paths, secrets, and auth differ per cloud. See [Platforms](../../reference/concepts/platforms.md).
3. **Operational drift** — [watermarks](../../reference/concepts/watermarks.md), partitioning, and [load strategies](../../reference/concepts/load-strategies.md) get reimplemented per job.
4. **Configuration sprawl** — environment-specific configs multiply across repos.
5. **Repetitive boilerplate** — the same patterns (read → transform → merge → watermark) rewritten hundreds of times with minor variations.

Problem 5 is the one nobody talks about. It's boring work — and it's exactly the kind of structured, pattern-heavy work that LLMs excel at.

## Our Approach: Metadata-Driven + AI-Native

Instead of encoding pipeline behavior in imperative code, DataCoolie externalizes it as **declarative metadata**:

- **[Connections](../../reference/concepts/metadata-model.md)** describe where data lives (local paths, S3, ADLS, Delta tables)
- **Dataflows** describe what moves where, with schema hints and load strategies
- **[Transforms](../../reference/concepts/transformers-and-pipeline.md)** describe column-level logic in a portable DSL
- **Operational controls** (watermarks, partitions, maintenance) are declared, not coded

Compatible canonical dataflow intent can run on Polars for development and
Spark for production, with engine-specific runners and runtime dependencies.

But the key insight is: **declarative metadata is a perfect interface for AI**. A JSON/YAML schema with clear semantics is exactly what LLMs can reliably generate, validate, and refactor.

## Where AI Fits In

### 1. AI Designs and Builds Your Project

The optional `datacoolie-discover` skill gathers source facts when they are unknown. `datacoolie-design` defines material contracts, and `datacoolie-build` creates the smallest required workspace, metadata, runners, and verified immutable build:

```
User: "I have parquet files in data/raw/ with orders and customers,
       need to load them into a silver Delta Lake layer with SCD2 on customers"

AI: → inspects source facts only when needed
    → defines the data contracts
    → scaffolds the required project sources
    → generates connections, dataflows, transforms
    → applies merge_upsert for orders, scd2 for customers
    → sets up watermark columns from detected timestamps
    → tests the exact generated build
```

No boilerplate. No copy-paste from a previous project. The AI reads the
schema contract and can produce valid metadata; the build validators still
provide the authoritative check before execution.

### 2. AI Validates and Lints Metadata

The installed DataCoolie CLI owns deterministic project validation, metadata
conversion, environment preparation, and build output. The AI skill supplies
authoring guidance but does not vendor another validator:

```bash
# Validate the authored project or one metadata document
dc validate --metadata-path resolved-metadata.json --format json

# From a target project containing datacoolie.yml, after the build skill
# has generated automation/build.py for that project
python automation/build.py
```

The AI assistant can run these checks inline as you iterate, catching issues before they reach production.

### 3. AI Evolves Metadata Over Time

When requirements change — new source columns, different load strategy, additional partitioning — the AI reads the existing metadata, understands the schema contract, and makes targeted edits. It doesn't need to understand Spark internals or Polars syntax. It only needs to understand **what you want** and map that to metadata fields.

This is the core value proposition: **metadata is a stable, schema-validated interface between human intent and machine execution**.

## What This Means in Practice

```bash
# Project preparation is CLI-owned; execution remains in a project runner
dc validate --project-dir orders --format json
dc build --project-dir orders --format json
```

```bash
# The runner selects the engine/platform explicitly for each host
python orders/runners/dev/run_local_polars.py
python orders/runners/prod/run_fabric_spark.py
```

```bash
# AI-assisted workflow
# 1. Describe what you need → AI generates metadata
# 2. Validate → AI catches schema errors and anti-patterns
# 3. Execute → a project-owned runner calls the framework Driver
# 4. Iterate → AI modifies metadata, not code
```

## Why Metadata > Code for the AI Era

Traditional ETL frameworks give you a DSL or SDK — you write code, AI helps you write code. But code has unlimited degrees of freedom. AI can hallucinate API calls, invent parameters, produce subtly wrong logic.

Metadata with a strict JSON Schema has **bounded degrees of freedom**. When
schema and lint validation run, the AI either produces valid metadata or it
doesn't, and mistakes are caught before execution. Runtime model construction
is not a substitute for those unknown-field checks.

| Approach | AI accuracy | Validation | Portability |
|----------|------------|------------|-------------|
| Imperative code (PySpark, pandas) | Variable — can hallucinate APIs | Requires tests | Engine-locked |
| SQL-based (dbt) | Good for SELECT, weak for ops | Compile-time | DB-locked |
| **Declarative metadata (DataCoolie)** | **High — bounded schema** | **Schema + lint** | **Engine + platform portable** |

---

Get started: [Installation guide](../../guide/getting-started/installation.md) | [Quickstart](../../guide/getting-started/quickstart-polars.md)

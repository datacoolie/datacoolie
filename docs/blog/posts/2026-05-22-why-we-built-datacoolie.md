---
date: 2026-05-22
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

1. **Engine lock-in** — code written for Polars doesn't run on Spark (and vice versa). See [Engines](../../concepts/engines.md).
2. **Platform coupling** — file paths, secrets, and auth differ per cloud. See [Platforms](../../concepts/platforms.md).
3. **Operational drift** — [watermarks](../../concepts/watermarks.md), partitioning, and [load strategies](../../concepts/load-strategies.md) get reimplemented per job.
4. **Configuration sprawl** — environment-specific configs multiply across repos.
5. **Repetitive boilerplate** — the same patterns (read → transform → merge → watermark) rewritten hundreds of times with minor variations.

Problem 5 is the one nobody talks about. It's boring work — and it's exactly the kind of structured, pattern-heavy work that LLMs excel at.

## Our Approach: Metadata-Driven + AI-Native

Instead of encoding pipeline behavior in imperative code, DataCoolie externalizes it as **declarative metadata**:

- **[Connections](../../concepts/metadata-model.md)** describe where data lives (local paths, S3, ADLS, Delta tables)
- **Dataflows** describe what moves where, with schema hints and load strategies
- **[Transforms](../../concepts/transformers-and-pipeline.md)** describe column-level logic in a portable DSL
- **Operational controls** (watermarks, partitions, maintenance) are declared, not coded

The same metadata runs on Polars for development and Spark for production — zero code changes.

But the key insight is: **declarative metadata is a perfect interface for AI**. A JSON/YAML schema with clear semantics is exactly what LLMs can reliably generate, validate, and refactor.

## Where AI Fits In

### 1. AI Scaffolds Your Project

The `datacoolie-init` skill introspects your data sources — scanning folders, parsing DDL, or listening to your natural-language description — and generates a complete project with metadata:

```
User: "I have parquet files in data/raw/ with orders and customers,
       need to load them into a silver Delta Lake layer with SCD2 on customers"

AI: → scaffolds project structure
    → introspects parquet schemas
    → generates connections, dataflows, transforms
    → applies merge_upsert for orders, scd2 for customers
    → sets up watermark columns from detected timestamps
```

No boilerplate. No copy-paste from a previous project. The AI reads the schema contract and produces valid metadata on the first pass.

### 2. AI Validates and Lints Metadata

The `datacoolie-metadata` skill validates your metadata against versioned JSON Schema, catches anti-patterns (missing merge keys, `inferSchema` in production, SCD2 without effective column), and suggests fixes:

```bash
# Validate against schema
datacoolie-metadata validate metadata/connections.json

# Lint for anti-patterns
datacoolie-metadata lint metadata/

# Convert between formats
datacoolie-metadata convert metadata/flows.json --to yaml

# Merge environment overlays
datacoolie-metadata merge base.json --overlay prod.yaml -o resolved.json
```

The AI assistant can run these checks inline as you iterate, catching issues before they reach production.

### 3. AI Evolves Metadata Over Time

When requirements change — new source columns, different load strategy, additional partitioning — the AI reads the existing metadata, understands the schema contract, and makes targeted edits. It doesn't need to understand Spark internals or Polars syntax. It only needs to understand **what you want** and map that to metadata fields.

This is the core value proposition: **metadata is a stable, schema-validated interface between human intent and machine execution**.

## What This Means in Practice

```bash
# Same metadata, different engines
datacoolie run --engine polars   # local dev, fast iteration
datacoolie run --engine spark    # production scale
```

```bash
# Same metadata, different platforms
datacoolie run --platform local       # laptop
datacoolie run --platform fabric      # Microsoft Fabric
datacoolie run --platform databricks  # Databricks
```

```bash
# AI-assisted workflow
# 1. Describe what you need → AI generates metadata
# 2. Validate → AI catches schema errors and anti-patterns
# 3. Run → framework handles the execution
# 4. Iterate → AI modifies metadata, not code
```

## Why Metadata > Code for the AI Era

Traditional ETL frameworks give you a DSL or SDK — you write code, AI helps you write code. But code has unlimited degrees of freedom. AI can hallucinate API calls, invent parameters, produce subtly wrong logic.

Metadata with a strict JSON Schema has **bounded degrees of freedom**. The AI either produces valid metadata or it doesn't — and validation catches mistakes instantly. There's no runtime surprise from a hallucinated function call.

| Approach | AI accuracy | Validation | Portability |
|----------|------------|------------|-------------|
| Imperative code (PySpark, pandas) | Variable — can hallucinate APIs | Requires tests | Engine-locked |
| SQL-based (dbt) | Good for SELECT, weak for ops | Compile-time | DB-locked |
| **Declarative metadata (DataCoolie)** | **High — bounded schema** | **Schema + lint** | **Engine + platform portable** |

---

Get started: [Installation guide](../../getting-started/installation.md) | [Quickstart](../../getting-started/quickstart-polars.md)

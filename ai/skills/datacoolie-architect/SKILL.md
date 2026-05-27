---
name: datacoolie-architect
description: >
  Design data platform architecture based on discovery findings.
  Reads discovery-report.md and produces a medallion architecture design document
  with stage definitions, engine strategy, infrastructure requirements, and partitioning.
  Use when user says "design architecture", "medallion", "plan layers", "stage design",
  "what infrastructure do I need", "how should I structure the pipeline",
  "bronze silver gold", "architecture design", or after completing source discovery.
---

# datacoolie-architect

Design data platform architecture from discovery findings. Produces a human-reviewable
architecture document that feeds into metadata generation and infrastructure provisioning.

## Scope

This skill handles: medallion layer design, stage planning, load strategy selection,
engine selection, infrastructure requirements, partitioning strategy, environment planning.

Does NOT handle: source discovery, metadata generation, infrastructure provisioning, project scaffolding.

## Prerequisites

- One or more discovery reports exist in `.datacoolie/discover/` (files: `yymmdd_{source-name}.md` + `yymmdd_{source-name}_schema.md`)
- If no discovery reports exist, prompt user to run source discovery first

## AI Workflow

### Step 1: Read All Discovery Reports

Read all discovery reports and schema inventories from `.datacoolie/discover/`. For each source:
- From `yymmdd_{source-name}_schema.md`: tables/endpoints, columns/fields, PKs, types, row estimates, FK relationships
- From `yymmdd_{source-name}.md`: source type (database/API/file/lakehouse), load frequency, CDC availability, access & connectivity, watermark candidates, load strategy recommendations

Build a unified picture across all sources — cross-source joins and dependencies inform silver/gold layer design.

### Step 2: Apply Decision Framework

Use the decision rules below to determine:
1. Number of medallion layers (2 vs 3)
2. Load strategy per source table
3. Engine per stage
4. Infrastructure resources needed
5. Partitioning approach

### Step 3: Generate Architecture Document

Compose the architecture document by filling `templates/architecture.tpl.md` with values
derived from the decision framework above. Write the result to:

```
.datacoolie/architect/yymmdd_architecture.md
```

Replace `{{ placeholders }}` with concrete values. For repeated row sections (stages,
infrastructure, partitions, risks), generate one row per table/resource using the rules.

### Step 4: Present for Review

Show the architecture document to the user. Ask:
> "Do you approve this architecture? Reply 'approve' to proceed, or describe changes."

**GATE**: Do NOT proceed to metadata generation or provisioning without explicit approval.
Iterate on the document until the user says "approve" or equivalent.

### Step 5: After Approval

Once approved, inform the user of next steps:
- Scaffold project structure and generate metadata (connections + dataflows) from the architecture
- Validate and lint the generated metadata
- Create infrastructure resources

---

## Decision Framework

### Layer Count: 2 vs 3 Medallion Layers

**Use 2 layers (bronze + gold):**
- ≤5 source tables total
- Simple transforms only (rename, cast, filter nulls)
- Single downstream consumer (one report, one API)
- No SCD2 or complex deduplication needed
- Prototype/POC phase

**Use 3 layers (bronze + silver + gold):**
- >5 source tables
- Complex joins across sources in gold
- Multiple consumers with different needs
- SCD2 or merge-upsert patterns needed
- Production workload with clear separation of concerns

**Decision**: Default to 3 layers unless the project clearly fits the 2-layer criteria.

### Load Strategy Selection

| Condition | Read Strategy | Load Type (write) | Rationale |
|-----------|---------------|-------------------|-----------|
| Small source (<100K rows/records), no history needed | Full read | `full_load` = `overwrite` | Cheap to reload entirely |
| Has reliable watermark column, append new rows | Incremental (watermark) | `append` | Efficient delta, no rewrites |
| Has watermark, reload recent partition window | Incremental (watermark) | `merge_overwrite` | Replace partition slice, not full table |
| Needs current state + has PK | Full or incremental | `merge_upsert` | Update existing, insert new |
| Needs full column replace on key match | Full or incremental | `merge_overwrite` | Replace all columns on match |
| Needs historical snapshots + has PK | Full or incremental | `scd2` | Track all historical states |
| Reference/lookup, rarely changes | Full read | `full_load` = `overwrite` | Simplest, always fresh |
| Source provides CDC stream (Debezium, CT) | CDC feed | `append` or `merge_upsert` | Use native change feed |

**Fallback**: If no good watermark exists and source is large → use `merge_overwrite` with date partition key (reload last N days, not the entire source).

### Engine Selection

| Condition | Engine | Rationale |
|-----------|--------|-----------|
| Data volume <10GB per run AND simple transforms | `polars` | Fast, no cluster, low cost |
| Data volume >10GB per run | `spark` | Distributed processing |
| Multi-source joins in a single stage | `spark` | Better join optimization |
| Complex window functions / aggregations | `spark` | Native support |
| File format conversion only (no transforms) | `polars` | I/O bound, Polars excels |
| Dev environment (any stage) | `polars` | Fast iteration, no infra |

**Strategy**: Start with Polars everywhere in dev. Only promote to Spark for stages
that demonstrably need it in production.

### Stage Naming Convention

```
{source_name}_source2bronze       # Raw ingestion from external source
{domain}_bronze2silver            # Cleanse, deduplicate, conform
{domain}_silver2gold              # Aggregate, join, serve
```

Where:
- `{source_name}` = the connection/system name (e.g., `erp`, `crm`, `api_orders`)
- `{domain}` = business domain grouping (e.g., `sales`, `finance`, `inventory`)

### Partitioning Strategy

Partition only when each resulting partition file would be **>128MB** (Parquet/Delta).
If partitioning produces files <128MB per partition, skip it — small files add read
overhead without benefit.

**Bronze (source2bronze) stages always partition by ingest date/timestamp**, regardless
of file size. This is required for watermark-based incremental reads and date-folder pruning.

| Condition | Partition Approach |
|-----------|-------------------|
| Bronze / source2bronze stage (any table) | Always partition by ingest date (`_ingest_date`) |
| Each partition file would be >128MB | Partition by that column |
| Table has date column + each date bucket >128MB | Partition by date (YYYY-MM-DD or YYYY-MM) |
| Table has category column + each category >128MB | Partition by category (region, tenant) |
| Total table <128MB OR partitions would produce files <128MB | No partitioning (except bronze) |
| Append-only table (events, logs) + daily volume >128MB | Partition by ingestion date |
| SCD2 table + monthly volume >128MB | Partition by `_valid_from` month |

### Infrastructure Mapping

| Architecture Layer | Fabric | AWS | Databricks | Local |
|--------------------|--------|-----|-----------|-------|
| Bronze | Lakehouse | S3 bucket | DBFS/Unity | `./data/bronze/` |
| Silver | Lakehouse | S3 bucket | DBFS/Unity | `./data/silver/` |
| Gold | Warehouse or Lakehouse | Redshift/Athena | Unity Catalog | `./data/gold/` |
| Metadata | — | — | — | `./metadata/` |

### Schedule Heuristics

| Source Characteristic | Suggested Schedule |
|----------------------|-------------------|
| Real-time / streaming | Continuous or every 5 min |
| Transactional (orders, events) | Hourly or every 15 min |
| Operational (CRM, ERP updates) | Daily (early morning) |
| Reference data (products, categories) | Daily or weekly |
| Aggregations / gold tables | After upstream completes (dependency) |
| Dev environment | Manual trigger only |

---

## Output Format

The architecture document follows the template at `templates/architecture.tpl.md`.

Key sections:
1. **Overview** — source count, platform, volume estimate
2. **Medallion Layers** — purpose and storage per layer
3. **Stage Definitions** — table mapping source→destination with strategy
4. **Infrastructure Requirements** — resources to provision per platform
5. **Engine Strategy** — which engine per stage pattern with rationale
6. **Partitioning Strategy** — partition columns per table
7. **Environment Differences** — how dev/test/prod differ
8. **Risks & Mitigations** — identified concerns

---

## Input/Output Contracts

| Direction | Artifact | Path |
|-----------|----------|------|
| Input | Discovery reports (all sources) | `.datacoolie/discover/yymmdd_{source-name}.md` |
| Input | Schema inventories (all sources) | `.datacoolie/discover/yymmdd_{source-name}_schema.md` |
| Output | Architecture document (single, spans all sources) | `.datacoolie/architect/yymmdd_architecture.md` |

---

## Error Handling

| Situation | Action |
|-----------|--------|
| No discovery report found | Prompt user to run source discovery first |
| Discovery report incomplete (many TODOs) | Warn user, proceed with available info, mark gaps in architecture |
| User rejects architecture | Ask what to change, iterate until approved |
| Platform not specified | Ask user which platform (Fabric/AWS/Databricks/Local) |
| Cannot determine load strategy (no watermark, large table) | Flag as risk, suggest `full_load` with partitioned overwrite |

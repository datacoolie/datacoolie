---
name: datacoolie-architect
description: >
  Design data platform architecture based on discovery findings.
  Supports incremental source onboarding — add new sources without redoing approved decisions.
  Reads discovery reports and produces a medallion architecture design document
  with stage-level contracts, engine strategy, infrastructure requirements, and partitioning.
  Use when user says "design architecture", "medallion", "plan layers", "add source",
  "what infrastructure do I need", "how should I structure the pipeline",
  "bronze silver gold", "architecture design", "update architecture",
  or after completing source discovery.
---

# datacoolie-architect

Design data platform architecture from discovery findings. Produces a human-reviewable
architecture document that feeds into metadata generation and infrastructure provisioning.

Supports two modes:
- **Create** — first-time architecture from scratch
- **Update** — add new sources or modify existing architecture incrementally

## Scope

This skill handles: medallion layer design, stage-level workload planning, load strategy selection,
engine selection, infrastructure requirements, partitioning strategy, environment planning,
incremental source onboarding, architecture versioning.

Does NOT handle: source discovery, metadata generation, infrastructure provisioning,
project scaffolding, dataflow object naming, column-level metadata implementation, or platform deploy commands.

## Prerequisites

- One or more discovery reports exist in `{project_name}_dcws/discover/` (files: `yymmdd_{source-name}.md` + `yymmdd_{source-name}_schema.csv`)
- If no discovery reports exist, prompt user to run source discovery first

## AI Workflow

### Step 0: Detect Mode

Check for an existing architecture document in `{project_name}_dcws/architecture/`:

| Condition | Mode |
|-----------|------|
| No `current.md` exists | **Create** — build from scratch |
| `current.md` exists | **Update/Amend** — diff discoveries against Source Registry and record any architecture amendment |

In **Update** mode, read the existing architecture and its Source Registry section. Compare
against current discovery reports in `{project_name}_dcws/discover/` to identify:
- **New sources** — discovery report exists but source not in registry
- **Modified sources** — discovery report is newer than the arch version date
- **Retired sources** — source in registry but discovery report removed

### Step 1: Read Discovery Reports

Read all discovery reports and schema inventories from `{project_name}_dcws/discover/`. For each source:
- From `yymmdd_{source-name}_schema.csv`: tables/endpoints, columns/fields, PKs, types, row estimates, FK relationships
- From `yymmdd_{source-name}.md`: source type (database/API/file/lakehouse), load frequency, CDC availability, access & connectivity, watermark candidates, load strategy recommendations

Build a unified picture across all sources — cross-source joins and dependencies inform silver/gold layer design.

**Update mode**: Focus on new/modified sources only. Read existing architecture to understand
already-approved decisions. Do NOT re-derive decisions for unchanged sources.

### Step 2: Apply Decision Framework

Use the decision rules below to determine:
1. Number of medallion layers (2 vs 3)
2. Load strategy per source table
3. Engine per stage or layer transition
4. Infrastructure resources needed
5. Partitioning approach

**Update mode**: Apply decisions only to new/modified sources. Preserve all existing
approved stage decisions. If new sources affect silver/gold layers (new joins, new
aggregations), propose additions rather than replacements.

### Step 3: Generate Architecture Document

**Create mode:** Fill `templates/architecture.tpl.md` with values from the decision framework.
Create `{project_name}_dcws/architecture/` only if it does not exist, then write to:

```
{project_name}_dcws/architecture/current.md
```

**Update mode:** Update the existing architecture document in place only when the change is approved:
1. Add new sources to Source Registry with status `New`
2. Mark modified sources as `Modified` in registry
3. Mark removed sources as `Retired` in registry
4. Add or adjust stage-level workload contracts
5. Avoid dataflow object names and column-level metadata details
6. Bump the version number
7. Add a Changelog entry describing what changed
8. For architecture changes discovered during implementation, write an amendment to `{project_name}_dcws/architecture/amendments/yymmdd_{change}.md`

#### Progressive Split Rules

The architecture starts as a single inline file. Split when it grows too large:

| Condition | Mode | Action |
|-----------|------|--------|
| Total document ≤ ~500 lines | **Inline** | Everything in one file |
| Total document > ~500 lines | **Layer Split** | Move Stage Contracts to `layers/{layer}.md` files. Master keeps Stage Summary Table + pointers to layer files |
| Any single layer file > ~300 lines | **Layer+Source Split** | Split that layer into `layers/{layer}/{source}.md` files |

When splitting:
- Master file always retains: header, Source Registry, Source Overview, Stage Summary Table, Infrastructure, Engine Strategy, Partitioning, Changelog, Approval
- Layer files contain: stage-level contracts for that layer only
- Use split-mode pointers in master to link to layer files

### Step 4: Present for Review

Show the architecture document to the user. In Update mode, highlight what changed:
- New sources and their proposed stage impacts
- Modified sources and what changed
- Any impact on silver/gold layers

Ask:
> "Do you approve this architecture? Reply 'approve' to record the architecture gate, or describe changes."

**GATE**: Do NOT proceed to init, metadata generation, workload implementation, or provisioning until the latest Markdown gate journal under `{project_name}_dcws/project_management/phases/architecture/gate-reviews/` has YAML frontmatter `status: approved`. Select the latest journal by `reviewed_at`; if missing, use the lexicographically greatest filename.
Iterate on the document until the user says "approve" or equivalent.

### Step 5: After Approval

Once approved:
- Create `{project_name}_dcws/project_management/phases/architecture/` only if missing
- Write or update the architecture gate journal under `{project_name}_dcws/project_management/phases/architecture/gate-reviews/`
- Update Source Registry statuses: `New` → `Active`, `Modified` → `Active`
- Inform user of next steps:
  - Scaffold `{project_name}_dcws/`
  - Generate metadata from stage-level contracts and discovery evidence
  - Implement and validate the next allowed workload stage

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
| Multi-source joins in a single workload stage | `spark` | Better join optimization |
| Complex window functions / aggregations | `spark` | Native support |
| File format conversion only (no transforms) | `polars` | I/O bound, Polars excels |
| Dev environment (any workload stage) | `polars` | Fast iteration, no infra |

**Strategy**: Start with Polars everywhere in dev. Only promote to Spark for workload stages
that demonstrably need it in production.

### Partitioning Strategy

Partition only when each resulting partition file would be **>128MB** (Parquet/Delta).
If partitioning produces files <128MB per partition, skip it — small files add read
overhead without benefit.

**Bronze (source to bronze) workload stages always partition by ingest date/timestamp**, regardless
of file size. This is required for watermark-based incremental reads and date-folder pruning.

| Condition | Partition Approach |
|-----------|-------------------|
| Bronze / source to bronze workload (any table) | Always partition by ingest date (`_ingest_date`) |
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
| Gold | Warehouse or Lakehouse | Redshift/S3 bucket | Unity Catalog | `./data/gold/` |
| Metadata | `{project_name}_dcws/metadata/` | `{project_name}_dcws/metadata/` | `{project_name}_dcws/metadata/` | `{project_name}_dcws/metadata/` |

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

The architecture document follows the template at `templates/architecture.tpl.md` and must start
with YAML frontmatter for document metadata. Split source/layer files under `architecture/layers/`
also start with YAML frontmatter.

Key sections:
1. **Overview** — source count, platform, volume estimate
2. **Source Registry** — tracked sources with status
3. **Source Overview** — connection characteristics per source
4. **Medallion Layers** — purpose and storage per layer
5. **Stage Summary Table** — mapping stage/layer boundaries with strategy
6. **Stage Contracts** — load pattern, grain, keys, freshness, quality gate, engine, and platform
7. **Infrastructure Requirements** — resources to provision per platform
8. **Engine Strategy** — which engine per layer transition with rationale
9. **Partitioning Strategy** — partition columns per table
10. **Environment Differences** — how dev/test/prod differ
11. **Risks & Mitigations** — identified concerns
12. **Changelog** — version history

---

## Input/Output Contracts

| Direction | Artifact | Path |
|-----------|----------|------|
| Input | Discovery reports (all sources) | `{project_name}_dcws/discover/yymmdd_{source-name}.md` |
| Input | Schema inventories (all sources) | `{project_name}_dcws/discover/yymmdd_{source-name}_schema.csv` |
| Input | Existing architecture (update mode) | `{project_name}_dcws/architecture/current.md` |
| Output | Architecture document (single, spans all sources) | `{project_name}_dcws/architecture/current.md` |
| Output | Architecture amendment | `{project_name}_dcws/architecture/amendments/yymmdd_{change}.md` |
| Output | Layer detail files (split mode only) | `{project_name}_dcws/architecture/layers/{layer}.md` |
| Output | Architecture gate journal | `{project_name}_dcws/project_management/phases/architecture/gate-reviews/yymmdd_architecture-review.md` |

---

## Error Handling

| Situation | Action |
|-----------|--------|
| No discovery report found | Prompt user to run source discovery first |
| Discovery report incomplete (many TODOs) | Warn user, proceed with available info, mark gaps in architecture |
| User rejects architecture | Ask what to change, iterate until approved |
| Platform not specified | Ask user which platform (Fabric/AWS/Databricks/Local) |
| Cannot determine load strategy (no watermark, large table) | Flag as risk, suggest `full_load` with partitioned overwrite |
| Update mode: existing architecture has no Source Registry | Backfill registry from existing stage summary before proceeding |

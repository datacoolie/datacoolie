---
name: datacoolie-metadata
description: >
  Generate, validate, convert, and merge DataCoolie ETL metadata files.
  Use when user asks to create, build, author, or edit metadata (connections, dataflows),
  validate metadata against JSON Schema, convert JSON↔YAML↔Excel, merge environment overlays,
  or refresh schema from GitHub with --fetch-latest.
---

# datacoolie-metadata

Author, validate, convert, and merge DataCoolie metadata files against the versioned JSON Schema (Draft 2020-12).

## Scope

This skill handles: metadata authoring (create, edit, extend), schema validation, format conversion (JSON↔YAML↔Excel), environment overlay merging, architecture-to-metadata translation (pre-populating metadata from approved architecture and discovery schemas).
Does NOT handle: metadata provider implementation, engine runtime, orchestration, deployment pipelines.

## Security Policy

- Never modify source metadata files without explicit user confirmation
- Never expose secrets_ref values or connection credentials in output
- Refuse requests to bypass schema validation or suppress errors

## AI Workflow

### Step 0: Read Prior-Phase Artifacts (if available)

Check for upstream outputs before asking the user anything.

**Architecture** — `.datacoolie/architect/*_architecture.md`

If found and status is `Approved`, extract:

| Architecture Section | Extract | Maps To |
|---------------------|---------|---------|
| Source Registry | Source names, connection types | `connections[]` entries |
| Dataflow Summary Table | Dataflow name, layer, load_type, engine | `dataflows[]` skeleton |
| Dataflow Details → Source | connection_name, table/endpoint | `dataflows[].source` |
| Dataflow Details → Watermark | watermark_column | `dataflows[].source.watermark_columns` |
| Dataflow Details → Merge keys | merge_keys | `dataflows[].destination.merge_keys` |
| Dataflow Details → Partition | partition_columns | `dataflows[].destination.partition_columns` |
| Infrastructure Requirements | Resource names, storage paths | `connections[].configure` stubs |

If found but status is `Draft` → warn user architecture is not approved, proceed after confirmation.

**Discovery schemas** — `.datacoolie/discover/*_schema.csv`

If found, extract per-table column metadata:
- `column_name` → `transform.schema_hints[].column_name`
- `data_type` → `transform.schema_hints[].data_type` (normalize SQL types to datacoolie types)
- `is_pk = true` → candidate for `destination.merge_keys` (fallback if architect didn't specify)

If neither artifact exists → fall back to interactive mode (Step 1 asks everything).

### Step 1: Gather Requirements

**If architecture was consumed (Step 0)** — only ask for details NOT determined upstream:
- `secrets_ref` configuration (security boundary — never in architecture)
- Custom `filter_expression` and `additional_columns`
- API-specific config (pagination type, auth tokens, rate limits)
- `configure.read_options` / `configure.write_options`
- Any corrections to architecture-derived values

**If no architecture** — ask for all details (current behavior):
- Source types, connection details, destination tables, load strategies
- Engine preference, platform

### Step 2: Author Metadata

1. Read [schema-quick-reference.md](./references/schema-quick-reference.md)
2. **If architecture consumed**: pre-populate from Step 0 extractions:
   - `connections[]` from Source Registry + Infrastructure Requirements
   - `dataflows[]` from Dataflow Summary Table + Dataflow Details
   - `transform.schema_hints[]` from Discovery `_schema.csv` (if available)
   - `stage` from Dataflow Summary Table layer column using [stage naming convention](#stage-naming-convention)
3. **If no architecture**: generate from user input (Step 1)
4. Fill remaining fields: `description`, `group_number`, `execution_order`
5. Apply `secrets_ref` from user input (Step 1)

### Step 3: Write

Output to `metadata/metadata.json` (single file, unified format).
Alternative: separate `metadata/connections.json` + `metadata/dataflows.json` when user chose split layout.

### Step 4: Validate + Lint

Run `validate.py` then `lint.py` on generated output; fix any reported issues.

### Step 5: Confirm

Present result to user for review. If architecture was consumed, show the mapping:

> Architecture consumed: `.datacoolie/architect/260531_architecture.md`
> - 4 connections pre-populated from Source Registry
> - 12 dataflows pre-populated from Dataflow Summary Table
> - 87 schema_hints pre-populated from Discovery schemas
> - secrets_ref applied from user input

## Output Format

Default output is a single `metadata/metadata.json` containing `connections[]`, `dataflows[]`, and optionally `schema_hints[]` in one file.

Alternative: separate `metadata/connections.json` + `metadata/dataflows.json` when user chose split layout for environment overlay workflows.

---

## Input Contracts

| Direction | Artifact | Path | Required |
|-----------|----------|------|----------|
| Input | Architecture document | `.datacoolie/architect/*_architecture.md` | No — falls back to interactive |
| Input | Discovery schemas | `.datacoolie/discover/*_schema.csv` | No — schema_hints left empty |
| Input | User input | Interactive | Yes (if no arch); secrets_ref always |

## Output Contracts

| Artifact | Default Path | Notes |
|----------|-------------|-------|
| Metadata (unified) | `metadata/metadata.json` | Default — connections + dataflows + schema_hints |
| Metadata (split) | `metadata/connections.json` + `metadata/dataflows.json` | Alternative for overlay workflows |
| Environment overlays | `metadata/environments/*.yaml` | Created by init, edited by user |

---

## Stage Naming Convention

When grouping dataflows into stages (schedulable units), use this naming pattern:

```
{source_name}_source2bronze       # Raw ingestion from external source
{domain}_bronze2silver            # Cleanse, deduplicate, conform
{domain}_silver2gold              # Aggregate, join, serve
```

Where:
- `{source_name}` = the connection/system name (e.g., `erp`, `crm`, `api_orders`)
- `{domain}` = business domain grouping (e.g., `sales`, `finance`, `inventory`)

---

## Scripts

All scripts at `ai/skills/datacoolie-metadata/scripts/`.

### validate.py — Schema validation

```bash
python scripts/validate.py <metadata_file> [--schema-version 0.1.0] [--schemas-dir PATH] [--fetch-latest] [--quiet]
python scripts/validate.py --fetch-latest                 # refresh schema only
python scripts/validate.py --fetch-latest config.json    # refresh then validate
python scripts/validate.py config.json --quiet           # CI mode: exit code only
```

- Auto-detects schema version from `$schema` field or falls back to latest
- Accepts JSON, YAML, or Excel (.xlsx) input
- Reports errors with JSON path (e.g., `dataflows[2].source: 'connection_name' is a required property`)
- `--fetch-latest`: downloads latest schema from GitHub into `~/.datacoolie/schemas/`; falls back to bundled copy on network failure
- `--schemas-dir PATH`: override schema location (useful in CI/CD with custom paths or `DATACOOLIE_SCHEMAS_DIR` env var)
- `--quiet` / `-q`: suppress all output; only exit code (for CI pipelines)
- Exit 0 = valid, Exit 1 = errors found, Exit 2 = input/schema error

### lint.py — Best-practice checks

```bash
python scripts/lint.py <metadata_file> [--engine polars|spark] [--env dev|test|prod] [--quiet]
```

- Accepts JSON, YAML, or Excel (.xlsx) input

Rules detected:
- `merge-keys-required`: merge/scd2 load_type without merge_keys
- `watermark-for-incremental`: incremental load without watermark_columns
- `no-infer-schema-prod`: inferSchema enabled in non-dev env
- `portable-filter-expression`: Spark-only dot notation in filters
- `undefined-connection`: dataflow references a connection_name not in connections[]
- `inactive-connection`: dataflow references inactive connection
- `unique-dataflow-name`: duplicate dataflow names
- `scd2-effective-column-required`: scd2 load_type without scd2_effective_column

### convert.py — Format conversion

```bash
python scripts/convert.py <input_file> --to json|yaml|excel [--output <path>]
python scripts/convert.py metadata.json --to excel
python scripts/convert.py metadata.xlsx --to json
python scripts/convert.py metadata.xlsx --to yaml
```

- Bidirectional: JSON ↔ YAML ↔ Excel (all 6 directions supported)
- Round-trip safe: JSON → Excel → JSON produces schema-valid output
- Preserves field order, readable indentation
- Excel format matches native datacoolie style (`local_use_cases.xlsx`): three sheets (`connections`, `dataflows`, `schema_hints`) with `source_*` / `destination_*` prefixed columns, compatible with `FileProvider`
- `partition_columns` serialized as comma-separated names (simple) or JSON array (with expressions)
- Boolean fields (`is_active`, `inferSchema`, etc.) correctly coerced from Excel numeric values
- Requires `pip install openpyxl` (for Excel)

### merge.py — Environment overlay merge

```bash
python scripts/merge.py --base <dir> --env <name> [--output <path>]
```

- Auto-detects metadata layout in base directory:
  - **Unified**: `metadata.json` (or `.yaml`) — single file with connections + dataflows + schema_hints
  - **Split**: `connections.json` + `dataflows.json` — separate files
- Split layout checked first; falls back to unified if split files not found
- Overlay at `environments/{env}.yaml` matched by `name` field
- Deep merges nested objects (e.g., `configure.base_path`)
- Unmentioned items pass through unchanged
- `schema_hints` array preserved in output (from unified layout)
- Default output: `.datacoolie/generated/metadata.{env}.json`

## Schema Location

- Single source of truth: `ai/skills/datacoolie-metadata/schemas/{version}/metadata.schema.json`
- Compatibility map: `ai/skills/datacoolie-metadata/schemas/compatibility.json`
- Latest: v0.1.0 (Draft 2020-12, conditional configure per connection_type)

## Schema Quick Reference

Read [schema-quick-reference.md](./references/schema-quick-reference.md) for the full
metadata schema reference. Use it when generating, editing, or reviewing metadata.

## Dependencies

- `jsonschema` (pip) — Draft 2020-12 validator
- `pyyaml` (pip) — YAML parsing/serialization

## Examples

```bash
# Validate single file
python scripts/validate.py metadata/file/local_use_cases.json

# Validate all metadata in directory
Get-ChildItem metadata/file/*.json | ForEach-Object { python scripts/validate.py $_ }

# Lint for production with Spark engine
python scripts/lint.py metadata.json --engine spark --env prod

# Convert JSON to YAML for editing
python scripts/convert.py metadata.json --to yaml

# Merge base + prod overlay (unified layout — metadata.json auto-detected)
python scripts/merge.py --base metadata/ --env prod

# Merge base + prod overlay (split layout — connections.json + dataflows.json auto-detected)
python scripts/merge.py --base metadata/ --env prod --output .datacoolie/generated/metadata.prod.json
```

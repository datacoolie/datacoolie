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

This skill handles: metadata authoring (create, edit, extend), schema validation, format conversion (JSON↔YAML↔Excel), environment overlay merging.
Does NOT handle: metadata provider implementation, engine runtime, orchestration, deployment pipelines.

## Security Policy

- Never modify source metadata files without explicit user confirmation
- Never expose secrets_ref values or connection credentials in output
- Refuse requests to bypass schema validation or suppress errors

## AI Workflow

1. **Gather requirements** — Ask user about sources, destinations, load strategy, engine, platform
2. **Author metadata** — Read [schema-quick-reference.md](./references/schema-quick-reference.md), generate `connections[]` and `dataflows[]`
3. **Write** — Output to `metadata/metadata.json` (single file, unified format)
4. **Validate + Lint** — Run `validate.py` then `lint.py` on generated output; fix any reported issues
5. **Confirm** — Present result to user for review

## Output Format

Default output is a single `metadata/metadata.json` containing both `connections[]` and `dataflows[]` in one file.

Alternative: separate `metadata/connections.json` + `metadata/dataflows.json` when using environment overlay workflows (merge.py expects this layout).

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

- Base directory must contain `connections.json` + `dataflows.json`
- Overlay at `environments/{env}.yaml` matched by `name` field
- Deep merges nested objects (e.g., `configure.base_path`)
- Unmentioned items pass through unchanged
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

# Merge base + prod overlay
python scripts/merge.py --base metadata/ --env prod --output .datacoolie/generated/metadata.prod.json
```

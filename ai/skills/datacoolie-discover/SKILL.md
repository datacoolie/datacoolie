---
name: datacoolie-discover
description: >
  Discover and profile data sources for DataCoolie ETL projects.
  Produces per-source discovery reports (operational context + schema inventory).
  Two retrieval methods: auto-introspection (connect to source) or interview (ask user).
  Supports multiple sources — each source gets its own report.
  Use when user says "discover sources", "explore data", "profile source", "what tables exist",
  "understand my data", "source assessment", "new data source", "connect to source",
  or before designing architecture for a new data platform.
---

# datacoolie-discover

Produce discovery reports that capture everything needed to design an ETL architecture for one or more data sources.

## Scope

This skill handles: source schema extraction, operational intelligence gathering, discovery report generation.
Does NOT handle: metadata generation, project scaffolding, architecture design.

## Multi-Source Model

Each source is discovered independently. One discovery run = one source = two output files.

```
.datacoolie/discover/
├── yymmdd_erp.md                # Discovery report (operational context + recommendations)
├── yymmdd_erp_schema.md         # Schema inventory (tables, columns, PKs, FKs, row estimates)
├── yymmdd_crm-api.md
├── yymmdd_crm-api_schema.md
├── yymmdd_file-exports.md
└── yymmdd_file-exports_schema.md
```

**Naming**: `yymmdd_{source-name}.md` and `yymmdd_{source-name}_schema.md` where `source-name` is a kebab-case identifier from the user.

When user has multiple sources, run discovery for each source separately. Sources can be discovered at different times — re-running discovery for one source does not affect others.

## Desired Output

Per source, two files:

### Discovery Report (`yymmdd_{source-name}.md`)

| Section | Content | Populated By |
|---|---|---|
| Source identity | Name, tech stack, sizing, hosting, owner | Auto or Interview |
| Schema summary | Table count, key tables, link to schema file | Auto or Interview |
| Data characteristics | History tracking, soft deletes, late-arriving data, formats, quality | Interview |
| Change capture | CDC availability, watermark candidates, incremental markers | Auto + Interview |
| Load patterns | Frequency, backfill needs, growth rate, peak windows | Interview |
| Access & connectivity | Protocol, auth, network, rate limits, environments | Interview |
| Performance | Query latency, timeouts, constraints | Interview |
| Recommendations | Load strategy per table, watermark candidates, risks | AI-generated |

Template: `templates/discovery-report.tpl.md`

### Schema Inventory (`yymmdd_{source-name}_schema.md`)

For database / file / lakehouse sources:

| Section | Content |
|---|---|
| Tables | Schema, table name, column count, PK, row estimate |
| Column details | Per table: column name, type (canonical with inline precision), nullable, FK reference, notes |

For API sources:

| Section | Content |
|---|---|
| Connection | Base URL, auth type, token URL, API spec, rate limit, default headers |
| Endpoints | Endpoint path, method, pagination type, data path, field count |
| Field details | Per endpoint: field name, type (canonical), nullable, FK reference, notes |

Template: `templates/schema-inventory.tpl.md`

## Retrieval Methods

### Method 1: Auto-Introspection

Connect to the source and extract schema automatically via terminal.

**When to use**: User provides a connection string, file path, or API spec.

1. Determine source type (database, file, API, lakehouse catalog)
2. Run introspection — see `references/source-introspection-guide.md`
3. Write schema inventory to `yymmdd_{source-name}_schema.md`
4. Proceed to interview for remaining sections (data characteristics, load patterns, etc.)

Key principles:
- Use whatever SQL client or CLI is available in the terminal
- Filter out system schemas (`information_schema`, `pg_catalog`, `sys`, `mysql`, `performance_schema`)
- For unlisted databases: (1) try INFORMATION_SCHEMA, (2) try `SHOW TABLES` / `DESCRIBE TABLE`, (3) consult docs
- Normalize discovered types to canonical forms (see Type Normalization Reference in the guide)

### Method 2: Interview

Ask the user questions to gather information that can't be auto-extracted.

**When to use**: No source access (firewall, no credentials), or to supplement auto-introspection results.

Questions are at `templates/interview-questions.md`. Key rules:
- Skip questions already answered by auto-introspection
- Ask conversationally — don't dump the full list
- Group related questions, ask a few at a time
- Fill answers directly into the report

### Workflow

1. Ask user for source name (used in filenames)
2. **User provides source access** → auto-introspect first, write schema file, then interview for remaining gaps
3. **User cannot provide access** → interview only (schema section filled manually or left for later)
4. **After both** → AI generates recommendations (load strategy, watermarks, risks)
5. Write report to `.datacoolie/discover/yymmdd_{source-name}.md`
6. If more sources to discover → repeat for next source
7. Inform user: "Discovery complete. Next step: design the architecture based on these reports."

## Security Policy

- Never store connection strings or credentials in the output report
- Never log credentials during introspection
- Connection strings read from environment variables or user prompt — never hardcoded
- Read-only operations only (SELECT on catalog views, no writes to source)
- Refuse requests to scan systems without user's explicit authorization

## Output Contracts

| Artifact | Path | Format |
|---|---|---|
| Discovery report | `.datacoolie/discover/yymmdd_{source-name}.md` | Markdown |
| Schema inventory | `.datacoolie/discover/yymmdd_{source-name}_schema.md` | Markdown |

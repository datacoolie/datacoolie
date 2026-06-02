# Architecture Design — {{ project_name }}

**Date:** {{ date }}
**Version:** v{{ version | default("1") }}
**Discovery Reports:** `.datacoolie/discover/`
**Platform:** {{ platform }}
**Status:** Draft — Awaiting Approval

---

## Overview

- **Source count:** {{ source_count }}
- **Target platform:** {{ platform }}
- **Estimated daily volume:** {{ estimated_volume }}
- **Medallion layers:** {{ layer_count }} ({{ layer_names }})

---

## Source Registry

| Source | Type | Discovered | Arch Version | Tables | Dataflows | Status |
|--------|------|-----------|-------------|--------|-----------|--------|
| {{ source_registry_rows }} |

**Status values:** `Active` — current; `New` — just added, pending approval; `Modified` — re-discovered with changes; `Retired` — removed from pipeline

---

## Source Overview

<!-- Inline mode: source details listed here. Split mode: see layers/source.md -->

<!-- Repeat for each source: -->
<!--
### {{ source_name }}

- **Type:** {{ connection_type }} (database / API / file / lakehouse)
- **Protocol:** {{ protocol }}
- **Schedule:** {{ schedule }}
- **CDC available:** {{ cdc_available }}
- **Estimated volume:** {{ volume }}
- **Access:** {{ access_notes }}
-->

---

## Architecture Diagram

```mermaid
flowchart LR
    subgraph Sources
        S1[{{ source_1 }}]
        S2[{{ source_2 }}]
    end

    subgraph Bronze
        B1[{{ source_1 }} tables]
        B2[{{ source_2 }} tables]
    end

    subgraph Silver
        SV1[{{ domain_1 }}]
    end

    subgraph Gold
        G1[{{ domain_1 }} aggregations]
    end

    S1 --> B1
    S2 --> B2
    B1 --> SV1
    B2 --> SV1
    SV1 --> G1
```

> _Replace node names with actual sources and domains. Add or remove nodes to match._

---

## Medallion Layers

### Bronze (Raw Ingestion)

- **Purpose:** Land source data with minimal transformation (schema enforcement only)
- **Storage:** {{ bronze_storage }}
- **Format:** Delta / Parquet
- **Retention:** {{ bronze_retention | default("90 days") }}
- **Naming:** `{source_name}/{table_name}/`

### Silver (Cleansed & Conformed)

- **Purpose:** Deduplicated, typed, business-ready entities
- **Storage:** {{ silver_storage }}
- **Format:** Delta
- **Retention:** {{ silver_retention | default("Unlimited") }}
- **Naming:** `{domain}/{entity_name}/`

### Gold (Aggregated / Serving)

- **Purpose:** Business metrics, reporting tables, API-ready datasets
- **Storage:** {{ gold_storage }}
- **Format:** Delta / Table
- **Retention:** {{ gold_retention | default("Unlimited") }}
- **Naming:** `{domain}/{metric_or_view_name}/`

---

## Dataflow Summary Table

| Dataflow | Layer | Source | Destination | Load Type | Engine | Schedule |
|----------|-------|--------|-------------|-----------|--------|----------|
| {{ dataflow_summary_rows }} |

> This table is always present in the master file regardless of split mode.

### Dataflow Details

<!-- Inline mode: dataflow details listed here. Split mode: see layers/{layer}.md -->

<!-- Repeat for each dataflow: -->
<!--
#### {{ dataflow_name }}

- **Layer:** {{ layer }} (bronze / silver / gold)
- **Source connection:** {{ connection_name }}
- **Source table / endpoint:** {{ table_or_endpoint }}
- **Destination:** {{ layer }}/{{ path }}
- **Load type:** {{ load_type }}
- **Watermark column:** {{ watermark_column | default("N/A") }}
- **Merge keys:** {{ merge_keys | default("N/A") }}
- **Engine:** {{ engine }}
- **Partition columns:** {{ partition_columns | default("None") }}
- **Transform logic:** {{ transform_description }}
-->

<!-- Split mode: replace the above with:
> **Split mode:** Dataflow details organized by layer:
> - [Source details](layers/source.md)
> - [Bronze dataflows](layers/bronze.md)
> - [Silver dataflows](layers/silver.md)
> - [Gold dataflows](layers/gold.md)
-->

---

## Infrastructure Requirements

| Platform | Resource Type | Name | Purpose |
|---|---|---|---|
| {{ infra_rows }} |

### Resource Details

<!-- Example for Fabric: -->
<!--
- **Workspace:** {workspace_name}
- **Bronze Lakehouse:** {name} — raw ingestion landing
- **Silver Lakehouse:** {name} — cleansed entities
- **Gold Warehouse:** {name} — serving / reporting layer
- **Key Vault:** {name} — secrets management
-->

---

## Engine Strategy

| Layer Transition | Engine | Rationale |
|---|---|---|
| source → bronze | polars | Lightweight I/O, no cluster overhead |
| bronze → silver | polars or spark | Simple transforms, data fits in memory |
| silver → gold | {{ gold_engine }} | {{ gold_engine_rationale }} |

---

## Partitioning Strategy

| Layer | Table | Partition Columns | Expression |
|---|---|---|---|
| {{ partition_rows }} |

---

## Environment Differences

| Aspect | Dev | Test | Prod |
|---|---|---|---|
| Engine | polars | mixed | mixed (per stage) |
| Storage | local files | cloud (test workspace) | cloud (prod workspace) |
| Schedule | manual trigger | daily | per stage definition |
| Data volume | sample (1000 rows) | full | full |
| Secrets | .env file | Key Vault (test) | Key Vault (prod) |
| Monitoring | console logs | basic alerts | full alerting + SLA |

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| {{ risk_rows }} |

---

## Changelog

### v{{ version | default("1") }} — {{ date }}
{{ changelog_entries }}

<!-- Older versions below, newest first -->

---

## Approval

> **Reviewer:** Please review the architecture above. Reply with:
> - `approve` — proceed to metadata generation and provisioning
> - Specific feedback — I will iterate on the design

**Status:** {{ status | default("⏳ Awaiting review") }}

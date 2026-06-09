# Bronze Layer - Stage Contract

> Part of [Architecture v{{ version }}](../current.md)

<!-- In Level 3 (per-source split), this file covers only {{ source_name }} -->

## Stage Contract

<!-- Repeat for each source-to-bronze source domain or workload boundary: -->

### {{ source_name }} to bronze

- **Source domain:** {{ source_name }}
- **Target grain:** {{ target_grain }}
- **Load pattern:** {{ load_pattern }}
- **Change detection:** {{ change_detection }}
- **Key strategy:** {{ key_strategy }}
- **Freshness target:** {{ freshness_target }}
- **Engine:** {{ engine }}
- **Platform/storage:** {{ platform_storage }}
- **Partitioning principle:** {{ partitioning_principle | default("_ingest_date required") }}
- **Quality gate:** {{ quality_gate }}
- **Backfill/rollback:** {{ backfill_rollback }}

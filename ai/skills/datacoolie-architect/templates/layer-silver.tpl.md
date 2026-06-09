# Silver Layer - Stage Contract

> Part of [Architecture v{{ version }}](../current.md)

<!-- In Level 3 (per-domain split), this file covers only {{ domain }} -->

## Stage Contract

<!-- Repeat for each bronze-to-silver domain or workload boundary: -->

### {{ domain }} bronze to silver

- **Source domains:** {{ source_domains }}
- **Target grain:** {{ target_grain }}
- **Load pattern:** {{ load_pattern }}
- **Change detection:** {{ change_detection }}
- **Key strategy:** {{ key_strategy }}
- **Freshness target:** {{ freshness_target }}
- **Engine:** {{ engine }}
- **Platform/storage:** {{ platform_storage }}
- **Deduplication contract:** {{ dedup_contract }}
- **Quality gate:** {{ quality_gate }}
- **Backfill/rollback:** {{ backfill_rollback }}

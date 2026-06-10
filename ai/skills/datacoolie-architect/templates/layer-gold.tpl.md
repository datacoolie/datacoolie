---
artifact_type: architecture_layer
project_name: "{{ project_name }}"
version: 'v{{ version }}'
layer: gold
stage: silver2gold
domain: '{{ domain | default("") }}'
parent: "../current.md"
status: draft
---

# Gold Layer - Stage Contract

> Part of [Architecture v{{ version }}](../current.md)

<!-- In Level 3 (per-domain split), this file covers only {{ domain }} -->

## Stage Contract

<!-- Repeat for each silver-to-gold domain or workload boundary: -->

### {{ domain }} silver to gold

- **Source domains:** {{ source_domains }}
- **Target grain:** {{ target_grain }}
- **Metric/consumer contract:** {{ metric_contract }}
- **Load pattern:** {{ load_pattern }}
- **Key strategy:** {{ key_strategy }}
- **Freshness target:** {{ freshness_target }}
- **Engine:** {{ engine }}
- **Platform/storage:** {{ platform_storage }}
- **Quality gate:** {{ quality_gate }}
- **Backfill/rollback:** {{ backfill_rollback }}
- **Consumers:** {{ consumers | default("TBD") }}

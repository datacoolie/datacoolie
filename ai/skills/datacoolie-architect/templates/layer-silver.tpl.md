# Silver Layer — Dataflow Details

> Part of [Architecture v{{ version }}](../yymmdd_architecture.md)

<!-- In Level 3 (per-domain split), this file covers only {{ domain }} -->

## Dataflows

<!-- Repeat for each bronze→silver dataflow: -->

### {{ domain }}.{{ entity_name }}

- **Source tables:** {{ source_tables }} (from bronze)
- **Destination:** silver/{{ domain }}/{{ entity_name }}/
- **Load type:** {{ load_type }}
- **Watermark column:** {{ watermark_column | default("N/A") }}
- **Merge keys:** {{ merge_keys | default("N/A") }}
- **Engine:** {{ engine }}
- **Partition columns:** {{ partition_columns | default("None") }}
- **Deduplication:** {{ dedup_columns | default("None") }}
- **Transform logic:** {{ transform_description }}

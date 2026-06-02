# Bronze Layer — Dataflow Details

> Part of [Architecture v{{ version }}](../yymmdd_architecture.md)

<!-- In Level 3 (per-source split), this file covers only {{ source_name }} -->

## Dataflows

<!-- Repeat for each source→bronze dataflow: -->

### {{ source_name }}.{{ table_name }} → bronze

- **Source connection:** {{ connection_name }}
- **Source table:** {{ source_table }}
- **Destination:** bronze/{{ source_name }}/{{ table_name }}/
- **Load type:** {{ load_type }}
- **Watermark column:** {{ watermark_column | default("N/A") }}
- **Merge keys:** {{ merge_keys | default("N/A") }}
- **Engine:** {{ engine }}
- **Partition columns:** {{ partition_columns | default("_ingest_date") }}
- **Transform logic:** {{ transform_description | default("Schema enforcement only") }}
- **Schedule:** {{ schedule }}

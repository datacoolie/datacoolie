# Gold Layer — Dataflow Details

> Part of [Architecture v{{ version }}](../yymmdd_architecture.md)

<!-- In Level 3 (per-domain split), this file covers only {{ domain }} -->

## Dataflows

<!-- Repeat for each silver→gold dataflow: -->

### {{ domain }}.{{ metric_or_view }}

- **Source tables:** {{ source_tables }} (from silver)
- **Destination:** gold/{{ domain }}/{{ metric_or_view }}/
- **Load type:** {{ load_type }}
- **Engine:** {{ engine }}
- **Partition columns:** {{ partition_columns | default("None") }}
- **Join logic:** {{ join_description | default("N/A") }}
- **Aggregation:** {{ aggregation_description | default("N/A") }}
- **Consumers:** {{ consumers | default("TBD") }}

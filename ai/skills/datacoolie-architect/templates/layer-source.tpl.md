---
artifact_type: architecture_source
project_name: "{{ project_name }}"
version: 'v{{ version }}'
source_name: "{{ source_name }}"
parent: "../current.md"
status: draft
---

# Source: {{ source_name }}

> Part of [Architecture v{{ version }}](../current.md)

## Connection

- **Type:** {{ connection_type }}
- **Protocol:** {{ protocol }}
- **Host/URL:** {{ host_or_url }}
- **Authentication:** {{ auth_method }}

## Characteristics

- **Estimated volume:** {{ volume }}
- **Table count:** {{ table_count }}
- **CDC available:** {{ cdc_available }}
- **Watermark candidates:** {{ watermark_candidates }}
- **Change frequency:** {{ change_frequency }}
- **Peak windows:** {{ peak_windows }}

## Access & Connectivity

- **Network:** {{ network_notes }}
- **Rate limits:** {{ rate_limits }}
- **Environments:** {{ environments }}

## Notes

{{ source_notes }}

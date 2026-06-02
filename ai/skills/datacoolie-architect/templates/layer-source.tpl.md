# Source: {{ source_name }}

> Part of [Architecture v{{ version }}](../yymmdd_architecture.md)

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

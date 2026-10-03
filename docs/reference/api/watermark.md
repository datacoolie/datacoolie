---
title: Watermark — Python API Reference | DataCoolie
description: Python API reference for DataCoolie watermark modules covering WatermarkManager and related contracts for incremental processing.
---

# Watermark

## ETL manager contract

When an ETL execution receives a watermark manager, the manager must provide
callable `get_watermark`, `save_watermark`, and `serialize` methods. The
orchestrator validates those capabilities before reading checkpoint state.
Source readers own merge semantics through
`BaseSourceReader.merge_watermark(existing, candidate)`; the manager's
`merge_watermark` method remains available as a standalone API but is not used
as an ETL compatibility fallback. Serialization is performed before the
destination write, so invalid checkpoint values cannot leave a successful
target mutation without a saveable state.

::: datacoolie.watermark.base
    options:
      members:
        - BaseWatermarkManager
        - WatermarkSerializer
        - serialize_watermark
        - deserialize_watermark
        - is_watermark_empty

::: datacoolie.watermark.watermark_manager

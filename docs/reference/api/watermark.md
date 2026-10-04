---
title: Watermark — Python API Reference | DataCoolie
description: Python API reference for DataCoolie watermark modules covering WatermarkManager and related contracts for incremental processing.
---

# Watermark

## ETL manager contract

When an ETL execution receives a watermark manager, the manager must provide
callable `get_watermark`, `save_watermark`, and `serialize` methods. The
orchestrator validates those capabilities before reading persisted state.
Source readers own merge semantics through
`BaseSourceReader.validate_watermark(value, *, boundary=...)` and
`BaseSourceReader.merge_watermark(existing, candidate: WatermarkCandidate | None) -> dict`.
Serialization is performed before the destination write, so invalid watermark
values cannot leave a successful target mutation without a saveable state.

`WatermarkRuntimeInfo` is the canonical per-attempt evidence model. It keeps
provider load status, loaded value, merge base, typed candidate, prepared save
payload, and save acknowledgement separate. A source result with no rows skips
transform, write, and save—even for bounded replay—and retains any candidate
with `save_status="not_attempted"`; a transformed-empty frame is still passed
to the destination writer.

`WatermarkCandidate` stores the pure `{values, kind}` observation returned by a
source. Candidate kinds are `observed_max`, `request_end`, `opaque`, and
`metadata`. Load status is `not_requested`, `loaded`, `reused`, or `load_error`;
save status is `not_requested`, `not_attempted`, `no_candidate`, `saved`, or
`save_error`.

::: datacoolie.watermark.contracts
    options:
      members:
        - WatermarkCandidateKind
        - WatermarkLoadStatus
        - WatermarkSaveStatus
        - WatermarkCandidate

::: datacoolie.watermark.runtime
    options:
      members:
        - WatermarkRuntimeInfo

::: datacoolie.watermark.base
    options:
      members:
        - BaseWatermarkManager
        - WatermarkSerializer
        - serialize_watermark
        - deserialize_watermark
        - is_watermark_empty

::: datacoolie.watermark.watermark_manager

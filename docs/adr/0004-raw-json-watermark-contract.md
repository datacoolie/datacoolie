# ADR-0004 — Metadata provider returns raw JSON watermark text

**Status** · Accepted

## Context

Watermark payloads use engine-specific sentinels (`__datetime__`,
`__date__`, `__time__`) that must round-trip through a deserialiser to
become Python objects. Early API of `BaseMetadataProvider.get_watermark`
returned a `dict` — the provider did the parsing.

Problems:

- Three providers duplicated the same sentinel decoder.
- Tests for watermark decoding proliferated across provider test files.
- Custom providers had to know the sentinel format.

## Decision

`BaseMetadataProvider.get_watermark(dataflow_id: str) -> Optional[str]` returns the
**raw JSON string** (or `None` if none is persisted). `WatermarkManager`
owns the decoding — it's the single place that knows the sentinel format.

Writing is symmetric: `update_watermark(dataflow_id, watermark_value, *, job_id, dataflow_run_id)` accepts a
serialised string.

## Consequences

- One decoder, one encoder, one place to add new sentinels.
- Custom metadata providers are simpler: just store/retrieve a string.
- Existing databases store watermarks as TEXT / JSON-typed columns either
  way — no migration needed.

## Related

- [Extending · Write a metadata provider](../extending/writing-a-metadata-provider.md)
- [Concepts · Watermarks](../concepts/watermarks.md)

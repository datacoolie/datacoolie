---
title: Write a Source Plugin — DataCoolie
description: Build a custom DataCoolie source plugin that reads data into the active engine dataframe contract and integrates with metadata.
---

# Write a source

**Prerequisites** · You want DataCoolie to read from a format/backend not covered by the built-ins · you know which engine(s) your reader supports.
**End state** · Source reader registered under a runtime format name and
exercised through the registry. Metadata consumption also requires a
connection shape and `format` value accepted by the authored metadata schema
and the active runtime. In the 0.2.0 schema, `format` is an enum of built-in
formats; omitting `connection_type` does not make a custom format valid for
`dc validate`.

## Minimal reader

This example reads objects from `source.configure.records` using the active
engine. Replace `_read_data` with your backend's decoder or client; keep the
range and observation steps in `_read_internal`.

```python
from typing import Any, Dict, Optional

from datacoolie.core.exceptions import ConfigurationError
from datacoolie.core.models.source import Source
from datacoolie.sources.base import BaseSourceReader
from datacoolie.sources.contracts import SourceReadRequest


class MyFormatReader(BaseSourceReader):
    def _supports_read_range(self) -> bool:
        # Opt in only when this reader can enforce SourceReadRange exactly.
        return True

    def _watermark_ordering_kinds(self, candidate: Dict[str, Any]) -> Dict[str, str]:
        # Use this only when the source proves that its row maxima are ordered.
        return self._typed_row_watermark_ordering_kinds(candidate)

    def _read_internal(
        self,
        source: Source,
        request: SourceReadRequest,
    ):
        self._set_source_action(
            {"reader": type(self).__name__, "path": source.path}
        )

        # Read the full dataset (or push the watermark predicate down here).
        df = self._read_data(source)

        # Apply the source-owned bounded range to the actual frame. Returning
        # True from _supports_read_range without enforcing this is not support.
        read_range = self._get_read_range()
        if read_range is not None:
            df = self._apply_read_range_filter(df)
        elif source.watermark_columns and (
            request.watermark_start or request.watermark_end
        ):
            df = self._apply_watermark_filter(
                df,
                source.watermark_columns,
                request.watermark_start or {},
                request.watermark_end,
            )

        df = self._apply_filter_expression(df, source)
        return self._finalize_read(
            df,
            source.watermark_columns,
            type(self).__name__,
            source.path or source.full_table_name or source.table or "myfmt source",
        )

    def _read_data(self, source: Source, configure: Optional[Dict[str, Any]] = None):
        # The fixture keeps this minimal reader runnable on every active
        # engine. Replace this decoder with the real backend integration in a
        # production plugin; keep the DataFrame boundary unchanged.
        del configure
        records = source.configure.get("records", [])
        if not isinstance(records, list) or not all(
            isinstance(record, dict) for record in records
        ):
            raise ConfigurationError(
                "MyFormatReader source.configure.records must be a list of objects"
            )
        return self._engine.create_dataframe(records)
```

The framework calls one typed public entry:
`read(source, *, request=SourceReadRequest(reference_time, ...))`.
The base template invokes `_read_internal(source, request)` once and returns a
`SourceReadResult` with `.frame`, `.rows_read`, the effective
`SourceReadSelection`, an optional typed `.candidate`, and a detached optional
`.runtime` projection. Subclasses normally implement `_read_internal` and
`_read_data` with format-specific logic; those protected seams are unchanged.

Direct callers consume `result.frame`; they do not call a second result method
or fetch mutable runtime state. If a custom reader replaces the public template,
it must preserve the same request/result contract.

See the [BaseSourceReader API reference](../reference/api/sources.md) for the
full public lifecycle and protected hook contract.

`SourceReadRequest.watermark_start` and `.watermark_end` are ordinary
lower/upper bounds. A bounded replay passes a `SourceReadRange` instead; the
request cannot combine it with explicit watermark bounds. `SourceReadRange` has
a column, start, end, lower operator (`>` or `>=`), and upper operator (`<` or
`<=`). Use `_get_read_range()` inside `_read_internal` and apply the exact range
before counting rows or calculating the typed candidate. The source-empty gate
still skips transform, write, and save when `result.rows_read == 0`, including
for bounded replay.

The reader returns a `WatermarkCandidate` through `result.candidate`. When
persistence is enabled, the driver calls
`merge_watermark(existing, candidate: WatermarkCandidate | None) -> dict`,
validates the merged value, writes the destination, then saves it. A candidate
is not persisted merely because a requested bound was supplied. At the Driver
persistence boundary, an empty candidate or a non-empty mapping whose values
are all `null` is discarded before merge and save. A reader that does not opt
into typed ordering keeps candidate values opaque.

## Required runtime contract

Every reader used by the ETL orchestrator must expose callable implementations
of `read`, `validate_watermark`, and `merge_watermark`. Built-in readers get
these methods from `BaseSourceReader`; a structurally compatible injected
reader is also valid, but it must implement the complete contract. Missing or
non-callable hooks fail before persisted watermark state is read, source I/O
starts, transformers or writers are constructed, or state is saved.

`SourceRuntimeInfo` is a detached projection returned as `result.runtime` when
available. It contains timing/status/message, nullable `rows_read`, the native
`source_action`, and the resolved `selection`; it has no watermark-before,
watermark-after, candidate-kind, or range compatibility fields. Runtime capture
cannot change selection, replacement, or retry behavior. If a source fails,
available runtime evidence is attached to the source error while the original
error remains authoritative.

When a `BaseWatermarkManager` is supplied to ETL, it must expose callable
`get_watermark`, `save_watermark`, and `serialize` methods. The reader owns
watermark semantics: the orchestrator calls the reader's `validate_watermark`
and `merge_watermark` operations, then calls manager `serialize` before writing
the destination. A serialization or validation failure prevents both
destination mutation and watermark save.

## Register

In **your** package's `pyproject.toml`:

```toml
[project.entry-points."datacoolie.sources"]
myfmt = "mypkg.readers:MyFormatReader"
```

After `pip install mypkg`, DataCoolie discovers the reader through the
`datacoolie.sources` entry point. This adds a runtime registry alias; it does
not extend the metadata JSON Schema or prove that the selected engine supports
the backend format. In DataCoolie 0.2.0, `dc validate` rejects custom values
such as `format: "myfmt"` because the schema enumerates built-in formats.
Omitting `connection_type` is not a CLI workaround: it only leaves the runtime
model's implicit default in direct programmatic construction. Verify the
installed metadata connection shape, schema, and backend capability before
publishing a new format name. Use a new alias for a packaged plugin; an entry
point that collides with a built-in name does not replace the built-in
registration.

For a runtime-only smoke check, register the class explicitly and construct it
through the public factory. The source factory passes `engine` to every reader
and passes `allowed_prefixes` only for the built-in `function` reader:

```python
from datacoolie import create_engine, create_source, source_registry
from mypkg.readers import MyFormatReader

engine = create_engine("polars")
source_registry.register("myfmt", MyFormatReader)
reader = create_source("myfmt", engine=engine)
```

This activates the runtime registry only; it does not make a `myfmt` metadata
document pass authored schema validation. Call `reader.read(source, request=...)`
with a programmatically constructed `Source` to exercise the reader contract,
then inspect `result.frame` and `result.candidate`, or use an installed entry
point in a separate integration environment.

```python
from datetime import datetime, timezone

from datacoolie.sources.contracts import SourceReadRequest

result = reader.read(
    source,
    request=SourceReadRequest(reference_time=datetime.now(timezone.utc)),
)
frame = result.frame
```

## Engine-specific branching

If your reader uses engine-specific APIs, dispatch on class type:

The following is backend pseudocode: replace the native objects and calls with
the API of your dataframe library. It is illustrative, not a ready-to-run
fixture.

```python
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.engines.polars_engine import PolarsEngine

if isinstance(self._engine, SparkEngine):
    df = self._engine.spark.read.format("my_format").load(path)
elif isinstance(self._engine, PolarsEngine):
    df = polars.read_my_format(path)
else:
    raise NotImplementedError(type(self._engine))
```

Prefer the engine's unified methods (`read_path`, `read_database`) whenever
possible — they handle path normalisation and options for you.

## Watermark push-down

When the backend supports early filtering, compile its predicate inside
`_read_internal` from `request.watermark_start`, `request.watermark_end`, or
`self._get_read_range()`, then pass that predicate explicitly to your backend
client. Watermark bounds are runtime inputs; they are not placed in
`source.configure` automatically. Preserve the selected column and effective
operators, bind values safely, and validate column names with the backend's
identifier rules. Apply a residual filter when the endpoint's boundaries are
broader than the requested range. Reject unsupported exact bounds before
reading; do not advertise `_supports_read_range()` solely because a parameter
was sent.

## Testing

Cover these cases:

- `SourceReadRequest` with empty bounds → full read
- `SourceReadRequest` with a populated `watermark_start` → incremental read
- `SourceReadRange(start, end)` → exact left-closed/right-open rows when the
  reader advertises bounded-read support
- Direct `reader.read(source, request=...)` → a `SourceReadResult` whose
  `.frame`, `.selection`, and `.candidate` agree with the source operation
- Missing connection option → public `SourceError` whose `__cause__` is the
  original `ConfigurationError`
- Backend-native error → public `SourceError` whose `__cause__` is the original
  `EngineError`; inspect the public error details for source context

The public replay example
[`range_source.py`](../examples/files/projects/function/functions/range_source.py)
contains a small reader with both ordinary and bounded paths. Its integration
check loads the class through `source_registry`, asserts the filtered IDs, and
checks the returned `.frame` and typed `.candidate` after each read.

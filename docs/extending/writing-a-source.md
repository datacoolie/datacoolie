# Write a source

**Prerequisites** · You want DataCoolie to read from a format/backend not covered by the built-ins · you know which engine(s) your reader supports.
**End state** · Source reader registered under a new format name, consumable from metadata with `format: "myfmt"`.

## Minimal reader

```python
from typing import Any, Dict, Optional

from datacoolie.sources.base import BaseSourceReader
from datacoolie.core.models import Source


class MyFormatReader(BaseSourceReader):
    def _read_internal(self, source: Source, watermark: Optional[Dict[str, Any]] = None):
        conn = source.connection
        path = f"{conn.base_path}/{source.schema_name}/{source.table}"

        # Read the full dataset (or push the watermark predicate down here)
        df = self._read_data(source, source.configure)

        # Apply watermark filter in memory when push-down is not supported
        if source.watermark_columns and watermark:
            df = self._apply_watermark_filter(df, source.watermark_columns, watermark)
        return df

    def _read_data(self, source: Source, configure: Optional[Dict[str, Any]] = None):
        conn = source.connection
        path = f"{conn.base_path}/{source.schema_name}/{source.table}"
        return self._engine.read_path(path, fmt="myfmt", options=conn.read_options)
```

The framework calls the public `read(source, watermark)` method; subclasses
implement `_read_internal` (and optionally `_read_data`) with format-specific
logic. Never override `read` directly — the base class wraps it with timing,
error handling, and runtime-info collection.

## Register

In **your** package's `pyproject.toml`:

```toml
[tool.poetry.plugins."datacoolie.sources"]
myfmt = "mypkg.readers:MyFormatReader"
```

After `pip install mypkg`, DataCoolie resolves `format: "myfmt"` in metadata
to your class automatically.

## Engine-specific branching

If your reader uses engine-specific APIs, dispatch on class type:

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

When the backend can filter during read, do it inside `_read_data` rather than
in memory:

```python
def _read_data(self, source: Source, configure=None):
    conn = source.connection
    if source.watermark_columns:
        wm_col = source.watermark_columns[0]
        query = f"SELECT * FROM src WHERE {wm_col} > :wm"
        return self._engine.read_database(
            query=query,
            options={**conn.read_options, "params": {"wm": (configure or {}).get(wm_col)}},
        )
    return self._engine.read_path(source.table, fmt="myfmt", options=conn.read_options)
```

## Testing

Cover the four common cases:

- Empty watermark → full read
- Populated watermark → incremental read
- Missing connection option → clear `ConfigurationError`
- Backend-native error → wrapped as `EngineError` with `details`

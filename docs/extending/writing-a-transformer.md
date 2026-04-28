# Write a transformer

**Prerequisites** · You have a per-row or per-batch transformation to apply between read and write.
**End state** · Transformer in the pipeline at a known order slot, registered via entry points.

## Minimal transformer

```python
from datacoolie.transformers.base import BaseTransformer
from datacoolie.core.models import DataFlow


class PiiMaskerTransformer(BaseTransformer):
    # Slots 40-60 are reserved for user plugins. Pick one.
    ORDER = 45

    @property
    def order(self) -> int:
        return self.ORDER

    def transform(self, df, dataflow: DataFlow):
        cfg = (dataflow.transform and dataflow.transform.additional.get("pii_mask")) or {}
        cols = cfg.get("columns", [])
        if not cols:
            self._mark_skipped()
            return df

        for c in cols:
            df = self._engine.add_column(df, c, f"sha2({c}, 256)")

        self._mark_applied(f"cols={len(cols)}")
        return df
```

## Register

```toml
[tool.poetry.plugins."datacoolie.transformers"]
pii_masker = "mypkg.transformers:PiiMaskerTransformer"
```

## Opt in from metadata

The driver's `DEFAULT_TRANSFORMERS` list does not include your plugin. To
use it:

```python
driver = DataCoolieDriver(engine=engine, metadata_provider=metadata)
driver._create_transformer_pipeline = lambda: build_pipeline(
    engine,
    names=["schema_converter", "deduplicator", "pii_masker", "system_column_adder", "column_name_sanitizer"],
)
```

Or subclass `DataCoolieDriver` and override `_create_transformer_pipeline`.

## Order slot cheat-sheet

| Slots | Who owns them |
|---|---|
| 0–9 | Reserved for future framework pre-cast work |
| **10** | `SchemaConverter` |
| **20** | `Deduplicator` |
| **30** | `ColumnAdder`, `SCD2ColumnAdder` |
| **40–60** | **Your plugins** |
| **70** | `SystemColumnAdder` |
| **80** | `PartitionHandler` |
| **90** | `ColumnNameSanitizer` |
| 100+ | Reserved for future framework post-sanitize work |

See [ADR-0003](../adr/0003-transformer-ordering-slots.md).

## Tracking labels

Call `_mark_applied()`, `_mark_applied("detail")`, or `_mark_skipped()` inside
`transform` so the ETL log records exactly what your transformer did. Without
a call, the default is to record your class name.

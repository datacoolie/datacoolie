---
title: Write a Transformer Plugin — DataCoolie
description: Build a custom DataCoolie transformer plugin, register it through Python entry points, and place it correctly in the pipeline ordering model.
---

# Write a transformer

**Prerequisites** · You have a per-row or per-batch transformation to apply between read and write.
**End state** · Transformer in the pipeline at a known order slot, registered via entry points.

## Minimal transformer

```python
from datacoolie.transformers.base import BaseTransformer
from datacoolie.core.models import DataFlow


class PiiMaskerTransformer(BaseTransformer):
    # Slots 40-50 are reserved for user plugins. Pick one.
    ORDER = 45

    @property
    def order(self) -> int:
        return self.ORDER

    def transform(self, df, dataflow: DataFlow):
        cfg = dataflow.transform.configure.get("pii_mask", {})
        cols = cfg.get("columns", [])
        if not cols:
            self._mark_skipped()
            return df

        for c in cols:
            df = self._engine.add_column(
                df, c, f"CASE WHEN {c} IS NULL THEN NULL ELSE '***' END"
            )

        self._mark_applied(f"cols={len(cols)}")
        return df
```

## Register

```toml
[project.entry-points."datacoolie.transformers"]
pii_masker = "mypkg.transformers:PiiMaskerTransformer"
```

## Opt in from metadata

Registering a transformer makes it resolvable, but does not add it to the
driver's `DEFAULT_TRANSFORMERS`. Use the documented driver extension hook:

```python
from datacoolie import transformer_registry
from datacoolie.orchestration.driver import DataCoolieDriver


class PiiDriver(DataCoolieDriver):
    def _create_transformer_pipeline(self):
        pipeline = super()._create_transformer_pipeline()
        pipeline.add_transformer(
            transformer_registry.get("pii_masker", engine=self._engine)
        )
        return pipeline


driver = PiiDriver(engine=engine, metadata_provider=metadata)
```

`TransformerPipeline` sorts the combined list by each transformer's `order`
when it runs.

## Order slot cheat-sheet

| Slots | Who owns them |
|---|---|
| 0–9 | Reserved for future framework pre-cast work |
| **10** | `SchemaConverter` |
| **20** | `Deduplicator` |
| **30** | `ColumnAdder` |
| **35** | `RowFilter` |
| **40–50** | **Your plugins** |
| **60** | `SCD2ColumnAdder` |
| **70** | `SystemColumnAdder` |
| **80** | `PartitionHandler` |
| **90** | `ColumnNameSanitizer` |
| 100+ | Reserved for future framework post-sanitize work |

See [ADR-0003](../adr/0003-transformer-ordering-slots.md).

## Tracking labels

Call `_mark_applied()`, `_mark_applied("detail")`, or `_mark_skipped()` inside
`transform` so the ETL log records exactly what your transformer did. Without
a call, the default is to record your class name.

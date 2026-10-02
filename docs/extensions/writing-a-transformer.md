---
title: Write a Transformer Plugin — DataCoolie
description: Build a custom DataCoolie transformer plugin, register it through Python entry points, and place it correctly in the pipeline ordering model.
---

# Write a transformer

**Prerequisites** · You have a per-row or per-batch transformation to apply between read and write.
**End state** · Transformer in the pipeline at a known order slot, registered via entry points.

For a complete local package installation and Driver run, start with the
[transformer tutorial](transformer-tutorial.md). This page explains the
implementation contract and ordering choices. For ordinary masking, first
consider the [built-in transform patterns](../guide/metadata/transform-patterns.md).

## Minimal transformer

The complete source for this pattern is available as
[source](../examples/source/plugins/pii_masker.py.md) or
[raw](../examples/files/plugins/pii_masker.py). Its packaging metadata is
[raw](../examples/files/plugins/pyproject.toml).

```python
from datacoolie.transformers.base import BaseTransformer
from datacoolie.core.models.dataflow import DataFlow


class PiiMaskerTransformer(BaseTransformer):
    # Slots 40-50 are reserved for user plugins. Pick one.
    ORDER = 45

    def __init__(self, engine) -> None:
        self._engine = engine

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
pii_masker = "pii_masker:PiiMaskerTransformer"
```

This matches the downloadable flat module `pii_masker.py`. If your package uses
another module path, change the entry-point value to its importable module and
class. Install the package in the Python environment that runs the Driver.

## Opt in from metadata

Registering a transformer makes it resolvable, but does not add it to the
framework's default transformer sequence. Use the protected Driver hook shown
below; check its [API signature](../reference/api/orchestration.md#datacoolie.orchestration.driver.DataCoolieDriver._create_transformer_pipeline)
and rerun your integration test when upgrading:

```python
from datacoolie import transformer_registry
from datacoolie.core.constants import ColumnCaseMode
from datacoolie.orchestration.driver import DataCoolieDriver


class PiiDriver(DataCoolieDriver):
    def _create_transformer_pipeline(
        self,
        dataflow_run_id=None,
        column_name_mode=ColumnCaseMode.LOWER,
    ):
        pipeline = super()._create_transformer_pipeline(
            dataflow_run_id=dataflow_run_id,
            column_name_mode=column_name_mode,
        )
        pipeline.add_transformer(
            transformer_registry.get("pii_masker", engine=self._engine)
        )
        return pipeline


driver = PiiDriver(engine=engine, metadata_provider=metadata)
```

Here `engine` and `metadata` are already configured instances, and the engine
has a platform attached. Otherwise supply `platform=` to the Driver; both
must share the same platform object. The
[tutorial](transformer-tutorial.md#run-through-the-driver) supplies a complete
runner. Its metadata uses `transform.configure.pii_mask.columns`; that map is
read by this plugin after the runner adds it to the pipeline.

`TransformerPipeline` sorts the combined list by each transformer's `order`
when it runs.

Always forward both `dataflow_run_id` and `column_name_mode` when overriding
this hook. The driver uses the run ID to correlate the framework-owned
`__dataflow_run_id` column with the `DataFlowRuntimeInfo` recorded for the same
execution, and the mode controls column-name normalization.

## Order slot cheat-sheet

| Slots | Who owns them |
|---|---|
| **5** | `ColumnValueTransformer` |
| **10** | `SchemaConverter` |
| **18** | `HashColumnAdder` |
| **20** | `Deduplicator` |
| **30** | `ColumnAdder` |
| **35** | `RowFilter` |
| **40–50** | **Your plugins** |
| **60** | `SCD2ColumnAdder` |
| **70** | `SystemColumnAdder` |
| **80** | `PartitionHandler` |
| **84** | `DataMasker` |
| **85** | `ColumnProjector` |
| **90** | `ColumnNameSanitizer` |
| Other slots | Reserved for future framework work or additional plugins; the pipeline sorts by numeric order |

See [ADR-0003](../project/decisions/0003-transformer-ordering-slots.md).

## Tracking labels

Call `_mark_applied()`, `_mark_applied("detail")`, or `_mark_skipped()` inside
`transform` so the execution log records exactly what your transformer did. Without
a call, the default is to record your class name.

## Test your implementation

Test with native DataFrames for every advertised engine. Check configured
columns, preserved nulls, unchanged output when no columns are configured,
ordering with other transforms, and failure for invalid column expressions.
The sample assumes simple SQL-safe column names; adapt expression construction
to the identifiers and data types your plugin supports. Do not treat it as a
general identifier-quoting or anonymization implementation.

Then test an installed package through a Driver run, using the same alias as
its entry point and checking business output. See the
[discovery troubleshooting steps](index.md#troubleshoot-discovery-and-activation)
and [transformer API](../reference/api/transformers.md).

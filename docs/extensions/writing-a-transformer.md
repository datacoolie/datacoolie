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

Use **40–50** for user plugins. The canonical
[built-in order and responsibilities](../reference/concepts/transformers-and-pipeline.md#the-twelve-built-ins-order-responsibility)
shows the surrounding steps; [ADR-0003](../project/decisions/0003-transformer-ordering-slots.md)
records the slot reservation. Choose distinct orders when your plugins depend
on each other; the pipeline sorts by numeric order.

## Report renamed and removed columns

A plugin that renames or drops columns must report its input-to-output mapping
with `_report_column_mapping()`. Without a declaration, the pipeline preserves
identical names and marks the mapping unknown if any input disappears. Even
dropping an unrelated column can then prevent watermark-window replacement.

This example expects `UpdatedAt` and `raw_payload` in its input. It renames the
watermark column and removes the payload using public engine operations:

```python
from datacoolie.transformers.base import BaseTransformer, ColumnMapping


class RenameAndDropTransformer(BaseTransformer):
    ORDER = 45

    def __init__(self, engine):
        self._engine = engine

    @property
    def order(self):
        return self.ORDER

    def transform(self, df, dataflow):
        before = self._engine.get_columns(df)
        renames = {"UpdatedAt": "updated_at"}
        result = self._engine.rename_columns(df, renames)
        result = self._engine.drop_columns(result, ["raw_payload"])
        after = self._engine.get_columns(result)
        self._report_column_mapping(
            ColumnMapping.from_columns(before, after, renames=renames)
        )
        self._mark_applied("rename watermark; drop raw_payload")
        return result
```

Add this plugin through the same Driver hook as the masking example. Report
the mapping on every invocation: the pipeline resets tracking before each
call. `from_columns()` maps unchanged inputs to themselves, declared renames
to their final names, and removed inputs to `None`. Supply all actual renames;
do not claim known lineage for a transformation you cannot describe.

The replacement writer receives bounds under the final `updated_at` name.
Removing a replacement watermark column, leaving lineage unknown, or mapping
two watermark columns to one output fails before the replacement write. See
[the mapping API](../reference/api/transformers.md#datacoolie.transformers.base.ColumnMapping)
and [watermark-window replacement](../guide/metadata/watermark-window-replacement.md).

## Tracking labels

Call `_mark_applied()`, `_mark_applied("detail")`, or `_mark_skipped()` inside
`transform` so the execution log records exactly what your transformer did. Without
a call, the default is to record your class name.

## Test your implementation

Test with native DataFrames for every advertised engine. Check configured
columns, preserved nulls, unchanged output when no columns are configured,
ordering with other transforms, and failure for invalid column expressions.
For renames/drops, run the plugin through `TransformerPipeline`, verify the
composed mapping on repeated calls, and check both successful window mapping
and rejection when the watermark column is removed.
The sample assumes simple SQL-safe column names; adapt expression construction
to the identifiers and data types your plugin supports. Do not treat it as a
general identifier-quoting or anonymization implementation.

Then test an installed package through a Driver run, using the same alias as
its entry point and checking business output. See the
[discovery troubleshooting steps](index.md#troubleshoot-discovery-and-activation)
and [transformer API](../reference/api/transformers.md).

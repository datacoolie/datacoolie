---
title: Run your first transformer plugin
description: Install a local transformer package and verify masked, null and unconfigured output through a DataCoolie Driver.
---

# Run your first transformer plugin

Use Python 3.11+ and a local filesystem. This tutorial installs the canonical
PII masker package, adds it to a Driver pipeline, and runs two CSV-to-Parquet
flows. One masks `email`; the other leaves it unchanged. All input is synthetic.
The example assumes simple SQL-safe column names.

## Install the package

Install the matching framework version with Polars support. For the current
preview/source-wheel route, follow the [installation guide](../guide/getting-started/installation.md).

```bash
python -m pip install "datacoolie[polars]==0.2.0"
```

Create a workspace with a `plugin/` subdirectory. Save the canonical
[pii_masker.py](../examples/files/plugins/pii_masker.py) and
[pyproject.toml](../examples/files/plugins/pyproject.toml) into `plugin/` with
those filenames. You can inspect the [rendered source](../examples/source/plugins/pii_masker.py.md).
From the workspace root, install the package:

```bash
python -m pip install ./plugin
python -c "from datacoolie import transformer_registry; assert 'pii_masker' in transformer_registry.list_plugins(); print('pii_masker: discovered')"
```

The entry point maps `pii_masker` to `pii_masker:PiiMaskerTransformer`. Run the
check in a fresh Python process from the workspace root, outside `plugin/`,
so imports use the installed package. Discovery alone does not execute a flow.

## Configure two flows

Save this as `metadata.json` in the workspace root:

```json
{
  "connections": [
    {
      "name": "source", "connection_type": "file", "format": "csv",
      "configure": {"base_path": "./data/input"}
    },
    {
      "name": "destination", "connection_type": "file", "format": "parquet",
      "configure": {"base_path": "./data/output"}
    }
  ],
  "dataflows": [
    {
      "name": "masked_customers", "stage": "plugin",
      "source": {"connection_name": "source", "table": "customers"},
      "transform": {"configure": {"pii_mask": {"columns": ["email"]}}},
      "destination": {
        "connection_name": "destination", "table": "masked_customers",
        "load_type": "overwrite"
      }
    },
    {
      "name": "unmasked_customers", "stage": "plugin",
      "source": {"connection_name": "source", "table": "customers"},
      "transform": {},
      "destination": {
        "connection_name": "destination", "table": "unmasked_customers",
        "load_type": "overwrite"
      }
    }
  ]
}
```

The `pii_mask` key is plugin-owned configuration. Registering its class does
not add it to the default pipeline; the runner below activates it explicitly.

## Run through the Driver

Save this complete script as `run.py` in the workspace root:

```python
from pathlib import Path

import polars as pl
from datacoolie import transformer_registry
from datacoolie.core.constants import ColumnCaseMode
from datacoolie.engines.polars_engine import PolarsEngine
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver
from datacoolie.platforms.local_platform import LocalPlatform


class PiiDriver(DataCoolieDriver):
    def _create_transformer_pipeline(
        self, dataflow_run_id=None, column_name_mode=ColumnCaseMode.LOWER
    ):
        pipeline = super()._create_transformer_pipeline(
            dataflow_run_id=dataflow_run_id, column_name_mode=column_name_mode
        )
        pipeline.add_transformer(
            transformer_registry.get("pii_masker", engine=self._engine)
        )
        return pipeline


input_dir = Path("data/input/customers")
input_dir.mkdir(parents=True, exist_ok=True)
Path("data/output").mkdir(parents=True, exist_ok=True)
input_file = input_dir / "customers.csv"
if not input_file.exists():
    input_file.write_text(
        "customer_id,email\n1,alice@example.com\n2,bob@example.com\n3,\n",
        encoding="utf-8",
    )

platform = LocalPlatform()
engine = PolarsEngine(platform=platform)
with PiiDriver(
    engine=engine,
    platform=platform,
    metadata_provider=FileProvider(config_path="metadata.json", platform=platform),
    state_base_path=".runtime",
) as driver:
    result = driver.run(stage="plugin")
assert result.failed == 0 and result.succeeded == 2


def emails(table):
    files = sorted(Path("data/output", table).glob("*.parquet"))
    assert files, f"Missing Parquet output for {table}"
    return pl.read_parquet(files).sort("customer_id").get_column("email").to_list()


assert emails("masked_customers") == ["***", "***", None]
assert emails("unmasked_customers") == ["alice@example.com", "bob@example.com", None]
print("pii_masker: 2 flows succeeded; masking, nulls and skipped configuration verified")
```

```bash
python run.py
```

The final message confirms two successful flows and both business outputs.
Parquet files are under `data/output/masked_customers/` and
`data/output/unmasked_customers/`; runtime records are under `.runtime/`.
Rerunning keeps three rows in each overwrite destination. Use a fresh workspace
to reset the fixture. The script preserves an existing input CSV, so its fixed
assertions must change if you adapt that input.

## Adapt and diagnose

Change the class and entry-point module path together, then reinstall the
package and start a fresh process. Add or remove configured columns in
`metadata.json`. Keep the Driver hook forwarding both arguments, and choose an
order using the [transformer guide](writing-a-transformer.md#order-slot-cheat-sheet).
Test each engine you advertise; this tutorial qualifies the local Polars path.

If discovery succeeds but masking does not happen, check that the runner uses
`PiiDriver`, the alias is `pii_masker`, and metadata has
`transform.configure.pii_mask.columns`. For import, constructor and alias
failures, follow [extension troubleshooting](index.md#troubleshoot-discovery-and-activation).

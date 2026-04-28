# Deploy to Microsoft Fabric

**Prerequisites** · Fabric workspace with an attached OneLake lakehouse · Spark notebook for `SparkEngine` or a native Python notebook for small `PolarsEngine` runs · `pip install datacoolie` in the notebook environment.
**End state** · DataCoolie pipeline running inside a Fabric notebook with `FabricPlatform` serving OneLake paths and Key Vault secrets.

## 1. Notebook bootstrap

```python
%pip install --quiet datacoolie
```

Install extra packages individually only when your pipeline needs them. For
example:

- `pip install sqlalchemy` for database metadata.
- `pip install httpx` for API metadata.
- `pip install openpyxl` for Excel metadata files.
- `pip install pyiceberg` for PyIceberg-based reads or writes.

Fabric already provides the Spark runtime, so you usually do not want a broad
bundle that pulls in every Spark and Polars-related dependency.

## 2. Start with the built-in Spark session

```python
from datacoolie.engines.spark_engine import SparkEngine
from datacoolie.platforms.fabric_platform import FabricPlatform
from datacoolie.metadata.file_provider import FileProvider
from datacoolie.orchestration.driver import DataCoolieDriver

platform = FabricPlatform()           # uses notebookutils under the hood
engine = SparkEngine(spark, platform=platform)   # `spark` is the built-in SparkSession
metadata = FileProvider(config_path="Files/metadata/orders.json", platform=platform)

with DataCoolieDriver(engine=engine, metadata_provider=metadata, base_log_path="Files/logs") as driver:
    driver.run(stage="ingest2bronze")
```

`spark` is the `SparkSession` provided by Fabric — do not create your own.

For small file and delta validation runs, you can skip Spark entirely and use a
native Python notebook with `PolarsEngine`. The checked-in example is
`sample_fabric_polars.ipynb` under the `usecase-sim/platforms/fabric/` assets.

## 3. Paths

- Use **ABFSS** paths or `Files/...` relative to the lakehouse:
  `abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Files/...`
- `FabricPlatform` normalises both forms.

## 4. Secrets

`FabricPlatform._fetch_secret` reads from Key Vault via
`notebookutils.credentials.getSecret(vault_url, secret_name)`. In
`secrets_ref`, the outer key is the vault URL, and each listed field must
already exist in `configure` with the Key Vault secret name as its current
value:

```json
{
  "configure": {
    "host": "db.contoso.net",
    "password": "sql-password"
  },
  "secrets_ref": {
    "https://myvault.vault.azure.net/": ["password"]
  }
}
```

## 5. Spark session tuning

For Spark notebooks, keep the first run narrow and reuse the session Fabric
already gives you. If the workload is tiny, prefer the native Python notebook
sample (`sample_fabric_polars.ipynb`) instead of spinning up a larger Spark
session. When you do want Polars with a smaller pool, put this in the first
cell before native Python execution starts:

```python
%%configure -f { "vCores": 2 }
```

## Reference assets

Use the Fabric platform guide in usecase-sim for the current sample notebooks,
metadata file, and setup notes:

- [`README.md`](https://github.com/datacoolie/datacoolie/blob/main/datacoolie/usecase-sim/platforms/fabric/README.md)

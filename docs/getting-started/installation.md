# Installation

DataCoolie is published to PyPI. The base package stays light;
engines, cloud SDKs, and metadata backends are **opt-in extras** so you only
install what you need.

## Pick your extras

```bash
# Minimal (no engine extras; rarely useful on its own)
pip install datacoolie

# Most common: one engine + one table format
pip install "datacoolie[polars,deltalake]"
pip install "datacoolie[spark,delta-spark]"

# Everything
pip install "datacoolie[all]"
```

## Extras reference

| Extra | Installs | Use when |
|---|---|---|
| `spark` | `pyspark>=3.5` | You want the Spark engine. |
| `polars` | `polars>=1.0` | You want the Polars engine. |
| `delta-spark` | `delta-spark>=3.0` | Spark + Delta Lake. |
| `deltalake` | `deltalake>=0.15` | Polars + Delta Lake (delta-rs). |
| `iceberg` | `pyiceberg>=0.6` | Apache Iceberg tables (any engine). |
| `aws` | `boto3>=1.28` | `AWSPlatform` (S3, Secrets Manager, Glue). |
| `api` | `httpx>=0.24` | `APIReader` and the API metadata provider. |
| `db` | `sqlalchemy>=2.0` | `DatabaseProvider` for metadata stored in an RDBMS. |
| `excel` | `fastexcel`, `openpyxl` | Reading Excel files as sources or metadata. |
| `fabric` | spark + delta-spark + polars + deltalake | Microsoft Fabric notebooks / Spark pools. |
| `databricks` | spark + delta-spark + polars + deltalake | Databricks Runtime. |
| `all` | everything above | Kitchen-sink local dev. |

## System requirements

| Component | Minimum | Notes |
|---|---|---|
| Python | **3.11** | Strict — the framework uses PEP 604 unions and `TypeVar` defaults. |
| Java | **17** for Spark | Only required when using `SparkEngine`. |
| RAM | 4 GB (Polars) · 8 GB (Spark) | Per executor; scale with your data. |
| Disk | — | Depends on your lakehouse layout. |

!!! warning "Windows timezones"
    On Windows, Python's `zoneinfo` needs `tzdata` to resolve IANA zones.
    `tzdata` is pulled in automatically via the `sys_platform == 'win32'` marker
    in `pyproject.toml`. If you vendor a custom wheel, install `tzdata` explicitly.

## Verify

```python
import datacoolie

print(datacoolie.__version__)

# Listing registered plugins proves the install wired up entry points.
print(sorted(datacoolie.engine_registry.names()))
print(sorted(datacoolie.platform_registry.names()))
```

Expected output (with `[all]`):

```text
0.1.0
['polars', 'spark']
['aws', 'databricks', 'fabric', 'local']
```

If one of your engines is missing, the extra for it is not installed — see
the table above.

## Next

→ [Quickstart · Polars](quickstart-polars.md)

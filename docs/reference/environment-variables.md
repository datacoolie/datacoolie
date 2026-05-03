---
title: Environment Variables Reference | DataCoolie
description: Reference for DataCoolie environment variables, runtime overrides, and when configuration should stay in metadata instead of shell state.
---

# Environment variables

DataCoolie does not define a fixed framework-wide environment-variable surface
such as `DATACOOLIE_*` settings. In the current codebase, direct environment
variable reads only happen in secret-resolution paths; most other
configuration flows through metadata, constructor arguments, or the host
runtime.

## Direct environment lookups in the library

| Pattern | Read by | When it is used |
|---|---|---|
| `{source}{key}` | `EnvResolver` | When `secrets_ref` uses an `env:<source>` resolver key. |
| `{source}{key}` | `LocalPlatform` | When `LocalPlatform` is the active native secret provider and `secrets_ref` uses an unprefixed source key. |

In both cases, `key` is the current value already stored in the relevant
`configure` field.

## `EnvResolver` (`env:<prefix>`)

`EnvResolver` reads from `os.environ` using:

```text
{prefix}{configure[field]}
```

Example:

```json
{
	"configure": {
		"password": "DB_PASSWORD"
	},
	"secrets_ref": {
		"env:APP_": ["password"]
	}
}
```

This resolves `configure["password"]` from `os.environ["APP_DB_PASSWORD"]`.

If you want no prefix, use `env:`:

```json
{
	"configure": {
		"password": "DB_PASSWORD"
	},
	"secrets_ref": {
		"env:": ["password"]
	}
}
```

This resolves from `os.environ["DB_PASSWORD"]`.

## `LocalPlatform` native provider

`LocalPlatform` is itself a native secret provider backed by `os.environ`.
Without a resolver prefix, it also reads:

```text
{source}{configure[field]}
```

Example:

```json
{
	"configure": {
		"password": "DB_PASSWORD"
	},
	"secrets_ref": {
		"APP_": ["password"]
	}
}
```

With `LocalPlatform`, this resolves `configure["password"]` from
`os.environ["APP_DB_PASSWORD"]`.

## Not a DataCoolie environment-variable contract

The following may still matter in some deployments, but they are not read by
DataCoolie itself as named environment-variable settings:

- AWS SDK variables such as `AWS_REGION`, `AWS_DEFAULT_REGION`, `AWS_PROFILE`,
	`AWS_ACCESS_KEY_ID`, and related credentials are handled by `boto3`'s own
	provider chain, not by a DataCoolie-specific lookup.
- Spark variables such as `SPARK_HOME` and `PYSPARK_PYTHON` are handled by the
	Spark runtime you launch or the `SparkSession` you pass in.
- Fabric and Databricks use `notebookutils` and `dbutils` in their notebook
	runtimes rather than a DataCoolie environment-variable layer.
- `TZ` is not consulted by the current library code; DataCoolie's internal log
	and runtime timestamps are built with explicit UTC-aware datetimes.

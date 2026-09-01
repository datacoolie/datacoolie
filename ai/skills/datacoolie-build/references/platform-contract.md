# DataCoolie Platform Runtime Contract

## Scope

- Read when selecting a platform constructor, execution host, native/external
  backend, storage path, credential flow, or platform extra.
- Owns project-authoring decisions for the built-in Local, AWS, Fabric, and
  Databricks platform facades.
- Does not own engine storage configuration, metadata syntax, deployment, or
  platform implementation tuning. Route those concerns to the engine/public
  API, schema reference, release skill, or shipped DataCoolie documentation.

## Platform is not the execution host

The platform is DataCoolie's file and secret adapter. The execution host is
where the runner process starts and determines parameter transport. A Python
process on a laptop, in CI, or in an Azure Function can use `FabricPlatform`;
a local or CI process can use `DatabricksPlatform`.

Keep `config.yaml` environment-to-platform mapping unchanged. Fix execution
host, platform runtime mode, engine, provider variant, and operation in the
selected runner and deployment configuration. Use a runner provider suffix
when authentication, session, or backend bootstrap differs.

## Built-in selection matrix

| Platform | Execution context | Constructor | Storage and secrets | Platform dependency |
|---|---|---|---|---|
| Local | Python process | `LocalPlatform()` or `LocalPlatform(base_path=...)` | Local filesystem; environment variables | No platform extra |
| AWS | AWS or external Python | `AWSPlatform(...)` | boto3 for S3, Secrets Manager, Glue, and Athena | `datacoolie[aws]` |
| Fabric native | Fabric notebook/job | `FabricPlatform(runtime="fabric")` | `notebookutils.fs` and Fabric credentials | No platform extra; Fabric supplies NotebookUtils and Spark |
| Fabric external | Laptop, function, CI, other Python | `FabricPlatform(runtime="external")` | Azure Data Lake and Key Vault SDKs | `datacoolie[fabric-external]` |
| Databricks native | Databricks notebook/job | `DatabricksPlatform(runtime="databricks")` | POSIX Volume content I/O plus `dbutils`; native secrets | No platform extra; Databricks supplies dbutils and Spark |
| Databricks external | Laptop, function, CI, other Python | `DatabricksPlatform(runtime="external")` | Databricks SDK Files/Secrets APIs | `datacoolie[databricks-external]` |

Compose the platform requirement with only the engine, source, metadata, and
format profiles that the host does not already provide.

`runtime="auto"` is suitable for user-authored or ad hoc code. A fixed
generated native or external runner uses the explicit mode so running it on
the wrong host fails closed instead of changing backend silently.

## Path contracts

- Local accepts normal local paths. With `base_path`, pass only relative paths;
  absolute paths and traversal or symlink escapes are rejected.
- AWS accepts `s3://bucket/key`. A plain key is valid only when the constructor
  supplies `bucket=`. `endpoint_url` enables S3-compatible storage such as
  MinIO or LocalStack but applies automatically only to the S3 client.
- Fabric external requires a qualified OneLake or ADLS Gen2 `abfs://`,
  `abfss://`, or HTTPS URI. It has no default workspace, lakehouse, item, or
  filesystem because the URI already identifies them. Relative paths remain a
  native NotebookUtils capability.
- Databricks portable storage uses
  `/Volumes/<catalog>/<schema>/<volume>/...`; `dbfs:/Volumes/...` is accepted
  and canonicalized. DBFS root and DBFS mounts are unsupported. Raw `s3://`,
  `abfss://`, and `gs://` paths are native-only and must not be generated for
  an external Databricks runner.

Pass path values unchanged to DataCoolie constructors and metadata. The
platform validates and interprets them; the engine independently owns Spark
Hadoop configuration, Polars storage options, catalogs, and table-format I/O.

## DataCoolie control storage

Use one persistent, environment-isolated control namespace for deployed
metadata, logs, and watermarks. Prefer a dedicated local directory, AWS S3
bucket, Fabric Lakehouse, or Databricks governed Volume that is separate from
business data. A policy-approved shared resource still needs distinct
environment paths and access controls.

Within the namespace, Release stages metadata in its temporary release candidate and activates the
fixed target `metadata` component through the stable runner current identity. The target may use a
filesystem path or a native object identity; do not require a `build_id` folder. Keep logs and
watermarks in separate mutable locations. The durable workspace and build remain the authoring
source of truth. The cloud metadata copy is only the release projection consumed by the runner.

## Credentials

- Local secrets come from environment variables.
- AWS uses the standard boto3 credential chain, including environment,
  profiles, shared credentials, and workload or instance roles. Do not embed
  keys in runners. A named `profile=` is an explicit developer choice.
- Fabric native uses the current Fabric identity. Fabric external lazily
  creates `DefaultAzureCredential` when `azure_credential` is omitted. Inject a
  `TokenCredential` only when the application intentionally owns identity
  selection.
- Databricks native uses dbutils. Databricks external lazily constructs a
  `WorkspaceClient` with Databricks unified authentication when
  `workspace_client` is omitted. Inject a client only when the application
  already owns it.

Credential or SDK absence is environment setup, not evidence that the public
platform is unsupported. Never log credential values or raw provider errors
that may contain them.

## File and metadata operations

All four platforms implement the same `BasePlatform` public file and secret
interface. `list_files(...)` and `get_file_info(...)` return `FileInfo` metadata.

- Use `file_exists`, `folder_exists`, `get_file_info`, `list_files`, or
  `list_folders` when only existence or metadata is needed.
- `read_file` and `read_bytes` return the complete file. Never use them as a
  head/stat/existence probe.
- Listing order is unspecified. Sort only at a consumer boundary that requires
  deterministic order.
- Keep append single-writer. Do not invent concurrent append coordination in a
  generated runner.
- Use the public platform methods instead of runner-owned SDK traversal, path
  normalization, or cloud-specific fallbacks.

## Runner rules

Parameter transport follows the execution host, not the selected platform:
Python CLI/environment/event input for a normal Python process, widgets for a
Databricks notebook/job, a tagged parameter cell for Fabric, and job arguments
for AWS Glue. After decoding only host transport, pass paths and one stage
scalar unchanged through the runner contract.

Do not install packages or restart the runtime inside a runner. Native hosts
attach the base package before execution; external environments install the
matching platform profile together with only the needed engine/source/format
profiles.

When metadata uses a Python function, the approved execution host—not the
DataCoolie platform adapter—determines WHL/ZIP compatibility. Load
`references/python-functions-contract.md`; Build produces exactly one artifact
and renders its fixed import prefix. Provision verifies reusable readiness and
Release attaches that exact artifact before execution.

When an external cloud adapter runs on premises, the platform remains the cloud
adapter while the scheduler, container, VM, or host remains the release target.
Verify outbound connectivity, workload identity, engine destination access, and
control-storage access independently.

## Unresolved Questions

None. Inspect the installed package when a third-party platform plugin or a
version-specific capability differs from these built-ins.

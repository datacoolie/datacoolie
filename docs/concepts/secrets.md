---
title: Secret Management — DataCoolie Concepts
description: Learn how secret providers and secret resolvers work in DataCoolie, including secrets_ref mapping and platform-backed credential resolution.
---

# Secrets

**TL;DR** DataCoolie has **two** secret interfaces: `BaseSecretProvider`
(the active platform's native backend) and `BaseSecretResolver` (selected by a
prefix on a `secrets_ref` source key). Values in `Connection.configure` name
the secret keys to fetch.

## Provider vs resolver

```mermaid
flowchart LR
    A[Connection.secrets_ref source] --> B{Known prefix before colon?}
    B -->|env:…| C[EnvResolver]
    B -->|custom:…| D[Custom resolver]
    B -->|no known prefix| E[NativeProviderResolver]
    C --> F[os.environ]
    D --> G[(Resolver-specific backend)]
    E --> H[BaseSecretProvider.get_secret]
    H --> I[(Fabric Key Vault / AWS SM / dbutils.secrets / env)]
```

- **Provider** (`BaseSecretProvider`) = **where** secrets live.
  Each platform is a native provider by subclassing `BasePlatform`, so every
  platform brings its own secret backend:
  Local uses `os.environ`, Fabric uses Azure Key Vault through
  `notebookutils.credentials`, Databricks uses `dbutils.secrets`, and AWS uses
  AWS Secrets Manager.
- **Resolver** (`BaseSecretResolver`) = **how** to resolve a key when the
  `secrets_ref` source begins with a registered prefix. Built-in `EnvResolver`
  handles sources such as `env:APP_`; you can add more.

An unrecognised or unprefixed source falls back to the active native provider.
A custom resolver owns its own backend access because the resolver contract is
only `resolve(key, source)`. See
[ADR-0002](../adr/0002-secret-provider-resolver-split.md).

## `secrets_ref` schema

`Connection.secrets_ref` maps each secret source to the `configure` fields that
should be resolved from that source. Each listed field must already exist in
`configure`, and its current value must be the vault key or secret name to look
up:

```json
{
  "configure": {
    "host": "db.internal",
    "port": 5432,
    "username": "db-user-secret",
    "password": "db-password-secret"
  },
  "secrets_ref": {
    "https://myvault.vault.azure.net/": ["password"],
    "env:": ["username"]
  }
}
```

At resolve time DataCoolie:

1. For each `source`, for each `field`: fetch the secret value from the
  provider and **replace** `configure[field]` with the resolved value.
2. Calls `connection.refresh_from_configure()` so first-class attributes
   (`database`, `catalog`) pick up resolved values.

If a field is listed in `secrets_ref` but missing from `configure`, DataCoolie
raises an error instead of guessing where the secret should be written.

Constraint: **a `field` must appear under exactly one `source`**. Listing the
same field under two sources is ambiguous and raises `ConfigurationError`.

## Built-in resolvers

Only one: `EnvResolver` for `env:*` lookups. Register more via the
`datacoolie.resolvers` entry-point group.

## `SecretStr` — Opaque secret wrapper

Resolved secret values are wrapped in `SecretStr`, an opaque object that
**prevents accidental exposure** through `str()`, `repr()`, `print()`,
f-strings, and tracebacks.  All public representations render `***`.

There is no extraction method on `SecretStr`. Framework code and extension
authors use two module helpers at I/O boundaries:

| Helper | Purpose |
|--------|---------|
| `unwrap_secret(value)` | Extract the raw `str` from a `SecretStr` (identity for plain strings) |
| `unwrap_configure(configure)` | Shallow-copy a configure dict, unwrapping top-level `SecretStr` values |

This replaces the earlier `SensitiveValueFilter` log filter approach.  Instead
of scrubbing secrets from log messages after the fact, the framework now
ensures secrets **never reach** log formatters in the first place.

!!! warning "Extension authors"
    If your plugin receives a `Connection.configure` dict, call
    `unwrap_configure(configure)` before passing values to external clients
    (HTTP auth, JDBC connection strings, etc.).  The wrapped values will not
    work as raw strings.

## Built-in providers

All four platforms. `AWSPlatform._fetch_secret` goes to AWS Secrets Manager;
`FabricPlatform` uses `notebookutils.credentials`; `DatabricksPlatform` uses
`dbutils.secrets`; `LocalPlatform` reads `os.environ`.

## Related

- [ADR-0002 · Secret provider / resolver split](../adr/0002-secret-provider-resolver-split.md)
- [Writing a secret resolver](../extending/writing-a-secret-resolver.md)

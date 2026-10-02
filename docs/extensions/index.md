---
title: Extend DataCoolie
description: Add custom DataCoolie sources, destinations, transformers, engines, platforms, metadata providers and secret resolvers.
---

# Extend DataCoolie

Use the smallest extension boundary that owns the behavior. A source reads
data, a destination writes it, a transformer changes the current DataFrame, an
engine supplies DataFrame operations, a platform owns path/secret I/O, and a
metadata provider hydrates metadata. Secret provider and resolver contracts
are separate.

Start with [run your first transformer plugin](transformer-tutorial.md) for a
complete local installation, activation and output check. To solve an existing
pipeline task, first check [metadata patterns](../guide/metadata/index.md),
including built-in transforms and [Python function sources](../guide/metadata/source-patterns.md#python-function-source).

## Choose the extension point

| Need | Guide |
|---|---|
| Read a new format or protocol | [Write a source](writing-a-source.md) |
| Write a new format or target | [Write a destination](writing-a-destination.md) |
| Transform the current DataFrame | [Write a transformer](writing-a-transformer.md) |
| Add a DataFrame library | [Write an engine](writing-an-engine.md) |
| Add filesystem, path or platform secret I/O | [Write a platform](writing-a-platform.md) |
| Serve metadata from a new backend | [Write a metadata provider](writing-a-metadata-provider.md) |
| Resolve a new `secrets_ref` prefix | [Write a secret resolver](writing-a-secret-resolver.md) |

Subclass the public base, implement its abstract methods and preserve its
call signatures. Register only where the contract defines an entry point, and
test the backend capabilities you advertise. Metadata providers are injected
as instances. Project-owned runners and Python functions have their own
packaging/import setup.

## Registration and activation

| Extension | Discovery | How it is used |
|---|---|---|
| Source / destination | `datacoolie.sources` / `datacoolie.destinations` | Driver factories select by connection `format`; custom names must also satisfy the metadata route's model/schema rules |
| Transformer | `datacoolie.transformers` | Runner adds the resolved instance to its transformer pipeline |
| Engine | `datacoolie.engines` | Runner calls `create_engine` and supplies the instance to the Driver |
| Platform | `datacoolie.platforms` | Runner calls `create_platform` and shares the instance with engine and Driver |
| Secret resolver | `datacoolie.resolvers` | Driver's default secret provider selects a resolver from a `secrets_ref` prefix |
| Metadata provider | Constructor injection | Runner passes a configured provider instance to the Driver |

Entry points advertise installed Python classes; discovery does not install
packages or prove backend support. A new format alias does not extend the
metadata schema or an engine's file-format implementation. Source and
destination guides explain the current boundaries. For exact group names and
exports, see the [entry-point reference](../reference/plugin-entry-points.md).

## Troubleshoot discovery and activation

Work through these checks in the Python environment that runs your runner:

| Failure | Check and next action |
|---|---|
| Alias absent | Confirm the distribution is installed and its `pyproject.toml` declares the correct group/name; reinstall after changing package metadata |
| Alias resolves to an unexpected class | Choose a unique alias: discovery skips names already registered, including built-ins such as `csv` and `delta` |
| Import fails | Import the declared `module:Class` directly; check packaged modules and optional backend dependencies |
| Registry construction fails | Preserve the base contract and constructor keywords passed by the caller; inspect the wrapped exception and its cause |
| Discovery succeeds but behavior is absent | Check the activation route above, runner selection and plugin configuration; `list_plugins()` / `is_available()` do not construct or execute the plugin |
| Correct plugin runs but output differs | Check advertised format/engine capabilities, transformer ordering, metadata and output assertions |

Registries discover once per process. After installing or updating a package,
start a fresh Python process or restart the notebook session before checking
again. Secret resolvers are cached singleton instances; avoid putting
per-run state in their constructors. Package installation belongs in environment
setup before runner startup.

For advanced in-process integration, explicit `registry.register(name, cls)`
replaces an existing registration, logs a warning and invalidates its singleton
cache. Use this deliberately; an installed entry point with the same name does
not override a registered built-in. See the [registry API](../reference/api/core.md).

Read the [Reference](../reference/index.md) for exact contracts and
the [project testing strategy](../project/testing.md) before publishing a
plugin.

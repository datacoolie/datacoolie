---
title: ADR-0002 — Secret Provider and Resolver Split | DataCoolie
description: Why DataCoolie separates secret storage access from secret value resolution so platforms and credentials stay decoupled.
---

# ADR-0002 — Split secret **provider** from secret **resolver**

**Status** · Accepted

## Context

Original design had a single `SecretResolver` that both parsed placeholder
strings and fetched values. Adding a new placeholder syntax (e.g.
`vault:…`) required reimplementing the fetch logic even when the underlying
backend was the same as an existing one.

Conversely, swapping backends (local env → Key Vault → Secrets Manager)
required touching the placeholder parser.

## Decision

Two responsibilities, two abstractions:

- **`BaseSecretProvider`** — *fetches* a secret from a backend
  (`fetch_secret(source, field) -> str`). Implemented by each platform
  (Fabric, Databricks, AWS, Local).
- **`BaseSecretResolver`** — *parses* a placeholder string and decides which
  provider call to make (`matches(value) -> bool`, `resolve(value,
  provider) -> str`).

Resolvers discovered via the `datacoolie.resolvers` entry-point group.
Providers are chosen by the active platform (no plugin lookup).

## Consequences

- Adding a new placeholder syntax = one class.
- Adding a new backend = one class.
- `EnvResolver` is the special case: resolves `env:FOO` from `os.environ`
  directly without hitting a provider.
- Users with legacy `SecretResolver` subclasses must migrate — we
  deliberately broke the API.

## Related

- [Extending · Write a secret resolver](../extending/writing-a-secret-resolver.md)

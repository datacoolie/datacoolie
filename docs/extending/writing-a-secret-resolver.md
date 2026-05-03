---
title: Write a Secret Resolver Plugin — DataCoolie
description: Build a custom DataCoolie secret resolver that maps metadata keys to runtime credentials without hardcoding secrets in configs.
---

# Write a secret resolver

**Prerequisites** · You have a placeholder syntax you want DataCoolie to recognise (e.g. `vault:path/to/secret`).
**End state** · Resolver registered, used automatically during `resolve_secrets` before dataflows execute.

## Minimal resolver

```python
from datacoolie.core.secret_resolver import BaseSecretResolver


class VaultResolver(BaseSecretResolver):
    PREFIX = "vault:"

    def matches(self, value: str) -> bool:
        return isinstance(value, str) and value.startswith(self.PREFIX)

    def resolve(self, value: str, provider) -> str:
        key = value[len(self.PREFIX):]
        # Any concrete BaseSecretProvider subclass works here, including the platform.
        return provider.fetch_secret(source="vault", field=key)
```

## Register

```toml
[project.entry-points."datacoolie.resolvers"]
vault = "mypkg.resolvers:VaultResolver"
```

## Usage from metadata

```json
{
  "configure": {"password": "vault:prod/db/customer"}
}
```

At runtime `resolve_secrets` scans every string in `configure`, finds the
resolver whose `matches()` returns `True`, and replaces the value with the
resolver's `resolve()` output. Fall-through values are untouched.

## Provider vs resolver split

- **Resolver** — *parses* the placeholder string (e.g. `vault:prod/db`).
- **Provider** — *fetches* the actual secret from a backend.

The built-in `EnvResolver` is the exception that handles both ends
(`env:FOO` is resolved from `os.environ["FOO"]` by the resolver itself).
For anything more elaborate, have the resolver call into a
`BaseSecretProvider`.

See [ADR-0002](../adr/0002-secret-provider-resolver-split.md).

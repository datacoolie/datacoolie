---
title: Write a Secret Resolver Plugin — DataCoolie
description: Build a custom DataCoolie secret resolver that maps metadata keys to runtime credentials without hardcoding secrets in configs.
---

# Write a secret resolver

**Prerequisites** · You want a `secrets_ref` source prefix to use a custom backend (for example `vault:prod/team`).
**End state** · Resolver registered, used automatically during `resolve_secrets` before dataflows execute.

## Minimal resolver

```python
from datacoolie.core.secret_resolver import BaseSecretResolver


class VaultResolver(BaseSecretResolver):
    def __init__(self):
        self._client = build_vault_client()

    def resolve(self, key: str, source: str) -> str:
        # key: current value of the listed configure field
        # source: text after "vault:" in the secrets_ref source
        return self._client.read(path=source, field=key)
```

## Register

```toml
[project.entry-points."datacoolie.resolvers"]
vault = "mypkg.resolvers:VaultResolver"
```

## Usage from metadata

```json
{
  "configure": {"password": "db_password"},
  "secrets_ref": {
    "vault:prod/db/customer": ["password"]
  }
}
```

At runtime the driver parses `vault:prod/db/customer`, resolves the `vault`
plugin as a cached singleton, and calls
`resolve(key="db_password", source="prod/db/customer")`. The returned value
replaces `configure["password"]` as a `SecretStr`. Resolver constructors are
called without arguments by the default driver lookup.

## Provider vs resolver split

- **Resolver** — selected by a registered prefix and fetches the secret through
  its own implementation.
- **Provider** — the platform-native fallback used when no registered prefix
  is selected.

The built-in `EnvResolver` is the exception that handles both ends
(`env:FOO` is resolved from `os.environ["FOO"]` by the resolver itself).
For anything more elaborate, initialise the required client inside the
resolver or configure it through process/runtime state.

See [ADR-0002](../adr/0002-secret-provider-resolver-split.md).

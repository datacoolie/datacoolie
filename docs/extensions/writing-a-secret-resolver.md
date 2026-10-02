---
title: Write a Secret Resolver Plugin — DataCoolie
description: Build a custom DataCoolie secret resolver that maps metadata keys to runtime credentials without hardcoding secrets in configs.
---

# Write a secret resolver

**Prerequisites** · You want a `secrets_ref` source prefix to use a custom
backend, such as `vault:prod/team`.
**End state** · A resolver registered under `datacoolie.resolvers`, discovered
by the Driver, and tested against the key/source contract.

Resolvers and platform secret providers are separate extension points. A
resolver is selected by a recognized prefix in a `secrets_ref` source; a
platform's `BaseSecretProvider` remains the native fallback when no registered
prefix is selected. See [Secret management](../reference/concepts/secrets.md)
for the runtime split.

## Implement the contract

Every resolver implements one method:

```python
from datacoolie.core.secrets.resolver import BaseSecretResolver


class MyResolver(BaseSecretResolver):
    def resolve(self, key: str, source: str) -> str:
        """Return one secret value for a key and backend-specific source."""
        ...
```

`key` is the current value of the listed `Connection.configure` field. It is
the secret name or field name to fetch. `source` is the text after the resolver
prefix and colon. A source without a recognized prefix is passed to the native
provider as the full source string instead. Return a plain `str`; the runtime
wraps it as `SecretStr` before placing it back into `configure`.

## Illustrative Vault skeleton

This skeleton shows the boundary only. `build_vault_client` and `read` are
placeholders for the SDK and authentication policy owned by your package; they
are not DataCoolie APIs:

```python
from datacoolie.core.secrets.resolver import BaseSecretResolver


class VaultResolver(BaseSecretResolver):
    def __init__(self):
        # Illustrative: construct or lazily prepare your backend client.
        self._client = build_vault_client()

    def resolve(self, key: str, source: str) -> str:
        # source is the text after "vault:".
        return self._client.read(path=source, field=key)
```

The default Driver lookup constructs a registered resolver without arguments
and caches one instance for the resolver name. Keep construction compatible
with a no-argument call, or arrange client configuration through your package's
process/runtime configuration. `resolve` may be called repeatedly for fields
and connections, so the resolver's client and any cache must be safe for the
concurrency your application uses. There is no framework `close()` hook for a
resolver; lifecycle management for a client remains inside the package or its
host process.

## Register the prefix

Declare the resolver in the package that contains it:

```toml
[project.entry-points."datacoolie.resolvers"]
vault = "mypkg.resolvers:VaultResolver"
```

Install that package in the same Python environment as the DataCoolie Driver.
The resolver registry discovers entry points lazily. You can inspect the
runtime registry while diagnosing an installation:

```python
from datacoolie import resolver_registry

print(resolver_registry.list_plugins())
```

The entry-point name (`vault`) is the prefix used before the first colon; it is
not the Python class name.

## Use it from metadata

The listed field must already exist in `configure`, and its current value is
the key/name supplied to the resolver:

```json
{
  "configure": {"password": "db_password"},
  "secrets_ref": {
    "vault:prod/db/customer": ["password"]
  }
}
```

At runtime DataCoolie splits the source at the first colon, looks up `vault`,
and calls:

```python
resolver.resolve(key="db_password", source="prod/db/customer")
```

The returned value replaces `configure["password"]` as a `SecretStr`, and the
connection refreshes its first-class fields from the resolved configuration.
Each configure field must appear under exactly one `secrets_ref` source. A
listed field that is missing from `configure` is an error; DataCoolie does not
guess where to write the result.

## Environment resolver semantics

The built-in `EnvResolver` also follows the same two-argument contract. It
looks up the concatenation `{source}{key}` in `os.environ`:

```json
{
  "configure": {"password": "DB_PASSWORD"},
  "secrets_ref": {"env:APP_": ["password"]}
}
```

This resolves `os.environ["APP_DB_PASSWORD"]`. With `env:` and an empty
source, the key alone is used. The source is a prefix, not the complete
environment variable name, and the configure value remains the key passed to
the resolver.

## Provider versus resolver

- **Resolver** — selected by a registered prefix and fetches the secret through
  its own backend. It receives only `key` and `source`.
- **Provider** — the active platform-native fallback used when the source has
  no recognized prefix. `BasePlatform` implements this provider boundary with
  `_fetch_secret(key, source)` and inherited caching.

If the secret already belongs to the active platform, omit the custom prefix
and let the native provider handle the full `secrets_ref` source. A resolver
does not automatically receive a platform or a `BaseSecretProvider`; inject
or construct the backend client according to your package's own boundary.

Extension code that passes `Connection.configure` to an external SDK must call
`unwrap_configure(configure)` first. Resolved values are opaque `SecretStr`
instances so logging and string conversion do not expose them.

See [ADR-0002](../project/decisions/0002-secret-provider-resolver-split.md)
for the provider/resolver design decision.

## Testing

Keep the default tests local and deterministic. Cover:

- `resolve(key, source)` with the exact key and source expected by your SDK;
- source arguments containing a colon suffix and backend-specific empty source
  behavior;
- repeated calls and concurrent calls when the client is shared;
- missing or denied secrets surfaced as a useful exception;
- `parse_source` behavior for a registered prefix, an unrecognized prefix,
  and an unprefixed source;
- installed entry-point discovery and no-argument singleton construction;
- a connection with multiple fields, plus missing or duplicate
  `secrets_ref` fields.

Use opt-in live tests for vault or cloud credentials. Do not put credentials,
network access, or a real secret value in the default unit suite.

## Troubleshooting

- **The custom resolver is never called** — verify the installed package's
  `datacoolie.resolvers` entry point and exact prefix. An unknown prefix falls
  back to the active native provider and passes the full source string there.
- **The environment variable name is wrong** — `EnvResolver` concatenates
  `source` and `key`; `env:APP_` plus `DB_PASSWORD` means
  `APP_DB_PASSWORD`.
- **The resolver sees an unexpected source** — only the text after the first
  colon is passed to a recognized resolver. Unprefixed or unrecognized sources
  remain unchanged for native-provider lookup.
- **Resolver construction fails during Driver setup** — default Driver lookup
  calls the resolver constructor with no arguments; move required settings into the
  package/runtime configuration or provide a resolver class with a no-argument
  constructor.
- **A backend client rejects a secret value** — unwrap `SecretStr` with
  `unwrap_configure` at the external-client boundary.
- **A listed field cannot be resolved** — add that field to `configure` with
  its backend key/name and list it under exactly one source.

## Related

- [Secret management concepts](../reference/concepts/secrets.md)
- [Secret resolver API](../reference/api/core.md)
- [Secret provider / resolver split](../project/decisions/0002-secret-provider-resolver-split.md)
- [Platform extension guide](writing-a-platform.md)

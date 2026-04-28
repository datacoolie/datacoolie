# Extending DataCoolie

DataCoolie is built for extension. Six plugin points cover the full pipeline:

| What you want to add | Base class | Entry-point group | Guide |
|---|---|---|---|
| Read a new format/protocol | `BaseSourceReader[DF]` | `datacoolie.sources` | [Write a source](writing-a-source.md) |
| Write a new format/target | `BaseDestinationWriter[DF]` | `datacoolie.destinations` | [Write a destination](writing-a-destination.md) |
| Transform rows before write | `BaseTransformer[DF]` | `datacoolie.transformers` | [Write a transformer](writing-a-transformer.md) |
| Add a DataFrame library | `BaseEngine[DF]` | `datacoolie.engines` | [Write an engine](writing-an-engine.md) |
| Resolve a new placeholder syntax | `BaseSecretResolver` | `datacoolie.resolvers` | [Write a secret resolver](writing-a-secret-resolver.md) |
| Serve metadata from a new backend | `BaseMetadataProvider` | *(constructor-injected — no entry point)* | [Write a metadata provider](writing-a-metadata-provider.md) |

## The contract pattern

Every guide follows the same pattern:

1. **Subclass** the base class.
2. **Implement** its abstract methods.
3. **Register** via entry points in your own package's `pyproject.toml`.
4. **Test** against the engine matrix you support.

Your package doesn't need to import the DataCoolie plugin registry — entry
points are discovered automatically.

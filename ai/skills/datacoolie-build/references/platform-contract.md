# DataCoolie Platform Runtime Contract

## Scope

- Read when selecting a platform constructor, execution host, native/external backend, storage
  path, credential flow, or platform extra.
- This reference owns host/platform decisions, capability gates, and credential safety.
- The public [platform concepts](https://datacoolie.github.io/datacoolie/reference/concepts/platforms/),
  [runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/),
  [provider guide](https://datacoolie.github.io/datacoolie/guide/providers/), and selected
  [platform guide](https://datacoolie.github.io/datacoolie/guide/platforms/) own constructors,
  paths, credentials, dependencies, and host setup. Do not copy those contracts here.

For the shared project/build contract, read the public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/).
The [Local](https://datacoolie.github.io/datacoolie/examples/runners/),
[AWS Glue](https://datacoolie.github.io/datacoolie/guide/platforms/aws-glue/),
[Fabric](https://datacoolie.github.io/datacoolie/guide/platforms/fabric/), and
[Databricks](https://datacoolie.github.io/datacoolie/guide/platforms/databricks/)
recipes supply the compatible runner and host-specific verification boundary.

## Platform and execution host

The selected platform is DataCoolie's file/secret adapter; the execution host is where the runner
starts and determines parameter transport. Do not infer one from the other. Keep the project's
environment-to-platform mapping unchanged and fix host, runtime mode, engine, provider variant, and
operation in the selected project runner and deployment configuration.

Use an explicit host/platform mode when a generated runner must fail closed on the wrong host.
Use a provider suffix only when authentication, session, or backend bootstrap differs. Read the
selected public platform guide for the exact constructor and path form.

## Capability and setup gates

1. Read the matching public platform/provider guide for the selected host and storage form.
2. Run `dc inspect capabilities --format json` and record installed capability evidence.
3. Check only the optional dependencies required by that combination.
4. Inspect installed public constructors/signatures and run a representative non-production access
   check when the requested outcome includes runtime use.

A missing SDK or optional dependency is environment setup, not proof that a public platform is
unsupported. Registry presence does not prove host mode, authentication, addressing, engine I/O,
or target access. Do not install packages or restart a runtime inside a runner.

## Path and credential ownership

Pass caller-owned metadata, SQL, artifact, state, watermark, and log roots to the provider/Driver
according to the public [runtime path ownership guide](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/).
The provider and selected platform interpret path forms; the runner must not normalize them with
cloud-specific SDK traversal or invent fallbacks. Keep control storage environment-isolated and
separate from business data where the project requires it.

Resolve credentials through the selected platform/provider or operator identity. Never embed or log
credential values, raw provider responses, or scheduler secrets. When an external cloud adapter runs
on premises, verify outbound connectivity, workload identity, engine destination access, and
control-storage access independently.

## Runner boundary

Parameter transport follows the execution host, not the selected platform. Decode only the host
transport, then pass paths and the selected stage through the runner contract. Load
`references/python-functions-contract.md` when metadata uses functions; packaging and attachment
remain build and execution-host concerns.

Use installed public APIs for file and secret operations. Do not install a platform profile inside a
runner, mutate target resources from a Build runner, or turn discovery output into a runtime import.

## Outcome-scoped verification

Apply [Build's outcome checks](../SKILL.md#build-and-local-verification). For requested runtime use, verify the selected host/platform mode, addressing, identity and engine/control-storage access independently; report host-only gaps.

# DataCoolie Framework Boundary

## Scope

- Read when deciding whether a requested pipeline combination stays native or needs a custom edge.
- Owns the capability-evidence order, framework-first decision, and unsupported-boundary rules.
- Does not own built-in inventory, metadata fields, runner parameters, replay/maintenance semantics,
  or verification-receipt fields. Route those to the public technical/reference pages, runner
  contracts, or the installed CLI validation commands named in `SKILL.md`.

## Capability decision

Evaluate the complete combination rather than individual component names:

```text
source + authentication + engine + transforms + destination + load + platform + dependencies
```

Read the relevant version-compatible public documentation first. Then use evidence in this order:

1. Run `dc inspect capabilities --format json` for the installed package and all six registries.
2. Check optional dependencies required by the selected registered capability.
3. Inspect public constructors and signatures for the selected implementations.
4. Run a targeted compatibility test with representative non-production data.
5. Reconcile any version-specific documentation claim with the installed evidence and record the
   effective version when they differ.

A missing optional dependency is setup work, not proof that a registered capability is unsupported.
Install the matching framework extra or runtime dependency, then test the combination. Registry
presence alone does not prove authentication, addressing, session, catalog, storage-option, or
engine compatibility.

For Local, AWS, Fabric, or Databricks, load `references/platform-contract.md` before choosing the
runtime mode, credential flow, path form, or platform extra. A registered Fabric or Databricks
facade does not prove that the current process is its native notebook runtime or that its external
SDK profile is installed.

Use `dc inspect capabilities --format json` whenever an inventory is needed. The installed runtime
and discovered plugins are authoritative; registry presence alone does not prove a complete
engine/platform/dependency combination.

## Framework-first implementation

Start with the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/)
and route authoring questions to its [connections](https://datacoolie.github.io/datacoolie/guide/metadata/connections/),
[dataflows](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/),
[source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/),
[transform patterns](https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/),
and [destination/load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/)
pages before applying this boundary checklist.

For a supported combination:

1. Express connections, dataflows, transforms, and load strategy in canonical metadata.
2. Validate the resolved environment artifact with `dc validate`; use the
   framework's published metadata schema index at
   `https://datacoolie.github.io/datacoolie/schema/index.json` as contract
   guidance, or the stable current-authoring alias at
   `https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json`.
   The alias is not a second Skill-owned schema or a network fallback; the
   installed CLI still validates against its local framework-compatible schema.
3. Construct the selected provider, platform, and engine through installed public APIs.
4. Execute through the matching DataCoolie driver operation.

Do not replace supported reads, writes, orchestration, logging, watermarking, slicing, retry, replay,
or maintenance behavior with bespoke code merely because it appears shorter. Exact metadata syntax
belongs to the public metadata reference and selected schema version;
`references/schema-quick-reference.md` is only an agent authoring checklist. Entrypoint behavior
belongs to `references/runner-contract.md` and its operational extension.

## Source expression order

Choose the least expressive native source form that preserves the required behavior:

1. Address the source object directly with a table/object/path or API endpoint when extracting that
   object as a whole. Do not replace a supported direct address with an equivalent `SELECT *`.
2. Use one bounded source query when source-side relational work is required, such as joins,
   projections, filters, aggregations, or set-based shaping that materially defines the extract.
3. Use a metadata-addressed Python function only when direct addressing and a bounded query cannot
   express verified multi-step or non-relational behavior.

Record evidence before moving down the order. Keep direct-address and query-capable parts native
even when one narrow custom function remains necessary. This reference owns the selection rule;
field syntax and examples remain in the public source-patterns guide and selected schema.

For a Delta or Iceberg query executed by Polars, relation discovery is native engine bootstrap, not
a Python-function fallback. Keep the query in `source.query` and load
`references/polars-qualified-sql.md` for the same-process registration contract.

## Unsupported boundary

Before adding custom code, record:

- Installed DataCoolie version, dependencies, and registry evidence.
- Exact unsupported dimension and reproducible result.
- Why metadata, configuration, dependency setup, or an installed plugin cannot solve it.
- Smallest adapter interface and a condition for removing it.

Keep every supported dimension native. An authentication gap may need only a credential/session
adapter; a transform gap may need one metadata-addressed function; a destination gap may need one
destination plugin while DataCoolie continues to own source and orchestration.

Discovery evidence can inform this decision but cannot become a runtime import. Do not generalize a
project's selected technologies into defaults for other projects.

## Proof and handoff

Fast source checks do not prove the selected combination. Build all configured environments, then
validate the exact generated artifacts for the requested verification slice. Execute its runner
when the Build host is compatible and the check is safe; follow Build's qualification rules to
distinguish artifact evidence from runtime evidence. A failed
compatibility test either returns to setup, narrows the custom boundary with evidence, or returns a
material change to design; it does not silently switch the whole pipeline to bespoke I/O.

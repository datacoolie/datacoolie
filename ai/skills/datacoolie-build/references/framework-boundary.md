# DataCoolie Framework Boundary

## Scope

- Read when deciding whether a requested pipeline combination stays native or needs a custom edge.
- This reference owns capability evidence, the framework-first decision, the source-expression
  order, and unsupported-boundary gates.
- Public metadata, CLI, platform, provider, runner, and operation pages own fields, defaults,
  paths, and framework behavior. Use those pages before relying on an agent reference.

## Capability decision

Evaluate the complete combination rather than individual component names:

```text
source + authentication + engine + transforms + destination + load + platform + dependencies
```

Read the version-compatible [metadata guide](https://datacoolie.github.io/datacoolie/guide/metadata/),
[platform concepts](https://datacoolie.github.io/datacoolie/reference/concepts/platforms/),
and the relevant [source](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/)
and [destination/load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/)
first. Then use evidence in this order:

1. Run `dc inspect capabilities --format json` for the installed package and discovered registries.
2. Check optional dependencies required by the selected registered capability.
3. Inspect public constructors and signatures for the selected implementations.
4. Run a targeted compatibility test with representative non-production data.
5. Reconcile any version-specific documentation claim with installed evidence and record the
   effective version when they differ.

A missing optional dependency is setup work, not proof that a registered capability is unsupported.
Registry presence alone does not prove authentication, addressing, session, catalog,
storage-option, or engine compatibility. For Local, AWS, Fabric, or Databricks, read
`references/platform-contract.md` before selecting a host/runtime mode or platform extra.

Use `dc inspect capabilities --format json` whenever an inventory is needed. The installed runtime
and discovered plugins are authoritative; do not turn discovery output into a runtime import.

## Framework-first implementation

Start with the public [Metadata Guide](https://datacoolie.github.io/datacoolie/guide/metadata/)
and route authoring questions to its [connections](https://datacoolie.github.io/datacoolie/guide/metadata/connections/),
[dataflows](https://datacoolie.github.io/datacoolie/guide/metadata/dataflows/),
[source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/),
[transform patterns](https://datacoolie.github.io/datacoolie/guide/metadata/transform-patterns/),
and [destination/load patterns](https://datacoolie.github.io/datacoolie/guide/metadata/destination-and-load-patterns/)
pages. The [metadata schema](https://datacoolie.github.io/datacoolie/reference/metadata-schema/#metadata-document),
[schema index](https://datacoolie.github.io/datacoolie/schema/index.json), and stable
[latest schema alias](https://datacoolie.github.io/datacoolie/schema/latest/metadata.schema.json)
own exact field contracts; the installed CLI remains the offline validation authority.

For a supported combination:

1. Keep connections, dataflows, transforms, and load strategy in canonical metadata.
2. Validate the requested metadata or built artifact with `dc validate --format json`.
3. Construct the selected provider, platform, and engine through installed public APIs.
4. Use the matching project-owned runner and one selected DataCoolie operation.

Do not replace supported reads, writes, orchestration, logging, watermarking, slicing, retry,
replay, or maintenance with bespoke code merely because it appears shorter. The
[project/CLI workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/),
[runtime configuration](https://datacoolie.github.io/datacoolie/guide/operations/runtime-configuration/),
and `references/runner-contract.md` own the applicable project and runner routes.

For a custom source, follow the public [source extension contract](https://datacoolie.github.io/datacoolie/extensions/writing-a-source/).
The ETL boundary has one typed reader entry:
`read(source, *, request=SourceReadRequest(reference_time, ...)) -> SourceReadResult`.
Callers consume `.frame` and use the result's `.selection` and `.candidate` for
execution evidence. A reader must also expose
`merge_watermark(existing, candidate: WatermarkCandidate | None) -> dict` and
`validate_watermark`; do not omit those required operations or rely on a
generic orchestrator fallback. There is no separate source runtime accessor
or legacy frame/result reader alias. A supplied watermark manager must provide
`get_watermark`, `save_watermark`, and `serialize`. The source owns watermark
merge semantics, and serialization is checked before destination mutation. Use
the public docs as the contract source of truth; this reference only routes the
agent to it.

## Source expression order

Choose the least expressive native source form that preserves the required behavior:

1. Address the source object directly with a table/object/path or API endpoint when extracting that
   object as a whole. Do not replace a supported direct address with an equivalent `SELECT *`.
2. Use one bounded source query when source-side relational work is required, such as joins,
   projections, filters, aggregations, or set-based shaping that materially defines the extract.
3. Use a metadata-addressed Python function only when direct addressing and a bounded query cannot
   express verified multi-step or non-relational behavior.

Record evidence before moving down the order. Keep direct-address and query-capable parts native
even when one narrow custom function remains necessary. Field syntax and examples belong to the
public [source patterns guide](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/)
and selected schema. For Delta or Iceberg SQL through Polars, relation discovery is native engine
bootstrap, not a Python-function fallback; use `references/polars-qualified-sql.md`.

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

## Outcome-scoped proof and handoff

For author/build-only requests, validate the requested metadata or artifact and report the exact
files, CLI result, environments, and skipped workload execution. Do not claim runtime evidence.
For an explicitly requested safe runtime check, derive expected selection, output, and state from
the matching public recipe/project criteria; independently inspect those results and identify
legitimate empty or skipped outcomes. Report performed checks, skipped host/dependency checks,
and unresolved questions.

Fast source checks do not prove the selected combination. A failed compatibility test returns to
setup, narrows the custom boundary with evidence, or returns a material change to design; it does
not silently switch the whole pipeline to bespoke I/O.

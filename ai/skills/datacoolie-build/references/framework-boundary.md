# DataCoolie Framework Boundary

## Scope

- Read when deciding whether a requested pipeline combination stays native or needs a custom edge.
- Owns the capability-evidence order, framework-first decision, and unsupported-boundary rules.
- Does not own built-in inventory, metadata fields, runner parameters, replay/maintenance semantics,
  or verification-receipt fields. Route those to the catalog, schema reference, runner contracts,
  or build validator named in `SKILL.md`.

## Capability decision

Evaluate the complete combination rather than individual component names:

```text
source + authentication + engine + transforms + destination + load + platform + dependencies
```

Use evidence in this order:

1. Run `scripts/inspect_capabilities.py` for the installed package, requirements, entry points, and
   all six registries.
2. Check optional dependencies required by the selected registered capability.
3. Inspect public constructors and signatures for the selected implementations.
4. Run a targeted compatibility test with representative non-production data.
5. Consult version-matched DataCoolie documentation only when installed evidence is insufficient.

A missing optional dependency is setup work, not proof that a registered capability is unsupported.
Install the matching framework extra or runtime dependency, then test the combination. Registry
presence alone does not prove authentication, addressing, session, catalog, storage-option, or
engine compatibility.

Use `references/capability-catalog.md` only when a built-in inventory is needed. The installed
runtime remains authoritative when that versioned snapshot differs.

## Framework-first implementation

For a supported combination:

1. Express connections, dataflows, transforms, and load strategy in canonical metadata.
2. Validate the resolved environment artifact against the bundled schema.
3. Construct the selected provider, platform, and engine through installed public APIs.
4. Execute through the matching DataCoolie driver operation.

Do not replace supported reads, writes, orchestration, logging, watermarking, slicing, retry, replay,
or maintenance behavior with bespoke code merely because it appears shorter. Exact metadata syntax
belongs to `references/schema-quick-reference.md`; entrypoint behavior belongs to
`references/runner-contract.md` and its operational extension.

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
field syntax and examples remain in `references/schema-quick-reference.md`.

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

Fast source checks do not prove the selected combination. Materialize the environment slice, run
the exact generated artifacts, and validate the explicit build-verification receipt. A failed
compatibility test either returns to setup, narrows the custom boundary with evidence, or returns a
material change to design; it does not silently switch the whole pipeline to bespoke I/O.

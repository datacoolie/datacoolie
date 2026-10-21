# Python functions adaptation checklist

Read the public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
for the functions-root and packaging contract, the public [function project recipe](https://datacoolie.github.io/datacoolie/examples/dataflows/#function-project-recipe)
for a complete adaptation, and the public [source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/)
when authoring `source.python_function`. This reference keeps only the agent decisions and evidence
needed to adapt a project runner.

## Build decision

- Treat functions as project build inputs. Use the project's configured functions roots and preserve
  each authored relative layout and import prefix; do not turn `functions` into a fixed runtime path.
- Packaging is decided by the installed CLI per configured root. Run
  `dc build --dry-run --format json` and record the returned root inventory and packaging decisions
  before relying on a package. Do not infer packaging from a filename or copy the CLI's rule here.
- Assemble and validate the exact artifact after the preview. The build manifest is artifact
  evidence; the runtime Driver does not use it to install or import functions.

## Runtime and host boundary

When the public runner contract uses `allowed_function_prefixes`, render it from the selected
project/build and follow the recipe's documented no-function path otherwise. Keep import prefixes
fixed in the project-owned runner. The execution host owns wheel/ZIP/source attachment, installation,
and import compatibility; Build does not install a package or replace that host check with another
Skill validator.

When adapting the public function recipe, preserve only its existing project-owned bootstrap and
fixed import prefixes. Do not add runtime-selected module roots, arbitrary search paths, package
installation, or runner-owned packaging.

## Outcome-scoped verification

Apply [Build's outcome checks](../SKILL.md#build-and-local-verification). Inspect each configured root's CLI packaging evidence and, when runtime use is requested, verify attachment/import on the selected host. Retain unavailable host-package/import checks as gaps.

## Unresolved questions

None. Resolve project-specific import and host constraints from the selected environment runner,
public recipe, and installed packaging evidence.

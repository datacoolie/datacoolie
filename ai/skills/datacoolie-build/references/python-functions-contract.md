# Python functions adaptation checklist

Read the public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
for the complete functions-root and packaging contract. Read the public
[source patterns](https://datacoolie.github.io/datacoolie/guide/metadata/source-patterns/)
when authoring `source.python_function`. This reference only keeps the agent
decisions and evidence needed to adapt a project runner.

Functions are build inputs, not Driver configuration. A project may configure
one or more functions roots; each root uses the singular `path` and is packaged
independently. Keep the authored relative layout and import prefix. Do not put
packaging choices in metadata or make `functions` a fixed runtime path.

```yaml
components:
  functions:
    - path: functions/loaders
      packaging: auto
    - path: functions/quality
      packaging: copy
```

For `auto`, confirm the CLI dry-run result before relying on a package:

1. valid Python build backend in the root → `wheel`;
2. root-level `__init__.py` → wrapped `zip`;
3. otherwise → source `copy`.

An `__init__.py` only below the configured root, such as
`loaders/__init__.py`, is a nested package signal and does not select ZIP for
the parent root. Configure the nested package as its own root or select an
explicit mode when that is the intended distribution boundary.

Use `dc build --dry-run --format json` and then validate the assembled artifact.
The environment manifest records each root and its resolved packaging. When
functions are present, `functions_artifact` is a list, including for one root.
The runtime Driver does not read this manifest and the execution host owns
attachment/import setup.

Render `allowed_function_prefixes` from the project/build that is being run;
pass `[]` when no function source is used. A project-owned import/signature test
may prove host compatibility, but the CLI does not install packages or replace
that host check with another Skill validator.

## Unresolved questions

None. Resolve project-specific import and host constraints from the selected
environment runner and installed packaging evidence.

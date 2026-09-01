# Python Function Build Contract

## Scope

Read only when resolved metadata uses `source.python_function` or an explicit prebuilt function
artifact is supplied. This reference owns authoring layout, package selection enforcement,
inspection, isolated import/signature validation, and Build evidence. It does not justify custom
code, provision library facilities, attach packages, or define platform commands.

## One-artifact invariant

The approved architecture selects `none`, `wheel`, or `zip`. A build emits no artifact or exactly
one artifact in that format; it never emits both. Wheel is the default. ZIP is explicit and limited
to a compatible host and simple pure-Python project code whose external imports are already in the
approved runtime. Release does not convert formats.

Use one project-specific top-level import package. Do not generate the generic package name
`functions` and do not accept the import prefix as a runner parameter.

## Authoring layouts

Wheel:

```text
functions/
  pyproject.toml
  src/
    project_package/
      __init__.py
      sources.py
```

ZIP:

```text
functions/
  project_package/
    __init__.py
    sources.py
```

`pyproject.toml` selects wheel; one importable package without it selects ZIP. Do not put this
packaging choice in runtime `config.yaml`. Wheel builds must produce exactly one pure-Python
`py3-none-any` wheel. ZIP content starts at the project package root and has one top-level package.

Do not bundle DataCoolie, Spark, Polars, platform SDKs, or compiled extensions. For a wheel, record
distribution and version from its metadata. For ZIP, both fields are null and identity is build ID
plus SHA-256. A changed wheel byte stream for an already-built distribution/version is rejected;
advance the version.

## Validation and runner handoff

Run `scripts/validate_functions.py` against the exact generated artifact and all resolved metadata.
It rejects unsafe archive members, ambiguous roots, prefix mismatches, missing callables, and
functions that cannot accept `engine`, `source`, `watermark_start`, and `watermark_end` directly or
through `**kwargs`. Validation imports from an isolated temporary location and never calls function
bodies.

The generated artifact lives under the fixed top-level build `functions/` component. The manifest
and Build receipt use singular `functions_artifact`. A successful function-backed
Build records a passed artifact-only `functions-artifact-import` check. Build-host function runtime
execution is optional evidence when the host is compatible; it is not required to stage a release
and does not replace target import and execution checks before activation.

Render `allowed_function_prefixes` to the exact manifest prefix; render `[]` without a function
artifact. Installation and attachment occur before the runner starts and belong to the prepared
runtime and Release, respectively.

## Unresolved Questions

None. An execution target incompatible with the approved format requires a new Design/Build scope,
not a second artifact variant.

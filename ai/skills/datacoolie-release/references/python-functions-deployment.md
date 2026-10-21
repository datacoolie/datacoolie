# Python functions upload note

Read the public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
and [function project recipe](https://datacoolie.github.io/datacoolie/examples/dataflows/#function-project-recipe)
for functions-root layout, packaging results, and host import setup. Release
consumes the exact retained environment projection produced by Build; it does
not rebuild, convert, install, attach, import, or execute functions.

Upload every retained environment file through the public release handoff's
ordered phases. The execution host or platform deployment workflow owns package attachment/import
and must use the build's
published inventory and the project's fixed import prefixes. A release receipt
is transfer evidence, not proof of import or runtime health.

Use [Release's evidence gate](../SKILL.md#verification). Hand the retained functions-root inventory and fixed import prefixes to the execution-host owner for attachment and import verification.

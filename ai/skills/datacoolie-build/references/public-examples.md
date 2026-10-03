# Public DataCoolie examples

The public examples library is the canonical source for new runner and project
adaptations. Use the published catalog when selecting an unfamiliar sample;
when a guide or sample is already known, follow its concrete file/project link
directly instead of taking an unnecessary catalog detour:

- Examples catalog: `https://datacoolie.github.io/datacoolie/examples/`
- Runner guide: `https://datacoolie.github.io/datacoolie/examples/runners/`
- Project and CLI workflow: `https://datacoolie.github.io/datacoolie/guide/cli/project/`

The catalog uses `guide` for usage instructions, `source` for a complete
readable projection, `raw` for original file bytes, `project-files` for the
matching complete project section in the catalog and `download` for one
complete project archive (currently `.zip`). Raw may render inline; it is not
an archive download.

## Retrieval protocol

1. Choose the closest entry in the Examples catalog when discovery is needed.
   Match the purpose, sample kind and engine/host before copying anything. If
   the current guide already identifies the sample, use its direct action links.
2. Follow the sample's `guide` action when prerequisites or adaptation context
   is needed. The guide explains prerequisites, companion files,
   invocation, adaptation points and the verification boundary.
3. Follow the explicit `source` action when reading in a browser, or the
   `raw` action when a tool needs the original file bytes. Do not invent a
   revision, branch or path from memory. The catalog/guide link is the source
   of truth for the published build.
4. For a multi-file project, open its `project-files` section, inspect the
   linked `source`/`raw` files, and use its one `download` action only when a
   complete checkout is needed. Do not download files one by one and
   reconstruct the project.
5. Verify the response body and, for an archive, its contents before adapting
   it. A local docs preview, a displayed Git SHA, or a successful HTTP status
   alone is not publication evidence. Do not silently fall back to `main` when
   a concrete link is unavailable.

When network access is unavailable, use an explicitly supplied, pinned checkout
of the same revision; its repository and raw paths are under
`docs/examples/files/`. The checkout is a retrieval source only and must not
become a second editable runner tree. Skills keep agent-specific workflow
decisions, approval gates and evidence handling, while the public examples own
framework construction and host-parameter contracts.

The catalog owns the runner and project inventory. Do not copy its list into a
Skill reference or create a second machine-readable registry. If an entry is
missing, report the uncovered canonical source instead of guessing a path.

## Recipe and evidence boundary

The selected guide and project recipe own prerequisites, working directory,
commands, adaptation points, expected selection/output/state, and rerun behavior.
For author/build-only requests, retrieve and validate the declared files or
archive without forcing workload execution. For an explicitly requested safe
runtime check, follow the public [build and execute workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/#build-and-execute)
and selected recipe: use the CLI-selected environment and retain its fresh-copy
execution, independent output/state, rerun and retained/current validation
evidence. Classify legitimate empty or skipped results and report skipped host,
dependency, or cloud checks. Keep source, raw, and archive content tied to the
same published revision and report body/content mismatches even when the HTTP
request succeeds.

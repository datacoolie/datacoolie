# Runner handoff boundary

The public [project workflow](https://datacoolie.github.io/datacoolie/guide/cli/project/)
defines the environment projection and release handoff. Release uploads that
projection; it does not choose a native job, notebook, function, scheduler, or
activation mechanism. A platform-owned workflow may consume the uploaded
`runners/` files afterward.

For that handoff, keep these identities explicit:

- the project environment and pinned `build_id`;
- the exact runner relative path under the uploaded environment tree;
- the target resource and execution-host runtime;
- any platform-specific package/import setup and its own verification.

Do not infer a deployment kind from a filename or platform name, and do not put
target identity or activation state in `datacoolie.yml` or the build manifest.
Those concerns belong to the platform's deployment policy. DataCoolie release
only records upload command outcomes and leaves unrelated target files intact.

For author/build-only requests, validate the runner bytes and artifact mapping
without execution. An authorized upload reports transfer evidence only. Route
entrypoint, runtime state, and host verification to the project/platform owner
with the matching public recipe; Release does not execute a runner or target
workload and records those checks as outside its scope.

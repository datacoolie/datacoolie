# Runner handoff boundary

The DataCoolie release workflow uploads the complete environment projection;
it does not choose a native job, notebook, function, scheduler, or activation
mechanism. A platform-owned workflow may consume the uploaded `runners/` files
afterward.

For that handoff, keep these identities explicit:

- the project environment and pinned `build_id`;
- the exact runner relative path under the uploaded environment tree;
- the target resource and execution-host runtime;
- any platform-specific package/import setup and its own verification.

Do not infer a deployment kind from a filename or platform name, and do not put
target identity or activation state in `datacoolie.yml` or the build manifest.
Those concerns belong to the platform's deployment policy. DataCoolie release
only records upload command outcomes and leaves unrelated target files intact.

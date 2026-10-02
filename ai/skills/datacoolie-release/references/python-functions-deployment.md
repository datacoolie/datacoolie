# Python functions upload note

Functions are ordinary files in the selected environment projection. Build may
produce several roots, each packaged as wheel, ZIP, or copied source according
to `datacoolie.yml`. Release does not rebuild, convert, install, attach, import,
or execute them.

Upload the complete retained environment tree, including every functions root,
in the same ordered two phases as other artifact files:

```text
<deployment_path>/artifacts/<build_id>/
<deployment_path>/current/
```

The execution host or a platform-owned deployment workflow may later attach a
specific package. That workflow must consume the root manifest inventory and
the project runner's fixed import prefixes; it must not infer a singular
`functions_artifact` field or consult a release receipt as proof of import or
runtime health.

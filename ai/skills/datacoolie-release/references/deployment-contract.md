# Upload contract

This reference defines the only mutation owned by the DataCoolie release
workflow: copying a locally verified environment projection to a configured
deployment path. It does not define target activation or workload execution.

## Source pinning

Resolve `current` once. Its root `manifest.json` supplies `build_id`; local CLI
validation must prove that `.builds/current` is byte-for-byte equivalent to
`.builds/artifacts/<build_id>`. After that check, read only the retained
environment directory:

```text
<project>/.builds/artifacts/<build_id>/<env>/
```

An explicit retained build may be used directly, but report that project/current
comparison was not performed unless the caller supplied it. Never upload moving
current bytes or re-resolve a selector halfway through a transfer.

## Ordered transfer

```text
1. upload retained/<env>/ -> <deployment_path>/artifacts/<build_id>/
2. if 1 succeeds, upload retained/<env>/ -> <deployment_path>/current/
```

Each operation maps relative files exactly and may overwrite a matching target
path. It must not delete or purge files that are absent from the source. A
failure in phase 1 prevents phase 2. A failure in phase 2 is a partial release;
the immutable artifact remains available for retry. There is no automatic
rollback, activation marker, remote checksum comparison, candidate staging,
package installation, scheduler creation, or job invocation.

## Adapter obligations

The target adapter is responsible for selecting the official platform copy
command and credentials. It must receive an argument list (not an unquoted shell
string), preserve spaces and Unicode paths, expose exit status and a redacted
error, and record the number of source files attempted. It must not put target
credentials in a project file, manifest, log, or release record.

## Target semantics

`deployment_path` comes only from `datacoolie.yml` at release time. The manifest
does not contain a target path. Remote `current` is a convenience projection;
consumers requiring reproducibility should address `artifacts/<build_id>`.
Unknown remote files remain intentionally. A subsequent cleanup policy, if
needed, belongs to the platform owner and is not implied by DataCoolie release.

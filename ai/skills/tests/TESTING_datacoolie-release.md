# Testing datacoolie-release

Run:

```bash
python ai/skills/tests/run_release.py
python -m pytest -o addopts='' ai/skills/tests/unit/test_release_receipt.py ai/skills/tests/unit/test_release_upload.py -q
```

The release contract is intentionally upload-only. Tests cover local validation,
build-ID pinning, destination lookup from `datacoolie.yml`, ordered immutable
artifact/current uploads, repeated-build retries, paths containing spaces,
partial failures, and local record validation. They must not make network calls,
delete target files, activate a resource, install packages, create a job, or
execute a runner.

Platform-specific copy commands and target activation remain outside this skill;
use the target owner's documented adapter and record its bounded, redacted
command outcome. `scripts/upload_local.py` is a local/fake target adapter for
contract tests; it rejects cloud URIs and does not represent a remote upload.

Behavioral cases are stored in `datacoolie-release/evals/evals.json` and are
decision definitions, not proof that a remote deployment was executed.

## Unresolved questions

None.

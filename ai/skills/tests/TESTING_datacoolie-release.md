# Testing datacoolie-release

Run:

```bash
python ai/skills/tests/run_release.py
```

Verify:

- Release requires an exact build ID, target slice, checksums, explicitly supplied successful build
  receipt path, and target authorization; it never selects latest evidence.
- Modified or incomplete builds and receipts without generated-runtime proof fail preflight through
  the release consumer validator, even when optional build automation is absent.
- Deploy and promotion never invoke materialization or functions packaging.
- Promotion reuses the source build, while rollback selects an explicit prior verified release
  rather than editing current metadata.
- CI references download and verify immutable artifacts without installed-skill paths.
- Production authorization is distinct from design or implementation approval.
- Cross-workflow acquisition identifies the exact source run or immutable artifact, not only its
  name.
- Deployment stages and verifies a candidate before activation; partial state remains failed.
- Release authorization is bound to the canonical exact deployment intent, including build slice,
  target identity, activation mechanism, and source/provision evidence.
- Provision handoff validation binds the exact requirements, plan approval, and resource observation.
- Promotion and rollback consume explicit successful source receipts and reject latest selection.
- Release receipts remain durable beyond an ephemeral CI job.

Behavioral cases are stored in `datacoolie-release/evals/evals.json`; the validator checks that the
eval contract remains present and machine-readable. Receipt unit tests verify build-slice binding,
authorization intent, upstream receipt semantics, source-release chains, target digests, build
integrity, and failed-release success gates.

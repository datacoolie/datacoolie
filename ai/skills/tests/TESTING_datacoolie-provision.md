# Testing datacoolie-provision

Run:

```bash
python ai/skills/tests/run_provision.py
```

Forward scenarios:

- Existing infrastructure skips provisioning.
- Missing resources produce a plan receipt without applying changes.
- Apply stops for explicit approval tied to the plan and environment.
- Data-bearing replacement requires a separate destructive approval, and a materially changed plan
  requires renewed approval.
- Metadata defects route back to build instead of triggering speculative infrastructure.
- Plan approval is bound to the exact environment and persisted plan hash.
- Existing resources outside Terraform state require approved reconciliation instead of implicit
  import or duplicate creation.
- Failed or partial applies cannot satisfy a successful apply receipt gate.
- Provision evidence excludes secrets and sensitive outputs.
- Provision never changes metadata, builds, or releases.
- Build or release supplies one exact requirements artifact and hash instead of an undefined
  resource-gap receipt type.

Behavioral cases are stored in `datacoolie-provision/evals/evals.json`; the validator checks that
the eval contract remains present and machine-readable. Receipt unit tests verify artifact hashes,
authorization binding, destructive approval, partial-state handling, and sensitive output rejection.

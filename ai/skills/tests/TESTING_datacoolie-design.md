# Testing datacoolie-design

Run from `datacoolie/`:

```bash
python ai/skills/tests/run_design.py
python -m pytest -o addopts="" ai/skills/tests/unit/test_design_approval.py \
  ai/skills/tests/unit/test_ai_workflow_contract.py -q
```

The suite checks:

- one neutral `architecture/current.md` template without named layers, stages, engines, formats,
  providers, environments, or amendment files;
- transition contracts, capability intent, runtime engine selection, quality, recovery, resource,
  release, handoff, and unresolved-question sections;
- a typed receipt whose path and SHA-256 match the final architecture bytes;
- stale receipt rejection, timezone-aware approval evidence, and explicit record confirmation;
- compatible implementation changes skip design while material changes require approval;
- design never authors metadata, runners, infrastructure, or release artifacts.

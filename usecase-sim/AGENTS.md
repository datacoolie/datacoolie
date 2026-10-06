# Simulator rules

Read the workspace entrypoint first, then `usecase-sim/cases/README.md` and the
owning `cases/<group>.json`. Paths in the catalog are relative to `datacoolie/`.
Portable rules remain in the workspace `.agents/instructions/`.

- Define the expected behavior from its source contract before adding a fixture.
- A case owns one primary behavior. Identity is `(fixture, flow)`, not flow name.
  Setup/readback flows are support references, never extra qualifications.
- `metadata/` owns local, Docker and AWS-MinIO simulation fixtures. Put primary
  flows in `metadata/file/<group>/` and shared profiles in `metadata/file/shared/`.
  Real-cloud smoke metadata is one ordinary JSON in `platforms/<platform>/`,
  with a few representative flows owned by a platform workflow case. Full feature
  matrices stay in local simulation. Group fixtures must match their primary group. Local feature evidence
  does not qualify the corresponding real-cloud platform.
  Broad workflows reference group documents and explicit order; compile them
  for export/upload instead of maintaining copied aggregate metadata.
- Fixtures must activate the branch: seeded old/new keys for merge, changed and
  unchanged history for SCD2, old schema for evolution, eligible/protected data
  for cleanup. Declare stateful run sequences and dependencies explicitly.
- Assert exact persisted data/state or the contract's expected failure. Exit-only
  success is smoke. Skip/unsupported cannot qualify a feature. Mutation probes
  must reject missing IDs, wrong versions/checkpoints and skipped operations.
- Preserve contract-permitted visible writes before failed acknowledgement/save;
  document recovery instead of assuming rollback atomicity.
- Keep sufficient pytest owners in pytest. API metadata and API data sources are
  distinct. Use representative profiles; add dimensions only where behavior differs.
- Catalog authored fixtures and generated recipes, never `.runtime` outputs or
  generated YAML/Excel as independent authored cases. Update catalog membership,
  expectations and gaps alongside each fixture, validator or recipe.
- Keep data/state case-owned under canonical runtime roots. Framework logs use
  shared `.runtime/logs`; select current job/session. Never reset shared services,
  databases, Docker volumes or cloud resources to qualify a focused case.
- Group execution uses only declared dedicated recipes with their complete
  invocation/dependency order. An unknown or empty group must fail before setup.
- Audit before delivery: `python usecase-sim/scripts/audit_cases.py --json`.
  Audit/list is read-only and must not start services or execute preparation.
- Report assertion coverage separately from runtime qualification. Receipts need
  revision, runtime/profile and command; other revisions are historical evidence.
- Runtime source bugs found by experiments require a reproducer and separate
  scope assessment; do not silently repair them as simulator organization work.

# Bounded Evidence Queries

## Scope

Use this reference only after normal introspection leaves one material database question open. It
helps the agent author one dialect-appropriate query for `scripts/probe_db.py`. It does not provide
an executable query bundle, replace source documentation, or select an ingestion strategy.

## Query Purposes

Choose only checks that can change the assessment decision:

| Question | Prefer bounded evidence such as |
|---|---|
| Can the candidate be null or tied? | aggregate null, distinct, duplicate, or maximum tie counts |
| Is ordering usable? | bounded min/max and ordered samples containing only keys plus the candidate |
| Can it reset or be reused? | bounded range comparison across source-defined partitions or periods |
| Which mutations are covered? | aggregate counts for insert, update, delete, or operation indicators |
| Are late changes possible? | aggregate difference between business and modification dates within an agreed window |
| Can the source filter efficiently? | current catalog/index facts or a bounded explain facility when the source safely supports it |

## Authoring Contract

1. Confirm the active source dialect and official semantics for the candidate.
2. Prefer aggregates that do not return business values. Select raw values only when the exact gap
   requires them, and retain the minimum keys/candidate columns in scratch.
3. Bound objects, predicates, rows, and time. Do not probe every object automatically.
4. Put exactly one self-contained read-only `SELECT` or `WITH` statement in a SQL file.
   `probe_db.py` does not bind query parameters, so replace placeholders with reviewed,
   non-secret dialect literals or source-native bounded expressions before execution.
5. Execute it with environment-backed connection information, `--max-rows`,
   `--timeout-seconds`, and a scratch JSON `--output`; the probe result is a JSON envelope, not CSV.
   Do not concatenate credentials or source values into the command.
6. Record the query purpose, scope, and limitation in `report.md`; normalize only supported source
   facts into observations or assessment decisions.

SQL fragments in source documentation or prior work are starting points only. Adapt and review the
single query before execution; never run a folder or reference page as a batch.

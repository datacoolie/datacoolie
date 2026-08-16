# Gap-Driven Discovery Questions

Use this only after scripted introspection. Ask the smallest set of questions needed to resolve a
downstream decision; omit anything already supported by evidence.

## Ownership And Scope

- Who owns the source and can confirm ambiguous keys, semantics, or access constraints?
- Which objects are in scope, and are any intentionally excluded?

## Change Semantics

- Does the source expose CDC, change tracking, transaction logs, or durable version tokens?
- Which candidate advances for inserts, updates, or every relevant change? Can it be null,
  duplicated, reset, reused, rewritten, or delivered out of order?
- Is an identity or sequence truly append-only? If rows can change, which additional value captures
  those updates?
- How are deletes represented? Distinguish durable tombstones or CDC events from hard deletes that
  disappear from ordinary source queries.
- If only a transaction or business date is available, what correction horizon and bounded
  lookback can safely cover late or backdated changes?
- If no reliable candidate exists, are full extraction duration, source load, cost, and target
  overwrite acceptable? Do not infer a universal row-count threshold.

## Extraction Constraints

- What extraction windows, concurrency limits, rate limits, or query timeouts apply?
- Are filters, partitions, pagination tokens, snapshots, or export APIs available?
- Are there network, authentication, or environment-specific restrictions relevant to inspection?

## Data Interpretation

- Which undeclared columns form a business or uniqueness key?
- Which time zone, encoding, precision, or schema-drift behavior cannot be learned from metadata?
- Which known source-quality conditions should downstream design account for?

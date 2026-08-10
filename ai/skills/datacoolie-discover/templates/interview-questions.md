# Gap-Driven Discovery Questions

Use this only after scripted introspection. Ask the smallest set of questions needed to resolve a
downstream decision; omit anything already supported by evidence.

## Ownership And Scope

- Who owns the source and can confirm ambiguous keys, semantics, or access constraints?
- Which objects are in scope, and are any intentionally excluded?

## Change Semantics

- Does the source expose CDC, change tracking, transaction logs, or durable version tokens?
- Which observed candidate records inserts and updates reliably? Can values arrive late, move
  backwards, or be rewritten?
- How are deletes represented, including hard deletes and soft-delete fields?
- Is there an overlap window or reconciliation rule for late changes?

## Extraction Constraints

- What extraction windows, concurrency limits, rate limits, or query timeouts apply?
- Are filters, partitions, pagination tokens, snapshots, or export APIs available?
- Are there network, authentication, or environment-specific restrictions relevant to inspection?

## Data Interpretation

- Which undeclared columns form a business or uniqueness key?
- Which time zone, encoding, precision, or schema-drift behavior cannot be learned from metadata?
- Which known source-quality conditions should downstream design account for?

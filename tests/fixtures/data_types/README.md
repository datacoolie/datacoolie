# Datatype reference cases

`reference_cases.json` is an independently authored contract fixture. The
expected target spellings are not generated from the resolver. Tests can load
the cases to detect accidental changes to source-dialect interpretation before
optional Spark, Polars, database, or table-format qualification runs.

`persisted_contract.json` is the contract for the canonical file-source
matrix. `database_vendor_contract.json` is intentionally separate: live
database fixtures can expose different source values (for example Oracle
`DATE` precision and `RAW` bytes) even when they use the same authored hint
matrix. Database qualification must use the contract that matches its owned
source snapshot, then compare both engines against that contract.

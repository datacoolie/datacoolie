# Synthetic discovery example

Run in an isolated workspace with the selected Python environment and database probe dependencies.
`$discover` is the absolute installed `datacoolie-discover` skill directory; `$python` is that
environment's Python executable. The commands below use PowerShell. They inspect only a synthetic
SQLite database and do not authorize access to another source.

```powershell
New-Item -ItemType Directory -Force .scratch/discover, discover | Out-Null
@'
import sqlite3
from pathlib import Path
path = Path('.scratch/discover/example.sqlite').resolve()
with sqlite3.connect(path) as conn:
    conn.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, UpdatedAt TEXT)')
    conn.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)')
conn.close()
print('sqlite:///' + path.as_posix())
'@ | & $python -
```

Set `$env:EXAMPLE_DB_URL` to the printed nonsecret connection URL. Use distinct files for each
probe. Inspect statuses: exit `0` means complete within declared scope, exit `3` is partial evidence
and requires review; any other nonzero exit blocks the example.

```powershell
& $python "$discover/scripts/introspect_db.py" --url-env EXAMPLE_DB_URL --source erp --max-objects 10 --output .scratch/discover/all.csv --status-output .scratch/discover/all.json
& $python "$discover/scripts/merge_observations.py" --probe-input .scratch/discover/all.csv .scratch/discover/all.json --output discover/observations.csv
& $python "$discover/scripts/assess_watermarks.py" --input discover/observations.csv --output .scratch/discover/assessment.csv --summary-output .scratch/discover/summary.json
```

Read the summary. A column named `UpdatedAt` in this fixture has no observed mutation semantics;
it is not a confirmed candidate. Create `.scratch/discover/decisions.json` with:

```json
{
  "schema_version": 1,
  "objects": [
    {
      "match": {"source":"erp","object_type":"table","catalog":"","schema":"main","object":"orders","source_operation":""},
      "outcome":"human_decision","candidates":[],
      "coverage":"Schema inspected; mutation and ordering semantics unknown",
      "limitations":"Synthetic empty table; no change coverage evidence",
      "fallback":"None","decision_required":"Choose load behavior after source-owner evidence"
    },
    {
      "match": {"source":"erp","object_type":"table","catalog":"","schema":"main","object":"customers","source_operation":""},
      "outcome":"human_decision","candidates":[],
      "coverage":"Schema inspected; source change behavior unknown",
      "limitations":"No reliable change signal observed",
      "fallback":"None","decision_required":"Assess refresh feasibility and source change semantics"
    }
  ]
}
```

```powershell
& $python "$discover/scripts/finalize_watermark_assessment.py" --input discover/observations.csv --decisions .scratch/discover/decisions.json --annotations-output .scratch/discover/annotations.json --report-output .scratch/discover/assessment.md
& $python "$discover/scripts/enrich_observations.py" --input discover/observations.csv --annotations .scratch/discover/annotations.json --output .scratch/discover/enriched.csv
```

Inspect the candidate, then promote it to `discover/observations.csv`. Assemble `discover/report.md`
with the generated table, probe scope and unresolved source questions. The decisions, status,
shortlist, annotations and intermediate report remain scratch evidence.

## Refresh with two probes

For this fixture only, the setup above is independent evidence that the whole source boundary is
exactly `main.orders` and `main.customers`. Copy
[the coverage template](../templates/source-coverage.example.json) to
`.scratch/discover/coverage.json` and set its `evidence` to this synthetic setup. For a real source,
obtain its verified inventory/boundary first; old observations do not prove current coverage.

```powershell
& $python "$discover/scripts/introspect_db.py" --url-env EXAMPLE_DB_URL --source erp --tables orders --max-objects 10 --output .scratch/discover/orders.csv --status-output .scratch/discover/orders.json
& $python "$discover/scripts/introspect_db.py" --url-env EXAMPLE_DB_URL --source erp --tables customers --max-objects 10 --output .scratch/discover/customers.csv --status-output .scratch/discover/customers.json
& $python "$discover/scripts/merge_observations.py" --base discover/observations.csv --replace-source erp --probe-input .scratch/discover/orders.csv .scratch/discover/orders.json --probe-input .scratch/discover/customers.csv .scratch/discover/customers.json --coverage-input .scratch/discover/coverage.json --output .scratch/discover/refresh.csv --diff-output .scratch/discover/diff.json
```

Inspect the diff before promotion, then reassess affected objects. Swapping the two status files
must fail even though both contain the same row count. Omitting the coverage declaration must also
fail: filtered completeness does not prove the whole source was inspected.

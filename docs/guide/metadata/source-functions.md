---
title: Python function sources | DataCoolie
description: Configure python function sources with metadata, examples and backend-specific guidance.
---

# Python function sources

[All source recipes](source-patterns.md).

## Python function source

Use when reading data needs custom Python logic—for example, combining several
steps or using an SDK—or when the built-in table/query readers do not cover the
source. If one database or lakehouse SQL statement (including CTEs) is enough,
prefer `source.query`.

```json
{
  "name":            "custom_src",
  "connection_type": "function",
  "format":          "function",
  "configure":       {}
}
```

The actual function path goes on `source.python_function`:

**Dataflow source block:**

```json
"source": {
  "connection_name":   "custom_src",
  "table":             "partner_orders",
  "python_function":   "mypkg.sources.load_orders",
  "watermark_columns": ["updated_at"],
  "configure": {
    "api_base": "https://partner.example.com"
  }
}
```

Define `load_orders(engine, source, watermark_start, watermark_end)` for
ordinary incremental reads (the framework passes all four as keyword
arguments). A bounded replay passes a separate `read_range` keyword and sets
`watermark_start=None` and `watermark_end=None`; a bounded function must accept
that keyword explicitly or through `**kwargs`. `read_range` carries the exact
source-owned column, start/end values, and comparison operators. Use it for
push-down when possible, and return rows that honor the range; the built-in
function reader also applies the exact residual range filter before it observes
the candidate watermark. It rejects a legacy function that cannot accept the
keyword. Return a DataFrame compatible with the active engine, or `None` when
there is no data. The function receives the full `Source` object, so it can use
`source.table`, `source.schema_name`, `source.configure`, and other fields as
meaningful inputs. In this example `table` may name the dataset that
`load_orders` reads; it is not necessarily just a display alias.
The framework uses `source.python_function` to choose the function.

To narrow which dotted function paths may be imported from metadata, pass
`allowed_function_prefixes` in `DataCoolieRunConfig`. This is a string-prefix
check, not a sandbox; only run trusted metadata and choose prefixes carefully.

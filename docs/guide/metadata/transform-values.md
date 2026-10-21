---
title: Value normalization | DataCoolie
description: Configure value normalization with metadata, examples and backend-specific guidance.
---

# Value normalization

[All transform recipes](transform-patterns.md).

## ColumnValueTransformer

**When to use:** incoming strings, nulls, or category codes need cleanup before
datatype conversion. Configure `transform.value_rules`. Each
[Value rule](../../reference/metadata-schema.md#value-rule)
specifies an `operation`, target `columns`, and any operation-specific options.
The rule's own `order` controls ordering **within** `value_rules`; the whole
value-rule transformer still runs at pipeline order 5.

The six operations below are individual items in `transform.value_rules`.
All except `fill_null` require string columns. Existing nulls stay null unless
you explicitly use `fill_null`.

### `trim`: remove surrounding spaces

```json
{"operation": "trim", "columns": ["email", "customer_name"]}
```

`"  Alice  "` becomes `"Alice"`; `"   "` becomes `""`. Only ASCII U+0020
spaces are removed. Tabs, newlines, and non-breaking spaces remain. One rule
can target several columns.

### `case`: lowercase or uppercase

Use `lower` for normalized emails or `upper` for codes:

```json
"transform": {
  "value_rules": [
    {"operation": "case", "columns": ["email"], "mode": "lower"},
    {"operation": "case", "columns": ["country_code"], "mode": "upper"}
  ]
}
```

`"Alice@Example.COM"` becomes `"alice@example.com"`; `"vn"` becomes `"VN"`.
These are the two supported modes; case conversion does not trim spaces.

### `regex_replace`: replace every matching substring

Remove formatting from a phone number:

```json
{"operation": "regex_replace", "columns": ["phone"], "pattern": "[^0-9]", "replacement": ""}
```

`"(+84) 123-456"` becomes `"84123456"`. Set a non-empty `replacement` to
replace matches with text; omitting it uses the empty string.

The pattern uses DataCoolie portable regex v1 rather than the complete Java
or Rust dialect. Use literals, explicit character classes/ranges, `.`, anchors,
grouping, alternation, and ordinary quantifiers. Lookaround, backreferences,
named groups, inline flags, `\d`/`\w`/`\s`/`\b`, possessive quantifiers, and
quantified nested groups are rejected while loading metadata. Patterns are
limited to 4,096 characters. Replacement text is always literal, so `$` and
backslash are emitted as written rather than expanding capture groups.

### `empty_to_null`: convert empty strings to null

```json
{"operation": "empty_to_null", "columns": ["middle_name", "country_code"]}
```

Only `""` becomes null. `"   "` remains unchanged unless an earlier `trim`
rule first removes its spaces. Non-empty values remain unchanged.

### `fill_null`: supply a typed default

```json
"transform": {
  "value_rules": [
    {"operation": "fill_null", "columns": ["country_code"], "value": "UNKNOWN"},
    {"operation": "fill_null", "columns": ["quantity"], "value": 0},
    {"operation": "fill_null", "columns": ["is_active"], "value": false}
  ]
}
```

Only nulls change. The example assumes `country_code` is already a string,
`quantity` an integer, and `is_active` a boolean. `value` must match the
**current** column type; schema hints run later and cannot make an incompatible
literal valid at this stage.

| Current column type | Example JSON `value` | Requirement |
|---|---|---|
| String | `"UNKNOWN"` | Use a JSON string, including `""` if intentional |
| Integer | `0` | Integer within the target range; `"0"` and booleans are rejected |
| Float | `0.5` | Finite JSON number |
| Decimal | `"0.00"` | Integer or decimal string that fits the column's precision/scale |
| Boolean | `false` | JSON boolean, not `"false"` or `0` |
| Date | `"2026-01-01"` | ISO date |
| Timezone-free timestamp | `"2026-01-01T00:00:00"` | ISO datetime without an offset |
| Timezone-aware timestamp | `"2026-01-01T00:00:00+00:00"` | ISO datetime with an offset |

Binary, nested, and untyped-null columns are not supported literal targets.
Invalid literals fail before the native expression is applied, independently
of Spark ANSI settings. Empty strings and NaN are not nulls for this operation.

### `map`: translate string codes

```json
{"operation": "map", "columns": ["status"], "mapping": {"A": "active", "I": "inactive"}, "on_unmapped": "keep"}
```

`"A"` becomes `"active"`; an unknown `"X"` stays `"X"` with `keep`, the
default. Keys and values must be strings, and matching is exact: `"a"` does
not match `"A"`. Use an earlier `case` rule if needed.

Use `null` when unknown codes should become null:

```json
{
  "operation": "map",
  "columns": ["status"],
  "mapping": {"A": "active", "I": "inactive"},
  "on_unmapped": "null"
}
```

Only `keep` and `null` are supported. This setting controls unmapped values,
not absent columns.

### Combine rules in a deliberate order

The following turns spaces-only input into `"UNKNOWN"`:

```json
"transform": {
  "value_rules": [
    {"operation": "trim", "columns": ["country_code"], "order": 10},
    {"operation": "empty_to_null", "columns": ["country_code"], "order": 20},
    {"operation": "fill_null", "columns": ["country_code"], "value": "UNKNOWN", "order": 30}
  ],
  "configure": {"missing_column_policy": "error"}
}
```

Rules default to order `100`; ties retain declaration order. An empty or omitted
`value_rules` list does nothing. Missing configured columns follow
[`transform.configure`](../../reference/metadata-schema.md#dataflowstransformconfigure);
see [Missing-column policy](transform-patterns.md#missing-column-policy) for the `error`/`ignore` cases.

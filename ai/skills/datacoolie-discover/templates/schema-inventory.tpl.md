---
artifact_type: schema_inventory
date: "{{ date }}"
source_name: "{{ source_name }}"
source_type: "{{ database | api | file | lakehouse }}"
discovery_report: "{{ date }}_{{ source_name }}.md"
---

# Schema Inventory — {{ source_name }}

<!-- Use the section matching your source type. Delete the other. -->

## Database / File / Lakehouse Sources

### Tables

| # | Schema | Table | Column Count | PK | Row Estimate | Notes |
|---|---|---|---|---|---|---|
| 1 | TODO | TODO | TODO | TODO | TODO | |

### Column Details — {schema}.{table_name}

| Column | Type | Nullable | FK | Notes |
|---|---|---|---|---|
| TODO | TODO | TODO | | |

> _Repeat for each table. Type uses canonical forms with inline precision: `decimal(18,2)`, `timestamp (yyyy-MM-dd HH:mm:ss)`, `string`. FK format: `→ schema.table.column` or empty. See source-introspection-guide.md Type Normalization Reference._

---

## API Sources

### Connection

| Property | Value |
|---|---|
| Base URL | TODO |
| Auth type | TODO (bearer / api_key / basic / oauth2_client_credentials / aws_sigv4) |
| Token URL | TODO _(if oauth2)_ |
| API spec | TODO (OpenAPI 3.0 at /path, GraphQL, OData, none) |
| Rate limit | TODO (requests/min) |
| Default headers | TODO _(if any)_ |

### Endpoints

| # | Endpoint | Method | Pagination | Data Path | Field Count | Notes |
|---|---|---|---|---|---|---|
| 1 | TODO | TODO | TODO | TODO | TODO | |

### Field Details — {endpoint}

| Field | Type | Nullable | FK | Notes |
|---|---|---|---|---|
| TODO | TODO | TODO | | |

> _Repeat for each endpoint. Type uses canonical forms. FK format: `→ /endpoint.field` or empty. Use dot notation for nested fields: `customer.id`, `items[].product_id`._

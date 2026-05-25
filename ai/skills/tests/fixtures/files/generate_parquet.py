"""
Generate a small Parquet sample file (sales.parquet) for file introspection testing.
Run once: python fixtures/files/generate_parquet.py
Requires: pip install pyarrow pandas
"""
import sys

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import os
except ImportError:
    print("ERROR: pyarrow not installed. Run: pip install pyarrow")
    sys.exit(1)

data = {
    "order_id":    [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
    "customer_id": [101,  102,  103,  101,  104,  105,  102,  106,  107,  101],
    "product_id":  [1,    4,    7,    2,    9,    3,    11,   6,    2,    5],
    "quantity":    [1,    2,    3,    1,    1,    2,    5,    1,    2,    1],
    "unit_price":  [999.0, 79.5, 24.99, 99.99, 399.0, 49.99, 4.99, 35.0, 99.99, 149.99],
    "discount":    [0.0,  0.0,  0.10, 0.0,  0.05, 0.0,  0.0,  0.15, 0.10, 0.0],
    "order_date":  ["2024-01-15","2024-01-16","2024-01-17","2024-01-22","2024-02-01",
                    "2024-02-05","2024-02-10","2024-02-14","2024-02-20","2024-03-01"],
    "status":      ["shipped","delivered","delivered","processing","shipped",
                    "delivered","delivered","cancelled","shipped","processing"],
    "region":      ["APAC","AMER","EMEA","APAC","AMER","APAC","EMEA","AMER","APAC","AMER"],
}

schema = pa.schema([
    ("order_id",    pa.int32()),
    ("customer_id", pa.int32()),
    ("product_id",  pa.int32()),
    ("quantity",    pa.int16()),
    ("unit_price",  pa.float64()),
    ("discount",    pa.float32()),
    ("order_date",  pa.string()),
    ("status",      pa.string()),
    ("region",      pa.string()),
])

table = pa.table(data, schema=schema)
out = os.path.join(os.path.dirname(__file__), "sales.parquet")
pq.write_table(table, out, compression="snappy")
print(f"Written: {out}  ({len(table)} rows, {len(table.schema)} columns)")

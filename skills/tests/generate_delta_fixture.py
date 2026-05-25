"""Generate a minimal Delta Lake table fixture for testing.

Creates: fixtures/files/delta_products/_delta_log/... + parquet data files.
Run once to generate. Requires: deltalake package.
"""
import os
import sys
from pathlib import Path

HERE = Path(__file__).parent

try:
    import deltalake
    import pyarrow as pa
except ImportError:
    print("ERROR: deltalake + pyarrow needed — pip install deltalake pyarrow")
    sys.exit(1)

# Create delta table at fixtures/files/delta_products/
delta_path = str(HERE / "fixtures" / "files" / "delta_products")

data = pa.table({
    "product_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
    "name": pa.array(["Widget", "Gadget", "Doohickey", "Thingamajig", "Whatchamacallit"]),
    "price": pa.array([9.99, 24.50, 3.75, 149.00, 0.50], type=pa.float64()),
    "category": pa.array(["tools", "electronics", "misc", "electronics", "misc"]),
    "in_stock": pa.array([True, True, False, True, True]),
})

deltalake.write_deltalake(delta_path, data, mode="overwrite")
print(f"✓ Delta table written to {delta_path}")
print(f"  Rows: {len(data)}, Columns: {data.num_columns}")

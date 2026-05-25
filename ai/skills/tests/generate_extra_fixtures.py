"""Generate avro and xlsx fixtures for file introspection testing."""
from pathlib import Path
import sys

HERE = Path(__file__).parent / "fixtures" / "files"

# --- Avro fixture ---
try:
    import fastavro
    import io

    schema = {
        "type": "record",
        "name": "Inventory",
        "fields": [
            {"name": "item_id", "type": "int"},
            {"name": "warehouse", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "unit_price", "type": "double"},
            {"name": "reorder_level", "type": "int"},
        ],
    }
    records = [
        {"item_id": 1, "warehouse": "US-EAST", "quantity": 100, "unit_price": 12.50, "reorder_level": 20},
        {"item_id": 2, "warehouse": "US-WEST", "quantity": 50, "unit_price": 8.75, "reorder_level": 10},
        {"item_id": 3, "warehouse": "EU-CENTRAL", "quantity": 200, "unit_price": 45.00, "reorder_level": 50},
    ]
    avro_path = HERE / "inventory.avro"
    with open(avro_path, "wb") as f:
        fastavro.writer(f, schema, records)
    print(f"✓ Avro fixture: {avro_path}")
except ImportError:
    print("SKIP: fastavro not installed — pip install fastavro")

# --- Excel fixture ---
try:
    import openpyxl

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "departments"
    ws.append(["dept_id", "dept_name", "manager", "budget", "headcount"])
    ws.append([1, "Engineering", "Alice", 500000.00, 42])
    ws.append([2, "Marketing", "Bob", 300000.00, 18])
    ws.append([3, "Finance", "Charlie", 200000.00, 12])
    ws.append([4, "HR", "Diana", 150000.00, 8])
    xlsx_path = HERE / "departments.xlsx"
    wb.save(xlsx_path)
    print(f"✓ Excel fixture: {xlsx_path}")
except ImportError:
    print("SKIP: openpyxl not installed — pip install openpyxl")

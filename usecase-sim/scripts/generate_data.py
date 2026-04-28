"""Generate sample input data into local filesystem, MinIO, and RDBMS source_orders.

Single entrypoint that replaces:
    data/generate_sample_data.py
    docker/bootstrap_minio.py
    docker/bootstrap_{postgres,mysql,mssql,oracle}.py

Targets (comma-separated via --targets):
    local     : write CSV/JSON/JSONL/Parquet/Parquet_dated/Parquet_hive/Avro/Excel/SQLite
                under usecase-sim/data/input/**
    minio     : upload data/input/** → s3://datacoolie-test/input/**
    pg        : create + seed source_orders in PostgreSQL
    mysql     : same for MySQL
    mssql     : same for SQL Server
    oracle    : same for Oracle

Usage:
    python usecase-sim/scripts/generate_data.py                          # all reachable targets
    python usecase-sim/scripts/generate_data.py --targets local,minio
    python usecase-sim/scripts/generate_data.py --targets pg
"""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from _common import (  # noqa: E402
    INPUT_DIR,
    MINIO_BUCKET,
    SAMPLE_COLUMNS,
    SAMPLE_ROWS,
    SOURCE_ORDERS_ROWS,
    ensure_bucket,
    port_open,
    setup_logging,
    upload_tree,
)

logger = setup_logging("generate_data")

ALL_TARGETS = ["local", "minio", "pg", "mysql", "mssql", "oracle"]


# ===========================================================================
# Local file writers
# ===========================================================================

def _write_csv(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(SAMPLE_COLUMNS)
        w.writerows(rows)
    logger.info("CSV   → %s (%d rows)", path, len(rows))


def _write_json(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [dict(zip(SAMPLE_COLUMNS, r)) for r in rows]
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)
    logger.info("JSON  → %s (%d rows)", path, len(rows))


def _write_jsonl(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(dict(zip(SAMPLE_COLUMNS, r)), ensure_ascii=False) + "\n")
    logger.info("JSONL → %s (%d rows)", path, len(rows))


def _write_parquet(rows, path: Path) -> None:
    import pyarrow as pa  # noqa: PLC0415
    import pyarrow.parquet as pq  # noqa: PLC0415
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows))
    table = pa.table({
        "order_id":      pa.array(cols[0], type=pa.int64()),
        "order_date":    pa.array(cols[1], type=pa.string()),
        "amount":        pa.array(cols[2], type=pa.string()),
        "quantity":      pa.array(cols[3], type=pa.int64()),
        "region":        pa.array(cols[4], type=pa.string()),
        "modified_at":   pa.array(cols[5], type=pa.string()),
        "customer_name": pa.array(cols[6], type=pa.string()),
        "status":        pa.array(cols[7], type=pa.string()),
    })
    pq.write_table(table, path)
    logger.info("PQT   → %s (%d rows)", path, len(rows))


def _write_parquet_dated(rows, base: Path) -> None:
    buckets: dict = defaultdict(list)
    for r in rows:
        y, m, d = str(r[1]).split("-")
        buckets[(y, m, d)].append(r)
    for (y, m, d), sub in sorted(buckets.items()):
        _write_parquet(sub, base / y / m / d / "data.parquet")


def _write_parquet_hive(rows, base: Path) -> None:
    buckets: dict = defaultdict(list)
    for r in rows:
        buckets[str(r[4])].append(r)
    for region, sub in sorted(buckets.items()):
        _write_parquet(sub, base / f"region={region}" / "data.parquet")


def _write_avro(rows, path: Path) -> None:
    try:
        import fastavro  # noqa: PLC0415
    except ImportError:
        logger.warning("fastavro not installed — skipping Avro")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "order_id", "type": "long"},
            {"name": "order_date", "type": "string"},
            {"name": "amount", "type": "string"},
            {"name": "quantity", "type": "long"},
            {"name": "region", "type": "string"},
            {"name": "modified_at", "type": "string"},
            {"name": "customer_name", "type": "string"},
            {"name": "status", "type": "string"},
        ],
    }
    records = [dict(zip(SAMPLE_COLUMNS, r)) for r in rows]
    with path.open("wb") as f:
        fastavro.writer(f, fastavro.parse_schema(schema), records)
    logger.info("AVRO  → %s (%d rows)", path, len(rows))


def _write_excel(rows, path: Path) -> None:
    try:
        from openpyxl import Workbook  # noqa: PLC0415
    except ImportError:
        logger.warning("openpyxl not installed — skipping XLSX")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "Orders"
    ws.append(list(SAMPLE_COLUMNS))
    for r in rows:
        ws.append(list(r))
    wb.save(path)
    logger.info("XLSX  → %s (%d rows)", path, len(rows))


def _write_sqlite(rows, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    try:
        con.execute(
            "CREATE TABLE IF NOT EXISTS orders ("
            "order_id INTEGER, order_date TEXT, amount TEXT, quantity INTEGER, "
            "region TEXT, modified_at TEXT, customer_name TEXT, status TEXT)"
        )
        con.execute("DELETE FROM orders")
        con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", rows)
        con.commit()
    finally:
        con.close()
    logger.info("SQLT  → %s (%d rows)", path, len(rows))


def generate_local() -> None:
    logger.info("--- target: local (%s) ---", INPUT_DIR)
    _write_csv  (SAMPLE_ROWS, INPUT_DIR / "csv"     / "sample" / "sample.csv")
    _write_json (SAMPLE_ROWS, INPUT_DIR / "json"    / "sample" / "sample.json")
    _write_jsonl(SAMPLE_ROWS, INPUT_DIR / "jsonl"   / "sample" / "sample.jsonl")
    _write_parquet(SAMPLE_ROWS, INPUT_DIR / "parquet" / "sample" / "sample.parquet")
    _write_avro (SAMPLE_ROWS, INPUT_DIR / "avro"    / "sample" / "sample.avro")
    _write_excel(SAMPLE_ROWS, INPUT_DIR / "excel"   / "sample" / "sample.xlsx")
    _write_sqlite(SAMPLE_ROWS, INPUT_DIR / "sqlite" / "orders.db")
    _write_parquet_dated(SAMPLE_ROWS, INPUT_DIR / "parquet_dated" / "sample")
    _write_parquet_hive (SAMPLE_ROWS, INPUT_DIR / "parquet_hive"  / "sample")


# ===========================================================================
# MinIO upload
# ===========================================================================

def generate_minio() -> None:
    logger.info("--- target: minio ---")
    if not port_open("localhost", 9000):
        logger.warning("MinIO not reachable at localhost:9000 — skipping")
        return
    if not ensure_bucket(bucket=MINIO_BUCKET):
        return
    upload_tree(INPUT_DIR, "input", bucket=MINIO_BUCKET)


# ===========================================================================
# RDBMS source_orders seeders
# ===========================================================================

def _seed_pg(host="localhost", port=5432, db="datacoolie",
             user="datacoolie", pw="datacoolie") -> None:
    import psycopg2  # noqa: PLC0415
    import psycopg2.extras  # noqa: PLC0415
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS source_orders (
                    order_id    TEXT PRIMARY KEY,
                    order_date  DATE NOT NULL,
                    amount      NUMERIC(18,2) NOT NULL,
                    quantity    INTEGER NOT NULL,
                    region      TEXT NOT NULL,
                    modified_at TIMESTAMP NOT NULL
                )
            """)
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO source_orders VALUES %s ON CONFLICT (order_id) DO NOTHING",
                SOURCE_ORDERS_ROWS,
            )
            cur.execute("SELECT count(*) FROM source_orders")
            logger.info("pg source_orders: %d rows", cur.fetchone()[0])
    finally:
        conn.close()


def _seed_mysql(host="localhost", port=3306, db="datacoolie",
                user="datacoolie", pw="datacoolie") -> None:
    import pymysql  # noqa: PLC0415
    conn = pymysql.connect(host=host, port=port, db=db, user=user, password=pw, charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS source_orders (
                    order_id    VARCHAR(50) PRIMARY KEY,
                    order_date  DATE NOT NULL,
                    amount      DECIMAL(18,2) NOT NULL,
                    quantity    INT NOT NULL,
                    region      VARCHAR(50) NOT NULL,
                    modified_at DATETIME NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)
            cur.executemany(
                "INSERT IGNORE INTO source_orders "
                "(order_id, order_date, amount, quantity, region, modified_at) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                SOURCE_ORDERS_ROWS,
            )
            conn.commit()
            cur.execute("SELECT count(*) FROM source_orders")
            logger.info("mysql source_orders: %d rows", cur.fetchone()[0])
    finally:
        conn.close()


def _seed_mssql(host="localhost", port=1433, db="datacoolie",
                user="sa", pw="Datacoolie@1") -> None:
    import pymssql  # noqa: PLC0415
    # Ensure database exists via master
    master = pymssql.connect(server=host, port=port, database="master", user=user, password=pw)
    try:
        master.autocommit(True)
        with master.cursor() as cur:
            safe = db.replace("]", "]]")
            cur.execute(f"IF DB_ID(N'{safe}') IS NULL CREATE DATABASE [{safe}]")
    finally:
        master.close()

    conn = pymssql.connect(server=host, port=port, database=db, user=user, password=pw)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                IF OBJECT_ID('dbo.source_orders', 'U') IS NULL
                CREATE TABLE dbo.source_orders (
                    order_id    NVARCHAR(50) PRIMARY KEY,
                    order_date  DATE NOT NULL,
                    amount      DECIMAL(18,2) NOT NULL,
                    quantity    INT NOT NULL,
                    region      NVARCHAR(50) NOT NULL,
                    modified_at DATETIME2 NOT NULL
                )
            """)
            conn.commit()
            for r in SOURCE_ORDERS_ROWS:
                cur.execute(
                    "INSERT INTO dbo.source_orders "
                    "(order_id, order_date, amount, quantity, region, modified_at) "
                    "SELECT %s,%s,%s,%s,%s,%s "
                    "WHERE NOT EXISTS (SELECT 1 FROM dbo.source_orders WHERE order_id = %s)",
                    (*r, r[0]),
                )
            conn.commit()
            cur.execute("SELECT count(*) FROM dbo.source_orders")
            logger.info("mssql source_orders: %d rows", cur.fetchone()[0])
    finally:
        conn.close()


def _seed_oracle(host="localhost", port=1521, service="FREEPDB1",
                 user="datacoolie", pw="datacoolie") -> None:
    import oracledb  # noqa: PLC0415
    conn = oracledb.connect(user=user, password=pw, dsn=f"{host}:{port}/{service}")
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DECLARE v_cnt NUMBER;
                BEGIN
                    SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name='SOURCE_ORDERS';
                    IF v_cnt = 0 THEN
                        EXECUTE IMMEDIATE '
                            CREATE TABLE source_orders (
                                order_id    VARCHAR2(50) PRIMARY KEY,
                                order_date  DATE NOT NULL,
                                amount      NUMBER(18,2) NOT NULL,
                                quantity    NUMBER(10) NOT NULL,
                                region      VARCHAR2(50) NOT NULL,
                                modified_at TIMESTAMP NOT NULL
                            )';
                    END IF;
                END;
            """)
            cur.executemany(
                """
                MERGE INTO source_orders tgt
                USING (SELECT :1 AS order_id, TO_DATE(:2,'YYYY-MM-DD') AS order_date,
                              :3 AS amount, :4 AS quantity, :5 AS region,
                              TO_TIMESTAMP(:6,'YYYY-MM-DD HH24:MI:SS') AS modified_at
                       FROM dual) src
                ON (tgt.order_id = src.order_id)
                WHEN NOT MATCHED THEN
                    INSERT (order_id, order_date, amount, quantity, region, modified_at)
                    VALUES (src.order_id, src.order_date, src.amount, src.quantity,
                            src.region, src.modified_at)
                """,
                SOURCE_ORDERS_ROWS,
            )
            conn.commit()
            cur.execute("SELECT count(*) FROM source_orders")
            logger.info("oracle source_orders: %d rows", cur.fetchone()[0])
    finally:
        conn.close()


_RDBMS_SEEDERS = {
    "pg":     ("localhost", 5432, _seed_pg),
    "mysql":  ("localhost", 3306, _seed_mysql),
    "mssql":  ("localhost", 1433, _seed_mssql),
    "oracle": ("localhost", 1521, _seed_oracle),
}


def generate_rdbms(target: str) -> None:
    host, port, seeder = _RDBMS_SEEDERS[target]
    logger.info("--- target: %s ---", target)
    if not port_open(host, port):
        logger.warning("%s not reachable at %s:%d — skipping", target, host, port)
        return
    try:
        seeder()
    except Exception as exc:
        logger.warning("%s seed failed: %s", target, exc)


# ===========================================================================
# Driver
# ===========================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Generate sample input data")
    parser.add_argument("--targets", default=",".join(ALL_TARGETS),
                        help="Comma-separated: " + ", ".join(ALL_TARGETS))
    args = parser.parse_args()

    requested = [t.strip().lower() for t in args.targets.split(",") if t.strip()]
    unknown = [t for t in requested if t not in ALL_TARGETS]
    if unknown:
        logger.error("Unknown targets: %s. Known: %s", unknown, ALL_TARGETS)
        return 2

    if "local" in requested:
        generate_local()
    if "minio" in requested:
        generate_minio()
    for t in ("pg", "mysql", "mssql", "oracle"):
        if t in requested:
            generate_rdbms(t)

    logger.info("=== generate_data complete ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())

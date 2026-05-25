"""
Seed Iceberg tables via Trino after docker compose starts.
Creates two schemas with sample tables + data for lakehouse discovery testing.

Requirements: pip install trino
Run from: datacoolie/ai/skills/tests/
Usage:    python fixtures/iceberg/seed_iceberg.py
"""
import sys
import time

try:
    import trino
except ImportError:
    print("ERROR: trino package not installed. Run: pip install trino")
    sys.exit(1)

HOST = "localhost"
PORT = 8090
CATALOG = "iceberg"

SETUP_SQL = [
    # -----------------------------------------------------------------------
    # Schema: sales — transactional data with partition
    # -----------------------------------------------------------------------
    "CREATE SCHEMA IF NOT EXISTS iceberg.sales",

    """CREATE TABLE IF NOT EXISTS iceberg.sales.transactions (
        txn_id      BIGINT,
        customer_id INTEGER,
        amount      DECIMAL(10,2),
        txn_date    DATE,
        region      VARCHAR,
        category    VARCHAR
    ) WITH (
        partitioning = ARRAY['region', 'month(txn_date)']
    )""",

    """CREATE TABLE IF NOT EXISTS iceberg.sales.customers (
        customer_id  INTEGER,
        full_name    VARCHAR,
        email        VARCHAR,
        country      VARCHAR,
        tier         VARCHAR,
        created_at   TIMESTAMP
    )""",

    # -----------------------------------------------------------------------
    # Schema: analytics — aggregated/reporting tables
    # -----------------------------------------------------------------------
    "CREATE SCHEMA IF NOT EXISTS iceberg.analytics",

    """CREATE TABLE IF NOT EXISTS iceberg.analytics.daily_revenue (
        report_date   DATE,
        region        VARCHAR,
        total_revenue DECIMAL(14,2),
        order_count   INTEGER
    ) WITH (
        partitioning = ARRAY['month(report_date)']
    )""",

    """CREATE TABLE IF NOT EXISTS iceberg.analytics.product_metrics (
        product_id    INTEGER,
        product_name  VARCHAR,
        category      VARCHAR,
        units_sold    INTEGER,
        revenue       DECIMAL(14,2),
        period_start  DATE,
        period_end    DATE
    )""",
]

INSERT_SQL = [
    # Transactions
    """INSERT INTO iceberg.sales.transactions VALUES
        (1, 101, 199.99, DATE '2024-01-15', 'APAC',  'Electronics'),
        (2, 102, 49.50,  DATE '2024-01-16', 'EMEA',  'Apparel'),
        (3, 103, 999.00, DATE '2024-01-17', 'AMER',  'Electronics'),
        (4, 104, 25.00,  DATE '2024-02-01', 'APAC',  'Food'),
        (5, 105, 150.00, DATE '2024-02-03', 'AMER',  'Apparel'),
        (6, 101, 75.00,  DATE '2024-02-10', 'APAC',  'Food'),
        (7, 106, 500.00, DATE '2024-03-05', 'EMEA',  'Electronics'),
        (8, 107, 35.00,  DATE '2024-03-12', 'AMER',  'Apparel')
    """,

    # Customers
    """INSERT INTO iceberg.sales.customers VALUES
        (101, 'Alice Johnson',  'alice@example.com',   'AU', 'Gold',   TIMESTAMP '2022-03-01 10:00:00'),
        (102, 'Bob Schmidt',    'bob@example.de',      'DE', 'Silver', TIMESTAMP '2022-05-15 09:30:00'),
        (103, 'Carol Smith',    'carol@example.com',   'US', 'Gold',   TIMESTAMP '2021-11-20 14:00:00'),
        (104, 'David Lin',      'david@example.cn',    'CN', 'Bronze', TIMESTAMP '2023-01-07 08:15:00'),
        (105, 'Eva Brown',      'eva@example.com',     'CA', 'Silver', TIMESTAMP '2022-08-30 11:45:00'),
        (106, 'Frank Mueller',  'frank@example.de',    'DE', 'Gold',   TIMESTAMP '2021-07-12 16:20:00'),
        (107, 'Grace Lee',      'grace@example.com',   'US', 'Bronze', TIMESTAMP '2023-04-18 13:00:00')
    """,

    # Daily revenue
    """INSERT INTO iceberg.analytics.daily_revenue VALUES
        (DATE '2024-01-15', 'APAC',  199.99, 1),
        (DATE '2024-01-16', 'EMEA',   49.50, 1),
        (DATE '2024-01-17', 'AMER',  999.00, 1),
        (DATE '2024-02-01', 'APAC',   25.00, 1),
        (DATE '2024-02-03', 'AMER',  150.00, 1),
        (DATE '2024-02-10', 'APAC',   75.00, 1),
        (DATE '2024-03-05', 'EMEA',  500.00, 1),
        (DATE '2024-03-12', 'AMER',   35.00, 1)
    """,

    # Product metrics
    """INSERT INTO iceberg.analytics.product_metrics VALUES
        (1,  'Laptop Pro 15',  'Electronics', 42, 41999.58, DATE '2024-01-01', DATE '2024-03-31'),
        (2,  'Wireless Earbuds','Electronics', 128, 12800.00, DATE '2024-01-01', DATE '2024-03-31'),
        (3,  'Running Shoes',  'Apparel',     89,  8010.00,  DATE '2024-01-01', DATE '2024-03-31'),
        (4,  'Winter Jacket',  'Apparel',     34,  5100.00,  DATE '2024-01-01', DATE '2024-03-31'),
        (5,  'Protein Bars',   'Food',        210, 3150.00,  DATE '2024-01-01', DATE '2024-03-31')
    """,
]


def wait_for_trino(max_retries: int = 20, delay: int = 5) -> trino.dbapi.Connection:
    """Poll Trino until it accepts connections."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = trino.dbapi.connect(host=HOST, port=PORT, user="admin", catalog=CATALOG)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            cur.fetchall()
            print(f"  Trino ready (attempt {attempt})")
            return conn
        except Exception as exc:
            print(f"  Waiting for Trino... [{attempt}/{max_retries}] ({exc})")
            time.sleep(delay)
    print("ERROR: Trino did not become ready in time.")
    sys.exit(1)


def run_sql(conn: trino.dbapi.Connection, statements: list[str]) -> None:
    cur = conn.cursor()
    for sql in statements:
        short = sql.strip().splitlines()[0][:80]
        print(f"  -> {short}")
        try:
            cur.execute(sql)
            cur.fetchall()  # consume result set
        except Exception as exc:
            # Table/schema already exists — skip
            if "already exists" in str(exc).lower():
                print(f"     (already exists, skipping)")
            else:
                print(f"     ERROR: {exc}")
                raise


def ensure_minio_bucket(bucket: str = "skills-test") -> None:
    """Create the MinIO bucket using boto3 (S3-compatible API)."""
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        print("  WARN: boto3 not installed — skipping bucket creation")
        return

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9010",   # MinIO host port
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=bucket)
        print(f"  MinIO bucket '{bucket}' already exists")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket)
            print(f"  Created MinIO bucket '{bucket}'")
        else:
            print(f"  WARN: unexpected bucket check error ({code}) — continuing")


if __name__ == "__main__":
    print("=== Seeding Iceberg tables via Trino ===")
    ensure_minio_bucket()
    conn = wait_for_trino()
    print("Running DDL...")
    run_sql(conn, SETUP_SQL)
    print("Inserting sample data...")
    run_sql(conn, INSERT_SQL)
    print("Done. Schemas: iceberg.sales, iceberg.analytics")
    print("Tables: transactions, customers, daily_revenue, product_metrics")

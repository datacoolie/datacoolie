---
title: Set up DataCoolie Studio | DataCoolie
description: Set up DataCoolie Studio with practical steps, screenshots and links to focused Studio workflows.
---

# Set up DataCoolie Studio

[Studio overview](index.md).

## Run Studio locally

Install the published package and start the local web app:

```powershell
python -m pip install datacoolie-studio
datacoolie-studio
```

The launcher creates the local workspace on first run, starts Studio at
`http://127.0.0.1:8765`, and opens it in your browser. Because it binds to the
loopback interface by default, other machines cannot connect unless you change
the host setting.

### Add only the cloud integrations you need

```powershell
python -m pip install "datacoolie-studio[s3]"       # Amazon S3
python -m pip install "datacoolie-studio[minio]"    # MinIO
python -m pip install "datacoolie-studio[adls]"     # Azure Data Lake Storage
python -m pip install "datacoolie-studio[onelake]"   # Microsoft OneLake / Fabric
python -m pip install "datacoolie-studio[gcs]"      # Google Cloud Storage
python -m pip install "datacoolie-studio[cloud]"    # All optional cloud providers above
```

Databricks SDK support is included in the base installation. Use
`datacoolie-studio[all]` when you also need every cloud integration and the
development, test, and packaging tools.

### Develop from source

Use the public repository only when you need to inspect or modify Studio:

=== "Windows PowerShell"

    ```powershell
    git clone https://github.com/datacoolie/datacoolie-studio.git
    cd datacoolie-studio
    py -3.11 -m venv .venv
    .\.venv\Scripts\Activate.ps1
    python -m pip install --upgrade pip
    python -m pip install -e .
    datacoolie-studio
    ```

=== "macOS or Linux"

    ```bash
    git clone https://github.com/datacoolie/datacoolie-studio.git
    cd datacoolie-studio
    python3.11 -m venv .venv
    source .venv/bin/activate
    python -m pip install --upgrade pip
    python -m pip install -e .
    datacoolie-studio
    ```

### Common launcher options

```powershell
datacoolie-studio --port 8765
datacoolie-studio --host 127.0.0.1
datacoolie-studio --data-dir .\.scratch\studio
datacoolie-studio --database-url "sqlite:///D:/data/datacoolie-studio/db/studio.db"
datacoolie-studio --no-open
datacoolie-studio --no-banner
```

The Studio workspace database currently supports synchronous SQLite URLs only.
PostgreSQL, MySQL, and other database types can be connected as user sources;
they are not engines for Studio's own workspace state.

Use `--no-open` when another process manages the browser. Keep the default
loopback host unless you have reviewed authentication, network access, database
sharing, and source permissions.

### Local workspace

By default, Studio stores its own state under
`~\.datacoolie\datacoolie-studio\`:

```text
db\studio.db
backups\
cache\read-models.sqlite3
cache\analytics.duckdb
cache\source-materializations\
logs\
```

These paths contain Studio state, backups, and disposable derived data. They do
not replace the original metadata, code, or execution and system logs you
configured as sources.

## Create your first workspace

1. Create a **Project**.
2. Add an **Environment**, such as `dev`, `test`, or `prod`.
3. Add a metadata file or scan a DataCoolie project.
4. Optionally add execution and system logs for Monitoring.
5. Optionally add Python code artifacts referenced by metadata.
6. Open **Metadata**, **Assets**, **Lineage**, or **Monitoring**.

Metadata is required. Logs and code artifacts are optional, so you can adopt
Studio incrementally.

## Troubleshooting the first workspace

| Symptom | Check |
|---|---|
| Metadata page is empty | Confirm at least one metadata source is enabled, readable, synchronized, and points to JSON, YAML, XLSX, or a project metadata folder. |
| Lineage is incomplete | Add referenced SQL/Python code, review unresolved references in Assets, and configure project reference mappings where automatic resolution is ambiguous. |
| Monitoring has no evidence | Add the correct execution and system log paths, validate them, synchronize the source, and confirm the selected date range includes runs. |
| Cloud source cannot be read | Install the matching provider extra, save the credential through Studio, test the connection, and verify URI and provider permissions. |
| Operational metrics look incomplete | Open Monitoring · Diagnostics and check linkage, evidence coverage, read/cache warnings, and log-source currency. |
| Browser does not open | Start with `datacoolie-studio --no-open`, then open `http://127.0.0.1:8765` manually. |

!!! warning "Before sharing Studio on a network"
    Studio is local-first and binds to `127.0.0.1` by default. Before changing
    the host or serving multiple users, review authentication, network access,
    the database configuration, and permissions for every connected source.

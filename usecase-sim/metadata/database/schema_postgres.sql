-- DataCoolie ETL Framework — PostgreSQL Metadata Schema
-- PostgreSQL 13+ compatible.  Column names (format, database, precision) do not
-- require quoting in PostgreSQL but are retained without quotes for clarity.

-- ============================================================================
-- dc_framework_connections
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_connections (
    connection_id   VARCHAR(36)  PRIMARY KEY,
    workspace_id    VARCHAR(36)  NOT NULL,
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    connection_type VARCHAR(50)  NOT NULL,
    format          VARCHAR(50)  NOT NULL,
    catalog         VARCHAR(200),
    database        VARCHAR(200),
    configure       TEXT,
    secrets_ref     TEXT,
    is_active       BOOLEAN  DEFAULT TRUE,
    version         INTEGER  DEFAULT 1,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    deleted_at      TIMESTAMP,
    created_by      VARCHAR(100),
    updated_by      VARCHAR(100)
);

-- ============================================================================
-- dc_framework_dataflows
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_dataflows (
    dataflow_id               VARCHAR(36)  PRIMARY KEY,
    workspace_id              VARCHAR(36)  NOT NULL,
    name                      VARCHAR(200) NOT NULL,
    description               TEXT,
    stage                     VARCHAR(100),
    group_number              INTEGER,
    execution_order           INTEGER,
    processing_mode           VARCHAR(20),

    source_connection_id      VARCHAR(36)  NOT NULL,
    source_schema             VARCHAR(100),
    source_table              VARCHAR(200),
    source_query              TEXT,
    source_python_function    VARCHAR(500),
    source_watermark_columns  TEXT,
    source_configure          TEXT,

    transform                 TEXT,

    destination_connection_id VARCHAR(36)  NOT NULL,
    destination_schema        VARCHAR(100),
    destination_table         VARCHAR(200) NOT NULL,
    destination_load_type     VARCHAR(50)  NOT NULL,
    destination_merge_keys    TEXT,
    destination_configure     TEXT,

    configure                 TEXT,

    is_active                 BOOLEAN  DEFAULT TRUE,
    version                   INTEGER  DEFAULT 1,
    created_at                TIMESTAMP,
    updated_at                TIMESTAMP,
    deleted_at                TIMESTAMP,
    created_by                VARCHAR(100),
    updated_by                VARCHAR(100)
);

-- ============================================================================
-- dc_framework_watermarks
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_watermarks (
    watermark_id    VARCHAR(36)  PRIMARY KEY,
    dataflow_id     VARCHAR(36)  NOT NULL UNIQUE,
    current_value   TEXT,
    previous_value  TEXT,
    job_id          VARCHAR(100),
    dataflow_run_id VARCHAR(100),
    updated_at      TIMESTAMP
);

-- ============================================================================
-- dc_framework_schema_hints
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_schema_hints (
    schema_hint_id   VARCHAR(36)  PRIMARY KEY,
    connection_id    VARCHAR(36)  NOT NULL,
    dataflow_id      VARCHAR(36),
    schema_name      VARCHAR(100),
    table_name       VARCHAR(200),
    column_name      VARCHAR(200) NOT NULL,
    data_type        VARCHAR(50)  NOT NULL,
    format           VARCHAR(200),
    precision        INTEGER,
    scale            INTEGER,
    default_value    VARCHAR(200),
    ordinal_position INTEGER,
    is_active        BOOLEAN  DEFAULT TRUE,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    deleted_at       TIMESTAMP
);

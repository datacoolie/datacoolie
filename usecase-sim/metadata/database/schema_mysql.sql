-- DataCoolie ETL Framework — MySQL Metadata Schema
-- MySQL 8.x compatible.  Reserved words (`format`, `precision`, `database`)
-- are quoted with backticks throughout.

-- ============================================================================
-- dc_framework_connections
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_connections (
    connection_id   VARCHAR(36)  PRIMARY KEY,
    workspace_id    VARCHAR(36)  NOT NULL,
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    connection_type VARCHAR(50)  NOT NULL,
    `format`        VARCHAR(50)  NOT NULL,
    catalog         VARCHAR(200),
    `database`      VARCHAR(200),
    configure       TEXT,
    secrets_ref     TEXT,
    is_active       TINYINT(1)   DEFAULT 1,
    version         INT          DEFAULT 1,
    created_at      DATETIME,
    updated_at      DATETIME,
    deleted_at      DATETIME,
    created_by      VARCHAR(100),
    updated_by      VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================================
-- dc_framework_dataflows
-- ============================================================================
CREATE TABLE IF NOT EXISTS dc_framework_dataflows (
    dataflow_id               VARCHAR(36)  PRIMARY KEY,
    workspace_id              VARCHAR(36)  NOT NULL,
    name                      VARCHAR(200) NOT NULL,
    description               TEXT,
    stage                     VARCHAR(100),
    group_number              INT,
    execution_order           INT,
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

    is_active                 TINYINT(1)   DEFAULT 1,
    version                   INT          DEFAULT 1,
    created_at                DATETIME,
    updated_at                DATETIME,
    deleted_at                DATETIME,
    created_by                VARCHAR(100),
    updated_by                VARCHAR(100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    updated_at      DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
    `format`         VARCHAR(200),
    `precision`      INT,
    scale            INT,
    default_value    VARCHAR(200),
    ordinal_position INT,
    is_active        TINYINT(1)   DEFAULT 1,
    created_at       DATETIME,
    updated_at       DATETIME,
    deleted_at       DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- DataCoolie ETL Framework — SQL Server Metadata Schema
-- SQL Server 2019+ / Azure SQL compatible.
-- Uses IF NOT EXISTS pattern via existence check.

-- ============================================================================
-- dc_framework_connections
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dc_framework_connections')
CREATE TABLE dc_framework_connections (
    connection_id   NVARCHAR(36)  PRIMARY KEY,
    workspace_id    NVARCHAR(36)  NOT NULL,
    name            NVARCHAR(100) NOT NULL,
    description     NVARCHAR(MAX),
    connection_type NVARCHAR(50)  NOT NULL,
    format          NVARCHAR(50)  NOT NULL,
    catalog         NVARCHAR(200),
    [database]      NVARCHAR(200),
    configure       NVARCHAR(MAX),
    secrets_ref     NVARCHAR(MAX),
    is_active       BIT           DEFAULT 1,
    version         INT           DEFAULT 1,
    created_at      DATETIME2,
    updated_at      DATETIME2,
    deleted_at      DATETIME2,
    created_by      NVARCHAR(100),
    updated_by      NVARCHAR(100)
);

-- ============================================================================
-- dc_framework_dataflows
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dc_framework_dataflows')
CREATE TABLE dc_framework_dataflows (
    dataflow_id               NVARCHAR(36)  PRIMARY KEY,
    workspace_id              NVARCHAR(36)  NOT NULL,
    name                      NVARCHAR(200) NOT NULL,
    description               NVARCHAR(MAX),
    stage                     NVARCHAR(100),
    group_number              INT,
    execution_order           INT,
    processing_mode           NVARCHAR(20),

    source_connection_id      NVARCHAR(36)  NOT NULL,
    source_schema             NVARCHAR(100),
    source_table              NVARCHAR(200),
    source_query              NVARCHAR(MAX),
    source_python_function    NVARCHAR(500),
    source_watermark_columns  NVARCHAR(MAX),
    source_configure          NVARCHAR(MAX),

    transform                 NVARCHAR(MAX),

    destination_connection_id NVARCHAR(36)  NOT NULL,
    destination_schema        NVARCHAR(100),
    destination_table         NVARCHAR(200) NOT NULL,
    destination_load_type     NVARCHAR(50)  NOT NULL,
    destination_merge_keys    NVARCHAR(MAX),
    destination_configure     NVARCHAR(MAX),

    configure                 NVARCHAR(MAX),

    is_active                 BIT           DEFAULT 1,
    version                   INT           DEFAULT 1,
    created_at                DATETIME2,
    updated_at                DATETIME2,
    deleted_at                DATETIME2,
    created_by                NVARCHAR(100),
    updated_by                NVARCHAR(100)
);

-- ============================================================================
-- dc_framework_watermarks
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dc_framework_watermarks')
CREATE TABLE dc_framework_watermarks (
    watermark_id    NVARCHAR(36)  PRIMARY KEY,
    dataflow_id     NVARCHAR(36)  NOT NULL UNIQUE,
    current_value   NVARCHAR(MAX),
    previous_value  NVARCHAR(MAX),
    job_id          NVARCHAR(100),
    dataflow_run_id NVARCHAR(100),
    updated_at      DATETIME2
);

-- ============================================================================
-- dc_framework_schema_hints
-- ============================================================================
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'dc_framework_schema_hints')
CREATE TABLE dc_framework_schema_hints (
    schema_hint_id   NVARCHAR(36)  PRIMARY KEY,
    connection_id    NVARCHAR(36)  NOT NULL,
    dataflow_id      NVARCHAR(36),
    schema_name      NVARCHAR(100),
    table_name       NVARCHAR(200),
    column_name      NVARCHAR(200) NOT NULL,
    data_type        NVARCHAR(50)  NOT NULL,
    format           NVARCHAR(200),
    [precision]      INT,
    scale            INT,
    default_value    NVARCHAR(200),
    ordinal_position INT,
    is_active        BIT           DEFAULT 1,
    created_at       DATETIME2,
    updated_at       DATETIME2,
    deleted_at       DATETIME2
);

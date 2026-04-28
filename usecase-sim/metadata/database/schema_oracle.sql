-- DataCoolie ETL Framework — Oracle Metadata Schema
-- Oracle 23c Free / Oracle XE compatible.
-- Uses PL/SQL blocks for IF NOT EXISTS pattern (Oracle lacks native IF NOT EXISTS in DDL).

-- ============================================================================
-- dc_framework_connections
-- ============================================================================
DECLARE
    v_cnt NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'dc_framework_CONNECTIONS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE dc_framework_connections (
                connection_id   VARCHAR2(36)   PRIMARY KEY,
                workspace_id    VARCHAR2(36)   NOT NULL,
                name            VARCHAR2(100)  NOT NULL,
                description     CLOB,
                connection_type VARCHAR2(50)   NOT NULL,
                format          VARCHAR2(50)   NOT NULL,
                catalog         VARCHAR2(200),
                database        VARCHAR2(200),
                configure       CLOB,
                secrets_ref     CLOB,
                is_active       NUMBER(1)      DEFAULT 1,
                version         NUMBER(10)     DEFAULT 1,
                created_at      TIMESTAMP,
                updated_at      TIMESTAMP,
                deleted_at      TIMESTAMP,
                created_by      VARCHAR2(100),
                updated_by      VARCHAR2(100)
            )
        ';
    END IF;
END;
/

-- ============================================================================
-- dc_framework_dataflows
-- ============================================================================
DECLARE
    v_cnt NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'dc_framework_DATAFLOWS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE dc_framework_dataflows (
                dataflow_id               VARCHAR2(36)   PRIMARY KEY,
                workspace_id              VARCHAR2(36)   NOT NULL,
                name                      VARCHAR2(200)  NOT NULL,
                description               CLOB,
                stage                     VARCHAR2(100),
                group_number              NUMBER(10),
                execution_order           NUMBER(10),
                processing_mode           VARCHAR2(20),

                source_connection_id      VARCHAR2(36)   NOT NULL,
                source_schema             VARCHAR2(100),
                source_table              VARCHAR2(200),
                source_query              CLOB,
                source_python_function    VARCHAR2(500),
                source_watermark_columns  CLOB,
                source_configure          CLOB,

                transform                 CLOB,

                destination_connection_id VARCHAR2(36)   NOT NULL,
                destination_schema        VARCHAR2(100),
                destination_table         VARCHAR2(200)  NOT NULL,
                destination_load_type     VARCHAR2(50)   NOT NULL,
                destination_merge_keys    CLOB,
                destination_configure     CLOB,

                configure                 CLOB,

                is_active                 NUMBER(1)      DEFAULT 1,
                version                   NUMBER(10)     DEFAULT 1,
                created_at                TIMESTAMP,
                updated_at                TIMESTAMP,
                deleted_at                TIMESTAMP,
                created_by                VARCHAR2(100),
                updated_by                VARCHAR2(100)
            )
        ';
    END IF;
END;
/

-- ============================================================================
-- dc_framework_watermarks
-- ============================================================================
DECLARE
    v_cnt NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'dc_framework_WATERMARKS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE dc_framework_watermarks (
                watermark_id    VARCHAR2(36)   PRIMARY KEY,
                dataflow_id     VARCHAR2(36)   NOT NULL UNIQUE,
                current_value   CLOB,
                previous_value  CLOB,
                job_id          VARCHAR2(100),
                dataflow_run_id VARCHAR2(100),
                updated_at      TIMESTAMP
            )
        ';
    END IF;
END;
/

-- ============================================================================
-- dc_framework_schema_hints
-- ============================================================================
DECLARE
    v_cnt NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_cnt FROM user_tables WHERE table_name = 'dc_framework_SCHEMA_HINTS';
    IF v_cnt = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE dc_framework_schema_hints (
                schema_hint_id   VARCHAR2(36)   PRIMARY KEY,
                connection_id    VARCHAR2(36)   NOT NULL,
                dataflow_id      VARCHAR2(36),
                schema_name      VARCHAR2(100),
                table_name       VARCHAR2(200),
                column_name      VARCHAR2(200)  NOT NULL,
                data_type        VARCHAR2(50)   NOT NULL,
                format           VARCHAR2(200),
                precision        NUMBER(10),
                scale            NUMBER(10),
                default_value    VARCHAR2(200),
                ordinal_position NUMBER(10),
                is_active        NUMBER(1)      DEFAULT 1,
                created_at       TIMESTAMP,
                updated_at       TIMESTAMP,
                deleted_at       TIMESTAMP
            )
        ';
    END IF;
END;
/

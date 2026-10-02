-- Preflight. The default schema is resolved for the connected login.
SELECT OBJECT_SCHEMA_NAME(object_id) AS table_schema, name
  FROM sys.tables
 WHERE name = 'dc_framework_dataflows';

SELECT s.name AS table_schema, c.name AS column_name, t.name AS data_type,
       c.is_nullable
  FROM sys.columns c
  JOIN sys.tables t0 ON t0.object_id = c.object_id
  JOIN sys.schemas s ON s.schema_id = t0.schema_id
  JOIN sys.types t ON t.user_type_id = c.user_type_id
 WHERE t0.name = 'dc_framework_dataflows'
   AND c.name = 'source_filter_expression';

-- Apply once after confirming source_filter_expression is absent. Change dbo
-- to the schema returned by the preflight when required.
IF COL_LENGTH('dbo.dc_framework_dataflows', 'source_filter_expression') IS NULL
    ALTER TABLE dbo.dc_framework_dataflows
        ADD source_filter_expression NVARCHAR(MAX) NULL;

-- Postflight.
SELECT s.name AS table_schema, c.name AS column_name, t.name AS data_type,
       c.is_nullable
  FROM sys.columns c
  JOIN sys.tables t0 ON t0.object_id = c.object_id
  JOIN sys.schemas s ON s.schema_id = t0.schema_id
  JOIN sys.types t ON t.user_type_id = c.user_type_id
 WHERE t0.name = 'dc_framework_dataflows'
   AND c.name = 'source_filter_expression';

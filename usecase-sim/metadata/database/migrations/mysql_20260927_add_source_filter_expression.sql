-- Preflight. Replace DATABASE() with the metadata schema name when needed.
SELECT TABLE_SCHEMA, TABLE_NAME
  FROM information_schema.tables
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'dc_framework_dataflows';

SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
  FROM information_schema.columns
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'dc_framework_dataflows'
   AND COLUMN_NAME = 'source_filter_expression';

-- Apply once after confirming source_filter_expression is absent. The
-- information_schema preflight keeps this portable across MySQL versions
-- where ADD COLUMN IF NOT EXISTS is not available.
ALTER TABLE dc_framework_dataflows
    ADD COLUMN source_filter_expression TEXT NULL;

-- Postflight.
SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
  FROM information_schema.columns
 WHERE TABLE_SCHEMA = DATABASE()
   AND TABLE_NAME = 'dc_framework_dataflows'
   AND COLUMN_NAME = 'source_filter_expression';

-- Preflight: both queries must identify the expected table/column state.
SELECT name
  FROM sqlite_master
 WHERE type = 'table'
   AND name = 'dc_framework_dataflows';

PRAGMA table_info(dc_framework_dataflows);

-- Apply once after confirming source_filter_expression is absent.
ALTER TABLE dc_framework_dataflows
    ADD COLUMN source_filter_expression TEXT;

-- Postflight.
PRAGMA table_info(dc_framework_dataflows);

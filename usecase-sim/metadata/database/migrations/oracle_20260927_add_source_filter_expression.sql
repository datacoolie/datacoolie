-- Preflight for the canonical source_filter_expression field. Oracle stores
-- unquoted object names in upper case.
SELECT table_name
  FROM user_tables
 WHERE table_name = 'DC_FRAMEWORK_DATAFLOWS';

SELECT column_name, nullable, data_type
  FROM user_tab_columns
 WHERE table_name = 'DC_FRAMEWORK_DATAFLOWS'
   AND column_name = 'SOURCE_FILTER_EXPRESSION';

-- Additive and rerunnable. Existing rows receive NULL.
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*) INTO v_count
      FROM user_tab_columns
     WHERE table_name = 'DC_FRAMEWORK_DATAFLOWS'
       AND column_name = 'SOURCE_FILTER_EXPRESSION';
    IF v_count = 0 THEN
        EXECUTE IMMEDIATE
            'ALTER TABLE DC_FRAMEWORK_DATAFLOWS '
            || 'ADD (SOURCE_FILTER_EXPRESSION CLOB)';
    END IF;
END;
/

-- Postflight.
SELECT column_name, nullable, data_type
  FROM user_tab_columns
 WHERE table_name = 'DC_FRAMEWORK_DATAFLOWS'
   AND column_name = 'SOURCE_FILTER_EXPRESSION';

-- Preflight.
SELECT to_regclass('public.dc_framework_dataflows') AS dataflows_table;
SELECT column_name, is_nullable, data_type
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name = 'dc_framework_dataflows'
   AND column_name = 'source_filter_expression';

-- Additive and rerunnable for PostgreSQL. Adjust the schema if the metadata
-- tables are not in public.
ALTER TABLE public.dc_framework_dataflows
    ADD COLUMN IF NOT EXISTS source_filter_expression TEXT NULL;

-- Postflight.
SELECT column_name, is_nullable, data_type
  FROM information_schema.columns
 WHERE table_schema = 'public'
   AND table_name = 'dc_framework_dataflows'
   AND column_name = 'source_filter_expression';

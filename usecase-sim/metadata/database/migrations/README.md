# Metadata schema migrations

These scripts are additive upgrades for an existing
`dc_framework_dataflows` table. They are owned beside the fresh-install DDL
and must be reviewed and run by the database operator before deploying a
runtime that reads `source_filter_expression`.

The scripts do not reset rows, dataflow IDs, foreign keys, or watermark rows.
They add one nullable column only. `setup_metadata.py` and
`DatabaseProvider.create_tables()` create missing tables; neither operation is
a migration for an already existing table.

## Procedure

1. Back up the metadata database and confirm the target workspace/runtime.
2. Run the dialect-specific preflight query in the selected SQL file. Stop if
   `dc_framework_dataflows` is missing or the column already exists unless the
   operator is intentionally resuming a partially completed change.
3. Run the `ALTER TABLE` statement using the native database client.
4. Run the postflight query and confirm that `source_filter_expression` is
   nullable and that existing row counts and watermark rows are unchanged.
5. Deploy the matching provider/API service and run
   `usecase-sim/metadata/database/verify_metadata.py` against the same
   workspace.

Typical invocation (replace connection arguments with the approved target):

| Dialect | Native client invocation |
|---|---|
| SQLite | `sqlite3 metadata.db < sqlite_20260927_add_source_filter_expression.sql` |
| PostgreSQL | `psql "$DATABASE_URL" -f postgresql_20260927_add_source_filter_expression.sql` |
| MySQL | `mysql --host HOST --user USER --password DATABASE < mysql_20260927_add_source_filter_expression.sql` |
| MSSQL | `sqlcmd -S SERVER -d DATABASE -i mssql_20260927_add_source_filter_expression.sql` |
| Oracle | `sqlplus USER/PASSWORD@SERVICE @oracle_20260927_add_source_filter_expression.sql` |

Run the preflight and `ALTER TABLE` as separate reviewed steps when the client
does not support interactive inspection. Keep credentials out of shell history
where the platform provides a safer login mechanism.

The migration is deliberately not executed by application startup or the
metadata seeder. Existing rows remain `NULL`; restoring an authored predicate
is a separate, reviewed data correction.

Files:

- `sqlite_20260927_add_source_filter_expression.sql`
- `postgresql_20260927_add_source_filter_expression.sql`
- `mysql_20260927_add_source_filter_expression.sql`
- `mssql_20260927_add_source_filter_expression.sql`
- `oracle_20260927_add_source_filter_expression.sql`

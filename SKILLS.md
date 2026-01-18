# Skills

Repository-specific workflows that keep changes stable and consistent.

## 1. Schema compare change
Use when modifying schema diffing, SQL generation, or normalization.
- Update `src/pg_schema_sync/__main__.py` (`fetch_*`, `compare_and_generate_migration`, `normalize_sql`).
- Add or adjust tests in `tests/`.
- Update `SPEC.md` and user docs (`README.md`, `MIGRATION_GUIDE.md`) if behavior changes.

## 2. Table DDL / ALTER change
Use when changing table metadata or CREATE/ALTER behavior.
- Update `fetch_tables_metadata`, `generate_create_table_ddl`, and `is_safe_type_change` in `src/pg_schema_sync/__main__.py`.
- Update table-focused tests in `tests/test_compare_tables.py`.
- Document changes in `SPEC.md`.

## 3. Data migration change
Use when adjusting data migration, FK handling, or sequence syncing.
- Update `src/pg_schema_sync/dataMig.py` and any supporting scripts.
- Review FK drop/recreate and `validate_fks.sql` generation.
- Update `MIGRATION_GUIDE.md` and `SPEC.md`.

## 4. MCP wrapper change
Use when modifying MCP server behavior or tool schemas.
- Update `mcp_server/index.py` and keep tool contracts consistent.
- Update `SPEC.md` and MCP section in `README.md` as needed.

## 5. Ops script change
Use when editing operational helper scripts.
- Update `check_connections.py`, `kill_*`, `migrate_clean.sh`, or `migrate_single_table.py`.
- Refresh `MIGRATION_GUIDE.md` to match operational steps.

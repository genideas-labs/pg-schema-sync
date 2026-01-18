# pg-schema-sync Specification

Status: living document

## 1. Purpose
pg-schema-sync compares a source and target PostgreSQL schema (public schema only), generates migration SQL to align the target with the source, and optionally applies the changes. It also supports a data migration flow that copies table data and reconciles sequences.

## 2. Components
- CLI: `pg-schema-sync` entrypoint in `src/pg_schema_sync/__main__.py`.
- Data migration engine: `src/pg_schema_sync/dataMig.py`.
- MCP wrapper: `mcp_server/index.py`.
- Ops helpers: `check_connections.py`, `kill_idle_transactions.py`, `kill_zombie_connections.py`, `kill_others.sh`, `migrate_clean.sh`.
- Stepwise runner: `migrate_stepwise.py`.
- Tests: `tests/` (unit tests for compare logic).

## 3. Configuration
CLI reads `config.yaml` from the current working directory by default or accepts `--config <path>` for an alternate file.

Expected shape:
```yaml
source:
  host: SOURCE_DB_HOST
  port: SOURCE_DB_PORT
  db: SOURCE_DB_NAME        # or dbname
  username: SOURCE_DB_USER  # or user
  password: SOURCE_DB_PASSWORD

targets:
  gcp_test:
    host: TARGET_DB_HOST
    port: TARGET_DB_PORT
    db: TARGET_DB_NAME      # or dbname
    username: TARGET_DB_USER
    password: TARGET_DB_PASSWORD
```

Notes:
- CLI requires `targets.gcp_test` and always connects to that target.
- For CLI, `db` is normalized to `dbname`, and `username` to `user`.
- MCP wrapper loads config from `PG_SYNC_CONFIG_PATH` and accepts `target_name` at runtime.

## 4. CLI Interface
Command:
```
pg-schema-sync [--config <path>] [--verify] [--commit | --no-commit] [--use-alter] [--with-data] [--skip-fk | --fk-not-valid] [--install-extensions | --no-install-extensions]
```

Flags:
- `--config <path>`: use a non-default config file.
- `--verify`: only reports object name differences (no SQL generation, no execution).
- `--commit` (default): generate SQL and apply changes to target (committed per DDL block).
- `--no-commit`: generate SQL files only; no changes applied.
- `--use-alter` (experimental): use `ALTER TABLE` for safe column changes, otherwise drop/recreate.
- `--with-data`: run data migration after schema changes.
- `--skip-fk`: skip foreign key migration.
- `--fk-not-valid`: add foreign keys as `NOT VALID` and emit a validation SQL file.
- `--install-extensions` / `--no-install-extensions`: detect missing extensions on target and add `CREATE EXTENSION` statements (default: enabled; allowlist-limited, currently `pg_trgm`).

Output files:
- `history/migrate.<target>.<timestamp>.sql`
- `history/skip.<target>.<timestamp>.sql`
- `history/validate_fks.<target>.<timestamp>.sql` (only when `--fk-not-valid` is used)

## 5. Schema Comparison Model
All queries are scoped to `public` schema.

Object types and source data:
- Enums: `pg_type` + `pg_enum` (DDL) and `enum_range` values.
- Tables: `information_schema.tables` and `information_schema.columns`, plus constraints from `pg_constraint` and `information_schema.table_constraints`.
- Foreign keys: `pg_constraint` with composite key support and ON UPDATE/DELETE actions.
- Views: `information_schema.views.view_definition`.
- Functions: `pg_get_functiondef` (regular functions only, non-C languages).
- Indexes: `pg_indexes`, excluding PK/UNIQUE-derived indexes.
- Sequences: `pg_class` (excluding identity sequences via `pg_depend`).

Comparison rules:
- `--verify` compares object names only.
- Enums compare sorted enum value lists; DDL is used only for recreation.
- Functions compare raw DDL strings (no normalization).
- Views, indexes, sequences compare `normalize_sql` output.
- Table defaults are not compared; column comparison uses name, type, and nullability only.

## 6. SQL Generation Rules

### 6.1 DDL normalization
`normalize_sql`:
- Strips `--` comments.
- Preserves dollar-quoted bodies by temporary replacement.
- Lowercases and normalizes whitespace and operators.

### 6.2 Table DDL generation
`generate_create_table_ddl`:
- Quotes column names and table name.
- Converts array types to `base[]` from `udt_name`.
- Marks columns as identity when `is_identity` is true or `nextval(...)` is present.
- Inline constraints for single-column PK/UNIQUE.
- Adds composite UNIQUE and composite PRIMARY KEY constraints.
- Hard-coded fallbacks for specific `USER-DEFINED` enum columns; otherwise defaults to `text`.

### 6.3 ALTER TABLE (experimental)
When `--use-alter` is set:
- Adds columns with `ALTER TABLE ... ADD COLUMN`.
- Drops columns with warning comments plus `ALTER TABLE ... DROP COLUMN`.
- Changes nullability with `ALTER TABLE ... SET/DROP NOT NULL` (warning for NOT NULL).
- Changes type only when `is_safe_type_change` returns true.
  - Safe: varchar length increase, varchar to text, integer widening, numeric to text/varchar.
  - Unsafe changes fall back to drop/recreate.

### 6.4 Enum handling
- Differences trigger `DROP TYPE ... CASCADE` and recreate using source DDL.

### 6.5 Views and functions
- Views are recreated with `DROP VIEW ... CASCADE` followed by source DDL.
- Functions are dropped and recreated when raw DDL differs.

### 6.6 Indexes
- PK/UNIQUE-derived indexes are excluded from compare.
- When an index differs or is missing, SQL uses a `DO $$ ... IF NOT EXISTS` block to add.

### 6.7 Foreign keys
- Generated from `pg_constraint` with composite key support.
- Migration only adds missing/different constraints; it does not drop existing FKs.
- `--fk-not-valid` appends `NOT VALID` and emits a validation SQL file for later execution.

### 6.8 Sequences
- Identity sequences are excluded from compare.
- For source-only sequences: update existing sequence restart value when possible, otherwise skip.
- For differing sequences: prefer `ALTER SEQUENCE ... RESTART WITH` when a restart value is present.

## 7. Data Migration
Triggered via `--with-data`.

Workflow:
1. Close and reopen connections to release locks.
2. Drop all FK constraints in the target (batch mode, lock timeout).
3. Copy data table-by-table in parallel:
   - `SELECT *` from source.
   - `INSERT` into target with `ON CONFLICT (id) DO NOTHING`.
   - Tables in `SKIP_TABLES` are skipped.
4. Recreate FKs as `NOT VALID`.
5. Generate `validate_fks.sql` for later manual validation.
6. Reconcile sequence values (identity and explicit sequences).

Serialization rules:
- Python lists become Postgres arrays when column type ends with `[]`.
- Lists/dicts fallback to JSON strings for JSON/JSONB-like columns.

## 8. Execution and Transactions
- `--commit` executes each generated SQL block individually and commits per block.
- On failure, the current block rolls back and execution stops.
- `--no-commit` still writes history files.
- `--verify` closes connections and exits without SQL generation.

## 9. MCP Wrapper
`mcp_server/index.py` exposes tools:
- `verify_schema`
- `generate_migration_sql`
- `apply_schema_migration`

Differences vs CLI:
- Uses `PG_SYNC_CONFIG_PATH` to find config.
- Accepts `target_name`, `exclude_tables`, and `exclude_indexes` per request.
- No sequences, FK comparison, or data migration.
- Table metadata excludes composite constraints and identity details.

## 10. Outputs and Logs
- SQL files are written under `history/`.
- Data migration emits `validate_fks.sql` in the working directory.
- Debug and progress output is printed to stdout/stderr; no structured logging.

## 11. Testing
- Unit tests in `tests/` focus on `compare_and_generate_migration` behavior.
- No integration tests for live database operations.

## 12. Limitations / Non-goals
- Only `public` schema is handled.
- Target-only objects are not dropped.
- Table default changes are not detected.
- Function overloading is not fully supported (functions keyed by name).
- Data migration assumes a conflict target on `id`.

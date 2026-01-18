# AGENTS

Guidance for automated agents working in this repository.

## Repo map (high level)
- `src/pg_schema_sync/__main__.py`: CLI entrypoint, schema diffing, SQL generation/execution, config loading.
- `src/pg_schema_sync/dataMig.py`: data migration pipeline, FK drop/recreate, sequence syncing, serialization.
- `mcp_server/index.py`: MCP wrapper server and tool definitions.
- `tests/`: unit tests for compare logic (enums/tables/views/functions/indexes).
- `check_connections.py`, `kill_idle_transactions.py`, `kill_zombie_connections.py`, `kill_others.sh`, `migrate_clean.sh`, `migrate_single_table.py`: operational helpers.
- `README.md`, `MIGRATION_GUIDE.md`, `testing.md`: user-facing docs.

## Runtime behavior to keep in mind
- CLI expects `config.yaml` in the current working directory and requires `targets.gcp_test`; it always connects to `gcp_test` even if multiple targets exist.
- `--verify` compares object names/counts only, not DDL or column-level differences.
- Functions are compared using raw DDL; views/indexes/sequences use `normalize_sql` (with dollar-quote preservation).
- `--use-alter` only applies when changes are deemed safe; otherwise tables are recreated with `DROP ... CASCADE`.
- Table DDL generation includes composite UNIQUE/PRIMARY constraints and has hard-coded fallbacks for some `USER-DEFINED` enum columns.
- Foreign keys are generated from `pg_constraint` metadata and are only added (no DROP) in migration SQL.
- Sequence handling skips identity sequences; differences are resolved via `ALTER SEQUENCE ... RESTART WITH` when possible.
- `--commit` executes each SQL block separately and commits per block; failures halt execution.
- `--with-data` closes/reopens connections, drops all target FKs, migrates data in parallel with `ON CONFLICT (id) DO NOTHING`, re-adds FKs as NOT VALID, and fixes sequence values.
- MCP wrapper uses `PG_SYNC_CONFIG_PATH` and a simplified compare pipeline (no sequences/FKs).

## Development workflow
- Install: `pip install -e .`
- Tests: `pytest`
- MCP server: `pip install -r mcp_server/requirements.txt` then `python mcp_server/index.py`

## Change checklist
- If you modify schema comparison or SQL generation:
  - Update `src/pg_schema_sync/__main__.py` and related tests in `tests/`.
  - Update `SPEC.md` and relevant user docs (`README.md`, `MIGRATION_GUIDE.md`).
- If you modify data migration or FK handling:
  - Update `src/pg_schema_sync/dataMig.py`.
  - Review `MIGRATION_GUIDE.md` for operational steps and warnings.
- If you change CLI flags or config behavior:
  - Update argument parsing in `src/pg_schema_sync/__main__.py`.
  - Update `README.md` and `SPEC.md`.
- If you change MCP wrapper behavior:
  - Update `mcp_server/index.py` schemas and tool docs.
  - Update `SPEC.md` and MCP section in `README.md` if needed.

## Safe defaults
- Prefer `--verify` or `--no-commit` for review before destructive operations.
- Avoid automatic DROP of target-only objects unless explicitly required.
- Keep SQL generation deterministic and preserve dollar-quoted function bodies.

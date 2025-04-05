#!/usr/bin/env python3
import os
import yaml
import psycopg2
from psycopg2 import sql as psycopg2_sql # Renamed to avoid conflict
import datetime
import re
import json
import sys
from modelcontextprotocol.sdk.python.server import (
    Server,
    StdioServerTransport,
    McpError,
    ErrorCode,
)
from modelcontextprotocol.sdk.python.types import (
    CallToolRequestSchema,
    ListToolsRequestSchema,
    ToolDefinition,
    JsonSchema,
)

# --- Constants ---
CONFIG_ENV_VAR = "PG_SYNC_CONFIG_PATH"
HISTORY_DIR = "history" # Relative to where the server runs, or consider absolute path

# --- Helper Functions (Adapted from pg-schema-sync) ---

# --- DB 연결 함수 ---
def get_connection(config):
    # Adjust keys for psycopg2
    if 'db' in config:
        config['dbname'] = config.pop('db')
    if 'username' in config:
        config['user'] = config.pop('username')
    conn = psycopg2.connect(**config)
    return conn

# --- SQL 정규화 함수 ---
def normalize_sql(sql_text):
    if not sql_text:
        return ""
    sql_text = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
    sql_text = sql_text.lower()
    sql_text = re.sub(r'\s+', ' ', sql_text)
    return sql_text.strip()

# --- Enum DDL 조회 ---
def fetch_enums(conn):
    cur = conn.cursor()
    query = """
    SELECT t.typname,
           'CREATE TYPE public.' || t.typname || ' AS ENUM (' ||
           string_agg(quote_literal(e.enumlabel), ', ' ORDER BY e.enumsortorder) ||
           ');' as ddl
    FROM pg_type t
    JOIN pg_enum e ON t.oid = e.enumtypid
    JOIN pg_namespace n ON t.typnamespace = n.oid
    WHERE n.nspname = 'public'
    GROUP BY t.typname;
    """
    cur.execute(query)
    enums = {typname: ddl for typname, ddl in cur.fetchall()}
    cur.close()
    return enums

# --- Enum Values 조회 ---
def fetch_enums_values(conn):
    cur = conn.cursor()
    enum_types_query = """
    SELECT t.typname
    FROM pg_type t
    JOIN pg_namespace n ON t.typnamespace = n.oid
    WHERE n.nspname = 'public' AND t.typtype = 'e';
    """
    cur.execute(enum_types_query)
    enum_names = [row[0] for row in cur.fetchall()]

    enums_values = {}
    for enum_name in enum_names:
        query_template = psycopg2_sql.SQL("SELECT enum_range(NULL::{})").format(
            psycopg2_sql.Identifier('public', enum_name)
        )
        try:
            cur.execute(query_template)
            values = cur.fetchone()[0] if cur.rowcount > 0 else []
            enums_values[enum_name] = sorted(values)
        except psycopg2.Error as e:
            print(f"Warning: Could not fetch values for enum {enum_name}. Error: {e}", file=sys.stderr)
            enums_values[enum_name] = []
            conn.rollback()

    cur.close()
    return enums_values

# --- Table Metadata (컬럼 정보) 조회 ---
def fetch_tables_metadata(conn, exclude_tables):
    cur = conn.cursor()
    params = []
    query_str = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type='BASE TABLE'
    """
    if exclude_tables:
        query_str += " AND table_name NOT IN %s"
        params.append(tuple(exclude_tables))

    cur.execute(query_str, params if params else None)
    tables_metadata = {}
    table_names = [row[0] for row in cur.fetchall()]

    for table_name in table_names:
        col_query = psycopg2_sql.SQL("""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = {}
        ORDER BY ordinal_position;
        """).format(psycopg2_sql.Literal(table_name))
        cur.execute(col_query)
        columns = []
        for col_name, data_type, is_nullable, col_default in cur.fetchall():
            columns.append({
                'name': col_name,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'default': col_default
            })
        tables_metadata[table_name] = columns
    cur.close()
    return tables_metadata

# --- Table DDL 생성 함수 ---
def generate_create_table_ddl(table_name, columns):
    col_defs = []
    for col in columns:
        col_def = f"{col['name']} {col['type']}"
        if col['default'] is not None:
            col_def += f" DEFAULT {col['default']}"
        if not col['nullable']:
            col_def += " NOT NULL"
        col_defs.append(col_def)
    # Note: Constraints (PK, FK, etc.) are not handled by this basic DDL generation
    return f"CREATE TABLE public.{table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"

# --- View DDL 조회 ---
def fetch_views(conn):
    cur = conn.cursor()
    query = """
    SELECT table_name, view_definition
    FROM information_schema.views
    WHERE table_schema = 'public';
    """
    cur.execute(query)
    views = {}
    for view_name, view_def in cur.fetchall():
        ddl = f"CREATE OR REPLACE VIEW public.{view_name} AS\n{view_def.rstrip(';')};"
        views[view_name] = ddl
    cur.close()
    return views

# --- Function DDL 조회 ---
def fetch_functions(conn):
    cur = conn.cursor()
    query = """
    SELECT p.proname || '_' || md5(pg_get_function_arguments(p.oid)) as func_sig, -- Use signature for uniqueness
           pg_get_functiondef(p.oid) as ddl
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    JOIN pg_language l ON p.prolang = l.oid
    WHERE n.nspname = 'public'
      AND p.prokind = 'f'
      AND l.lanname != 'c';
    """
    cur.execute(query)
    # Use function signature as key because names can be overloaded
    functions = {func_sig: ddl for func_sig, ddl in cur.fetchall()}
    cur.close()
    return functions

# --- Index DDL 조회 ---
def fetch_indexes(conn, exclude_indexes):
    cur = conn.cursor()
    params = []
    query_str = """
    SELECT indexname, indexdef as ddl
    FROM pg_indexes
    WHERE schemaname = 'public'
    """
    if exclude_indexes:
        query_str += " AND indexname NOT IN %s"
        params.append(tuple(exclude_indexes))

    cur.execute(query_str, params if params else None)
    indexes = {}
    pkey_indexes = {}
    for indexname, ddl in cur.fetchall():
        if indexname.endswith('_pkey'):
            pkey_indexes[indexname] = ddl
        else:
            # Normalize index DDL for comparison (e.g., remove schema qualification if inconsistent)
            normalized_ddl = ddl # Simple normalization for now
            indexes[indexname] = normalized_ddl
    cur.close()
    return indexes, pkey_indexes

# --- 비교 후 migration SQL 생성 ---
def compare_and_generate_migration(src_data, tgt_data, obj_type, src_enum_ddls=None):
    migration_sql = []
    skipped_sql = []
    src_keys = set(src_data.keys())
    tgt_keys = set(tgt_data.keys())

    # Source only
    for name in src_keys - tgt_keys:
        ddl = ""
        if obj_type == "TABLE":
            ddl = generate_create_table_ddl(name, src_data[name])
        elif obj_type == "TYPE":
             ddl = src_enum_ddls.get(name, f"-- ERROR: DDL not found for Enum {name}")
        else: # View, Function, Index
             ddl = src_data[name]
        migration_sql.append(f"-- CREATE {obj_type} {name}\n{ddl}\n")

    # In both, compare
    for name in src_keys.intersection(tgt_keys):
        are_different = False
        ddl = "" # DDL for recreation if different

        if obj_type == "TABLE":
            src_cols = src_data[name]
            tgt_cols = tgt_data[name]
            # Basic comparison (can be enhanced)
            if len(src_cols) != len(tgt_cols) or \
               any(sc['name'] != tc['name'] or \
                   normalize_sql(sc['type']) != normalize_sql(tc['type']) or \
                   sc['nullable'] != tc['nullable'] # or \
                   # normalize_sql(str(sc.get('default',''))) != normalize_sql(str(tc.get('default',''))) # Default comparison is tricky
                   for sc, tc in zip(src_cols, tgt_cols)):
                are_different = True
                ddl = generate_create_table_ddl(name, src_cols)
        elif obj_type == "TYPE": # Enum
            src_values = src_data[name]
            tgt_values = tgt_data[name]
            if src_values != tgt_values:
                are_different = True
                ddl = src_enum_ddls.get(name, f"-- ERROR: DDL not found for Enum {name}")
        else: # View, Function, Index
            src_ddl_norm = normalize_sql(src_data[name])
            tgt_ddl_norm = normalize_sql(tgt_data[name])
            if src_ddl_norm != tgt_ddl_norm:
                are_different = True
                ddl = src_data[name]

        if are_different:
            # Use DROP IF EXISTS ... CASCADE for simplicity, but be CAREFUL
            migration_sql.append(f"-- {obj_type} {name} differs. Recreating.\nDROP {obj_type.upper()} IF EXISTS public.{name} CASCADE;\n{ddl}\n")
        else:
            original_ddl = ""
            if obj_type == "TABLE":
                 original_ddl = generate_create_table_ddl(name, src_data[name])
            elif obj_type == "TYPE":
                 original_ddl = src_enum_ddls.get(name, "")
            else:
                 original_ddl = src_data.get(name, "")

            skipped_sql.append(f"-- {obj_type} {name} is up-to-date; skipping.\n")
            if original_ddl:
                 commented_ddl = '\n'.join([f"-- {line}" for line in original_ddl.strip().splitlines()])
                 skipped_sql.append(commented_ddl + "\n")

    # Target only objects are currently ignored (DROP logic could be added if needed)

    return migration_sql, skipped_sql

# --- Verification Report Generation ---
def generate_verification_report(src_objs, tgt_objs, obj_type):
    report = {}
    src_names = set(src_objs.keys())
    tgt_names = set(tgt_objs.keys())
    report['source_count'] = len(src_names)
    report['target_count'] = len(tgt_names)
    report['source_only'] = sorted(list(src_names - tgt_names))
    report['target_only'] = sorted(list(tgt_names - src_names))
    report['synced'] = not report['source_only'] and not report['target_only']
    return report

# --- Load Config ---
def load_config():
    config_path = os.environ.get(CONFIG_ENV_VAR)
    if not config_path:
        raise McpError(ErrorCode.InvalidRequest, f"Environment variable {CONFIG_ENV_VAR} is not set.")
    if not os.path.exists(config_path):
         raise McpError(ErrorCode.InvalidRequest, f"Config file not found at path specified by {CONFIG_ENV_VAR}: {config_path}")

    try:
        with open(config_path, 'r', encoding='utf-8') as stream:
            config = yaml.safe_load(stream)
            if not config:
                raise McpError(ErrorCode.InvalidRequest, f"Config file is empty or invalid: {config_path}")
        # Basic validation
        if 'source' not in config or not isinstance(config['source'], dict):
             raise McpError(ErrorCode.InvalidRequest, "'source' configuration is missing or invalid.")
        if 'targets' not in config or not isinstance(config['targets'], dict):
             raise McpError(ErrorCode.InvalidRequest, "'targets' configuration is missing or invalid.")
        return config
    except yaml.YAMLError as exc:
        raise McpError(ErrorCode.InvalidRequest, f"Error parsing config file {config_path}: {exc}")
    except Exception as e:
        raise McpError(ErrorCode.InternalError, f"Error reading config file {config_path}: {e}")

# --- Get Target Config ---
def get_target_config(config, target_name):
     if target_name not in config['targets']:
         raise McpError(ErrorCode.InvalidParams, f"Target '{target_name}' not found in config file.")
     return config['targets'][target_name]

# --- MCP Server Implementation ---

class PgSchemaSyncServer:
    def __init__(self):
        self.server = Server(
            {"name": "pg-schema-sync-wrapper", "version": "0.1.0"},
            {"capabilities": {"resources": {}, "tools": {}}},
        )
        self.setup_tool_handlers()
        self.server.onerror = lambda error: print(f"[MCP Error] {error}", file=sys.stderr)

    def setup_tool_handlers(self):
        # --- Tool Schemas ---
        base_input_schema = JsonSchema(
            type="object",
            properties={
                "target_name": JsonSchema(type="string", description="Name of the target database configuration key in config.yaml"),
                "exclude_tables": JsonSchema(type="array", items=JsonSchema(type="string"), description="List of table names to exclude", default=[]),
                "exclude_indexes": JsonSchema(type="array", items=JsonSchema(type="string"), description="List of index names to exclude", default=[]),
                "use_alter": JsonSchema(type="boolean", description="EXPERIMENTAL: Use ALTER TABLE for column add/drop", default=False),
            },
            required=["target_name"],
        )

        verify_output_schema = JsonSchema(
            type="object",
            properties={
                "report": JsonSchema(type="object", description="Detailed verification report per object type"),
                "overall_status": JsonSchema(type="string", description="Overall sync status message")
            }
        )

        generate_output_schema = JsonSchema(
            type="object",
            properties={
                "migration_sql": JsonSchema(type="string", description="Generated SQL statements for migration"),
                "skipped_sql": JsonSchema(type="string", description="SQL comments for objects that were skipped (up-to-date)"),
                "migration_filename": JsonSchema(type="string", description="Suggested filename for the migration SQL"),
                "skipped_filename": JsonSchema(type="string", description="Suggested filename for the skipped SQL"),
            }
        )

        apply_output_schema = JsonSchema(
            type="object",
            properties={
                "success": JsonSchema(type="boolean", description="Whether the migration was applied successfully"),
                "message": JsonSchema(type="string", description="Status message or error details"),
                "log": JsonSchema(type="string", description="Log of executed statements (if applicable)"),
            }
        )

        # --- ListTools Handler ---
        async def list_tools_handler(request):
            return {
                "tools": [
                    ToolDefinition(
                        name="verify_schema",
                        description="Verify schema differences between source and a target database.",
                        inputSchema=base_input_schema,
                        outputSchema=verify_output_schema,
                    ),
                    ToolDefinition(
                        name="generate_migration_sql",
                        description="Generate migration SQL to update target schema based on source, without applying.",
                        inputSchema=base_input_schema,
                        outputSchema=generate_output_schema,
                    ),
                    ToolDefinition(
                        name="apply_schema_migration",
                        description="Generate and APPLY migration SQL to update target schema based on source.",
                        inputSchema=base_input_schema,
                        outputSchema=apply_output_schema,
                    ),
                ]
            }
        self.server.set_request_handler(ListToolsRequestSchema, list_tools_handler)

        # --- CallTool Handler ---
        async def call_tool_handler(request):
            tool_name = request.params.name
            args = request.params.arguments

            try:
                config = load_config()
                target_name = args['target_name']
                target_db_config = get_target_config(config, target_name)
                source_db_config = config['source']
                exclude_tables = args.get('exclude_tables', [])
                exclude_indexes = args.get('exclude_indexes', [])
                use_alter_flag = args.get('use_alter', False) # Get the flag

                print(f"Connecting to source DB...")
                src_conn = get_connection(source_db_config.copy()) # Use copy
                print(f"Connecting to target DB ({target_name})...")
                tgt_conn = get_connection(target_db_config.copy()) # Use copy

            except McpError as e:
                raise e # Propagate MCP specific errors
            except psycopg2.Error as e:
                raise McpError(ErrorCode.InternalError, f"Database connection error: {e}")
            except Exception as e:
                 raise McpError(ErrorCode.InternalError, f"Error during setup: {e}")

            try:
                # --- Fetch Data ---
                print("Fetching source data...")
                src_enum_ddls = fetch_enums(src_conn)
                src_enum_values = fetch_enums_values(src_conn)
                src_tables_meta = fetch_tables_metadata(src_conn, exclude_tables)
                src_views = fetch_views(src_conn)
                src_functions = fetch_functions(src_conn)
                src_indexes, src_pkey_indexes = fetch_indexes(src_conn, exclude_indexes)

                print("Fetching target data...")
                tgt_enum_ddls = fetch_enums(tgt_conn)
                tgt_enum_values = fetch_enums_values(tgt_conn)
                tgt_tables_meta = fetch_tables_metadata(tgt_conn, exclude_tables)
                tgt_views = fetch_views(tgt_conn)
                tgt_functions = fetch_functions(tgt_conn)
                tgt_indexes, tgt_pkey_indexes = fetch_indexes(tgt_conn, exclude_indexes)

                src_conn.close() # Close source connection early if possible

                # --- Tool Specific Logic ---
                if tool_name == "verify_schema":
                    print("Verifying schema...")
                    report = {}
                    all_synced = True
                    report['enums'] = generate_verification_report(src_enum_ddls, tgt_enum_ddls, "Enums")
                    all_synced &= report['enums']['synced']
                    report['tables'] = generate_verification_report(src_tables_meta, tgt_tables_meta, "Tables")
                    all_synced &= report['tables']['synced']
                    report['views'] = generate_verification_report(src_views, tgt_views, "Views")
                    all_synced &= report['views']['synced']
                    report['functions'] = generate_verification_report(src_functions, tgt_functions, "Functions")
                    all_synced &= report['functions']['synced']
                    report['indexes'] = generate_verification_report(src_indexes, tgt_indexes, "Indexes")
                    all_synced &= report['indexes']['synced']

                    status_msg = "Schemas are synchronized." if all_synced else "Schema differences found."

                    return {"content": [{"type": "json", "json": {"report": report, "overall_status": status_msg}}]}

                elif tool_name == "generate_migration_sql" or tool_name == "apply_schema_migration":
                    print("Generating migration SQL...")
                    all_migration_sql = []
                    all_skipped_sql = []

                    # Pass use_alter flag to compare function for TABLE type
                    mig_sql, skip_sql = compare_and_generate_migration(src_enum_values, tgt_enum_values, "TYPE", src_enum_ddls=src_enum_ddls)
                    all_migration_sql.extend(mig_sql)
                    all_skipped_sql.extend(skip_sql)

                    mig_sql, skip_sql = compare_and_generate_migration(src_tables_meta, tgt_tables_meta, "TABLE", use_alter=use_alter_flag)
                    all_migration_sql.extend(mig_sql)
                    all_skipped_sql.extend(skip_sql)

                    mig_sql, skip_sql = compare_and_generate_migration(src_views, tgt_views, "VIEW")
                    all_migration_sql.extend(mig_sql)
                    all_skipped_sql.extend(skip_sql)

                    mig_sql, skip_sql = compare_and_generate_migration(src_functions, tgt_functions, "FUNCTION")
                    all_migration_sql.extend(mig_sql)
                    all_skipped_sql.extend(skip_sql)

                    mig_sql, skip_sql = compare_and_generate_migration(src_indexes, tgt_indexes, "INDEX")
                    all_migration_sql.extend(mig_sql)
                    all_skipped_sql.extend(skip_sql)

                    migration_sql_str = "\n".join(all_migration_sql)
                    skipped_sql_str = "\n".join(all_skipped_sql)

                    # Generate filenames (optional, but helpful)
                    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
                    migration_filename = f"migrate.{target_name}.{timestamp}.sql"
                    skipped_filename = f"skip.{target_name}.{timestamp}.sql"

                    if tool_name == "generate_migration_sql":
                         tgt_conn.close() # Close target connection as it's not needed further
                         return {"content": [{"type": "json", "json": {
                             "migration_sql": migration_sql_str,
                             "skipped_sql": skipped_sql_str,
                             "migration_filename": migration_filename,
                             "skipped_filename": skipped_filename,
                         }}]}

                    elif tool_name == "apply_schema_migration":
                        if not all_migration_sql:
                            tgt_conn.close()
                            return {"content": [{"type": "json", "json": {"success": True, "message": "No migration SQL to execute.", "log": ""}}]}

                        print(f"Applying migration SQL to target ({target_name})...")
                        execution_log = []
                        execution_successful = True
                        try:
                            with tgt_conn.cursor() as cur:
                                for i, sql_block in enumerate(all_migration_sql):
                                    sql_content = "\n".join(line for line in sql_block.strip().splitlines() if not line.strip().startswith('--'))
                                    if not sql_content.strip():
                                        continue

                                    statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
                                    log_entry = f"-- Executing Block {i+1} ({len(statements)} statements) --"
                                    print(log_entry)
                                    execution_log.append(log_entry)

                                    for j, statement in enumerate(statements):
                                        log_stmt = f"Executing: {statement[:150]}{'...' if len(statement) > 150 else ''}"
                                        print(log_stmt)
                                        execution_log.append(log_stmt)
                                        try:
                                            cur.execute(statement)
                                        except psycopg2.Error as e:
                                            error_msg = f"Error executing statement: {e}"
                                            print(error_msg, file=sys.stderr)
                                            execution_log.append(f"ERROR: {error_msg}")
                                            print("Rolling back transaction...", file=sys.stderr)
                                            tgt_conn.rollback()
                                            execution_successful = False
                                            break # Stop executing statements in this block
                                        except Exception as e:
                                            error_msg = f"Unexpected error executing statement: {e}"
                                            print(error_msg, file=sys.stderr)
                                            execution_log.append(f"ERROR: {error_msg}")
                                            print("Rolling back transaction...", file=sys.stderr)
                                            tgt_conn.rollback()
                                            execution_successful = False
                                            break # Stop executing statements in this block
                                    if not execution_successful:
                                        break # Stop executing further blocks

                            if execution_successful:
                                print("Committing transaction...")
                                tgt_conn.commit()
                                message = "Migration applied successfully."
                                print(message)
                                execution_log.append("-- Committed --")
                            else:
                                message = "Migration failed. Transaction rolled back. See log for details."
                                print(message, file=sys.stderr)
                                execution_log.append("-- Rolled Back --")

                            return {"content": [{"type": "json", "json": {
                                "success": execution_successful,
                                "message": message,
                                "log": "\n".join(execution_log),
                            }}]}

                        except Exception as e:
                            tgt_conn.rollback() # Ensure rollback on outer errors
                            raise McpError(ErrorCode.InternalError, f"Error during SQL execution: {e}")
                        finally:
                             if tgt_conn:
                                 tgt_conn.close()

                else:
                    raise McpError(ErrorCode.MethodNotFound, f"Unknown tool: {tool_name}")

            except McpError as e:
                 raise e
            except psycopg2.Error as e:
                 # Ensure connections are closed on error if they were opened
                 if 'src_conn' in locals() and src_conn and not src_conn.closed: src_conn.close()
                 if 'tgt_conn' in locals() and tgt_conn and not tgt_conn.closed: tgt_conn.close()
                 raise McpError(ErrorCode.InternalError, f"Database operation error: {e}")
            except Exception as e:
                 if 'src_conn' in locals() and src_conn and not src_conn.closed: src_conn.close()
                 if 'tgt_conn' in locals() and tgt_conn and not tgt_conn.closed: tgt_conn.close()
                 raise McpError(ErrorCode.InternalError, f"An unexpected error occurred: {e}")

        self.server.set_request_handler(CallToolRequestSchema, call_tool_handler)


    async def run(self):
        transport = StdioServerTransport()
        await self.server.connect(transport)
        print("pg-schema-sync MCP wrapper server running on stdio", file=sys.stderr)
        await self.server.listen()


if __name__ == "__main__":
    import asyncio
    server = PgSchemaSyncServer()
    asyncio.run(server.run())

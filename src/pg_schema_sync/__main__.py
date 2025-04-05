#!/usr/bin/env python3
import psycopg2
from psycopg2 import sql # SQL 식별자 안전 처리용
import yaml # YAML 라이브러리 임포트
import datetime # 타임스탬프용
import os # 디렉토리 생성용
import argparse # 커맨드라인 인수 처리용
import re # SQL 정규화용

# --- 제외할 객체 목록 ---
# Liquibase 등 마이그레이션 도구 관련 테이블 또는 기타 제외 대상
EXCLUDE_TABLES = ['databasechangelog', 'databasechangeloglock']
# 관련 인덱스 또는 기타 제외 대상
EXCLUDE_INDEXES = ['databasechangeloglock_pkey'] # 필요시 타겟 전용 인덱스 추가

# --- DB 연결 함수 ---
def get_connection(config):
    conn = psycopg2.connect(**config)
    return conn

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
    """Enum 타입별 값 목록을 조회합니다."""
    cur = conn.cursor()
    # 먼저 public 스키마의 모든 enum 타입 이름을 가져옵니다.
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
        # 각 enum 타입의 값 목록을 조회합니다.
        # psycopg2.sql 사용하여 안전하게 식별자 처리
        query_template = sql.SQL("SELECT enum_range(NULL::{})").format(
            sql.Identifier('public', enum_name)
        )
        try:
            cur.execute(query_template)
            # 결과는 튜플 형태의 리스트 [(value1, value2, ...)] 이므로 첫번째 요소 사용
            values = cur.fetchone()[0] if cur.rowcount > 0 else []
            enums_values[enum_name] = sorted(values) # 일관된 비교를 위해 정렬
        except psycopg2.Error as e:
            print(f"Warning: Could not fetch values for enum {enum_name}. Error: {e}")
            enums_values[enum_name] = [] # 오류 발생 시 빈 리스트로 처리
            conn.rollback() # 오류 발생 시 트랜잭션 롤백

    cur.close()
    return enums_values

# --- Table Metadata (컬럼 정보) 조회 ---
def fetch_tables_metadata(conn):
    """테이블별 컬럼 메타데이터(이름, 타입, Null여부, 기본값)를 조회합니다."""
    cur = conn.cursor()
    params = []
    query_str = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type='BASE TABLE'
    """
    if EXCLUDE_TABLES:
        query_str += " AND table_name NOT IN %s"
        params.append(tuple(EXCLUDE_TABLES))

    cur.execute(query_str, params if params else None)
    tables_metadata = {}
    table_names = [row[0] for row in cur.fetchall()]

    for table_name in table_names:
        col_query = f"""
        SELECT column_name,
               data_type,
               is_nullable,
               column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position;
        """
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

# --- Table DDL 생성 함수 (메타데이터 기반 - 필요 시 사용) ---
def generate_create_table_ddl(table_name, columns):
    """컬럼 메타데이터로부터 CREATE TABLE DDL을 생성합니다."""
    col_defs = []
    for col in columns:
        col_def = f"{col['name']} {col['type']}"
        if col['default'] is not None:
            # 기본값에 타입 캐스팅이 포함될 수 있으므로 그대로 사용
            col_def += f" DEFAULT {col['default']}"
        if not col['nullable']:
            col_def += " NOT NULL"
        col_defs.append(col_def)
    return f"CREATE TABLE public.{table_name} (\n    " + ",\n    ".join(col_defs) + "\n);"

# --- View DDL 조회 ---
def fetch_views(conn):
    """뷰 DDL을 information_schema.views.view_definition을 사용하여 조회합니다."""
    cur = conn.cursor()
    query = """
    SELECT table_name,
           view_definition
    FROM information_schema.views
    WHERE table_schema = 'public';
    """
    cur.execute(query)
    views = {}
    for view_name, view_def in cur.fetchall():
        # view_definition은 SELECT 문만 포함하므로 CREATE OR REPLACE VIEW 추가
        # view_definition 끝에 세미콜론이 있을 수 있으므로 제거 후 추가
        ddl = f"CREATE OR REPLACE VIEW public.{view_name} AS\n{view_def.rstrip(';')};"
        views[view_name] = ddl
    # 중복 코드 제거: 위에서 이미 views 딕셔너리에 할당함
    cur.close()
    return views

# --- Function DDL 조회 ---
def fetch_functions(conn):
    cur = conn.cursor()
    query = """
    SELECT p.proname,
           pg_get_functiondef(p.oid) as ddl
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    JOIN pg_language l ON p.prolang = l.oid  -- Join with pg_language
    WHERE n.nspname = 'public'
      AND p.prokind = 'f'  -- Filter for regular functions
      AND l.lanname != 'c'; -- Filter out C language functions
    """

    cur.execute(query)
    functions = {proname: ddl for proname, ddl in cur.fetchall()}
    cur.close()
    return functions

# --- Index DDL 조회 (기본 키 인덱스 분리) ---
def fetch_indexes(conn):
    """인덱스 DDL을 조회하되, 기본 키(_pkey) 인덱스를 분리하여 반환합니다."""
    cur = conn.cursor()
    params = []
    # EXCLUDE_INDEXES 목록에 있는 것만 제외하고 일단 모두 조회
    query_str = """
    SELECT indexname,
           indexdef as ddl
    FROM pg_indexes
    WHERE schemaname = 'public'
    """
    if EXCLUDE_INDEXES:
        query_str += " AND indexname NOT IN %s"
        params.append(tuple(EXCLUDE_INDEXES))

    cur.execute(query_str, params if params else None)

    indexes = {} # 비교 대상 인덱스
    pkey_indexes = {} # 기본 키 인덱스 (정보용)

    for indexname, ddl in cur.fetchall():
        if indexname.endswith('_pkey'):
            pkey_indexes[indexname] = ddl
        else:
            indexes[indexname] = ddl

    cur.close()
    return indexes, pkey_indexes # 두 개의 딕셔너리 반환

# --- 비교 후 migration SQL 생성 (타입별 로직 분기, Enum DDL 참조 추가) ---
def compare_and_generate_migration(src_data, tgt_data, obj_type, src_enum_ddls=None):
    """
    소스와 타겟 데이터를 비교하여 마이그레이션 SQL과 건너뛴 SQL을 생성합니다.
    obj_type에 따라 비교 방식을 다르게 적용합니다.
    Enum 타입의 DDL 생성을 위해 src_enum_ddls 딕셔너리가 필요합니다.
    """
    migration_sql = []
    skipped_sql = []
    src_keys = set(src_data.keys())
    tgt_keys = set(tgt_data.keys())

    # 소스에만 있는 객체 처리
    for name in src_keys - tgt_keys:
        if obj_type == "TABLE":
            # 테이블 메타데이터로부터 DDL 생성
            ddl = generate_create_table_ddl(name, src_data[name])
        else: # 다른 타입은 src_data에 DDL이 있다고 가정
             ddl = src_data[name]
        migration_sql.append(f"-- CREATE {obj_type} {name}\n{ddl}\n")

    # 양쪽에 모두 있는 객체 비교 처리
    for name in src_keys.intersection(tgt_keys):
        are_different = False
        ddl = "" # 변경 시 사용할 DDL (주로 소스 기준)

        if obj_type == "TABLE":
            # 테이블: 컬럼 메타데이터 비교
            src_cols = src_data[name]
            tgt_cols = tgt_data[name]
            # 컬럼 수, 이름, 타입, Null 여부, 기본값 등을 비교
            # 주의: 기본값 비교는 복잡할 수 있음 (표현 방식 차이)
            # 간단하게 컬럼 수와 이름/타입/Null여부만 비교하거나, 더 정교한 비교 로직 필요
            if len(src_cols) != len(tgt_cols) or \
               any(sc['name'] != tc['name'] or \
                   normalize_sql(sc['type']) != normalize_sql(tc['type']) or \
                   sc['nullable'] != tc['nullable'] # or \
                   # normalize_sql(str(sc['default'])) != normalize_sql(str(tc['default'])) # 기본값 비교는 불안정할 수 있음
                   for sc, tc in zip(src_cols, tgt_cols)):
                are_different = True
                ddl = generate_create_table_ddl(name, src_cols) # 변경 시 소스 기준으로 DDL 생성
        elif obj_type == "TYPE": # Enum 타입 가정
            # Enum: 값 목록 비교 (fetch_enums_values 결과 사용)
            src_values = src_data[name]
            tgt_values = tgt_data[name]
            if src_values != tgt_values:
                are_different = True
                # Enum DDL은 src_enum_ddls 에서 가져옴
                ddl = src_enum_ddls.get(name, f"-- ERROR: DDL not found for Enum {name}")
        else:
            # 나머지 타입 (View, Function, Index): 정규화된 DDL 비교 (src_data가 DDL 딕셔너리라고 가정)
            src_ddl_norm = normalize_sql(src_data[name])
            tgt_ddl_norm = normalize_sql(tgt_data[name])
            if src_ddl_norm != tgt_ddl_norm:
                are_different = True
                ddl = src_data[name] # 변경 시 소스 DDL 사용

        # 비교 결과에 따라 SQL 생성
        if are_different:
            migration_sql.append(f"-- {obj_type} {name} differs. Recreating.\nDROP {obj_type.upper()} IF EXISTS public.{name} CASCADE;\n{ddl}\n")
        else:
            # 동일한 경우: 스킵 처리 (옵션 C)
            # 스킵 시 사용할 DDL (소스 기준)
            original_ddl = ""
            if obj_type == "TABLE":
                 original_ddl = generate_create_table_ddl(name, src_data[name])
            elif obj_type == "TYPE":
                 original_ddl = src_enum_ddls.get(name, "") # 스킵 로그용 Enum DDL
            else: # View, Function, Index 등
                 original_ddl = src_data.get(name, "") # src_data가 DDL 딕셔너리라고 가정

            skipped_sql.append(f"-- {obj_type} {name} is up-to-date; skipping.\n")
            if original_ddl: # DDL이 있는 경우만 주석 처리하여 추가
                 commented_ddl = '\n'.join([f"-- {line}" for line in original_ddl.strip().splitlines()])
                 skipped_sql.append(commented_ddl + "\n")

    # 타겟에만 있는 객체는 현재 처리하지 않음 (필요 시 추가)

    return migration_sql, skipped_sql


# --- SQL 정규화 함수 ---
def normalize_sql(sql_text):
    """SQL 문자열에서 주석 제거, 소문자 변환, 공백 정규화 수행"""
    if not sql_text:
        return ""
    # -- 스타일 주석 제거
    sql_text = re.sub(r'--.*$', '', sql_text, flags=re.MULTILINE)
    # /* */ 스타일 주석 제거 (간단한 경우만 처리, 중첩 불가)
    # sql_text = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL) # 필요 시 추가
    # 소문자로 변환
    sql_text = sql_text.lower()
    # 여러 공백 (스페이스, 탭, 개행 포함)을 단일 스페이스로 변경
    sql_text = re.sub(r'\s+', ' ', sql_text)
    # 앞뒤 공백 제거
    return sql_text.strip()


# --- 검증 결과 출력 함수 ---
def print_verification_report(src_objs, tgt_objs, obj_type):
    """소스와 타겟 객체 목록을 비교하고 결과를 출력합니다."""
    print(f"\nVerifying {obj_type}...")
    src_names = set(src_objs.keys())
    tgt_names = set(tgt_objs.keys())

    source_only = sorted(list(src_names - tgt_names))
    target_only = sorted(list(tgt_names - src_names))

    print(f"  Source Count: {len(src_names)}")
    print(f"  Target Count: {len(tgt_names)}")

    if source_only:
        print(f"  Objects only in Source ({len(source_only)}): {', '.join(source_only)}")
    else:
        print("  Objects only in Source (0): None")

    if target_only:
        print(f"  Objects only in Target ({len(target_only)}): {', '.join(target_only)}")
    else:
        print("  Objects only in Target (0): None")

    is_synced = not source_only and not target_only
    status = "Synced" if is_synced else "Differences found"
    print(f"  Status: {status}")
    return is_synced


def main():
    # --- 커맨드라인 인수 파싱 ---
    parser = argparse.ArgumentParser(description="Compare source and target PostgreSQL schemas and generate/apply migration SQL, or verify differences.")
    # Verification flag
    parser.add_argument('--verify', action='store_true',
                        help="Only verify schema differences (object names and counts) without generating/executing SQL.")
    # Commit flag (only relevant if --verify is not used)
    parser.add_argument('--commit', action=argparse.BooleanOptionalAction, default=True,
                        help="Execute the generated migration SQL on the target database. Use --no-commit to only generate files. Ignored if --verify is used.")
    args = parser.parse_args()
    # --- 인수 파싱 끝 ---

    # 타임스탬프 생성 (YYYYMMDDHHMMSS 형식)
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    # history 디렉토리 확인 및 생성
    history_dir = "history"
    os.makedirs(history_dir, exist_ok=True)

    # config.yaml 파일 로드
    try:
        with open("config.yaml", 'r', encoding='utf-8') as stream:
            config = yaml.safe_load(stream)
            if not config:
                print("Error: config.yaml is empty or invalid.")
                return
    except FileNotFoundError:
        print("Error: config.yaml not found.")
        return
    except yaml.YAMLError as exc:
        print(f"Error parsing config.yaml: {exc}")
        return
    except Exception as e:
        print(f"An unexpected error occurred while reading config.yaml: {e}")
        return

    # 설정 유효성 검사 및 추출
    if 'source' not in config or not isinstance(config['source'], dict):
        print("Error: 'source' configuration is missing or invalid in config.yaml.")
        return
    if 'targets' not in config or not isinstance(config['targets'], dict) or 'gcp_test' not in config['targets'] or not isinstance(config['targets']['gcp_test'], dict):
        print("Error: 'targets.gcp_test' configuration is missing or invalid in config.yaml.")
        return

    source_config = config['source']
    target_config = config['targets']['gcp_test']

    # psycopg2에서 사용하는 키 이름으로 조정 ('db' -> 'dbname', 'username' -> 'user')
    # source 설정 조정
    if 'db' in source_config:
        source_config['dbname'] = source_config.pop('db')
    if 'username' in source_config:
        source_config['user'] = source_config.pop('username')

    # target 설정 조정
    if 'db' in target_config:
        target_config['dbname'] = target_config.pop('db')
    if 'username' in target_config:
        target_config['user'] = target_config.pop('username')


    # 연결
    try:
        print("Connecting to source database...")
        src_conn = get_connection(source_config)
        print("Connecting to target database (gcp_test)...")
        tgt_conn = get_connection(target_config)
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return
    except Exception as e:
        print(f"An unexpected error occurred during connection: {e}")
        return


    # --- 데이터 조회 ---
    print("Fetching Enum DDLs...")
    src_enum_ddls = fetch_enums(src_conn) # DDL 생성 및 스킵 로그용
    tgt_enum_ddls = fetch_enums(tgt_conn) # 스킵 로그용

    print("Fetching Enum Values...")
    src_enum_values = fetch_enums_values(src_conn) # 비교용
    tgt_enum_values = fetch_enums_values(tgt_conn) # 비교용

    print("Fetching Table Metadata...")
    src_tables_meta = fetch_tables_metadata(src_conn) # 비교 및 DDL 생성용
    tgt_tables_meta = fetch_tables_metadata(tgt_conn) # 비교용

    print("Fetching View DDLs...")
    src_views = fetch_views(src_conn) # 비교 및 DDL 생성용
    tgt_views = fetch_views(tgt_conn) # 비교용

    print("Fetching Function DDLs...")
    src_functions = fetch_functions(src_conn) # 비교 및 DDL 생성용
    tgt_functions = fetch_functions(tgt_conn) # 비교용

    print("Fetching Index DDLs...")
    src_indexes, src_pkey_indexes = fetch_indexes(src_conn) # 비교 및 DDL 생성용 + 정보용
    tgt_indexes, tgt_pkey_indexes = fetch_indexes(tgt_conn) # 비교용 + 정보용
    # --- 데이터 조회 끝 ---


    # --- 검증 모드 처리 ---
    if args.verify:
        print("\n--- Schema Verification Mode ---")
        all_synced = True
        # 검증 시에는 이름 목록만 비교
        all_synced &= print_verification_report(src_enum_ddls, tgt_enum_ddls, "Enums (Types)")
        all_synced &= print_verification_report(src_tables_meta, tgt_tables_meta, "Tables")
        all_synced &= print_verification_report(src_views, tgt_views, "Views")
        all_synced &= print_verification_report(src_functions, tgt_functions, "Functions")
        # 비교 대상 인덱스만 검증 리포트에 사용
        all_synced &= print_verification_report(src_indexes, tgt_indexes, "Indexes (excluding _pkey)")

        # 타겟에만 있는 _pkey 인덱스 정보 출력
        target_only_pkeys = sorted(list(set(tgt_pkey_indexes.keys()) - set(src_pkey_indexes.keys())))
        if target_only_pkeys:
            print(f"\n  Info: Found target-only primary key indexes (ignored in comparison): {', '.join(target_only_pkeys)}")

        print("\n--- Verification Summary ---")
        if all_synced:
            print("Source and target schemas are perfectly synchronized (based on object names).")
        else:
            print("Schema differences found. Please review the report above.")

        # 검증 모드에서는 연결만 닫고 종료
        src_conn.close()
        if tgt_conn:
            tgt_conn.close()
        print("\nConnections closed.")
        return # 여기서 함수 종료
    # --- 검증 모드 처리 끝 ---


    # --- 마이그레이션/파일 생성 모드 (기존 로직) ---
    print("\n--- Migration Generation Mode ---")
    all_migration_sql = [] # 실제 마이그레이션 SQL 저장
    all_skipped_sql = []   # 건너뛴 SQL 저장

    # 순서: enum, table, view, function, index
    print("Comparing Enums (Values)...")
    # Enum 비교 시 값 목록(values)을 사용하고, DDL 생성을 위해 src_enum_ddls 전달
    mig_sql, skip_sql = compare_and_generate_migration(src_enum_values, tgt_enum_values, "TYPE", src_enum_ddls=src_enum_ddls)
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql) # 이제 스킵된 Enum DDL도 올바르게 포함됨

    print("Comparing Tables (Metadata)...")
    mig_sql, skip_sql = compare_and_generate_migration(src_tables_meta, tgt_tables_meta, "TABLE")
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)

    print("Comparing Views (DDL)...")
    mig_sql, skip_sql = compare_and_generate_migration(src_views, tgt_views, "VIEW")
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)

    print("Comparing Functions (DDL)...")
    mig_sql, skip_sql = compare_and_generate_migration(src_functions, tgt_functions, "FUNCTION")
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)

    print("Comparing Indexes (DDL, excluding _pkey)...")
    # 비교 대상 인덱스만 마이그레이션 생성에 사용
    mig_sql, skip_sql = compare_and_generate_migration(src_indexes, tgt_indexes, "INDEX")
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)
    # --- 비교 및 SQL 생성 끝 ---

    # Source 연결은 여기서 닫아도 됨
    src_conn.close()
    print("Source connection closed.")

    # 파일명 생성 (target_name은 config에서 가져온 첫번째 타겟 키로 가정)
    # TODO: 여러 타겟을 처리해야 하는 경우 로직 수정 필요
    target_name = list(config['targets'].keys())[0] if config.get('targets') else 'unknown_target'

    migration_filename = os.path.join(history_dir, f"migrate.{target_name}.{timestamp}.sql")
    skipped_filename = os.path.join(history_dir, f"skip.{target_name}.{timestamp}.sql")

    # 마이그레이션 SQL 파일 저장
    try:
        with open(migration_filename, "w", encoding="utf-8") as f:
            f.write("\n".join(all_migration_sql))
        print(f"Migration SQL written to {migration_filename}")
    except IOError as e:
        print(f"Error writing migration file {migration_filename}: {e}")


    # 건너뛴 SQL 파일 저장
    try:
        with open(skipped_filename, "w", encoding="utf-8") as f:
            f.write("\n".join(all_skipped_sql))
        print(f"Skipped SQL written to {skipped_filename}")
    except IOError as e:
        print(f"Error writing skipped file {skipped_filename}: {e}")

    # --- 마이그레이션 SQL 실행 (commit 옵션이 True일 경우) ---
    if args.commit:
        if not all_migration_sql:
            print("No migration SQL to execute.")
        else:
            print(f"\nExecuting migration SQL on target database ({target_name})...")
            execution_successful = True
            try:
                with tgt_conn.cursor() as cur:
                    # 각 SQL 블록 처리
                    for i, sql_block in enumerate(all_migration_sql):
                        # 블록 내 주석 제외 및 실제 실행할 SQL 추출
                        sql_content = "\n".join(line for line in sql_block.strip().splitlines() if not line.strip().startswith('--'))
                        if not sql_content.strip():
                            continue # 실행할 내용 없으면 다음 블록으로

                        # SQL 블록을 개별 문장으로 분리 (세미콜론 기준, 간단한 분리)
                        # 주의: 문자열 내 세미콜론 등 복잡한 경우 완벽하지 않을 수 있음
                        statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]

                        print(f"--- Executing Block {i+1} ({len(statements)} statements) ---")
                        # 각 문장 실행
                        for j, statement in enumerate(statements):
                            try:
                                print(f"  Executing statement {j+1}: {statement[:100]}{'...' if len(statement) > 100 else ''}")
                                cur.execute(statement)
                            except psycopg2.Error as e:
                                print(f"\nError executing statement {j+1} in block {i+1}:")
                                print(f"  Statement: {statement}")
                                print(f"  Error: {e}")
                                print("Rolling back transaction...")
                                tgt_conn.rollback()
                                print("Transaction rolled back.")
                                execution_successful = False
                                break # 현재 블록의 나머지 문장 실행 중단
                            except Exception as e:
                                print(f"\nAn unexpected error occurred during statement {j+1} execution:")
                                print(f"  Statement: {statement}")
                                print(f"  Error: {e}")
                                print("Rolling back transaction...")
                                tgt_conn.rollback()
                                print("Transaction rolled back.")
                                execution_successful = False
                                break # 현재 블록의 나머지 문장 실행 중단

                        if not execution_successful:
                            break # 오류 발생 시 전체 실행 중단

                # 모든 블록 및 문장이 성공적으로 실행된 경우에만 커밋
                if execution_successful:
                    tgt_conn.commit()
                    print("\nMigration SQL executed successfully and committed.")

            except Exception as e: # 커서 생성 등 외부 try 블록의 예외 처리
                print(f"\nAn unexpected error occurred during SQL execution setup: {e}")
                print("Rolling back transaction...")
                tgt_conn.rollback()
                print("Transaction rolled back.")
            except Exception as e:
                print(f"\nAn unexpected error occurred during SQL execution: {e}")
                print("Rolling back transaction...")
                tgt_conn.rollback()
                print("Transaction rolled back.")

    # --- SQL 실행 끝 ---

    # Target 연결 닫기 (SQL 실행 후)
    if tgt_conn:
        tgt_conn.close()
        print("Target connection closed.")


if __name__ == '__main__':
    main()

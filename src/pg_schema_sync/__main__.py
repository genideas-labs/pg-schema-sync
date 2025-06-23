#!/usr/bin/env python3
import psycopg2
from psycopg2 import sql # SQL 식별자 안전 처리용
import yaml # YAML 라이브러리 임포트
import datetime # 타임스탬프용
import os # 디렉토리 생성용
import argparse # 커맨드라인 인수 처리용
import re # SQL 정규화용
from collections import defaultdict
from dataMig import run_data_migration_parallel, compare_row_counts
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
# --- Table Metadata (컬럼 정보) 조회 ---
def fetch_tables_metadata(conn):
    cur = conn.cursor()

    # 1. 테이블 목록 가져오기
    cur.execute("""
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    table_names = [row[0] for row in cur.fetchall()]

    # 2. 제약조건 정보: FK / UNIQUE / PRIMARY
    cur.execute("""
    SELECT
      tc.constraint_name,
      tc.constraint_type,
      tc.table_name,
      kcu.column_name,
      ccu.table_name AS foreign_table,
      ccu.column_name AS foreign_column
    FROM information_schema.table_constraints AS tc
    LEFT JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
    LEFT JOIN information_schema.constraint_column_usage AS ccu
      ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
    WHERE tc.table_schema = 'public';
    """)

    fk_lookup = {}
    unique_col_flags = {}
    primary_col_flags = {}
    composite_uniques_temp = defaultdict(list)
    composite_primaries_temp = defaultdict(list)

    for constraint_name, constraint_type, table, column, ref_table, ref_col in cur.fetchall():
        if constraint_type == 'FOREIGN KEY' and ref_table and ref_col:
            fk_lookup[(table, column)] = {'table': ref_table, 'column': ref_col}
        elif constraint_type == 'UNIQUE':
            if column:
                composite_uniques_temp[(table, constraint_name)].append(column)
        elif constraint_type == 'PRIMARY KEY':
            if column:
                composite_primaries_temp[(table, constraint_name)].append(column)

    for (table, constraint), cols in composite_uniques_temp.items():
        if len(cols) == 1:
            unique_col_flags[(table, cols[0])] = True
        elif len(cols) > 1:
            pass  # 복합 키는 나중에 처리

    for (table, constraint), cols in composite_primaries_temp.items():
        if len(cols) == 1:
            primary_col_flags[(table, cols[0])] = True
        elif len(cols) > 1:
            pass  # 복합 키는 나중에 처리
    
    # 최종 composite 구조 생성
    final_composite_uniques = defaultdict(list)
    for (table, constraint_name), cols in composite_uniques_temp.items():
        if len(cols) > 1:
            # 중복 제거하면서 순서 유지
            seen = set()
            deduped = []
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    deduped.append(c)
            final_composite_uniques[table].append((constraint_name, deduped))

    final_composite_primaries = {}
    for (table, constraint_name), cols in composite_primaries_temp.items():
        if len(cols) > 1:
            # 중복 제거
            seen = set()
            deduped = []
            for c in cols:
                if c not in seen:
                    seen.add(c)
                    deduped.append(c)
            final_composite_primaries[table] = deduped

    # 3. 컬럼 정보 수집
    tables_metadata = {}
    for table_name in table_names:
        cur.execute("""
        SELECT column_name, data_type, is_nullable, udt_name, column_default, is_identity
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position;
        """, (table_name,))

        columns = []
        for col_name, data_type, is_nullable, udt_name, col_default, is_identity in cur.fetchall():
            col_type = data_type
            if data_type == 'ARRAY':
                base_type = udt_name.lstrip('_')
                col_type = base_type + '[]'

            col_data = {
                'name': col_name,
                'type': col_type,
                'nullable': is_nullable == 'YES',
                'default': col_default,
                'identity': is_identity == 'YES'
            }
            if (table_name, col_name) in fk_lookup:
                col_data['foreign_key'] = fk_lookup[(table_name, col_name)]
            if (table_name, col_name) in unique_col_flags:
                col_data['unique'] = True
            if (table_name, col_name) in primary_col_flags:
                col_data['primary_key'] = True

            columns.append(col_data)

        tables_metadata[table_name] = columns

    cur.close()
    return tables_metadata, final_composite_uniques, final_composite_primaries

# def fetch_tables_metadata(conn):
#     """테이블별 컬럼 메타데이터(이름, 타입, Null여부, 기본값, identity, FK, UNIQUE)를 조회합니다."""
#     cur = conn.cursor()
#     params = []
#     query_str = """
#     SELECT table_name
#     FROM information_schema.tables
#     WHERE table_schema = 'public' AND table_type='BASE TABLE'
#     """
#     if EXCLUDE_TABLES:
#         query_str += " AND table_name NOT IN %s"
#         params.append(tuple(EXCLUDE_TABLES))

#     cur.execute(query_str, params if params else None)
#     tables_metadata = {}
#     table_names = [row[0] for row in cur.fetchall()]

#     fk_lookup = {}
#     unique_lookup = set()
#     pk_lookup = set()

#     # 전체 FK 정보 미리 조회
#     cur.execute("""
#     SELECT
#     tc.constraint_type,
#         tc.table_name,
#         kcu.column_name,
#         ccu.table_name AS foreign_table_name,
#         ccu.column_name AS foreign_column_name
#     FROM information_schema.table_constraints AS tc
#     LEFT JOIN information_schema.key_column_usage AS kcu
#         ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
#     LEFT JOIN information_schema.constraint_column_usage AS ccu
#         ON tc.constraint_name = ccu.constraint_name AND tc.table_schema = ccu.table_schema
#     WHERE tc.table_schema = 'public';
#     """)


#     for constraint_type, table_name, column_name, ref_table, ref_column in cur.fetchall():
#         if not column_name:
#             continue  # 복합 키의 일부가 아닐 수 있음
#         if constraint_type == 'FOREIGN KEY' and ref_table and ref_column:
#             fk_lookup[(table_name, column_name)] = {"table": ref_table, "column": ref_column}
#         elif constraint_type == 'UNIQUE':
#             unique_lookup.add((table_name, column_name))
#         elif constraint_type == 'PRIMARY KEY':
#             pk_lookup.add((table_name, column_name))


#     # 테이블별 컬럼 조회
#     for table_name in table_names:
#         col_query = f"""
#         SELECT column_name,
#                data_type,
#                is_nullable,
#                udt_name,
#                column_default,
#                is_identity
#         FROM information_schema.columns
#         WHERE table_schema = 'public' AND table_name = %s
#         ORDER BY ordinal_position;
#         """
#         cur.execute(col_query, (table_name,))
#         columns = []
#         for col_name, data_type, is_nullable, udt_name, col_default, is_identity in cur.fetchall():
#             col_type = data_type
#             if data_type == 'ARRAY':
#                 base_type = udt_name.lstrip('_')
#                 col_type = base_type + '[]' if base_type else 'text[]'  # fallback

#             col_data = {
#                 'name': col_name,
#                 'type': col_type,
#                 'nullable': is_nullable == 'YES',
#                 'default': col_default,
#                 'identity': is_identity == 'YES'
#             }
#             if (table_name, col_name) in fk_lookup:
#                 col_data["foreign_key"] = fk_lookup[(table_name, col_name)]
#             if (table_name, col_name) in unique_lookup:
#                 col_data["unique"] = True
#             if (table_name, col_name) in pk_lookup:
#                 col_data["primary_key"] = True  # ✅ 여기에 추가
#             columns.append(col_data)
#         tables_metadata[table_name] = columns

#     cur.close()
#     return tables_metadata



# --- Table DDL 생성 함수 (메타데이터 기반 - 필요 시 사용) ---
def generate_create_table_ddl(table_name, columns, 
                              composite_uniques=None, 
                              composite_primaries=None):
    """컬럼 메타데이터와 복합 제약 조건으로 CREATE TABLE DDL 생성"""
    composite_uniques = composite_uniques or {}
    composite_primaries = composite_primaries or {}

    col_defs = []
    table_constraints = []
    enum_ddls = []

    for col in columns:
        col_type = col['type']
        quoted_col_name = f'"{col["name"]}"'

        # 사용자 정의 enum 타입 처리
        if isinstance(col_type, str) and col_type.upper() == 'USER-DEFINED':
            if table_name in ("menu_item_opts_set_schema", "menu_item_opts_schema") and col['name'] == "type":
                col_type = "public.option_type"
                enum_ddls.append(
                    """DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'option_type') THEN
    CREATE TYPE public.option_type AS ENUM ('additional', 'substitution');
  END IF;
END$$;"""
                )
            elif table_name == "menu" and col['name'] == "onboarding_status":
                col_type = "public.p2_onboarding_status"
                enum_ddls.append(
                    """DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'p2_onboarding_status') THEN
    CREATE TYPE public.p2_onboarding_status AS ENUM ('NOT_STARTED', 'STEP1', 'STEP2', 'STEP3', 'COMPLETED');
  END IF;
END$$;"""
                )
            elif table_name in {"order_menu_items", "order_payments", "orders"} and col['name'] == "status":
                col_type = "public.order_status"
                enum_ddls.append(
                    """DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'order_status') THEN
    CREATE TYPE public.order_status AS ENUM ('new', 'accepted', 'canceled', 'banned', 'cooking', 'pickup', 'prepayment', 'done');
  END IF;
END$$;"""
                )

        # ✅ inline 컬럼 정의 처리
        is_identity = col.get("identity", False)
        is_primary = col.get("primary_key", False)
        is_unique = col.get("unique", False)
        is_nullable = col.get("nullable", True)
        default_val = col.get("default")

        if is_identity and is_primary:
            col_def = f'{quoted_col_name} BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY'
        else:
            col_def = f"{quoted_col_name} {col_type}"
            if is_identity:
                col_def += " GENERATED BY DEFAULT AS IDENTITY"
            if default_val is not None:
                col_def += f" DEFAULT {default_val}"
            if not is_nullable:
                col_def += " NOT NULL"
            if is_unique:
                col_def += " UNIQUE"
            if is_primary:
                col_def += " PRIMARY KEY"

        col_defs.append(col_def)
    print("composite_uniques",composite_uniques)
    # ✅ 복합 UNIQUE 제약조건
    if table_name in composite_uniques:
        for constraint_name, cols in composite_uniques[table_name]:
            quoted_cols = ", ".join(f'"{c}"' for c in cols)
            table_constraints.append(
                f'CONSTRAINT "{constraint_name}" UNIQUE ({quoted_cols})'
            )

    # ✅ 복합 PRIMARY KEY 제약조건
    if table_name in composite_primaries:
        cols = composite_primaries[table_name]
        quoted_cols = ", ".join(f'"{col}"' for col in cols)
        constraint_name = f"{table_name}_pkey"
        table_constraints.append(f'CONSTRAINT {constraint_name} PRIMARY KEY ({quoted_cols})')
    print("table_constraints",table_constraints)
    # 전체 CREATE TABLE DDL
    all_defs = col_defs + table_constraints
    table_ddl = f'CREATE TABLE public."{table_name}" (\n    ' + ",\n    ".join(all_defs) + "\n);"

    return "\n\n".join(enum_ddls + [table_ddl])


# def generate_foreign_key_ddls(tables_metadata):
#     """모든 foreign key를 ALTER TABLE DDL로 생성"""
#     fk_ddls = []
#     for table_name, columns in tables_metadata.items():
#         for col in columns:
#             if "foreign_key" in col:
#                 fk = col["foreign_key"]
#                 constraint_name = f"{table_name}_{col['name']}_fkey"
#                 ddl = (
#                     f'ALTER TABLE public."{table_name}" '
#                     f'ADD CONSTRAINT "{constraint_name}" '
#                     f'FOREIGN KEY ("{col["name"]}") '
#                     f'REFERENCES public."{fk["table"]}" ("{fk["column"]}");'
#                 )
#                 fk_ddls.append(ddl)
#     return fk_ddls

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
    """인덱스 DDL을 조회하되, UNIQUE/PRIMARY KEY 제약조건으로 생성된 인덱스는 제외합니다."""
    cur = conn.cursor()

    # 1. constraint에서 생성된 인덱스 이름들 수집
    cur.execute("""
    SELECT conname
    FROM pg_constraint
    WHERE contype IN ('u', 'p')  -- UNIQUE or PRIMARY KEY
      AND connamespace = 'public'::regnamespace;
    """)
    constraint_index_names = {row[0] for row in cur.fetchall()}

    # 2. pg_indexes에서 일반 인덱스 조회
    cur.execute("""
    SELECT indexname,
           indexdef
    FROM pg_indexes
    WHERE schemaname = 'public';
    """)

    indexes = {}
    pkey_indexes = {}

    for indexname, ddl in cur.fetchall():
        if indexname in constraint_index_names:
            # ✅ UNIQUE/PK constraint에서 유래한 인덱스는 무시
            continue
        if indexname.endswith('_pkey'):
            pkey_indexes[indexname] = ddl
        else:
            indexes[indexname] = ddl

    cur.close()
    return indexes, pkey_indexes


# --- Sequence DDL 조회 ---
def fetch_sequences(conn):
    """시퀀스 DDL을 조회합니다."""
    cur = conn.cursor()
    
    # 시퀀스 목록 조회
    query = """
    SELECT 
        c.relname as sequence_name
    FROM pg_class c 
    JOIN pg_namespace n ON c.relnamespace = n.oid 
    WHERE n.nspname = 'public' AND c.relkind = 'S'
    ORDER BY c.relname;
    """
    cur.execute(query)
    rows = cur.fetchall()
    
    sequences = {}
    
    for row in rows:
        seq_name = row[0]
        
        # 시퀀스의 현재 값 확인
        try:
            cur.execute(f"SELECT last_value, is_called FROM public.{seq_name}")
            current_last_value, current_is_called = cur.fetchone()
        except Exception as e:
            print(f"Warning: Could not fetch current value for sequence {seq_name}: {e}")
            current_last_value, current_is_called = None, False
        
        # 기본 CREATE SEQUENCE DDL 생성
        ddl_parts = [f"CREATE SEQUENCE public.{seq_name}"]
        
        # 현재 값 설정 (시퀀스가 이미 사용된 경우)
        if current_is_called and current_last_value is not None:
            ddl_parts.append(f"RESTART WITH {current_last_value}")
        
        ddl = " ".join(ddl_parts) + ";"
        sequences[seq_name] = ddl
    
    cur.close()
    return sequences

def sync_sequence_values(src_conn, tgt_conn, sequence_names):
    """시퀀스의 현재 값을 소스에서 타겟으로 동기화합니다."""
    print("\n--- Syncing Sequence Values ---")
    
    with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
        for seq_name in sequence_names:
            try:
                # 소스 시퀀스의 현재 값 조회
                src_cur.execute(f"SELECT last_value, is_called FROM public.{seq_name}")
                src_last_value, src_is_called = src_cur.fetchone()
                
                # 타겟 시퀀스의 현재 값 조회
                tgt_cur.execute(f"SELECT last_value, is_called FROM public.{seq_name}")
                tgt_last_value, tgt_is_called = tgt_cur.fetchone()
                
                print(f"  📊 {seq_name}:")
                print(f"    Source: last_value={src_last_value}, is_called={src_is_called}")
                print(f"    Target: last_value={tgt_last_value}, is_called={tgt_is_called}")
                
                # 값이 다른 경우에만 업데이트
                if src_last_value != tgt_last_value:
                    # 시퀀스 값을 소스와 동일하게 설정
                    setval_sql = f"SELECT setval('public.{seq_name}', {src_last_value}, {src_is_called})"
                    print(f"    Executing: {setval_sql}")
                    tgt_cur.execute(setval_sql)
                    
                    # 업데이트 후 값 확인
                    tgt_cur.execute(f"SELECT last_value, is_called FROM public.{seq_name}")
                    new_tgt_last_value, new_tgt_is_called = tgt_cur.fetchone()
                    print(f"    After setval: last_value={new_tgt_last_value}, is_called={new_tgt_is_called}")
                    
                    print(f"  ✅ {seq_name}: {tgt_last_value} → {src_last_value}")
                else:
                    print(f"  ⏭️  {seq_name}: already synced ({src_last_value})")
                    
            except Exception as e:
                print(f"  ❌ {seq_name}: failed to sync - {e}")
                import traceback
                traceback.print_exc()

# --- 안전한 타입 변경 판단 함수 ---
def is_safe_type_change(old_type, new_type):
    """암시적 변환이 가능하고 안전한 타입 변경인지 판단합니다."""
    old_type_norm = normalize_sql(old_type)
    new_type_norm = normalize_sql(new_type)

    # varchar 길이 증가 또는 text로 변경
    if old_type_norm.startswith('character varying') and (new_type_norm.startswith('character varying') or new_type_norm == 'text'):
        try:
            old_len_match = re.search(r'\((\d+)\)', old_type_norm)
            new_len_match = re.search(r'\((\d+)\)', new_type_norm)
            old_len = int(old_len_match.group(1)) if old_len_match else float('inf')
            new_len = int(new_len_match.group(1)) if new_len_match else float('inf')
            # 길이가 같거나 증가하는 경우 또는 text로 변경하는 경우 안전
            return new_len >= old_len or new_type_norm == 'text'
        except:
            return False # 길이 파싱 실패 시 안전하지 않음으로 간주
    # 숫자 타입 확장 (smallint -> int -> bigint)
    elif old_type_norm == 'smallint' and new_type_norm in ['integer', 'bigint']:
        return True
    elif old_type_norm == 'integer' and new_type_norm == 'bigint':
        return True
    # 숫자 -> 문자열 (일반적으로 안전)
    elif old_type_norm in ['smallint', 'integer', 'bigint', 'numeric', 'real', 'double precision'] and \
         (new_type_norm.startswith('character varying') or new_type_norm == 'text'):
         return True
    # TODO: 다른 안전한 변환 추가 가능 (예: timestamp -> timestamptz)

    return False # 그 외는 안전하지 않음으로 간주

# --- 비교 후 migration SQL 생성 (타입별 로직 분기, Enum DDL 참조 추가, ALTER TABLE 지원 추가) ---
def compare_and_generate_migration(src_data, tgt_data, obj_type, src_enum_ddls=None, use_alter=False,
                                 src_composite_uniques=None, tgt_composite_uniques=None,
                                 src_composite_primaries=None, tgt_composite_primaries=None):
    """
    소스와 타겟 데이터를 비교하여 마이그레이션 SQL과 건너뛴 SQL을 생성합니다.
    obj_type에 따라 비교 방식을 다르게 적용합니다.
    use_alter=True일 경우, 테이블 컬럼 추가/삭제에 대해 ALTER TABLE 사용 시도.
    Enum 타입의 DDL 생성을 위해 src_enum_ddls 딕셔너리가 필요합니다.
    """
    migration_sql = []
    skipped_sql = []
    alter_statements = [] # 함수 시작 시 초기화
    src_keys = set(src_data.keys())
    tgt_keys = set(tgt_data.keys())

    # 소스에만 있는 객체 처리
    for name in src_keys - tgt_keys:
        if obj_type == "TABLE":
            ddl = generate_create_table_ddl(
                        name,
                        src_data[name],
                        composite_uniques=src_composite_uniques,
                        composite_primaries=src_composite_primaries
                        )

        elif obj_type == "TYPE": # 소스에만 있는 Enum 처리
            ddl = src_enum_ddls.get(name, f"-- ERROR: DDL not found for Enum {name}")
        elif obj_type == "SEQUENCE": # 소스에만 있는 Sequence 처리
            ddl = src_data.get(name, f"-- ERROR: DDL not found for Sequence {name}")
        elif obj_type == "INDEX":
            raw_ddl = src_data.get(name, f"-- ERROR: DDL not found for Index {name}")
            ddl = f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = '{name}'
                        ) THEN
                            {raw_ddl};
                        END IF;
                    END$$;
                    """.strip()    
        else: # View, Function, Index 등
            ddl = src_data.get(name, f"-- ERROR: DDL not found for {obj_type} {name}")
        migration_sql.append(f"-- CREATE {obj_type} {name}\n{ddl}\n")

    # 양쪽에 모두 있는 객체 비교 처리
    for name in src_keys.intersection(tgt_keys):
        are_different = False
        ddl = "" # 변경 시 사용할 DDL (주로 소스 기준)

        if obj_type == "TABLE":
            src_cols_map = {col['name']: col for col in src_data[name]}
            tgt_cols_map = {col['name']: col for col in tgt_data[name]}
            src_col_names = set(src_cols_map.keys())
            tgt_col_names = set(tgt_cols_map.keys())

            cols_to_add = src_col_names - tgt_col_names
            cols_to_drop = tgt_col_names - src_col_names
            cols_to_compare = src_col_names.intersection(tgt_col_names)

            # alter_statements = [] # 여기서 초기화 제거
            needs_recreate = False # ALTER로 처리 불가능한 변경이 있는지 여부

            # 공통 컬럼이 하나도 없고, 추가/삭제할 컬럼이 있다면 재 생성 필요 (버그 수정)
            if not cols_to_compare and (cols_to_add or cols_to_drop):
                 needs_recreate = True

            # 컬럼 정의 비교 (타입, Null 여부 등) - needs_recreate가 아직 False일 때만 수행
            if not needs_recreate:
                for col_name in cols_to_compare:
                    src_col = src_cols_map[col_name]
                    tgt_col = tgt_cols_map[col_name]
                    src_type_norm = normalize_sql(src_col['type'])
                    tgt_type_norm = normalize_sql(tgt_col['type'])

                    # 1. 타입 변경 확인
                    if src_type_norm != tgt_type_norm:
                        if use_alter and is_safe_type_change(tgt_type_norm, src_type_norm):
                            # 안전한 타입 변경이면 ALTER TYPE 추가
                            quoted_col_name = f'"{col_name}"' # 따옴표 추가
                            alter_statements.append(f"ALTER TABLE public.{name} ALTER COLUMN {quoted_col_name} TYPE {src_col['type']};")
                        else:
                            # 안전하지 않은 타입 변경이면 재 생성 필요
                            needs_recreate = True
                            break

                    # 2. Null 허용 여부 변경 확인 (타입이 동일할 때만 고려)
                    elif src_col['nullable'] != tgt_col['nullable']:
                        if use_alter:
                            if src_col['nullable'] is False: # NOT NULL로 변경
                                alter_statements.append(f"-- WARNING: Setting NOT NULL on column {col_name} may fail if existing data contains NULLs.")
                                quoted_col_name = f'"{col_name}"' # 따옴표 추가
                                alter_statements.append(f"ALTER TABLE public.{name} ALTER COLUMN {quoted_col_name} SET NOT NULL;")
                            else: # NULL 허용으로 변경
                                quoted_col_name = f'"{col_name}"' # 따옴표 추가
                                alter_statements.append(f"ALTER TABLE public.{name} ALTER COLUMN {quoted_col_name} DROP NOT NULL;")
                        else:
                             # use_alter=False 이면 재 생성 필요
                             needs_recreate = True
                             break

            # ALTER 문 생성 (컬럼 추가/삭제) - needs_recreate가 False이고 use_alter=True일 때만
            if not needs_recreate and use_alter:
                if cols_to_add:
                    for col_name in cols_to_add:
                        col = src_cols_map[col_name]
                        col_def = f"{col['name']} {col['type']}"
                        if col['default'] is not None:
                            col_def += f" DEFAULT {col['default']}"
                        if not col['nullable']:
                            col_def += " NOT NULL"
                        # sql.Identifier 사용 위해 conn 객체 필요 -> 임시 처리 (f-string 오류 수정)
                        default_clause = f" DEFAULT {col['default']}" if col.get('default') is not None else ""
                        not_null_clause = " NOT NULL" if not col.get('nullable', True) else ""
                        # 컬럼 이름에 따옴표 추가 (psycopg2.sql.Identifier 대신 임시 사용)
                        quoted_col_name = f'"{col_name}"'
                        alter_statements.append(f"ALTER TABLE public.{name} ADD COLUMN {quoted_col_name} {col['type']}{default_clause}{not_null_clause};")
                if cols_to_drop:
                    for col_name in cols_to_drop:
                        # 컬럼 삭제는 위험하므로 주석 추가
                        alter_statements.append(f"-- WARNING: Dropping column {col_name} may cause data loss.")
                        # 컬럼 이름에 따옴표 추가 (psycopg2.sql.Identifier 대신 임시 사용)
                        quoted_col_name = f'"{col_name}"'
                        alter_statements.append(f"ALTER TABLE public.{name} DROP COLUMN {quoted_col_name};")

                if alter_statements: # ALTER 문이 생성된 경우 (추가/삭제/변경 포함)
                    migration_sql.append(f"-- ALTER TABLE {name} for column changes\n" + "\n".join(alter_statements) + "\n")
                    are_different = True # 마이그레이션 SQL이 생성되었으므로 different로 처리
                else:
                    # ALTER 문 없고, needs_recreate도 False이면 변경 없음
                    are_different = False

            # 재 생성 필요 여부 최종 결정
            # needs_recreate가 True이면 무조건 재 생성
            if needs_recreate:
                are_different = True
                ddl = generate_create_table_ddl(
                    name,
                    src_data[name],
                    composite_uniques=src_composite_uniques,
                        composite_primaries=src_composite_primaries
                )

                alter_statements = [] # ALTER 문은 무시
            # use_alter=False 이고 컬럼 구성이 다르면 재 생성
            elif not use_alter and (len(src_cols_map) != len(tgt_cols_map) or \
                                    any(sc['name'] != tc['name'] or \
                                        normalize_sql(sc['type']) != normalize_sql(tc['type']) or \
                                        sc['nullable'] != tc['nullable']
                                        for sc, tc in zip(src_data[name], tgt_data[name]))):
                 are_different = True
                 ddl = generate_create_table_ddl(
                        name,
                        src_data[name],
                        composite_uniques=src_composite_uniques,
                        composite_primaries=src_composite_primaries
                        )

                 alter_statements = [] # ALTER 문은 무시
            elif not alter_statements:
                 # 재 생성 필요 없고, ALTER 문도 없으면 변경 없음
                 are_different = False
        elif obj_type == "INDEX":
            if name not in tgt_data:
                ddl = src_data[name]
                ddl = f"""
                        DO $$
                        BEGIN
                            IF NOT EXISTS (
                                SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = '{name}'
                            ) THEN
                                {ddl};
                            END IF;
                        END$$;
                        """.strip()
                migration_sql.append(f"-- INDEX {name} differs or missing. Adding.\n{ddl}\n")
                continue
            else:
                if normalize_sql(src_data[name]) != normalize_sql(tgt_data[name]):
                    ddl = src_data[name]
                    ddl = f"""
                            DO $$
                            BEGIN
                                IF NOT EXISTS (
                                    SELECT 1 FROM pg_indexes WHERE schemaname = 'public' AND indexname = '{name}'
                                ) THEN
                                    {ddl};
                                END IF;
                            END$$;
                            """.strip()
                    migration_sql.append(f"-- INDEX {name} differs. Replacing.\n{ddl}\n")
                else:
                    commented = '\n'.join([f"-- {line}" for line in src_data[name].strip().splitlines()])
                    skipped_sql.append(f"-- INDEX {name} is up-to-date; skipping.\n{commented}\n")
                continue
        elif obj_type == "TYPE": # Enum 타입 가정
            src_values = src_data[name]
            tgt_values = tgt_data[name]
            if src_values != tgt_values:
                are_different = True
                # Enum DDL은 src_enum_ddls 에서 가져옴
                ddl = src_enum_ddls.get(name, f"-- ERROR: DDL not found for Enum {name}")
        elif obj_type == "FUNCTION":
            # 함수는 원본 DDL로 비교 (정규화 시 달러 인용 문제 발생 가능성)
            if src_data[name] != tgt_data[name]:
                are_different = True
                ddl = src_data[name]
        elif obj_type == "FOREIGN_KEY":
            if name not in tgt_data:
                are_different = True
                ddl = src_data[name]
            else:
                src_ddl = normalize_sql(src_data[name])
                tgt_ddl = normalize_sql(tgt_data[name])
                if src_ddl != tgt_ddl:
                    are_different = True
                    ddl = src_data[name]

            if are_different:
                # ✅ DROP 없이 추가만 시도
                migration_sql.append(f"-- FOREIGN_KEY {name} differs or missing. Adding.\n{ddl}\n")
            else:
                # 스킵 처리
                commented = '\n'.join([f"-- {line}" for line in src_data[name].strip().splitlines()])
                skipped_sql.append(f"-- FOREIGN_KEY {name} is up-to-date; skipping.\n{commented}\n")
            
            continue  # 👈 중복 방지를 위해 이후 공통 처리 블록 건너뜀
        elif obj_type == "SEQUENCE": # 양쪽에 있는 Sequence 처리
            # 시퀀스가 테이블에서 사용 중일 수 있으므로 DROP 대신 ALTER 사용
            src_ddl_norm = normalize_sql(src_data[name])
            tgt_ddl_norm = normalize_sql(tgt_data[name])
            if src_ddl_norm != tgt_ddl_norm:
                # RESTART WITH 값만 추출하여 ALTER SEQUENCE 사용
                restart_match = re.search(r'RESTART WITH (\d+)', src_data[name])
                if restart_match:
                    restart_value = restart_match.group(1)
                    ddl = f"ALTER SEQUENCE public.{name} RESTART WITH {restart_value};"
                    migration_sql.append(f"-- ALTER SEQUENCE {name} to sync current value\n{ddl}\n")
                else:
                    # RESTART WITH가 없으면 기본 CREATE SEQUENCE 사용
                    ddl = src_data[name]
                    migration_sql.append(f"-- SEQUENCE {name} differs. Recreating.\nDROP SEQUENCE IF EXISTS public.{name} CASCADE;\n{ddl}\n")
            else:
                # 동일한 경우 스킵
                commented = '\n'.join([f"-- {line}" for line in src_data[name].strip().splitlines()])
                skipped_sql.append(f"-- SEQUENCE {name} is up-to-date; skipping.\n{commented}\n")
            continue  # 중복 방지를 위해 이후 공통 처리 블록 건너뜀
        else:
            # 나머지 타입 (View, Index, Sequence): 정규화된 DDL 비교
            src_ddl_norm = normalize_sql(src_data[name])
            tgt_ddl_norm = normalize_sql(tgt_data[name])
            if src_ddl_norm != tgt_ddl_norm:
                are_different = True
                ddl = src_data[name] # 변경 시 소스 DDL 사용

        # 비교 결과에 따라 SQL 생성 (TABLE 타입은 위에서 처리됨)
        if obj_type == "FOREIGN_KEY" and are_different:
            # FOREIGN KEY는 DROP CONSTRAINT 없이 그냥 ADD CONSTRAINT만 시도
            migration_sql.append(f"-- FOREIGN_KEY {name} differs or missing. Adding.\n{ddl}\n")
        elif obj_type != "TABLE" and are_different:
            # TABLE 외 다른 타입이 다르거나, TABLE이 ALTER 불가하여 재 생성 필요한 경우
            action = "Recreating" if obj_type != "FUNCTION" else "Updating" # 함수는 Update로 표시 (DROP/CREATE 동일)
            migration_sql.append(f"-- {obj_type} {name} differs. {action}.\nDROP {obj_type.upper()} IF EXISTS public.{name} CASCADE;\n{ddl}\n")
        elif obj_type == "TABLE" and are_different and not alter_statements:
             # TABLE이 다르지만 ALTER 문이 생성되지 않은 경우 (재 생성 필요)
             migration_sql.append(f"-- TABLE {name} differs significantly. Recreating.\nDROP TABLE IF EXISTS public.{name} CASCADE;\n{ddl}\n")
        elif not are_different and not alter_statements: # 테이블 포함 모든 타입이 동일하고 ALTER 문도 없는 경우
            # 동일한 경우: 스킵 처리
            original_ddl = ""
            if obj_type == "TABLE":
                 original_ddl = generate_create_table_ddl(
                        name,
                        src_data[name],
                        composite_uniques=src_composite_uniques,
                        composite_primaries=src_composite_primaries
                        )
            elif obj_type == "TYPE":
                 original_ddl = src_enum_ddls.get(name, "") # 스킵 로그용 Enum DDL
            else: # View, Function, Index, Sequence 등
                 original_ddl = src_data.get(name, "") # src_data가 DDL 딕셔너리라고 가정

            skipped_sql.append(f"-- {obj_type} {name} is up-to-date; skipping.\n")
            if original_ddl: # DDL이 있는 경우만 주석 처리하여 추가
                 commented_ddl = '\n'.join([f"-- {line}" for line in original_ddl.strip().splitlines()])
                 skipped_sql.append(commented_ddl + "\n")

    # 타겟에만 있는 객체는 현재 처리하지 않음 (필요 시 추가)

    return migration_sql, skipped_sql


# --- SQL 정규화 함수 ---
def normalize_sql(sql_text):
    """SQL 문자열에서 주석 제거, 소문자 변환, 공백 정규화 수행 (달러 인용 문자열 보호)"""
    if not sql_text:
        return ""

    # 달러 인용 문자열 추출 및 임시 치환
    dollar_quoted_strings = []
    def replace_dollar_quoted(match):
        dollar_quoted_strings.append(match.group(0))
        return f"__DOLLAR_QUOTED_STRING_{len(dollar_quoted_strings)-1}__"

    # 정규 표현식 수정: 시작과 끝 태그가 동일해야 함 ($tag$...$tag$)
    # 태그는 비어있거나, 문자로만 구성될 수 있음
    sql_text_no_dollars = re.sub(r"(\$([a-zA-Z_]\w*)?\$).*?\1", replace_dollar_quoted, sql_text, flags=re.DOTALL)

    # -- 스타일 주석 제거
    processed_sql = re.sub(r'--.*$', '', sql_text_no_dollars, flags=re.MULTILINE)
    # /* */ 스타일 주석 제거 (간단한 경우만 처리, 중첩 불가)
    # processed_sql = re.sub(r'/\*.*?\*/', '', processed_sql, flags=re.DOTALL) # 필요 시 추가

    # 소문자로 변환 (달러 인용 제외 부분만)
    processed_sql = processed_sql.lower()
    # 괄호, 쉼표, 세미콜론 주변 공백 제거
    processed_sql = re.sub(r'\s*([(),;])\s*', r'\1', processed_sql)
    # 등호(=) 등 연산자 주변 공백 제거 (더 많은 연산자 추가 가능)
    processed_sql = re.sub(r'\s*([=<>!+-/*%])\s*', r'\1', processed_sql)
    # 여러 공백 (스페이스, 탭, 개행 포함)을 단일 스페이스로 변경
    processed_sql = re.sub(r'\s+', ' ', processed_sql)
    # 앞뒤 공백 제거
    processed_sql = processed_sql.strip()

    # 임시 치환된 달러 인용 문자열 복원
    for i, original_string in enumerate(dollar_quoted_strings):
        processed_sql = processed_sql.replace(f"__DOLLAR_QUOTED_STRING_{i}__", original_string)

    # 마지막 세미콜론 제거 (옵션)
    return processed_sql.rstrip(';')


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

def extract_foreign_keys(metadata):
    """
    { "table.col->ref_table.ref_col": DDL } 형태로 반환
    """
    fk_map = {}
    for table_name, columns in metadata.items():
        for col in columns:
            fk = col.get("foreign_key")
            if fk:
                constraint_key = f"{table_name}.{col['name']}->{fk['table']}.{fk['column']}"
                constraint_name = f"{table_name}_{col['name']}_fkey"
                ddl = (
                    f'ALTER TABLE public."{table_name}" '
                    f'ADD CONSTRAINT "{constraint_name}" '
                    f'FOREIGN KEY ("{col["name"]}") '
                    f'REFERENCES public."{fk["table"]}" ("{fk["column"]}");'
                )
                fk_map[constraint_key] = ddl
    return fk_map

def main():
    # --- 커맨드라인 인수 파싱 ---
    parser = argparse.ArgumentParser(description="Compare source and target PostgreSQL schemas and generate/apply migration SQL, or verify differences.")
    # Verification flag
    parser.add_argument('--verify', action='store_true',
                        help="Only verify schema differences (object names and counts) without generating/executing SQL.")
    # Commit flag (only relevant if --verify is not used)
    parser.add_argument('--commit', action=argparse.BooleanOptionalAction, default=True,
                        help="Execute the generated migration SQL on the target database. Use --no-commit to only generate files. Ignored if --verify is used.")
    # Experimental ALTER TABLE flag
    parser.add_argument('--use-alter', action='store_true', default=False,
                        help="EXPERIMENTAL: Use ALTER TABLE for column additions/deletions instead of DROP/CREATE. Use with caution.")
    parser.add_argument('--with-data', action='store_true',
                    help="Include data migration after schema changes")
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
    src_tables_meta, src_composite_uniques, src_composite_primaries = fetch_tables_metadata(src_conn)
    tgt_tables_meta, tgt_composite_uniques, tgt_composite_primaries = fetch_tables_metadata(tgt_conn)


    print("Fetching View DDLs...")
    src_views = fetch_views(src_conn) # 비교 및 DDL 생성용
    tgt_views = fetch_views(tgt_conn) # 비교용

    print("Fetching Function DDLs...")
    src_functions = fetch_functions(src_conn) # 비교 및 DDL 생성용
    tgt_functions = fetch_functions(tgt_conn) # 비교용

    print("Fetching Index DDLs...")
    src_indexes, src_pkey_indexes = fetch_indexes(src_conn) # 비교 및 DDL 생성용 + 정보용
    tgt_indexes, tgt_pkey_indexes = fetch_indexes(tgt_conn) # 비교용 + 정보용

    print("Fetching Sequence DDLs...")
    print("  Fetching from source database...")
    src_sequences = fetch_sequences(src_conn) # 비교 및 DDL 생성용
    print("  Fetching from target database...")
    tgt_sequences = fetch_sequences(tgt_conn) # 비교용
    print(f"  Source sequences count: {len(src_sequences)}")
    print(f"  Target sequences count: {len(tgt_sequences)}")
    # --- 데이터 조회 끝 ---


    # --- 검증 모드 처리 ---
    if args.verify:
        print("\n--- Schema Verification Mode ---")
        all_synced = True
        # 검증 시에는 이름 목록만 비교
        all_synced &= print_verification_report(src_enum_ddls, tgt_enum_ddls, "Enums (Types)")
        all_synced &= print_verification_report(src_sequences, tgt_sequences, "Sequences")
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
    if args.use_alter:
        print("--- Using experimental ALTER TABLE mode ---")
    all_migration_sql = [] # 실제 마이그레이션 SQL 저장
    all_skipped_sql = []   # 건너뛴 SQL 저장

    # 순서: enum, sequence, table, view, function, index
    print("Comparing Enums (Values)...")
    # Enum 비교 시 값 목록(values)을 사용하고, DDL 생성을 위해 src_enum_ddls 전달
    mig_sql, skip_sql = compare_and_generate_migration(src_enum_values, tgt_enum_values, "TYPE", src_enum_ddls=src_enum_ddls)
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)

    print("Comparing Sequences (DDL)...")
    mig_sql, skip_sql = compare_and_generate_migration(src_sequences, tgt_sequences, "SEQUENCE")
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)

    print("Comparing Tables (Metadata)...")
    # use_alter 옵션 전달
    mig_sql, skip_sql = compare_and_generate_migration(src_tables_meta, tgt_tables_meta, "TABLE", 
                                                       use_alter=args.use_alter, src_enum_ddls=src_enum_ddls,
                                                       src_composite_uniques = src_composite_uniques,tgt_composite_uniques= tgt_composite_uniques,
                                                       src_composite_primaries = src_composite_primaries, tgt_composite_primaries = tgt_composite_primaries  ) # src_enum_ddls 전달 추가
    all_migration_sql.extend(mig_sql)
    all_skipped_sql.extend(skip_sql)
    # 테이블 메타데이터를 기반으로 FK DDL 생성



    print("Comparing Foreign Keys...")  # 👈 이 부분 추가

    src_fk_map = extract_foreign_keys(src_tables_meta)
    tgt_fk_map = extract_foreign_keys(tgt_tables_meta)

    mig_sql, skip_sql = compare_and_generate_migration(src_fk_map, tgt_fk_map, "FOREIGN_KEY")
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

    if args.with_data:
        print("\n-- Running only Data Migration --")
        try:
            run_data_migration_parallel(src_conn ,src_tables_meta)
            print("\nData migration completed and committed.")

            # 시퀀스 값 동기화 실행 (데이터 마이그레이션 후)
            common_sequences = set(src_sequences.keys()) & set(tgt_sequences.keys())
            if common_sequences:
                sync_sequence_values(src_conn, tgt_conn, common_sequences)
                tgt_conn.commit()
                print("Sequence values synchronized and committed.")

            # 검증: 모든 테이블의 row count 비교
            table_list = list(src_tables_meta.keys())
            diffs = compare_row_counts(src_conn, tgt_conn, table_list)
            if diffs:
                print("\n❗ Row count mismatches detected:")
                for tbl, (src_cnt, tgt_cnt) in diffs.items():
                    print(f"  - {tbl}: source={src_cnt}, target={tgt_cnt}")
            else:
                print("\n✅ All row counts match between source and target.")

        except Exception as e:
            print(f"Error during data migration: {e}")
            print("Transaction rolled back.")
        finally:
            src_conn.close()
            print("Connections closed.")
        return  
    # --- 마이그레이션 SQL 실행 (commit 옵션이 True일 경우) ---
    elif args.commit:
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

                        # SQL 블록 전체를 단일 문장으로 실행
                        # (compare_and_generate_migration에서 이미 완전한 DDL 단위로 생성됨)
                        if not sql_content.strip(): # 추가된 빈 블록 스킵
                            continue

                        print(f"--- Executing Block {i+1} (1 statement) ---")
                        try:
                            # sql_content 전체를 실행
                            print(f"  Executing statement 1: {sql_content[:100]}{'...' if len(sql_content) > 100 else ''}")
                            cur.execute(sql_content)
                        except psycopg2.Error as e:
                            print(f"\nError executing block {i+1}:")
                            print(f"  Block Content: {sql_content}")
                            print(f"  Error: {e}")
                            print("Rolling back transaction...")
                            tgt_conn.rollback()
                            print("Transaction rolled back.")
                            execution_successful = False
                            break # 오류 발생 시 전체 실행 중단
                        except Exception as e:
                            print(f"\nAn unexpected error occurred during block {i+1} execution:")
                            print(f"  Block Content: {sql_content}")
                            print(f"  Error: {e}")
                            print("Rolling back transaction...")
                            tgt_conn.rollback()
                            print("Transaction rolled back.")
                            execution_successful = False
                            break # 오류 발생 시 전체 실행 중단

                # 모든 블록 및 문장이 성공적으로 실행된 경우에만 커밋
                if execution_successful:
                    tgt_conn.commit()
                    print("\nMigration SQL executed successfully and committed.")
                    
                    # 시퀀스 값 동기화 실행
                    common_sequences = set(src_sequences.keys()) & set(tgt_sequences.keys())
                    print(f"\n--- Sequence Sync Debug ---")
                    print(f"Source sequences: {list(src_sequences.keys())}")
                    print(f"Target sequences: {list(tgt_sequences.keys())}")
                    print(f"Common sequences: {list(common_sequences)}")
                    
                    if common_sequences:
                        print(f"Calling sync_sequence_values with {len(common_sequences)} sequences...")
                        sync_sequence_values(src_conn, tgt_conn, common_sequences)
                        tgt_conn.commit()
                        print("Sequence values synchronized and committed.")
                    else:
                        print("No common sequences found, skipping sequence sync.")
                    
                    src_conn.close()
                    tgt_conn.close()
                    print("Connections closed.")
                else:
                    print("Migration SQL execution failed, skipping sequence sync.")
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


if __name__ == '__main__':
    main()

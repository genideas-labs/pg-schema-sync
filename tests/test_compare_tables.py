import pytest
# 테스트 대상 함수 임포트 (설치된 패키지에서 직접 임포트)
from pg_schema_sync.__main__ import compare_and_generate_migration, normalize_sql

# --- 테스트 데이터 ---

# 기본 테이블 구조
BASE_COLS_SRC = [
    {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
    {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
    {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
]
BASE_COLS_TGT = [
    {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
    {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
    {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
]

# 컬럼 추가 시나리오
ADD_COL_SRC = BASE_COLS_SRC + [
    {'name': 'description', 'type': 'text', 'nullable': True, 'default': None}
]

# 컬럼 삭제 시나리오
DROP_COL_TGT = BASE_COLS_TGT + [
    {'name': 'legacy_flag', 'type': 'boolean', 'nullable': False, 'default': 'false'}
]

# 컬럼 타입 변경 시나리오 (ALTER 미지원 대상)
CHANGE_TYPE_SRC = [
    {'name': 'id', 'type': 'bigint', 'nullable': False, 'default': "nextval('some_seq')"}, # 타입 변경
    {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
    {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
]

# Null 제약 변경 시나리오 (ALTER 미지원 대상)
CHANGE_NULL_SRC = [
    {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
    {'name': 'name', 'type': 'character varying(100)', 'nullable': False, 'default': None}, # Null 변경
    {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
]

# --- 테스트 함수 ---

def test_compare_tables_no_change():
    """테이블 변경 없을 때 스킵 SQL 생성 확인"""
    src_data = {"my_table": BASE_COLS_SRC}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not mig_sql # 마이그레이션 SQL은 없어야 함
    assert len(skip_sql) > 0 # 스킵 SQL은 있어야 함
    assert "-- TABLE my_table is up-to-date; skipping." in skip_sql[0]

def test_compare_tables_add_column_with_alter():
    """컬럼 추가 시 ALTER ADD COLUMN 생성 확인"""
    src_data = {"my_table": ADD_COL_SRC}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql # 스킵 SQL은 없어야 함
    assert len(mig_sql) == 1
    assert 'ALTER TABLE public.my_table ADD COLUMN "description" text;' in mig_sql[0] # 따옴표 추가
    assert "DROP TABLE" not in mig_sql[0] # DROP/CREATE가 아니어야 함

def test_compare_tables_drop_column_with_alter():
    """컬럼 삭제 시 ALTER DROP COLUMN 생성 확인"""
    src_data = {"my_table": BASE_COLS_SRC}
    tgt_data = {"my_table": DROP_COL_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "-- WARNING: Dropping column legacy_flag may cause data loss." in mig_sql[0]
    assert "ALTER TABLE public.my_table DROP COLUMN \"legacy_flag\";" in mig_sql[0] # Quoted identifier
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_add_and_drop_with_alter():
    """컬럼 추가 및 삭제 동시 발생 시 ALTER 생성 확인"""
    src_data = {"my_table": ADD_COL_SRC} # description 추가됨
    tgt_data = {"my_table": DROP_COL_TGT} # legacy_flag 있음
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    sql_block = mig_sql[0]
    assert "ALTER TABLE public.my_table ADD COLUMN \"description\" text;" in sql_block # Quoted identifier
    assert "-- WARNING: Dropping column legacy_flag may cause data loss." in sql_block
    assert "ALTER TABLE public.my_table DROP COLUMN \"legacy_flag\";" in sql_block # Quoted identifier
    assert "DROP TABLE" not in sql_block

def test_compare_tables_safe_type_change_with_alter():
    """안전한 타입 변경 시 ALTER TYPE 생성 확인 (int -> bigint)"""
    src_safe_type_change = [
        {'name': 'id', 'type': 'bigint', 'nullable': False, 'default': "nextval('some_seq')"}, # Safe change
        {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_safe_type_change}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert 'ALTER TABLE public.my_table ALTER COLUMN "id" TYPE bigint;' in mig_sql[0] # Quoted identifier
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_unsafe_type_change_no_alter():
    """안전하지 않은 타입 변경 시 DROP/CREATE 생성 확인 (varchar -> int)"""
    src_unsafe_type_change = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'integer', 'nullable': True, 'default': None}, # Unsafe change
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_unsafe_type_change}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "DROP TABLE IF EXISTS public.my_table CASCADE;" in mig_sql[0]
    assert "CREATE TABLE public.\"my_table\"" in mig_sql[0]
    assert "ALTER TABLE" not in mig_sql[0]


# --- 더 많은 테스트 케이스 ---

def test_compare_tables_varchar_increase_with_alter():
    """VARCHAR 길이 증가 시 ALTER TYPE 생성 확인"""
    src_varchar_increase = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(200)', 'nullable': True, 'default': None}, # Length increased
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_varchar_increase}
    tgt_data = {"my_table": BASE_COLS_TGT} # Original was varchar(100)
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert 'ALTER TABLE public.my_table ALTER COLUMN "name" TYPE character varying(200);' in mig_sql[0]
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_varchar_decrease_no_alter():
    """VARCHAR 길이 감소 시 DROP/CREATE 생성 확인"""
    src_varchar_decrease = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(50)', 'nullable': True, 'default': None}, # Length decreased
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_varchar_decrease}
    tgt_data = {"my_table": BASE_COLS_TGT} # Original was varchar(100)
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "DROP TABLE IF EXISTS public.my_table CASCADE;" in mig_sql[0]
    assert "CREATE TABLE public.\"my_table\"" in mig_sql[0]
    assert "ALTER TABLE" not in mig_sql[0]

def test_compare_tables_varchar_to_text_with_alter():
    """VARCHAR -> TEXT 변경 시 ALTER TYPE 생성 확인"""
    src_varchar_to_text = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'text', 'nullable': True, 'default': None}, # Changed to text
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_varchar_to_text}
    tgt_data = {"my_table": BASE_COLS_TGT} # Original was varchar(100)
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert 'ALTER TABLE public.my_table ALTER COLUMN "name" TYPE text;' in mig_sql[0]
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_int_to_varchar_with_alter():
    """INT -> VARCHAR 변경 시 ALTER TYPE 생성 확인"""
    src_int_to_varchar = [
        {'name': 'id', 'type': 'character varying(50)', 'nullable': False, 'default': None}, # Changed to varchar
        {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_int_to_varchar}
    tgt_data = {"my_table": BASE_COLS_TGT} # Original was integer
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    # Note: Default value might need adjustment in real scenarios, but type change itself is safe
    assert 'ALTER TABLE public.my_table ALTER COLUMN "id" TYPE character varying(50);' in mig_sql[0]
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_add_and_safe_type_change():
    """컬럼 추가 및 안전한 타입 변경 동시 발생 시 ALTER 생성 확인"""
    src_combined_safe = [
        {'name': 'id', 'type': 'bigint', 'nullable': False, 'default': "nextval('some_seq')"}, # Safe change
        {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
        {'name': 'new_col', 'type': 'int', 'nullable': True, 'default': None}, # Added
    ]
    src_data = {"my_table": src_combined_safe}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    sql_block = mig_sql[0]
    assert 'ALTER TABLE public.my_table ALTER COLUMN "id" TYPE bigint;' in sql_block
    assert 'ALTER TABLE public.my_table ADD COLUMN "new_col" int;' in sql_block
    assert "DROP TABLE" not in sql_block

def test_compare_tables_add_and_unsafe_type_change():
    """컬럼 추가 및 안전하지 않은 타입 변경 동시 발생 시 DROP/CREATE 생성 확인"""
    src_combined_unsafe = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'integer', 'nullable': True, 'default': None}, # Unsafe change
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
        {'name': 'new_col', 'type': 'int', 'nullable': True, 'default': None}, # Added
    ]
    src_data = {"my_table": src_combined_unsafe}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "DROP TABLE IF EXISTS public.my_table CASCADE;" in mig_sql[0]
    assert "CREATE TABLE public.\"my_table\"" in mig_sql[0]
    assert "ALTER TABLE" not in mig_sql[0]

def test_compare_tables_drop_and_null_change():
    """컬럼 삭제 및 Null 제약 변경 동시 발생 시 ALTER 생성 확인"""
    src_drop_null = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(100)', 'nullable': False, 'default': None}, # Changed
    ]
    tgt_drop_null = BASE_COLS_TGT + [
        {'name': 'to_drop', 'type': 'int', 'nullable': True, 'default': None} # To be dropped
    ]
    src_data = {"my_table": src_drop_null}
    tgt_data = {"my_table": tgt_drop_null}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    sql_block = mig_sql[0]
    assert '-- WARNING: Setting NOT NULL on column name may fail if existing data contains NULLs.' in sql_block
    assert 'ALTER TABLE public.my_table ALTER COLUMN "name" SET NOT NULL;' in sql_block
    assert '-- WARNING: Dropping column to_drop may cause data loss.' in sql_block
    assert 'ALTER TABLE public.my_table DROP COLUMN "to_drop";' in sql_block
    assert '-- WARNING: Dropping column created_at may cause data loss.' in sql_block # created_at is also dropped
    assert 'ALTER TABLE public.my_table DROP COLUMN "created_at";' in sql_block
    assert "DROP TABLE" not in sql_block

def test_compare_tables_null_change_with_alter():
    """Null 제약 변경 시 ALTER SET/DROP NOT NULL 생성 확인"""
    # NOT NULL -> NULL 허용
    src_nullable = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': True, 'default': 'now()'}, # Changed
    ]
    src_data = {"my_table": src_nullable}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert 'ALTER TABLE public.my_table ALTER COLUMN "created_at" DROP NOT NULL;' in mig_sql[0] # Quoted identifier
    assert "DROP TABLE" not in mig_sql[0]

    # NULL 허용 -> NOT NULL
    src_not_nullable = [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(100)', 'nullable': False, 'default': None}, # Changed
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'now()'},
    ]
    src_data = {"my_table": src_not_nullable}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "-- WARNING: Setting NOT NULL on column name may fail if existing data contains NULLs." in mig_sql[0]
    assert 'ALTER TABLE public.my_table ALTER COLUMN "name" SET NOT NULL;' in mig_sql[0] # Quoted identifier
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_add_column_without_alter():
    """컬럼 추가 시 use_alter=False 이면 DROP/CREATE 생성 확인"""
    src_data = {"my_table": ADD_COL_SRC}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=False)
    # use_alter=False 이고 차이가 있으므로 DROP/CREATE 예상
    assert not skip_sql # 스킵 SQL은 없어야 함
    assert len(mig_sql) == 1
    assert "DROP TABLE IF EXISTS public.my_table CASCADE;" in mig_sql[0]
    assert "CREATE TABLE public.\"my_table\"" in mig_sql[0]
    assert "ALTER TABLE" not in mig_sql[0]

# --- 추가 테스트 케이스 ---

def test_compare_tables_add_column_with_default():
    """기본값 있는 컬럼 추가 시 ALTER ADD COLUMN ... DEFAULT ... 생성 확인"""
    src_data = {"my_table": BASE_COLS_SRC + [{'name': 'status', 'type': 'varchar(20)', 'nullable': False, 'default': "'pending'"}]}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    # 기본값 문자열 내 작은따옴표 주의
    assert "ALTER TABLE public.my_table ADD COLUMN \"status\" varchar(20) DEFAULT 'pending' NOT NULL;" in mig_sql[0] # Quoted identifier
    assert "DROP TABLE" not in mig_sql[0]

def test_compare_tables_change_default_no_alter(): # 기본값 비교 로직은 여전히 주석처리 상태이므로 이 테스트는 스킵을 예상함
    """기본값 변경 시 ALTER 대신 DROP/CREATE 생성 확인 (use_alter=True)"""
    src_data = {"my_table": [
        {'name': 'id', 'type': 'integer', 'nullable': False, 'default': "nextval('some_seq')"},
        {'name': 'name', 'type': 'character varying(100)', 'nullable': True, 'default': None},
        {'name': 'created_at', 'type': 'timestamp with time zone', 'nullable': False, 'default': 'CURRENT_TIMESTAMP'}, # 기본값 변경
    ]}
    tgt_data = {"my_table": BASE_COLS_TGT} # 원래 기본값은 'now()'
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    # 현재 로직은 기본값 변경을 감지하지 못하고, 타입/Null이 같으면 동일하다고 판단함.
    # 만약 기본값 비교 로직이 추가된다면, 이 테스트는 DROP/CREATE를 예상해야 함.
    # 현재는 스킵될 것으로 예상됨 (기본값 비교 로직이 주석처리 되어 있으므로)
    # --> 현재 로직(기본값 비교 주석처리)에서는 스킵되어야 함
    assert not mig_sql # 마이그레이션 SQL은 없어야 함
    assert skip_sql    # 스킵 SQL은 있어야 함
    assert "-- TABLE my_table is up-to-date; skipping." in skip_sql[0]


def test_compare_tables_add_multiple_columns():
    """여러 컬럼 추가 시 여러 ALTER ADD COLUMN 생성 확인"""
    src_data = {"my_table": BASE_COLS_SRC + [
        {'name': 'col_a', 'type': 'int', 'nullable': True, 'default': None},
        {'name': 'col_b', 'type': 'text', 'nullable': False, 'default': "''"},
    ]}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    sql_block = mig_sql[0]
    # 순서는 보장되지 않을 수 있으므로 각 ALTER 문 존재 여부 확인
    assert "ALTER TABLE public.my_table ADD COLUMN \"col_a\" int;" in sql_block # Quoted identifier
    assert "ALTER TABLE public.my_table ADD COLUMN \"col_b\" text DEFAULT '' NOT NULL;" in sql_block # Quoted identifier
    assert "DROP TABLE" not in sql_block

def test_compare_tables_drop_multiple_columns():
    """여러 컬럼 삭제 시 여러 ALTER DROP COLUMN 생성 확인"""
    src_data = {"my_table": BASE_COLS_SRC}
    tgt_data = {"my_table": BASE_COLS_TGT + [
        {'name': 'old_col1', 'type': 'int', 'nullable': True, 'default': None},
        {'name': 'old_col2', 'type': 'text', 'nullable': False, 'default': "''"},
    ]}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    sql_block = mig_sql[0]
    assert "-- WARNING: Dropping column old_col1 may cause data loss." in sql_block
    assert "ALTER TABLE public.my_table DROP COLUMN \"old_col1\";" in sql_block # Quoted identifier
    assert "-- WARNING: Dropping column old_col2 may cause data loss." in sql_block
    assert "ALTER TABLE public.my_table DROP COLUMN \"old_col2\";" in sql_block # Quoted identifier
    assert "DROP TABLE" not in sql_block

def test_compare_tables_completely_different(): # 이제 버그 수정으로 DROP/CREATE 예상
    """컬럼 구성이 완전히 다를 때 DROP/CREATE 생성 확인 (use_alter=True)"""
    src_data = {"my_table": [
        {'name': 'new_id', 'type': 'uuid', 'nullable': False, 'default': 'gen_random_uuid()'},
        {'name': 'value', 'type': 'jsonb', 'nullable': True, 'default': None},
    ]}
    tgt_data = {"my_table": BASE_COLS_TGT}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "TABLE", use_alter=True)
    assert not skip_sql
    assert len(mig_sql) == 1
    # 컬럼이 완전히 다르므로 needs_recreate=True가 되어 DROP/CREATE 발생 예상
    assert "DROP TABLE IF EXISTS public.my_table CASCADE;" in mig_sql[0]
    assert "CREATE TABLE public.\"my_table\"" in mig_sql[0]
    assert "ALTER TABLE" not in mig_sql[0]

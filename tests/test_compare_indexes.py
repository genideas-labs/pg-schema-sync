import pytest
from pg_schema_sync.__main__ import compare_and_generate_migration, normalize_sql

# --- 테스트 데이터 ---

INDEX_DDL_SRC = {
    "idx_users_on_email": "CREATE UNIQUE INDEX idx_users_on_email ON public.users USING btree (email);",
    "idx_orders_on_user_id": "CREATE INDEX idx_orders_on_user_id ON public.orders USING btree (user_id);"
}
INDEX_DDL_TGT = {
    "idx_users_on_email": "CREATE UNIQUE INDEX idx_users_on_email ON public.users USING btree (email);", # 동일
    "idx_orders_on_user_id": "CREATE INDEX idx_orders_on_user_id ON public.orders USING btree (user_id, created_at);" # 컬럼 추가됨
}

# --- 테스트 함수 ---

def test_compare_indexes_definition_changed():
    """Index 정의 변경 시 DROP/CREATE 생성 확인"""
    mig_sql, skip_sql = compare_and_generate_migration(INDEX_DDL_SRC, INDEX_DDL_TGT, "INDEX")
    # idx_users_on_email은 스킵되어야 함
    assert len(skip_sql) > 0
    assert "-- INDEX idx_users_on_email is up-to-date; skipping." in skip_sql[0]
    # idx_orders_on_user_id는 변경되어야 함
    assert len(mig_sql) == 1
    assert "DO $$" in mig_sql[0]
    assert "IF NOT EXISTS" in mig_sql[0]
    assert "CREATE INDEX idx_orders_on_user_id ON public.orders USING btree (user_id);" in mig_sql[0] # 소스 기준 DDL
    assert "DROP INDEX" not in mig_sql[0]

def test_compare_indexes_no_change():
    """Index 변경 없을 때 스킵 확인"""
    src_data = {"idx_users_on_email": INDEX_DDL_SRC["idx_users_on_email"]}
    tgt_data = {"idx_users_on_email": INDEX_DDL_TGT["idx_users_on_email"]}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "INDEX")
    assert not mig_sql
    assert len(skip_sql) > 0
    assert "-- INDEX idx_users_on_email is up-to-date; skipping." in skip_sql[0]

def test_compare_indexes_source_only():
    """소스에만 Index가 있을 때 CREATE 생성 확인"""
    src_data = {"new_idx": "CREATE INDEX new_idx ON public.some_table USING btree (some_column);"}
    tgt_data = {}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "INDEX")
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "-- CREATE INDEX new_idx" in mig_sql[0]
    assert "CREATE INDEX new_idx ON public.some_table USING btree (some_column);" in mig_sql[0]

def test_normalize_sql_for_indexes():
    """Index DDL 정규화 확인"""
    ddl1 = "CREATE UNIQUE INDEX my_idx ON public.my_table USING btree (col1, col2 DESC);"
    ddl2 = "create unique index my_idx on public.my_table using btree ( col1 , col2 desc ) ;"
    assert normalize_sql(ddl1) == normalize_sql(ddl2)

# TODO: EXCLUDE_INDEXES 테스트 추가 필요

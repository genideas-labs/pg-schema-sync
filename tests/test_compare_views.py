import pytest
from pg_schema_sync.__main__ import compare_and_generate_migration, normalize_sql

# --- 테스트 데이터 ---

VIEW_DDL_SRC = {
    "active_users_view": "CREATE OR REPLACE VIEW public.active_users_view AS SELECT id, name FROM users WHERE is_active = true;",
    "order_summary_view": "CREATE OR REPLACE VIEW public.order_summary_view AS SELECT order_id, sum(amount) AS total FROM order_items GROUP BY order_id;"
}
VIEW_DDL_TGT = {
    "active_users_view": "CREATE OR REPLACE VIEW public.active_users_view AS SELECT id, name, email FROM users WHERE is_active = true;", # email 추가됨
    "order_summary_view": "CREATE OR REPLACE VIEW public.order_summary_view AS SELECT order_id, sum(amount) AS total FROM order_items GROUP BY order_id;" # 동일
}

# --- 테스트 함수 ---

def test_compare_views_definition_changed():
    """View 정의 변경 시 CREATE OR REPLACE VIEW 생성 확인"""
    mig_sql, skip_sql = compare_and_generate_migration(VIEW_DDL_SRC, VIEW_DDL_TGT, "VIEW")
    # order_summary_view는 스킵되어야 함
    assert len(skip_sql) > 0
    assert "-- VIEW order_summary_view is up-to-date; skipping." in skip_sql[0]
    # active_users_view는 변경되어야 함
    assert len(mig_sql) == 1
    assert "DROP VIEW IF EXISTS public.active_users_view CASCADE;" in mig_sql[0]
    assert "CREATE OR REPLACE VIEW public.active_users_view AS SELECT id, name FROM users WHERE is_active = true;" in mig_sql[0] # 소스 기준 DDL

def test_compare_views_no_change():
    """View 변경 없을 때 스킵 확인"""
    src_data = {"order_summary_view": VIEW_DDL_SRC["order_summary_view"]}
    tgt_data = {"order_summary_view": VIEW_DDL_TGT["order_summary_view"]}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "VIEW")
    assert not mig_sql
    assert len(skip_sql) > 0
    assert "-- VIEW order_summary_view is up-to-date; skipping." in skip_sql[0]

def test_compare_views_source_only():
    """소스에만 View가 있을 때 CREATE OR REPLACE VIEW 생성 확인"""
    src_data = {"new_view": "CREATE OR REPLACE VIEW public.new_view AS SELECT 1;"}
    tgt_data = {}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "VIEW")
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "-- CREATE VIEW new_view" in mig_sql[0]
    assert "CREATE OR REPLACE VIEW public.new_view AS SELECT 1;" in mig_sql[0]

def test_normalize_sql_for_views():
    """View DDL 정규화 확인 (공백, 대소문자 무시)"""
    ddl1 = "CREATE OR REPLACE VIEW public.my_view AS SELECT col1, col2 FROM my_table WHERE id = 1;"
    ddl2 = "create or replace view public.my_view as select col1, col2 from my_table where id=1 ;"
    ddl3 = """
    CREATE OR REPLACE VIEW public.my_view AS
        SELECT col1,
               col2
          FROM my_table
         WHERE id = 1;
    """
    assert normalize_sql(ddl1) == normalize_sql(ddl2)
    assert normalize_sql(ddl1) == normalize_sql(ddl3)

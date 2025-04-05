import pytest
from pg_schema_sync.__main__ import compare_and_generate_migration, normalize_sql

# --- 테스트 데이터 ---

FUNC_DDL_SRC = {
    "get_user_count": """
CREATE OR REPLACE FUNCTION public.get_user_count()
 RETURNS integer
 LANGUAGE sql
AS $function$
    SELECT count(*)::integer FROM users;
$function$;
""",
    "calculate_total": """
CREATE OR REPLACE FUNCTION public.calculate_total(price numeric, quantity integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN price * quantity * 1.1; -- Apply 10% tax
END;
$function$;
"""
}
FUNC_DDL_TGT = {
    "get_user_count": """
CREATE OR REPLACE FUNCTION public.get_user_count()
 RETURNS integer
 LANGUAGE sql
AS $function$
    SELECT count(*) FROM users; -- ::integer 캐스팅 없음
$function$;
""",
    "calculate_total": """
CREATE OR REPLACE FUNCTION public.calculate_total(price numeric, quantity integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN price * quantity * 1.1; -- Apply 10% tax
END;
$function$;
""" # 동일
}

# --- 테스트 함수 ---

def test_compare_functions_definition_changed():
    """Function 정의 변경 시 DROP/CREATE 생성 확인"""
    mig_sql, skip_sql = compare_and_generate_migration(FUNC_DDL_SRC, FUNC_DDL_TGT, "FUNCTION")
    # calculate_total은 스킵되어야 함
    assert len(skip_sql) > 0
    assert "-- FUNCTION calculate_total is up-to-date; skipping." in skip_sql[0]
    # get_user_count는 변경되어야 함
    assert len(mig_sql) == 1
    assert "DROP FUNCTION IF EXISTS public.get_user_count CASCADE;" in mig_sql[0]
    # CREATE FUNCTION은 원본 DDL 그대로 사용됨
    assert FUNC_DDL_SRC["get_user_count"].strip() in mig_sql[0]

def test_compare_functions_no_change():
    """Function 변경 없을 때 스킵 확인"""
    src_data = {"calculate_total": FUNC_DDL_SRC["calculate_total"]}
    tgt_data = {"calculate_total": FUNC_DDL_TGT["calculate_total"]}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "FUNCTION")
    assert not mig_sql
    assert len(skip_sql) > 0
    assert "-- FUNCTION calculate_total is up-to-date; skipping." in skip_sql[0]

def test_compare_functions_source_only():
    """소스에만 Function이 있을 때 CREATE 생성 확인"""
    src_data = {"new_func": "CREATE OR REPLACE FUNCTION public.new_func() RETURNS void LANGUAGE sql AS $$ $$;"}
    tgt_data = {}
    mig_sql, skip_sql = compare_and_generate_migration(src_data, tgt_data, "FUNCTION")
    assert not skip_sql
    assert len(mig_sql) == 1
    assert "-- CREATE FUNCTION new_func" in mig_sql[0]
    assert "CREATE OR REPLACE FUNCTION public.new_func() RETURNS void LANGUAGE sql AS $$ $$;" in mig_sql[0]

def test_normalize_sql_for_functions():
    """Function DDL 정규화 확인 (주석, 공백, 대소문자 무시)"""
    ddl1 = """
CREATE OR REPLACE FUNCTION public.my_func(p_id integer) -- comment
 RETURNS text
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN 'ID: ' || p_id::text; -- another comment
END;
$function$;
"""
    ddl2 = "create or replace function public.my_func ( p_id integer ) returns text language plpgsql as $function$ begin return 'ID: ' || p_id::text; end; $function$"
    assert normalize_sql(ddl1) == normalize_sql(ddl2)

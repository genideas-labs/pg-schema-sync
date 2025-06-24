#!/usr/bin/env python3
from psycopg2 import sql # SQL 식별자 안전 처리용
import json
import concurrent.futures
from collections import defaultdict, deque, OrderedDict
import yaml # YAML 라이브러리 임포트
import psycopg2
from psycopg2 import sql # SQL 식별자 안전 처리용

def get_connection(config):
    conn = psycopg2.connect(**config)
    return conn
SKIP_TABLES = {'slow_request_logs', 'member_action_log'}

def migrate_single_table(source_config, target_config, table_name, table_meta):
    
    try:
        print("Connecting to target database (gcp_test)...")
        tgt_conn = get_connection(target_config)
        src_conn = get_connection(source_config)
        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
            print(f"  Migrating data for table: {table_name}")
            src_cur.execute(f'SELECT * FROM public."{table_name}"')
            rows = src_cur.fetchall()

            if not rows:
                print(f"    No data found in source {table_name}, skipping.")
                return True, None

            column_names = [desc[0] for desc in src_cur.description]
            quoted_column_names = [f'"{col}"' for col in column_names]
            values_placeholders = ", ".join(["%s"] * len(column_names))

            conflict_clause = "ON CONFLICT (id) DO NOTHING"

            column_type_map = {col['name']: col['type'] for col in table_meta}

            insert_sql = f'''
                INSERT INTO public."{table_name}" ({", ".join(quoted_column_names)})
                VALUES ({values_placeholders})
                {conflict_clause}
            '''

            serialized_rows = [
                tuple(
                    serialize_value(val, column_type_map.get(col_name))
                    for val, col_name in zip(row, column_names)
                )
                for row in rows
            ]
            # 기존
            tgt_cur.executemany(insert_sql, serialized_rows)
            tgt_conn.commit()
            print(f"    Inserted {len(rows)} rows into {table_name}")

            # 변경
            # batch_insert(tgt_conn, tgt_cur, insert_sql, serialized_rows, table_name, batch_size=1000)
        return True, None

    except Exception as e:
        # 롤백하고 에러 리포트
        print(f"  ❌ {table_name}: fail migrate")
        if tgt_conn and not tgt_conn.closed:
            tgt_conn.rollback()
            src_conn.rollback()
        return False, str(e)

    finally:
        # 2) 항상 닫아 준다
        try:
            if tgt_conn and not tgt_conn.closed:
                tgt_conn.close()
                src_conn.close()
        except:
            pass

def serialize_value(val, pg_type=None):
    if isinstance(val, list):
        if pg_type and (pg_type.endswith('[]') or pg_type.startswith('_')):
            if not val:
                return '{}'
            escaped_items = []
            for v in val:
                if isinstance(v, str):
                    # 문자열 원소일 경우 이스케이프
                    escaped_items.append(f'"{v.replace(chr(34), r"\\\"")}"')
                elif isinstance(v, dict):
                    # dict → JSON 문자열 → 다시 이스케이프
                    json_str = json.dumps(v).replace('"', r'\"')
                    escaped_items.append(f'"{json_str}"')
                else:
                    escaped_items.append(str(v))
            return '{' + ','.join(escaped_items) + '}'
        else:
            return json.dumps(val)
    elif isinstance(val, (dict, set)):
        return json.dumps(val)
    return val


def run_data_migration_parallel(src_conn ,src_tables_meta, max_total_attempts=10):
    sorted_table_meta = sort_tables_by_fk_dependency(src_tables_meta)
    print("\n--- Starting Parallel Data Migration ---")
    print(f"sorted table list: {list(sorted_table_meta.keys())}")
    # remaining_tables = list(sorted_table_meta.items())  # ✅ 리스트로 만들어야 여러 번 순회 가능
    remaining_tables = [
    (tbl, meta)
    for tbl, meta in sorted_table_meta.items()
    # 우선 slow_request_logs, member_action_log 로그가 너무 많아 제외하고 테스트
    if tbl not in SKIP_TABLES
    ]

    table_errors = defaultdict(str)
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
    target_config = config['targets']['gcp_test']
    source_config = config['source']

    
    for attempt in range(1, max_total_attempts + 1):
        print(f"\n=== Migration Attempt {attempt} ===")
        if not remaining_tables:
            break

        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            future_to_table = {
                executor.submit(migrate_single_table, source_config, target_config, table_name, table_meta): table_name
                for table_name, table_meta in remaining_tables
            }

            next_round = []
            for future in concurrent.futures.as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    success, error_msg = future.result()
                    if not success:
                        table_meta = sorted_table_meta[table_name]
                        next_round.append((table_name, table_meta))
                        table_errors[table_name] = error_msg or f"Failed on attempt {attempt}"
                except Exception as exc:
                    table_meta = sorted_table_meta[table_name]
                    next_round.append((table_name, table_meta))
                    table_errors[table_name] = str(exc)

            remaining_tables = next_round  # ✅ 튜플의 리스트 형태 유지

    if remaining_tables:
        print("\n--- Data Migration Completed with Failures ---")
        for table_name, _ in remaining_tables:
            print(f"  ❌ {table_name}: {table_errors[table_name]}")
    else:
        print("\n✅ All tables migrated successfully.")
        # ✅ 연결 닫기
    
    

from collections import defaultdict, deque, OrderedDict

def sort_tables_by_fk_dependency(tables_metadata):
    graph = defaultdict(set)  # {A: {B}} → A는 B에 종속됨 (즉, B → A)
    in_degree = defaultdict(int)

    for table, columns in tables_metadata.items():
        in_degree.setdefault(table, 0)
        for col in columns:
            fk = col.get("foreign_key")
            if fk:
                ref_table = fk["table"]
                graph[ref_table].add(table)
                in_degree[table] += 1

    # 모든 테이블을 이름 길이 순으로 정렬
    all_tables_sorted_by_length = sorted(tables_metadata.keys(), key=lambda x: len(x))
    
    # 의존성이 없는 테이블들을 이름 길이 순으로 정렬
    independent_tables = [t for t in all_tables_sorted_by_length if in_degree[t] == 0]
    queue = deque(independent_tables)
    sorted_tables = []

    while queue:
        current = queue.popleft()
        sorted_tables.append(current)
        
        # 의존성이 해결된 테이블들을 이름 길이 순으로 정렬하여 큐에 추가
        new_dependents = []
        for dependent in graph[current]:
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                new_dependents.append(dependent)
        
        # 새로운 의존성 해결된 테이블들을 이름 길이 순으로 정렬하여 큐에 추가
        new_dependents.sort(key=lambda x: len(x))
        queue.extend(new_dependents)

    if len(sorted_tables) < len(tables_metadata):
        print("⚠️ Warning: Cyclic dependency detected among tables!")
        remaining = set(tables_metadata) - set(sorted_tables)
        # 남은 테이블들도 이름 길이 순으로 정렬
        remaining_sorted = sorted(remaining, key=lambda x: len(x))
        sorted_tables.extend(remaining_sorted)

    # ✅ OrderedDict으로 정렬된 결과 반환
    return OrderedDict((table, tables_metadata[table]) for table in sorted_tables)

def batch_insert(tgt_conn, tgt_cur, insert_sql, serialized_rows, table_name, batch_size=1000):
    total = len(serialized_rows)
    for i in range(0, total, batch_size):
        batch = serialized_rows[i:i + batch_size]
        try:
            tgt_cur.executemany(insert_sql, batch)
            tgt_conn.commit()
            print(f"    ✅ Batch {i // batch_size + 1}: Inserted {len(batch)} rows into {table_name}")
        except Exception as e:
            tgt_conn.rollback()
            print(f"    ❌ Batch {i // batch_size + 1}: Failed to insert {len(batch)} rows into {table_name}")
            print(f"       Error: {e}")

def compare_row_counts(src_conn, tgt_conn, table_names):
    """
    src_conn, tgt_conn: psycopg2 커넥션
    table_names: 비교할 테이블명 리스트
    반환값: {table: (src_count, tgt_count)} 형태로, 차이가 있는 테이블만 담아서 리턴
    """
    diffs = {}
    with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
        for tbl in table_names:
            src_cur.execute(sql.SQL('SELECT COUNT(*) FROM public.{}').format(sql.Identifier(tbl)))
            src_count = src_cur.fetchone()[0]
            tgt_cur.execute(sql.SQL('SELECT COUNT(*) FROM public.{}').format(sql.Identifier(tbl)))
            tgt_count = tgt_cur.fetchone()[0]
            if src_count != tgt_count:
                diffs[tbl] = (src_count, tgt_count)
    return diffs
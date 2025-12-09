#!/usr/bin/env python3
import json
from collections import defaultdict, deque, OrderedDict
import yaml
import psycopg2
from psycopg2 import sql
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

def get_connection(config):
    conn = psycopg2.connect(**config)
    return conn
SKIP_TABLES = {'slow_request_logs', 'member_action_log'}

def migrate_single_table_with_conn(src_conn, tgt_conn, table_name, table_meta):
    """연결을 재사용하여 단일 테이블 데이터를 마이그레이션합니다."""
    try:
        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
            src_cur.execute(f'SELECT * FROM public."{table_name}"')
            rows = src_cur.fetchall()

            if not rows:
                print(f"  ⏭️  {table_name}: No data, skipped", flush=True)
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
            
            tgt_cur.executemany(insert_sql, serialized_rows)
            tgt_conn.commit()
            print(f"  ✅ {table_name}: Inserted {len(rows)} rows", flush=True)
        return True, None

    except Exception as e:
        # 롤백하고 에러 리포트
        tgt_conn.rollback()
        print(f"  ❌ {table_name}: {type(e).__name__}: {str(e)}", flush=True)
        return False, str(e)

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

def get_all_foreign_keys(conn):
    """타겟 DB의 모든 FK 제약조건 정보를 가져옵니다."""
    with conn.cursor() as cur:
        cur.execute("""
        SELECT 
            conrelid::regclass AS table_name,
            conname AS constraint_name,
            pg_get_constraintdef(pc.oid) AS constraint_def
        FROM pg_constraint pc
        JOIN pg_namespace n ON n.oid = pc.connamespace
        WHERE pc.contype = 'f' AND n.nspname = 'public'
        ORDER BY table_name, conname;
        """)
        return cur.fetchall()

def drop_all_foreign_keys(conn):
    """모든 FK 제약조건을 배치로 DROP합니다 (빠른 처리)."""
    print("\n🔓 Dropping all FK constraints (batch mode)...", flush=True)
    fks = get_all_foreign_keys(conn)
    
    if not fks:
        print("  No FK constraints found.")
        return []
    
    print(f"  Found {len(fks)} FK constraints to drop.", flush=True)
    
    # 배치 크기 (적절한 크기로 빠르게 처리하면서도 실패 시 재시도 가능)
    BATCH_SIZE = 20
    dropped_count = 0
    failed_count = 0
    
    with conn.cursor() as cur:
        # lock timeout 설정 - 외부 충돌은 이미 해결되었으므로 적당히 설정
        cur.execute("SET lock_timeout = '10s';")
        print(f"  ⏱️  Lock timeout set to 10 seconds", flush=True)
        
        for i in range(0, len(fks), BATCH_SIZE):
            batch = fks[i:i+BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(fks) + BATCH_SIZE - 1) // BATCH_SIZE
            
            try:
                # 배치 전체 실행
                for table_name, constraint_name, _ in batch:
                    drop_sql = f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS "{constraint_name}";'
                    cur.execute(drop_sql)
                    dropped_count += 1
                
                # 배치마다 커밋
                conn.commit()
                print(f"  ✅ Batch {batch_num}/{total_batches}: Dropped {len(batch)} FKs ({dropped_count}/{len(fks)} total)", flush=True)
                
            except Exception as e:
                conn.rollback()
                print(f"  ⚠️  Batch {batch_num} failed, retrying one by one...", flush=True)
                
                # 실패한 배치는 하나씩 재시도
                for table_name, constraint_name, _ in batch:
                    try:
                        drop_sql = f'ALTER TABLE {table_name} DROP CONSTRAINT IF EXISTS "{constraint_name}";'
                        cur.execute(drop_sql)
                        conn.commit()
                        dropped_count += 1
                    except Exception as e2:
                        conn.rollback()
                        failed_count += 1
                        if 'lock timeout' in str(e2).lower():
                            print(f"    ⏭️  Skipped (busy): {table_name}.{constraint_name}", flush=True)
                        else:
                            print(f"    ✗ Failed: {table_name}.{constraint_name}: {e2}", flush=True)
    
    print(f"\n✅ Dropped {dropped_count}/{len(fks)} FK constraints (Failed: {failed_count}).\n", flush=True)
    return fks

def recreate_foreign_keys_not_valid(conn, fks):
    """FK 제약조건을 배치로 NOT VALID로 재생성합니다 (빠른 처리)."""
    print("\n🔗 Recreating FK constraints (NOT VALID, batch mode)...", flush=True)
    
    if not fks:
        print("  No FK constraints to recreate.")
        return
    
    # 배치 크기 (적절한 크기로 빠르게 처리하면서도 실패 시 재시도 가능)
    BATCH_SIZE = 20
    added_count = 0
    failed_count = 0
    
    with conn.cursor() as cur:
        # lock timeout 설정 - 외부 충돌은 이미 해결되었으므로 적당히 설정
        cur.execute("SET lock_timeout = '10s';")
        print(f"  ⏱️  Lock timeout set to 10 seconds", flush=True)
        
        for i in range(0, len(fks), BATCH_SIZE):
            batch = fks[i:i+BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(fks) + BATCH_SIZE - 1) // BATCH_SIZE
            
            try:
                # 배치 전체 실행
                for table_name, constraint_name, constraint_def in batch:
                    add_sql = f'ALTER TABLE {table_name} ADD CONSTRAINT "{constraint_name}" {constraint_def} NOT VALID;'
                    cur.execute(add_sql)
                    added_count += 1
                
                # 배치마다 커밋
                conn.commit()
                print(f"  ✅ Batch {batch_num}/{total_batches}: Added {len(batch)} FKs ({added_count}/{len(fks)} total)", flush=True)
                
            except Exception as e:
                conn.rollback()
                print(f"  ⚠️  Batch {batch_num} failed, retrying one by one...", flush=True)
                
                # 실패한 배치는 하나씩 재시도
                for table_name, constraint_name, constraint_def in batch:
                    try:
                        add_sql = f'ALTER TABLE {table_name} ADD CONSTRAINT "{constraint_name}" {constraint_def} NOT VALID;'
                        cur.execute(add_sql)
                        conn.commit()
                        added_count += 1
                    except Exception as e2:
                        conn.rollback()
                        failed_count += 1
                        if 'lock timeout' in str(e2).lower():
                            print(f"    ⏭️  Skipped (busy): {table_name}.{constraint_name}", flush=True)
                        else:
                            print(f"    ✗ Failed: {table_name}.{constraint_name}: {e2}", flush=True)
    
    print(f"\n✅ Recreated {added_count}/{len(fks)} FK constraints (Failed: {failed_count}).\n", flush=True)

def generate_validate_script(fks, output_file='validate_fks.sql'):
    """FK VALIDATE 스크립트를 파일로 생성합니다 (나중에 트래픽 없는 시간대에 실행)."""
    print(f"\n📝 Generating VALIDATE script: {output_file}", flush=True)
    
    if not fks:
        print("  No FK constraints to validate.")
        return
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("-- FK VALIDATE Script\n")
        f.write("-- 이 스크립트는 트래픽이 적은 시간대에 실행하세요.\n")
        f.write("-- VALIDATE는 전체 테이블을 스캔하므로 시간이 오래 걸릴 수 있습니다.\n")
        f.write(f"-- Total: {len(fks)} FK constraints\n\n")
        f.write("-- Progress tracking:\n")
        f.write("-- \\timing on\n\n")
        
        for idx, (table_name, constraint_name, _) in enumerate(fks, 1):
            f.write(f"-- [{idx}/{len(fks)}] Validating {table_name}.{constraint_name}\n")
            f.write(f"ALTER TABLE {table_name} VALIDATE CONSTRAINT \"{constraint_name}\";\n")
            if idx % 10 == 0:
                f.write(f"-- Progress: {idx}/{len(fks)} completed\n")
            f.write("\n")
        
        f.write("-- All FK constraints validated!\n")
    
    print(f"✅ VALIDATE script generated: {output_file}", flush=True)
    print(f"   Run this script later with: psql -f {output_file}\n", flush=True)

def run_data_migration_parallel(src_conn, src_tables_meta, src_composite_fks=None, max_total_attempts=10):
    # FK 의존성 정렬이 필요 없음 - FK를 미리 DROP하므로
    print("\n--- Starting Parallel Data Migration ---")
    print(f"Total tables to migrate: {len(src_tables_meta)}")
    
    remaining_tables = [
        (tbl, meta)
        for tbl, meta in src_tables_meta.items()
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
    
    # 연결 풀 생성 (병렬 처리용)
    MAX_WORKERS = 5
    connection_pool = []
    
    print(f"\n🔌 Creating connection pool ({MAX_WORKERS} workers)...", flush=True)
    for i in range(MAX_WORKERS):
        src_conn = get_connection(source_config)
        tgt_conn = get_connection(target_config)
        connection_pool.append((src_conn, tgt_conn))
    print(f"  Connection pool ready: {len(connection_pool)} worker connections", flush=True)
    
    # 연결 할당을 위한 lock
    pool_lock = threading.Lock()
    available_connections = list(range(MAX_WORKERS))
    
    def get_conn_from_pool():
        """연결 풀에서 연결 쌍 가져오기"""
        with pool_lock:
            if available_connections:
                idx = available_connections.pop(0)
                return idx, connection_pool[idx]
            return None, (None, None)
    
    def return_conn_to_pool(idx):
        """연결 풀에 반환"""
        with pool_lock:
            available_connections.append(idx)
    
    def migrate_table_worker(table_name, table_meta):
        """Worker 함수: 연결 풀에서 연결 가져와서 테이블 마이그레이션"""
        conn_idx, (src_conn, tgt_conn) = get_conn_from_pool()
        try:
            return migrate_single_table_with_conn(src_conn, tgt_conn, table_name, table_meta)
        finally:
            return_conn_to_pool(conn_idx)
    
    try:
        # 1. 타겟 DB에서 모든 FK 저장 후 DROP (첫 번째 연결 사용)
        dropped_fks = drop_all_foreign_keys(connection_pool[0][1])
        
        # 2. 데이터 마이그레이션 (병렬 처리, 연결 풀 재사용)
        print(f"\n📊 Migrating {len(remaining_tables)} tables in parallel ({MAX_WORKERS} workers)...", flush=True)
        
        for attempt in range(1, max_total_attempts + 1):
            if not remaining_tables:
                break
            
            print(f"\n=== Migration Attempt {attempt}/{max_total_attempts} ===", flush=True)
            next_round = []
            completed = 0
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_table = {
                    executor.submit(migrate_table_worker, table_name, table_meta): table_name
                    for table_name, table_meta in remaining_tables
                }
                
                # as_completed로 완료되는 대로 처리 (순서 무관)
                for future in as_completed(future_to_table):
                    table_name = future_to_table[future]
                    try:
                        success, error_msg = future.result()
                        completed += 1
                        
                        if not success:
                            table_meta = src_tables_meta[table_name]
                            next_round.append((table_name, table_meta))
                            table_errors[table_name] = error_msg or f"Failed on attempt {attempt}"
                        
                        # 진행상황 (매 10개마다)
                        if completed % 10 == 0:
                            print(f"  Progress: {completed}/{len(remaining_tables)} tables", flush=True)
                    except Exception as exc:
                        table_meta = src_tables_meta[table_name]
                        next_round.append((table_name, table_meta))
                        table_errors[table_name] = str(exc)
                        completed += 1
            
            print(f"  Completed: {completed}/{len(remaining_tables)} tables", flush=True)
            remaining_tables = next_round
        
        # 3. FK 재생성 (NOT VALID) (첫 번째 연결 사용)
        recreate_foreign_keys_not_valid(connection_pool[0][1], dropped_fks)
        
    finally:
        # 연결 풀 모두 닫기
        print("\n🔌 Closing connection pool...", flush=True)
        for src_conn, tgt_conn in connection_pool:
            try:
                src_conn.close()
                tgt_conn.close()
            except:
                pass
        print("  Connection pool closed.", flush=True)
    
    # 4. VALIDATE 스크립트 생성 (나중에 수동 실행)
    generate_validate_script(dropped_fks, output_file='validate_fks.sql')
    
    if remaining_tables:
        print("\n--- Data Migration Completed with Failures ---")
        for table_name, _ in remaining_tables:
            print(f"  ❌ {table_name}: {table_errors[table_name]}")
    else:
        print("\n✅ All tables migrated successfully.")
        print("✅ 데이터 마이그레이션 완료")
    
    

from collections import defaultdict, deque, OrderedDict

def sort_tables_by_fk_dependency(tables_metadata, composite_fks=None):
    graph = defaultdict(set)  # {A: {B}} → A는 B에 종속됨 (즉, B → A)
    in_degree = defaultdict(int)
    fk_count = 0

    # 1. 단일 컬럼 FK 처리
    for table, columns in tables_metadata.items():
        in_degree.setdefault(table, 0)
        for col in columns:
            fk = col.get("foreign_key")
            if fk:
                ref_table = fk["table"]
                graph[ref_table].add(table)
                in_degree[table] += 1
                fk_count += 1
    
    # 2. 복합 FK 처리 (새로 추가)
    composite_fk_count = 0
    if composite_fks:
        for table, fk_list in composite_fks.items():
            in_degree.setdefault(table, 0)
            for fk_info in fk_list:
                ref_table = fk_info['ref_table']
                # 중복 카운트 방지: 이미 단일 FK로 추가된 경우 제외
                if table not in graph[ref_table]:
                    graph[ref_table].add(table)
                    in_degree[table] += 1
                    composite_fk_count += 1
    
    print(f"\n🔗 FK Dependencies detected:")
    print(f"  - Single column FKs: {fk_count}")
    print(f"  - Composite FKs: {composite_fk_count}")
    print(f"  - Total FK relationships: {fk_count + composite_fk_count}")

    # 위상 정렬 (Topological Sort)
    # 의존성이 없는 테이블들(부모 테이블)부터 시작
    independent_tables = sorted([t for t in tables_metadata.keys() if in_degree[t] == 0])
    queue = deque(independent_tables)
    sorted_tables = []

    print(f"  - Independent tables (no FK dependencies): {len(independent_tables)}")

    while queue:
        current = queue.popleft()
        sorted_tables.append(current)
        
        # 현재 테이블에 의존하는 테이블들의 in_degree 감소
        # 즉, 현재 테이블(부모)을 참조하는 자식 테이블들 확인
        for dependent in sorted(graph[current]):  # 알파벳 순 정렬로 일관성 유지
            in_degree[dependent] -= 1
            if in_degree[dependent] == 0:
                # 모든 의존성이 해결되면 큐에 추가
                queue.append(dependent)

    # 순환 참조 감지
    if len(sorted_tables) < len(tables_metadata):
        print("\n⚠️ Warning: Cyclic dependency detected among tables!")
        remaining = set(tables_metadata) - set(sorted_tables)
        print(f"  - Tables with circular dependencies: {sorted(remaining)}")
        # 순환 참조가 있는 테이블들은 알파벳 순으로 추가
        sorted_tables.extend(sorted(remaining))

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
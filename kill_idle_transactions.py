#!/usr/bin/env python3
import psycopg2
import yaml
import sys

# config.yaml 읽기
with open("config.yaml", 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

target_config = config['targets']['gcp_test']

# 연결 (autocommit 모드로 설정)
conn = psycopg2.connect(
    host=target_config['host'],
    port=target_config['port'],
    dbname=target_config['dbname'],
    user=target_config['user'],
    password=target_config['password']
)

# ✅ autocommit 활성화: 각 명령이 독립적으로 실행됨
conn.autocommit = True

print("🔍 Looking for 'idle in transaction' connections...\n")

with conn.cursor() as cur:
    # 1. 먼저 확인
    cur.execute("""
        SELECT pid, usename, state, state_change, LEFT(query, 100) as query
        FROM pg_stat_activity 
        WHERE datname = 'postgres' 
          AND state IN ('idle', 'idle in transaction')
          AND pid != pg_backend_pid();
    """)
    
    idle_txns = cur.fetchall()
    
    if not idle_txns:
        print("✅ No 'idle in transaction' connections found!")
        conn.close()
        exit(0)
    
    print(f"⚠️  Found {len(idle_txns)} 'idle in transaction' connection(s):\n")
    for row in idle_txns:
        print(f"PID: {row[0]}, User: {row[1]}, State: {row[2]}")
        print(f"  Since: {row[3]}")
        print(f"  Last Query: {row[4]}\n")
    
    # 2. 사용자 확인
    # --yes 플래그가 있으면 자동 승인
    auto_approve = '--yes' in sys.argv or '-y' in sys.argv
    
    if auto_approve:
        print("🤖 Auto-approve mode (--yes flag detected)\n")
        response = 'yes'
    else:
        try:
            response = input("❓ Do you want to terminate these connections? (yes/no): ").strip().lower()
        except EOFError:
            print("\n❌ No input provided. Use --yes flag for non-interactive mode.")
            conn.close()
            exit(1)
    
    if response in ['yes', 'y']:
        print("\n🔪 Terminating connections...")
        
        terminated = 0
        failed = 0
        skipped = 0
        
        for row in idle_txns:
            pid = row[0]
            username = row[1]
            
            # superuser는 건너뛰기 (supabase_admin 등)
            if username in ['supabase_admin', 'supabase_storage_admin']:
                print(f"  ⏭️  Skipped PID {pid} (superuser: {username})")
                skipped += 1
                continue
            
            try:
                cur.execute(f"SELECT pg_terminate_backend({pid});")
                result = cur.fetchone()[0]
                if result:
                    print(f"  ✅ Terminated PID {pid} ({username})")
                    terminated += 1
                else:
                    print(f"  ❌ Failed to terminate PID {pid}")
                    failed += 1
            except Exception as e:
                print(f"  ❌ Error terminating PID {pid}: {e}")
                failed += 1
        
        print(f"\n📊 Result: {terminated} terminated, {failed} failed, {skipped} skipped")
        
        if terminated > 0:
            print("\n✅ You can now retry the migration!")
    else:
        print("\n❌ Operation cancelled.")

conn.close()



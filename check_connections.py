#!/usr/bin/env python3
import psycopg2
import yaml

# config.yaml 읽기
with open("config.yaml", 'r', encoding='utf-8') as f:
    config = yaml.safe_load(f)

target_config = config['targets']['gcp']

# 연결
conn = psycopg2.connect(
    host=target_config['host'],
    port=target_config['port'],
    dbname=target_config['dbname'],
    user=target_config['user'],
    password=target_config['password']
)

print("🔍 Checking active connections on target database...\n")

with conn.cursor() as cur:
    # 1. 활성 연결
    print("=" * 80)
    print("1. Active Connections:")
    print("=" * 80)
    cur.execute("""
        SELECT 
            pid,
            usename,
            application_name,
            client_addr,
            state,
            LEFT(query, 60) as query_preview,
            state_change
        FROM pg_stat_activity 
        WHERE datname = 'postgres' 
          AND state != 'idle'
        ORDER BY state_change;
    """)
    rows = cur.fetchall()
    if rows:
        for row in rows:
            print(f"PID: {row[0]}, User: {row[1]}, App: {row[2]}, State: {row[4]}")
            print(f"  Query: {row[5]}")
            print(f"  Changed: {row[6]}\n")
    else:
        print("No active connections (only idle ones)\n")
    
    # 2. Lock 정보
    print("=" * 80)
    print("2. Current Locks:")
    print("=" * 80)
    cur.execute("""
        SELECT 
            l.pid,
            l.mode,
            l.granted,
            a.usename,
            a.state,
            LEFT(a.query, 60) as query_preview
        FROM pg_locks l
        JOIN pg_stat_activity a ON l.pid = a.pid
        WHERE l.relation IS NOT NULL
        ORDER BY l.granted, l.pid
        LIMIT 20;
    """)
    rows = cur.fetchall()
    if rows:
        for row in rows:
            granted = "✅ GRANTED" if row[2] else "⏳ WAITING"
            print(f"PID: {row[0]}, Mode: {row[1]}, {granted}")
            print(f"  User: {row[3]}, State: {row[4]}")
            print(f"  Query: {row[5]}\n")
    else:
        print("No locks found\n")
    
    # 3. 연결 통계
    print("=" * 80)
    print("3. Connection Statistics:")
    print("=" * 80)
    cur.execute("""
        SELECT state, COUNT(*) as count
        FROM pg_stat_activity
        WHERE datname = 'postgres'
        GROUP BY state
        ORDER BY count DESC;
    """)
    rows = cur.fetchall()
    for row in rows:
        print(f"  {row[0]}: {row[1]} connections")

conn.close()

print("\n✅ Check completed!")
print("\n💡 If you see many active connections, consider:")
print("   - Running migration during off-peak hours")
print("   - Terminating idle connections")
print("   - Increasing lock_timeout further")



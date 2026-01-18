#!/usr/bin/env python3
"""타겟 DB의 좀비 커넥션(idle in transaction)을 정리하는 스크립트"""
import psycopg2
import yaml

with open('config.yaml') as f:
    config = yaml.safe_load(f)

conn = psycopg2.connect(**config['targets']['gcp'])
cur = conn.cursor()

# 현재 활성 연결 확인
cur.execute("""
SELECT 
    pid,
    usename,
    state,
    state_change,
    now() - state_change as duration,
    LEFT(query, 80) as query_preview
FROM pg_stat_activity
WHERE datname = 'postgres'
  AND pid != pg_backend_pid()
ORDER BY state_change;
""")

print('\n=== Target DB 활성 연결 ===')
print(f'{"PID":<8} {"User":<12} {"State":<20} {"Duration":<20} {"Query"}')
print('-' * 120)

idle_in_transaction_pids = []
for row in cur.fetchall():
    pid, usename, state, state_change, duration, query = row
    print(f'{pid:<8} {usename:<12} {(state or "None"):<20} {str(duration):<20} {query or "(none)"}')
    if state == 'idle in transaction':
        idle_in_transaction_pids.append(pid)

print()
if idle_in_transaction_pids:
    print(f'⚠️  좀비 커넥션 발견: {len(idle_in_transaction_pids)}개')
    print(f'   PIDs: {idle_in_transaction_pids}')
    print()
    
    # 자동으로 종료
    for pid in idle_in_transaction_pids:
        try:
            cur.execute(f'SELECT pg_terminate_backend({pid});')
            print(f'  ✓ Terminated PID {pid}')
        except Exception as e:
            print(f'  ✗ Failed to terminate PID {pid}: {e}')
    
    conn.commit()
    print('\n✅ 좀비 커넥션 정리 완료!\n')
else:
    print('✅ 좀비 커넥션 없음\n')

conn.close()


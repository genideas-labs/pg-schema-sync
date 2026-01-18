#!/usr/bin/env python3
"""Payment DB의 좀비/idle 커넥션을 정리하는 스크립트"""
import psycopg2
import yaml

print("🔧 Cleaning up Payment DB connections...")
print("=" * 60)

with open('config_payment.yaml') as f:
    config = yaml.safe_load(f)

# Target (GCP Payment DB) 연결
target_config = config['targets']['gcp']
conn = psycopg2.connect(**target_config)
conn.autocommit = True

with conn.cursor() as cur:
    # 1. 현재 활성 연결 확인
    cur.execute("""
    SELECT 
        pid,
        usename,
        state,
        state_change,
        now() - state_change as duration,
        LEFT(query, 60) as query_preview
    FROM pg_stat_activity
    WHERE datname = 'postgres'
      AND pid != pg_backend_pid()
    ORDER BY state_change;
    """)
    
    print('\n📊 Current Active Connections:')
    print(f'{"PID":<8} {"User":<20} {"State":<20} {"Duration":<15} {"Query"}')
    print('-' * 120)
    
    idle_pids = []
    idle_in_tx_pids = []
    
    for row in cur.fetchall():
        pid, usename, state, state_change, duration, query = row
        print(f'{pid:<8} {usename:<20} {(state or "None"):<20} {str(duration):<15} {query or "(none)"}')
        
        if state == 'idle':
            idle_pids.append(pid)
        elif state == 'idle in transaction':
            idle_in_tx_pids.append(pid)
    
    print()
    print(f'📈 Summary:')
    print(f'  - Idle connections: {len(idle_pids)}')
    print(f'  - Idle in transaction (zombie): {len(idle_in_tx_pids)}')
    print()
    
    # 2. 좀비 커넥션 종료 (idle in transaction)
    if idle_in_tx_pids:
        print(f'🔪 Terminating {len(idle_in_tx_pids)} zombie connections...')
        terminated = 0
        failed = 0
        
        for pid in idle_in_tx_pids:
            try:
                cur.execute(f'SELECT pg_terminate_backend({pid});')
                result = cur.fetchone()[0]
                if result:
                    print(f'  ✅ Terminated PID {pid}')
                    terminated += 1
                else:
                    print(f'  ❌ Failed PID {pid}')
                    failed += 1
            except Exception as e:
                print(f'  ❌ Error PID {pid}: {e}')
                failed += 1
        
        print(f'\n📊 Zombie connections: {terminated} terminated, {failed} failed')
    else:
        print('✅ No zombie connections found!')
    
    # 3. 오래된 idle 커넥션 종료 (5분 이상)
    if idle_pids:
        print(f'\n🔍 Checking old idle connections (>5 min)...')
        cur.execute("""
        SELECT pid
        FROM pg_stat_activity
        WHERE datname = 'postgres'
          AND state = 'idle'
          AND pid != pg_backend_pid()
          AND now() - state_change > interval '5 minutes';
        """)
        
        old_idle_pids = [row[0] for row in cur.fetchall()]
        
        if old_idle_pids:
            print(f'🔪 Terminating {len(old_idle_pids)} old idle connections...')
            terminated = 0
            failed = 0
            
            for pid in old_idle_pids:
                try:
                    cur.execute(f'SELECT pg_terminate_backend({pid});')
                    result = cur.fetchone()[0]
                    if result:
                        print(f'  ✅ Terminated PID {pid}')
                        terminated += 1
                    else:
                        print(f'  ❌ Failed PID {pid}')
                        failed += 1
                except Exception as e:
                    print(f'  ❌ Error PID {pid}: {e}')
                    failed += 1
            
            print(f'\n📊 Old idle connections: {terminated} terminated, {failed} failed')
        else:
            print('  ✅ No old idle connections found!')

conn.close()

print()
print("=" * 60)
print("✅ Payment DB cleanup completed!")
print()



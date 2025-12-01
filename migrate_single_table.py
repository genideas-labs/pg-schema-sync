#!/usr/bin/env python3
"""
단일 테이블 마이그레이션 스크립트
특정 테이블만 다시 마이그레이션할 때 사용
"""
import sys
import yaml
from src.pg_schema_sync.dataMig import migrate_single_table, get_connection

# 테이블 메타데이터 조회 함수 (간단 버전)
def fetch_table_metadata(conn, table_name):
    """특정 테이블의 메타데이터를 조회합니다."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, udt_name, column_default, is_identity
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
        """, (table_name,))
        
        columns = []
        for col_name, data_type, is_nullable, udt_name, col_default, is_identity in cur.fetchall():
            col_type = data_type
            if data_type == 'ARRAY':
                base_type = udt_name.lstrip('_')
                col_type = base_type + '[]'
            
            col_data = {
                'name': col_name,
                'type': col_type,
                'nullable': is_nullable == 'YES',
                'default': col_default,
                'identity': is_identity == 'YES'
            }
            columns.append(col_data)
        
        # PK 정보 조회
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
                AND tc.table_name = %s
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
        """, (table_name,))
        
        pk_columns = [row[0] for row in cur.fetchall()]
        
        # 컬럼에 PK 정보 추가
        for col in columns:
            if col['name'] in pk_columns:
                col['primary_key'] = True
        
        # UNIQUE 정보 조회
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu 
                ON tc.constraint_name = kcu.constraint_name 
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
                AND tc.table_name = %s
                AND tc.constraint_type = 'UNIQUE'
            ORDER BY kcu.ordinal_position;
        """, (table_name,))
        
        unique_columns = [row[0] for row in cur.fetchall()]
        
        # 컬럼에 UNIQUE 정보 추가
        for col in columns:
            if col['name'] in unique_columns:
                col['unique'] = True
        
        return columns


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python migrate_single_table.py <table_name>")
        print("Example: python migrate_single_table.py deleted_menu_items")
        sys.exit(1)
    
    table_name = sys.argv[1]
    
    print(f"🚀 Starting migration for table: {table_name}\n")
    
    # config.yaml 읽기
    try:
        with open("config.yaml", 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ Error reading config.yaml: {e}")
        sys.exit(1)
    
    source_config = config['source']
    target_config = config['targets']['gcp_test']
    
    # psycopg2 형식으로 변환
    if 'db' in source_config:
        source_config['dbname'] = source_config.pop('db')
    if 'username' in source_config:
        source_config['user'] = source_config.pop('username')
    
    if 'db' in target_config:
        target_config['dbname'] = target_config.pop('db')
    if 'username' in target_config:
        target_config['user'] = target_config.pop('username')
    
    # 소스에서 테이블 메타데이터 조회
    print(f"📊 Fetching metadata for {table_name}...")
    try:
        src_conn = get_connection(source_config)
        table_meta = fetch_table_metadata(src_conn, table_name)
        src_conn.close()
        
        if not table_meta:
            print(f"❌ Table '{table_name}' not found in source database")
            sys.exit(1)
        
        print(f"   Found {len(table_meta)} columns")
        
        # PK 확인
        pk_cols = [col['name'] for col in table_meta if col.get('primary_key')]
        if pk_cols:
            print(f"   Primary Key: {', '.join(pk_cols)}")
        else:
            print(f"   ⚠️  No Primary Key found")
        
    except Exception as e:
        print(f"❌ Error fetching metadata: {e}")
        sys.exit(1)
    
    # 마이그레이션 실행
    print(f"\n🔄 Migrating data...")
    success, error = migrate_single_table(source_config, target_config, table_name, table_meta)
    
    if success:
        print(f"\n✅ Successfully migrated table: {table_name}")
    else:
        print(f"\n❌ Failed to migrate table: {table_name}")
        print(f"   Error: {error}")
        sys.exit(1)


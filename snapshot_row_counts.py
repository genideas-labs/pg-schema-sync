#!/usr/bin/env python3
"""
테이블별 row 카운트 스냅샷을 생성하는 스크립트
"""
import json
import yaml
import psycopg2
from datetime import datetime
import argparse
from pathlib import Path


def get_connection(config):
    """데이터베이스 연결을 생성합니다."""
    return psycopg2.connect(**config)


def get_all_tables(conn):
    """public 스키마의 모든 테이블 목록을 가져옵니다."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        return [row[0] for row in cur.fetchall()]


def get_row_counts(conn, tables, verbose=True):
    """각 테이블의 row 카운트를 가져옵니다."""
    row_counts = {}
    with conn.cursor() as cur:
        for table in tables:
            try:
                cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
                count = cur.fetchone()[0]
                row_counts[table] = count
                if verbose:
                    print(f"  ✓ {table}: {count:,} rows")
            except Exception as e:
                if verbose:
                    print(f"  ✗ {table}: Error - {e}")
                row_counts[table] = -1  # 에러 표시
    
    return row_counts


def create_snapshot_from_conn(conn, output_file=None, db_name=None, verbose=True):
    """
    이미 열려있는 연결을 사용하여 스냅샷을 생성합니다.
    
    Args:
        conn: psycopg2 연결 객체
        output_file: 출력 파일명 (기본값: snapshot_{timestamp}.json)
        db_name: 스냅샷에 기록할 데이터베이스 이름
        verbose: 상세 출력 여부
    
    Returns:
        생성된 스냅샷 파일 경로
    """
    if verbose:
        print(f"\n📸 Creating snapshot for database: {db_name or 'unknown'}")
    
    try:
        # 테이블 목록 가져오기
        if verbose:
            print("\n📋 Fetching table list...")
        tables = get_all_tables(conn)
        if verbose:
            print(f"  Found {len(tables)} tables")
        
        # Row 카운트 가져오기
        if verbose:
            print("\n📊 Counting rows for each table...")
        row_counts = get_row_counts(conn, tables, verbose=verbose)
        
        # 통계 계산
        total_rows = sum(count for count in row_counts.values() if count >= 0)
        error_tables = [table for table, count in row_counts.items() if count < 0]
        
        # 데이터베이스 정보 추출
        with conn.cursor() as cur:
            cur.execute("SELECT current_database()")
            current_db = cur.fetchone()[0]
        
        # 스냅샷 데이터 구성
        snapshot = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "database": db_name or current_db,
                "total_tables": len(tables),
                "total_rows": total_rows,
                "error_tables": error_tables
            },
            "tables": row_counts
        }
        
        # 파일로 저장
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            db_label = db_name or current_db
            output_file = f"snapshot_{db_label}_{timestamp}.json"
        
        # snapshots 디렉토리 생성
        snapshots_dir = Path("snapshots")
        snapshots_dir.mkdir(exist_ok=True)
        
        output_path = snapshots_dir / output_file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, indent=2, ensure_ascii=False)
        
        if verbose:
            print(f"\n✅ Snapshot saved: {output_path}")
            print(f"   Total tables: {len(tables)}")
            print(f"   Total rows: {total_rows:,}")
            if error_tables:
                print(f"   ⚠️  Tables with errors: {len(error_tables)}")
        
        return str(output_path)
        
    except Exception as e:
        if verbose:
            print(f"❌ Error creating snapshot: {e}")
        raise


def create_snapshot(config, output_file=None, db_name=None):
    """
    데이터베이스의 테이블별 row 카운트 스냅샷을 생성합니다.
    
    Args:
        config: 데이터베이스 연결 설정
        output_file: 출력 파일명 (기본값: snapshot_{timestamp}.json)
        db_name: 스냅샷에 기록할 데이터베이스 이름
    """
    print(f"\n📸 Creating snapshot for database: {config.get('host')}:{config.get('port')}/{config.get('dbname')}")
    
    # 데이터베이스 연결
    try:
        conn = get_connection(config)
        print("  ✓ Database connection established")
    except Exception as e:
        print(f"  ✗ Failed to connect to database: {e}")
        return None
    
    try:
        return create_snapshot_from_conn(conn, output_file, db_name)
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(
        description='PostgreSQL 데이터베이스의 테이블별 row 카운트 스냅샷을 생성합니다.'
    )
    parser.add_argument(
        '--db',
        choices=['source', 'target'],
        default='source',
        help='스냅샷을 생성할 데이터베이스 (source 또는 target, 기본값: source)'
    )
    parser.add_argument(
        '--output',
        '-o',
        help='출력 파일명 (기본값: snapshot_{db}_{timestamp}.json)'
    )
    parser.add_argument(
        '--config',
        default='config.yaml',
        help='설정 파일 경로 (기본값: config.yaml)'
    )
    
    args = parser.parse_args()
    
    # config.yaml 읽기
    try:
        with open(args.config, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"❌ Error: {args.config} 파일을 찾을 수 없습니다.")
        return
    except yaml.YAMLError as e:
        print(f"❌ Error: {args.config} 파싱 중 오류 발생: {e}")
        return
    
    # 데이터베이스 설정 가져오기
    if args.db == 'source':
        db_config = config.get('source')
        db_name = 'source'
    else:
        target_configs = config.get('targets', {})
        if not target_configs:
            print("❌ Error: config.yaml에 target 설정이 없습니다.")
            return
        # 첫 번째 target 사용
        target_key = list(target_configs.keys())[0]
        db_config = target_configs[target_key]
        db_name = f'target_{target_key}'
    
    if not db_config:
        print(f"❌ Error: {args.db} 데이터베이스 설정을 찾을 수 없습니다.")
        return
    
    # 스냅샷 생성
    create_snapshot(db_config, args.output, db_name)


if __name__ == '__main__':
    main()


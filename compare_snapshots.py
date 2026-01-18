#!/usr/bin/env python3
"""
두 개의 row 카운트 스냅샷 파일을 비교하는 스크립트
"""
import json
import argparse
from pathlib import Path
from datetime import datetime


def load_snapshot(file_path):
    """스냅샷 파일을 로드합니다."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"❌ Error: {file_path} 파일을 찾을 수 없습니다.")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Error: {file_path} JSON 파싱 중 오류 발생: {e}")
        return None


def compare_snapshots(snapshot1_path, snapshot2_path, verbose=False, output_file=None):
    """
    두 개의 스냅샷을 비교합니다.
    
    Args:
        snapshot1_path: 첫 번째 스냅샷 파일 경로
        snapshot2_path: 두 번째 스냅샷 파일 경로
        verbose: 상세 출력 여부
        output_file: 비교 결과를 저장할 파일 경로 (None이면 저장 안함)
    
    Returns:
        (is_identical, report): 동일 여부와 비교 리포트 텍스트
    """
    print("\n🔍 Loading snapshots...")
    
    # 스냅샷 로드
    snapshot1 = load_snapshot(snapshot1_path)
    snapshot2 = load_snapshot(snapshot2_path)
    
    if not snapshot1 or not snapshot2:
        return False, ""
    
    # 리포트 텍스트를 저장할 리스트
    report_lines = []
    
    def add_line(text=""):
        """리포트에 라인 추가 및 출력"""
        report_lines.append(text)
        print(text)
    
    # 메타데이터 출력
    add_line("\n📊 Snapshot Information:")
    add_line(f"\n  Snapshot 1: {snapshot1_path}")
    add_line(f"    - Timestamp: {snapshot1['metadata']['timestamp']}")
    add_line(f"    - Database: {snapshot1['metadata']['database']}")
    if 'host' in snapshot1['metadata']:
        add_line(f"    - Host: {snapshot1['metadata']['host']}")
    add_line(f"    - Tables: {snapshot1['metadata']['total_tables']}")
    add_line(f"    - Total Rows: {snapshot1['metadata']['total_rows']:,}")
    
    add_line(f"\n  Snapshot 2: {snapshot2_path}")
    add_line(f"    - Timestamp: {snapshot2['metadata']['timestamp']}")
    add_line(f"    - Database: {snapshot2['metadata']['database']}")
    if 'host' in snapshot2['metadata']:
        add_line(f"    - Host: {snapshot2['metadata']['host']}")
    add_line(f"    - Tables: {snapshot2['metadata']['total_tables']}")
    add_line(f"    - Total Rows: {snapshot2['metadata']['total_rows']:,}")
    
    # 테이블 목록 비교
    tables1 = set(snapshot1['tables'].keys())
    tables2 = set(snapshot2['tables'].keys())
    
    only_in_1 = tables1 - tables2
    only_in_2 = tables2 - tables1
    common_tables = tables1 & tables2
    
    add_line("\n" + "=" * 80)
    add_line("📋 TABLE COMPARISON")
    add_line("=" * 80)
    
    # 테이블 구조 차이
    if only_in_1:
        add_line(f"\n⚠️  Tables only in Snapshot 1: {len(only_in_1)}")
        if verbose:
            for table in sorted(only_in_1):
                count = snapshot1['tables'][table]
                add_line(f"    - {table}: {count:,} rows")
    
    if only_in_2:
        add_line(f"\n⚠️  Tables only in Snapshot 2: {len(only_in_2)}")
        if verbose:
            for table in sorted(only_in_2):
                count = snapshot2['tables'][table]
                add_line(f"    - {table}: {count:,} rows")
    
    # Row 카운트 차이 분석
    add_line(f"\n📊 Common tables: {len(common_tables)}")
    
    differences = []
    matches = []
    
    for table in sorted(common_tables):
        count1 = snapshot1['tables'][table]
        count2 = snapshot2['tables'][table]
        
        if count1 != count2:
            diff = count2 - count1
            diff_pct = (diff / count1 * 100) if count1 > 0 else float('inf')
            differences.append({
                'table': table,
                'count1': count1,
                'count2': count2,
                'diff': diff,
                'diff_pct': diff_pct
            })
        else:
            matches.append(table)
    
    # 결과 출력
    add_line("\n" + "=" * 80)
    add_line("📈 ROW COUNT COMPARISON")
    add_line("=" * 80)
    
    if differences:
        add_line(f"\n⚠️  Tables with different row counts: {len(differences)}")
        add_line("\n{:<40} {:>15} {:>15} {:>15} {:>10}".format(
            "Table", "Snapshot 1", "Snapshot 2", "Difference", "Change %"
        ))
        add_line("-" * 100)
        
        # 차이가 큰 순서로 정렬
        differences.sort(key=lambda x: abs(x['diff']), reverse=True)
        
        for item in differences:
            table = item['table']
            count1 = item['count1']
            count2 = item['count2']
            diff = item['diff']
            diff_pct = item['diff_pct']
            
            # 차이 표시
            if diff > 0:
                diff_str = f"+{diff:,}"
                pct_str = f"+{diff_pct:.2f}%"
            else:
                diff_str = f"{diff:,}"
                pct_str = f"{diff_pct:.2f}%"
            
            add_line("{:<40} {:>15,} {:>15,} {:>15} {:>10}".format(
                table[:39], count1, count2, diff_str, pct_str
            ))
    else:
        add_line("\n✅ No differences found in row counts!")
    
    add_line(f"\n✅ Tables with matching row counts: {len(matches)}")
    if verbose and matches:
        for table in matches[:10]:  # 처음 10개만 출력
            count = snapshot1['tables'][table]
            add_line(f"    - {table}: {count:,} rows")
        if len(matches) > 10:
            add_line(f"    ... and {len(matches) - 10} more tables")
    
    # 요약
    add_line("\n" + "=" * 80)
    add_line("📝 SUMMARY")
    add_line("=" * 80)
    
    total_issues = len(only_in_1) + len(only_in_2) + len(differences)
    
    if total_issues == 0:
        add_line("\n✅ Snapshots are IDENTICAL!")
        add_line(f"   - All {len(common_tables)} tables have matching row counts")
        is_identical = True
    else:
        add_line(f"\n⚠️  Snapshots have DIFFERENCES:")
        if only_in_1:
            add_line(f"   - Tables only in Snapshot 1: {len(only_in_1)}")
        if only_in_2:
            add_line(f"   - Tables only in Snapshot 2: {len(only_in_2)}")
        if differences:
            add_line(f"   - Tables with different row counts: {len(differences)}")
            total_diff = sum(item['diff'] for item in differences)
            if total_diff > 0:
                add_line(f"   - Total row difference: +{total_diff:,} rows")
            else:
                add_line(f"   - Total row difference: {total_diff:,} rows")
        add_line(f"   - Tables with matching row counts: {len(matches)}")
        is_identical = False
    
    # 리포트를 파일로 저장
    if output_file:
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(report_lines))
            print(f"\n📄 Comparison report saved: {output_file}")
        except IOError as e:
            print(f"\n❌ Error saving report: {e}")
    
    return is_identical, '\n'.join(report_lines)


def main():
    parser = argparse.ArgumentParser(
        description='두 개의 row 카운트 스냅샷 파일을 비교합니다.'
    )
    parser.add_argument(
        'snapshot1',
        help='첫 번째 스냅샷 파일 경로'
    )
    parser.add_argument(
        'snapshot2',
        help='두 번째 스냅샷 파일 경로'
    )
    parser.add_argument(
        '--verbose',
        '-v',
        action='store_true',
        help='상세 출력 모드'
    )
    
    args = parser.parse_args()
    
    # 비교 실행
    is_identical, _ = compare_snapshots(args.snapshot1, args.snapshot2, args.verbose)
    
    # 종료 코드 설정 (스크립트에서 사용 가능)
    exit(0 if is_identical else 1)


if __name__ == '__main__':
    main()


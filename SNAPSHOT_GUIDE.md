# 테이블 Row 카운트 스냅샷 가이드

데이터베이스의 각 테이블별 row 카운트를 스냅샷으로 저장하고 비교하는 도구입니다.

## 🚀 자동 스냅샷 & 검증 (권장)

**`--with-data` 옵션을 사용하면 데이터 마이그레이션 시 자동으로 스냅샷을 생성하고 검증합니다!**

```bash
# 스키마 + 데이터 마이그레이션 + 자동 검증
python -m pg_schema_sync --with-data
```

이 명령어는 자동으로:
1. **마이그레이션 전**: Source DB의 스냅샷 생성
2. **마이그레이션 실행**: 데이터 복사
3. **마이그레이션 후**: Target DB의 스냅샷 생성
4. **자동 비교**: 두 스냅샷을 비교하여 검증 리포트 생성

### 출력 파일

- `snapshots/snapshot_source_YYYYMMDD_HHMMSS.json` - Source DB 스냅샷
- `snapshots/snapshot_target_YYYYMMDD_HHMMSS.json` - Target DB 스냅샷
- `history/validation_report.{target}.{timestamp}.txt` - 검증 리포트

---

## 📸 수동 스냅샷 생성 (선택사항)

자동 검증이 필요 없는 경우에만 수동으로 스냅샷을 생성할 수 있습니다.

### 스냅샷 생성 (`snapshot_row_counts.py`)

### 기본 사용법

```bash
# Source 데이터베이스 스냅샷 생성
python snapshot_row_counts.py --db source

# Target 데이터베이스 스냅샷 생성
python snapshot_row_counts.py --db target
```

### 옵션

- `--db`: 스냅샷을 생성할 데이터베이스 선택 (`source` 또는 `target`, 기본값: `source`)
- `--output, -o`: 출력 파일명 지정 (기본값: `snapshot_{db}_{timestamp}.json`)
- `--config`: 설정 파일 경로 (기본값: `config.yaml`)

### 사용 예시

```bash
# Source DB 스냅샷 생성 (자동 파일명)
python snapshot_row_counts.py --db source

# Target DB 스냅샷 생성 (커스텀 파일명)
python snapshot_row_counts.py --db target --output before_migration.json

# 다른 설정 파일 사용
python snapshot_row_counts.py --db source --config custom_config.yaml
```

### 출력 예시

```
📸 Creating snapshot for database: aws-0-ap-northeast-2.pooler.supabase.com:6543/postgres
  ✓ Database connection established

📋 Fetching table list...
  Found 45 tables

📊 Counting rows for each table...
  ✓ users: 1,234 rows
  ✓ orders: 5,678 rows
  ✓ products: 890 rows
  ...

✅ Snapshot saved: snapshots/snapshot_source_20250101_123456.json
   Total tables: 45
   Total rows: 125,678
```

### 스냅샷 파일 형식

생성된 JSON 파일에는 다음 정보가 포함됩니다:

```json
{
  "metadata": {
    "timestamp": "2025-01-01T12:34:56",
    "database": "source",
    "host": "example.com",
    "port": 5432,
    "total_tables": 45,
    "total_rows": 125678,
    "error_tables": []
  },
  "tables": {
    "users": 1234,
    "orders": 5678,
    "products": 890
  }
}
```

---

### 스냅샷 비교 (`compare_snapshots.py`)

### 기본 사용법

```bash
python compare_snapshots.py <snapshot1> <snapshot2>
```

### 옵션

- `--verbose, -v`: 상세 출력 모드 (일치하는 테이블 목록도 표시)

### 사용 예시

```bash
# 두 스냅샷 비교
python compare_snapshots.py snapshots/before.json snapshots/after.json

# 상세 모드로 비교
python compare_snapshots.py snapshots/source.json snapshots/target.json --verbose
```

### 출력 예시

```
🔍 Loading snapshots...

📊 Snapshot Information:

  Snapshot 1: snapshots/snapshot_source_20250101_123456.json
    - Timestamp: 2025-01-01T12:34:56
    - Database: source
    - Host: source.example.com
    - Tables: 45
    - Total Rows: 125,678

  Snapshot 2: snapshots/snapshot_target_20250101_234567.json
    - Timestamp: 2025-01-01T23:45:67
    - Database: target
    - Host: target.example.com
    - Tables: 45
    - Total Rows: 125,680

================================================================================
📋 TABLE COMPARISON
================================================================================

📊 Common tables: 45

================================================================================
📈 ROW COUNT COMPARISON
================================================================================

⚠️  Tables with different row counts: 2

Table                                    Snapshot 1      Snapshot 2      Difference   Change %
----------------------------------------------------------------------------------------------------
orders                                        5,678           5,680              +2    +0.04%
products                                        890             890               0     0.00%

✅ Tables with matching row counts: 43

================================================================================
📝 SUMMARY
================================================================================

⚠️  Snapshots have DIFFERENCES:
   - Tables with different row counts: 2
   - Total row difference: +2 rows
   - Tables with matching row counts: 43
```

---

## 💡 사용 시나리오

### 1. 데이터 마이그레이션 검증 (자동, 권장)

```bash
# 한 번의 명령으로 마이그레이션 + 검증
python -m pg_schema_sync --with-data

# 결과:
# ✅ 스키마 생성
# ✅ 데이터 마이그레이션
# ✅ 시퀀스 동기화
# ✅ 자동 스냅샷 생성
# ✅ 자동 검증 리포트
```

### 2. 마이그레이션 전후 수동 비교

```bash
# 마이그레이션 전 스냅샷
python snapshot_row_counts.py --db source --output before_migration.json
python snapshot_row_counts.py --db target --output before_migration_target.json

# 마이그레이션 실행
python -m pg_schema_sync --with-data

# 결과 확인 (자동으로 생성된 리포트 확인)
cat history/validation_report.*.txt
```

### 3. 주기적인 데이터 모니터링

```bash
# 매일 스냅샷 생성 (크론잡 등으로 자동화 가능)
python snapshot_row_counts.py --db source

# 어제와 오늘 스냅샷 비교
python compare_snapshots.py \
  snapshots/snapshot_source_20250101_000000.json \
  snapshots/snapshot_source_20250102_000000.json
```

### 4. Source와 Target DB 동기화 확인

```bash
# Source 스냅샷
python snapshot_row_counts.py --db source

# Target 스냅샷
python snapshot_row_counts.py --db target

# 비교 (가장 최신 파일 사용)
python compare_snapshots.py \
  snapshots/snapshot_source_*.json \
  snapshots/snapshot_target_*.json
```

---

## 📂 파일 구조

```
pg-schema-sync/
├── snapshot_row_counts.py    # 스냅샷 생성 스크립트
├── compare_snapshots.py       # 스냅샷 비교 스크립트
├── config.yaml                # 데이터베이스 설정
└── snapshots/                 # 스냅샷 파일 저장 디렉토리 (자동 생성)
    ├── snapshot_source_20250101_123456.json
    ├── snapshot_target_20250101_234567.json
    └── ...
```

---

## ⚙️ 설정 (`config.yaml`)

스크립트는 기존 `config.yaml` 파일의 데이터베이스 설정을 사용합니다:

```yaml
source:
  host: source.example.com
  port: 5432
  dbname: postgres
  user: postgres
  password: password123

targets:
  gcp:
    host: target.example.com
    port: 5432
    dbname: postgres
    user: postgres
    password: password456
```

---

## 🚀 빠른 시작

### 자동 방식 (권장)

```bash
# 데이터 마이그레이션 + 자동 검증을 한 번에!
python -m pg_schema_sync --with-data

# 검증 리포트 확인
cat history/validation_report.*.txt
```

### 수동 방식

```bash
# 1. Source DB 스냅샷 생성
python snapshot_row_counts.py --db source

# 2. Target DB 스냅샷 생성
python snapshot_row_counts.py --db target

# 3. 비교 (최신 파일 2개 사용)
python compare_snapshots.py \
  snapshots/snapshot_source_*.json \
  snapshots/snapshot_target_*.json
```

---

## 📝 참고사항

- **`--with-data` 옵션 사용 시**: 스냅샷이 자동으로 생성되고 비교되어 검증 리포트가 생성됩니다.
- 스냅샷 파일은 `snapshots/` 디렉토리에 자동으로 저장됩니다.
- 검증 리포트는 `history/` 디렉토리에 저장됩니다.
- 파일명은 `snapshot_{db}_{timestamp}.json` 형식으로 자동 생성됩니다.
- 비교 시 종료 코드를 사용하여 스크립트 자동화가 가능합니다:
  - `0`: 스냅샷이 동일함
  - `1`: 스냅샷에 차이가 있음
- 테이블 접근 오류가 발생한 경우 row 카운트가 `-1`로 기록됩니다.

## 🎯 마이그레이션 워크플로우

```
1. 스키마 마이그레이션 (선택)
   $ python -m pg_schema_sync
   
2. 데이터 마이그레이션 + 자동 검증
   $ python -m pg_schema_sync --with-data
   
   [자동 실행 단계]
   ① Source DB 스냅샷 생성
   ② 데이터 마이그레이션 실행
   ③ Target DB 스냅샷 생성
   ④ 스냅샷 비교 및 검증
   ⑤ 검증 리포트 생성
   
3. 결과 확인
   $ cat history/validation_report.*.txt
```


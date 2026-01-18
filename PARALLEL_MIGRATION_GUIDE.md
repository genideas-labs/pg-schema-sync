# 병렬 마이그레이션 가이드

Menu DB와 Payment DB를 동시에 마이그레이션하는 방법입니다.

## 📁 설정 파일

- `config_menu.yaml`: SUPABASE MENU DB → GCP PROD MENU DB
- `config_payment.yaml`: SUPABASE PAYMENT DB → GCP PROD PAYMENT DB

## 🔧 사전 준비

가상환경이 없다면 먼저 생성하세요:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**참고**: 스크립트가 자동으로 `venv` 또는 `.venv`를 찾아서 활성화합니다.

## 📝 마이그레이션 단계

각 스크립트는 자동으로 다음을 실행합니다:

1. **venv 활성화** - 가상환경 자동 활성화
2. **Step 1: 스키마 마이그레이션** - 테이블, 인덱스, FK 등 스키마 생성
3. **Step 2: 데이터 마이그레이션** - 실제 데이터 복사 및 시퀀스 초기화

## 🚀 병렬 실행 방법

### 방법 1: 두 개의 터미널에서 수동 실행

**터미널 1 (Menu DB)**

```bash
cd /Users/iseong-won/Desktop/OQ/pg-schema-sync
./migrate_menu.sh
```

**터미널 2 (Payment DB)**

```bash
cd /Users/iseong-won/Desktop/OQ/pg-schema-sync
./migrate_payment.sh
```

### 방법 2: 백그라운드로 동시 실행

```bash
# 두 마이그레이션을 동시에 백그라운드로 실행
./migrate_menu.sh > menu_migration.log 2>&1 &
./migrate_payment.sh > payment_migration.log 2>&1 &

# 진행 상황 확인
tail -f menu_migration.log      # 다른 터미널에서
tail -f payment_migration.log   # 또 다른 터미널에서

# 또는 동시에 보기
tail -f menu_migration.log payment_migration.log
```

### 방법 3: tmux 사용 (권장)

```bash
# tmux 세션 시작
tmux new -s migration

# 좌우 분할
Ctrl+b %

# 왼쪽 창에서
./migrate_menu.sh

# 오른쪽 창으로 이동 (Ctrl+b → 화살표)
# 오른쪽 창에서
./migrate_payment.sh

# 세션에서 나가기: Ctrl+b d
# 다시 연결: tmux attach -t migration
```

## ⚙️ 개별 옵션으로 실행

스크립트 대신 직접 실행할 수도 있습니다:

```bash
# venv 활성화 (필요한 경우)
source venv/bin/activate

# Menu DB - 스키마만 확인 (commit 안 함)
python -m src.pg_schema_sync --config config_menu.yaml --no-commit

# Menu DB - Step 1: 스키마만 마이그레이션
python -m src.pg_schema_sync --config config_menu.yaml --commit

# Menu DB - Step 2: 데이터 마이그레이션 (스키마 완료 후)
python -m src.pg_schema_sync --config config_menu.yaml --commit --with-data

# Payment DB - Step 1: 스키마만 마이그레이션
python -m src.pg_schema_sync --config config_payment.yaml --commit

# Payment DB - Step 2: 데이터 마이그레이션 (스키마 완료 후)
python -m src.pg_schema_sync --config config_payment.yaml --commit --with-data
```

### 옵션 설명

- `--config CONFIG_FILE`: 사용할 설정 파일 지정
- `--commit`: 생성된 SQL을 target DB에 실행 (기본값: true)
- `--no-commit`: SQL 파일만 생성하고 실행 안 함
- `--with-data`: 데이터 마이그레이션 실행 (**스키마 마이그레이션 완료 후 사용**)
- `--verify`: 스키마 차이만 확인 (SQL 생성 안 함)

### ⚠️ 중요: 실행 순서

1. **먼저** `--commit` (스키마만)
2. **그 다음** `--commit --with-data` (데이터)

스크립트(`migrate_menu.sh`, `migrate_payment.sh`)는 이 순서를 자동으로 처리합니다.

## 📊 모니터링

각 마이그레이션은 독립적으로:

- SQL 파일 생성: `history/migrate.gcp.YYYYMMDDHHMMSS.sql`
- 검증 리포트: `history/validation_report.gcp.YYYYMMDDHHMMSS.txt`
- 스냅샷 생성: `snapshots/snapshot_*.json`

## ⚠️ 주의사항

1. **DB 연결 제한**: 각 마이그레이션이 동시에 많은 연결을 사용할 수 있습니다.

   - 현재 설정: 최대 5개의 병렬 연결 (`MAX_WORKERS = 5`)
   - Menu + Payment 동시 실행 시 최대 10개 연결 사용

2. **네트워크 대역폭**: 대용량 데이터 전송 시 네트워크 속도를 고려하세요.

3. **로그 관리**: 각 마이그레이션의 로그를 별도로 저장하여 추적하세요.

4. **충돌 방지**: Menu와 Payment는 별도 DB이므로 서로 영향을 주지 않습니다.

## 🔍 진행 상황 확인

```bash
# 프로세스 확인
ps aux | grep pg_schema_sync

# 연결 확인 (source DB)
psql -h aws-0-ap-northeast-2.pooler.supabase.com -U postgres.hszfgulbsaxwqiinsjca -d postgres -c "SELECT count(*) FROM pg_stat_activity WHERE usename LIKE 'postgres%';"

# 연결 확인 (target DB)
psql -h 34.158.215.6 -U postgres -d postgres -c "SELECT count(*) FROM pg_stat_activity;"
```

## ✅ 완료 확인

두 마이그레이션이 모두 완료되면:

```bash
# Menu DB 검증
python -m src.pg_schema_sync --config config_menu.yaml --verify

# Payment DB 검증
python -m src.pg_schema_sync --config config_payment.yaml --verify
```

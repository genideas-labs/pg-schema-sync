# pg-schema-sync 명세서

상태: 상시 갱신 문서

## 1. 목적
pg-schema-sync는 소스와 타겟 PostgreSQL 스키마(공개 스키마만)를 비교하고, 소스 기준으로 타겟을 맞추기 위한 마이그레이션 SQL을 생성하거나 적용합니다. 또한 테이블 데이터를 복사하고 시퀀스를 보정하는 데이터 마이그레이션 흐름을 지원합니다.

## 2. 구성 요소
- CLI: `src/pg_schema_sync/__main__.py`의 `pg-schema-sync` 엔트리포인트.
- 데이터 마이그레이션 엔진: `src/pg_schema_sync/dataMig.py`.
- MCP 래퍼: `mcp_server/index.py`.
- 운영 도우미: `check_connections.py`, `kill_idle_transactions.py`, `kill_zombie_connections.py`, `kill_others.sh`, `migrate_clean.sh`.
- 단계별 실행 스크립트: `migrate_stepwise.py`.
- 테스트: `tests/` (비교 로직 유닛 테스트).

## 3. 설정
CLI는 기본적으로 현재 작업 디렉터리의 `config.yaml`을 읽고, 다른 파일은 `--config <path>`로 지정합니다.

예상 형태:
```yaml
source:
  host: SOURCE_DB_HOST
  port: SOURCE_DB_PORT
  db: SOURCE_DB_NAME        # 또는 dbname
  username: SOURCE_DB_USER  # 또는 user
  password: SOURCE_DB_PASSWORD

targets:
  gcp_test:
    host: TARGET_DB_HOST
    port: TARGET_DB_PORT
    db: TARGET_DB_NAME      # 또는 dbname
    username: TARGET_DB_USER
    password: TARGET_DB_PASSWORD
```

메모:
- CLI는 `targets.gcp_test`를 요구하며 항상 해당 타겟으로 연결합니다.
- CLI에서 `db`는 `dbname`으로, `username`은 `user`로 정규화됩니다.
- MCP 래퍼는 `PG_SYNC_CONFIG_PATH`에서 설정을 읽고 런타임에 `target_name`을 받습니다.

## 4. CLI 인터페이스
명령:
```
pg-schema-sync [--config <path>] [--verify] [--commit | --no-commit] [--use-alter] [--with-data] [--skip-fk | --fk-not-valid] [--install-extensions | --no-install-extensions]
```

플래그:
- `--config <path>`: 기본 `config.yaml` 대신 지정 경로 사용.
- `--verify`: 객체 이름 차이만 보고(DDL/실행 없음).
- `--commit` (기본값): SQL 생성 및 타겟에 적용(DDL 블록 단위 커밋).
- `--no-commit`: SQL 파일만 생성하고 적용하지 않음.
- `--use-alter` (실험적): 안전한 컬럼 변경은 `ALTER TABLE`을 사용, 그 외는 재생성.
- `--with-data`: 스키마 변경 후 데이터 마이그레이션 실행.
- `--skip-fk`: FK 마이그레이션을 건너뜀.
- `--fk-not-valid`: FK를 `NOT VALID`로 추가하고 검증 SQL 파일을 생성.
- `--install-extensions` / `--no-install-extensions`: 타겟에 없는 확장을 감지해 `CREATE EXTENSION`을 추가(기본값: 활성화, allowlist 기반이며 현재 `pg_trgm`, `postgis`, `vector`).

출력 파일:
- `history/migrate.<target>.<timestamp>.sql`
- `history/skip.<target>.<timestamp>.sql`
- `history/validate_fks.<target>.<timestamp>.sql` (`--fk-not-valid` 사용 시)

## 5. 스키마 비교 모델
모든 쿼리는 `public` 스키마에 한정됩니다.

객체 유형 및 데이터 소스:
- Enum: `pg_type` + `pg_enum` (DDL) 및 `enum_range` 값.
- 테이블: `information_schema.tables`, `information_schema.columns`, `pg_constraint`, `information_schema.table_constraints`.
- FK: `pg_constraint` (복합 키와 ON UPDATE/DELETE 지원).
- 뷰: `information_schema.views.view_definition`.
- 함수: `pg_get_functiondef` (일반 함수만, C 언어 제외).
- 인덱스: `pg_indexes` (PK/UNIQUE 파생 인덱스 제외).
- 시퀀스: `pg_class` (identity 시퀀스는 `pg_depend`로 제외).

비교 규칙:
- `--verify`는 객체 이름만 비교합니다.
- Enum은 값 목록(정렬)을 비교하고, 재생성 시 DDL을 사용합니다.
- 함수는 원본 DDL 문자열을 그대로 비교합니다.
- 뷰/인덱스/시퀀스는 `normalize_sql` 결과를 비교합니다.
- 테이블 기본값은 비교하지 않으며, 컬럼 비교는 이름/타입/Null 허용만 사용합니다.

## 6. SQL 생성 규칙

### 6.1 DDL 정규화
`normalize_sql`:
- `--` 주석 제거.
- 달러 인용 본문을 임시 치환하여 보호.
- 소문자화 및 공백/연산자 정규화.

### 6.2 테이블 DDL 생성
`generate_create_table_ddl`:
- 컬럼/테이블 이름을 따옴표로 감쌈.
- 배열 타입을 `udt_name` 기반으로 `base[]` 형태로 변환.
- `is_identity` 또는 `nextval(...)` 존재 시 identity로 판단.
- 단일 컬럼 PK/UNIQUE는 인라인 제약으로 생성.
- 복합 UNIQUE/PRIMARY 제약을 추가.
- 일부 `USER-DEFINED` enum 컬럼에 대해 하드코딩 폴백, 그 외는 `text`로 대체.

### 6.3 ALTER TABLE (실험적)
`--use-alter`가 설정된 경우:
- 컬럼 추가: `ALTER TABLE ... ADD COLUMN`.
- 컬럼 삭제: 경고 주석 + `ALTER TABLE ... DROP COLUMN`.
- Null 제약 변경: `ALTER TABLE ... SET/DROP NOT NULL` (NOT NULL 시 경고).
- 타입 변경: `is_safe_type_change`가 true일 때만 수행.
  - 안전: varchar 길이 증가, varchar→text, 정수 확장, 숫자→문자열.
  - 안전하지 않은 변경은 drop/recreate로 전환.

### 6.4 Enum 처리
- 차이가 있으면 `DROP TYPE ... CASCADE` 후 소스 DDL로 재생성.

### 6.5 뷰와 함수
- 뷰는 `DROP VIEW ... CASCADE` 후 소스 DDL로 재생성.
- 함수는 DDL이 다르면 drop/recreate 처리.

### 6.6 인덱스
- PK/UNIQUE 파생 인덱스는 비교 대상에서 제외.
- 인덱스가 다르거나 없으면 `DO $$ ... IF NOT EXISTS`로 추가.

### 6.7 외래 키
- `pg_constraint` 기반으로 복합 키를 지원.
- 마이그레이션은 누락/변경된 FK를 추가만 하며 DROP하지 않음.
- `--fk-not-valid` 사용 시 `NOT VALID`로 추가하고 검증 SQL 파일을 생성.

### 6.8 시퀀스
- identity 시퀀스는 비교에서 제외.
- 소스 전용 시퀀스는 가능하면 재시작 값만 업데이트, 아니면 스킵.
- 서로 다른 시퀀스는 `ALTER SEQUENCE ... RESTART WITH`를 우선 사용.

## 7. 데이터 마이그레이션
`--with-data`로 실행됩니다.

워크플로우:
1. 연결을 닫았다가 다시 열어 락을 해제.
2. 타겟의 모든 FK 제거(배치 모드, lock timeout 적용).
3. 테이블별 병렬 데이터 복사:
   - 소스에서 `SELECT *`.
   - 타겟에 `ON CONFLICT DO NOTHING`으로 `INSERT`.
   - 생성 컬럼은 제외하고, `GENERATED ALWAYS` IDENTITY는 `OVERRIDING SYSTEM VALUE`를 사용.
   - `SKIP_TABLES`는 제외.
4. FK를 `NOT VALID`로 재생성.
5. 이후 수동 검증을 위한 `validate_fks.sql` 생성.
6. 시퀀스 값 동기화(IDENTITY 및 명시적 시퀀스).

직렬화 규칙:
- 컬럼 타입이 `[]`로 끝나면 Python list를 Postgres 배열로 변환.
- JSON/JSONB 계열은 list/dict를 JSON 문자열로 저장.

## 8. 실행 및 트랜잭션
- `--commit`은 생성된 SQL 블록을 개별 실행하고 블록마다 커밋합니다.
- 실패 시 해당 블록은 롤백하고 실행을 중단합니다.
- `--no-commit`도 history 파일은 생성합니다.
- `--verify`는 연결만 닫고 SQL 생성 없이 종료합니다.

## 9. MCP 래퍼
`mcp_server/index.py`가 제공하는 도구:
- `verify_schema`
- `generate_migration_sql`
- `apply_schema_migration`

CLI 대비 차이점:
- `PG_SYNC_CONFIG_PATH`로 설정을 로드.
- `target_name`, `exclude_tables`, `exclude_indexes`를 요청마다 지정.
- 시퀀스/FK 비교 및 데이터 마이그레이션 없음.
- 테이블 메타데이터는 복합 제약과 identity 세부 정보를 제외.

## 10. 출력 및 로그
- SQL 파일은 `history/`에 기록됩니다.
- 데이터 마이그레이션은 작업 디렉터리에 `validate_fks.sql`을 생성합니다.
- 디버그/진행 로그는 stdout/stderr에 출력되며 구조화 로깅은 없습니다.

## 11. 테스트
- 유닛 테스트는 `tests/`에서 비교 로직을 검증합니다.
- 라이브 DB 통합 테스트는 제공하지 않습니다.

## 12. 제한 사항 / 비목표
- `public` 스키마만 처리합니다.
- 타겟 전용 객체는 삭제하지 않습니다.
- 테이블 기본값 변경은 감지하지 않습니다.
- 함수 오버로딩은 완전히 지원되지 않습니다(이름 기반 키).
- 데이터 마이그레이션은 `id`를 충돌 키로 가정하지 않고 `ON CONFLICT DO NOTHING`을 사용합니다.

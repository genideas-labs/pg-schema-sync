# AGENTS

이 저장소에서 작업하는 자동화 에이전트를 위한 안내입니다.

## 리포 맵 (상위)
- `src/pg_schema_sync/__main__.py`: CLI 엔트리포인트, 스키마 비교, SQL 생성/실행, 설정 로딩.
- `src/pg_schema_sync/dataMig.py`: 데이터 마이그레이션 파이프라인, FK 삭제/재생성, 시퀀스 동기화, 직렬화.
- `mcp_server/index.py`: MCP 래퍼 서버와 도구 정의.
- `tests/`: 비교 로직 유닛 테스트 (enum/table/view/function/index).
- `check_connections.py`, `kill_idle_transactions.py`, `kill_zombie_connections.py`, `kill_others.sh`, `migrate_clean.sh`, `migrate_single_table.py`: 운영용 헬퍼.
- `migrate_stepwise.py`: 스키마 마이그레이션 단계별 실행(대화형) 스크립트.
- `README.md`, `MIGRATION_GUIDE.md`, `testing.md`: 사용자 문서.

## 런타임 동작 요약
- CLI는 현재 작업 디렉터리의 `config.yaml`을 기대하며 `targets.gcp_test`가 필요합니다. 타겟이 여러 개여도 항상 `gcp_test`로 연결합니다.
- `--verify`는 객체 이름/개수만 비교하며 DDL이나 컬럼 수준 차이는 비교하지 않습니다.
- 함수는 원본 DDL로 비교하고, 뷰/인덱스/시퀀스는 `normalize_sql`을 사용합니다(달러 인용 보호).
- `--use-alter`는 안전하다고 판단된 변경에만 적용되며, 그 외에는 `DROP ... CASCADE`로 테이블을 재생성합니다.
- 테이블 DDL 생성에는 복합 UNIQUE/PRIMARY 제약이 포함되며, 일부 `USER-DEFINED` enum 컬럼에 대해 하드코딩된 폴백이 있습니다.
- 외래 키는 `pg_constraint` 메타데이터로 생성하며 마이그레이션 SQL에서는 추가만 합니다(DROP 없음).
- 시퀀스는 IDENTITY 시퀀스를 제외하며, 차이는 가능하면 `ALTER SEQUENCE ... RESTART WITH`로 맞춥니다.
- `--commit`은 각 SQL 블록을 개별 실행하고 블록마다 커밋합니다. 실패 시 실행을 중단합니다.
- `--with-data`는 연결을 닫고 다시 열며, 타겟 FK를 모두 제거하고 `ON CONFLICT (id) DO NOTHING`으로 병렬 데이터 이관 후 FK를 NOT VALID로 재추가하고 시퀀스를 보정합니다.
- MCP 래퍼는 `PG_SYNC_CONFIG_PATH`를 사용하며 단순화된 비교 파이프라인을 사용합니다(시퀀스/FK 없음).

## 개발 워크플로우
- 설치: `pip install -e .`
- 테스트: `pytest`
- MCP 서버: `pip install -r mcp_server/requirements.txt` 후 `python mcp_server/index.py`

## 변경 체크리스트
- 스키마 비교/SQL 생성 로직 변경 시:
  - `src/pg_schema_sync/__main__.py` 및 `tests/` 관련 테스트 업데이트.
  - `SPEC.md` 및 사용자 문서(`README.md`, `MIGRATION_GUIDE.md`) 업데이트.
- 데이터 마이그레이션 또는 FK 처리 변경 시:
  - `src/pg_schema_sync/dataMig.py` 업데이트.
  - 운영 단계/주의사항을 `MIGRATION_GUIDE.md`에 반영.
- CLI 플래그나 config 동작 변경 시:
  - `src/pg_schema_sync/__main__.py` 인수 파싱 업데이트.
  - `README.md`, `SPEC.md` 업데이트.
- MCP 래퍼 동작 변경 시:
  - `mcp_server/index.py` 스키마 및 도구 문서 업데이트.
  - 필요 시 `SPEC.md`와 `README.md`의 MCP 섹션 업데이트.

## 안전한 기본값
- 파괴적인 작업 전에 `--verify` 또는 `--no-commit`을 우선 사용하세요.
- 명시 요청이 없으면 타겟 전용 객체는 자동 DROP하지 않습니다.
- SQL 생성은 결정적이어야 하며 달러 인용 함수 본문을 보존해야 합니다.

# Skills

이 저장소의 변경을 안정적이고 일관되게 유지하기 위한 리포지토리 전용 워크플로우입니다.

## 1. 스키마 비교 변경
스키마 diff, SQL 생성, 정규화 로직을 수정할 때 사용합니다.
- `src/pg_schema_sync/__main__.py`의 `fetch_*`, `compare_and_generate_migration`, `normalize_sql`을 업데이트합니다.
- `tests/`의 테스트를 추가하거나 조정합니다.
- 동작 변경 시 `SPEC.md`와 사용자 문서(`README.md`, `MIGRATION_GUIDE.md`)를 업데이트합니다.

## 2. 테이블 DDL / ALTER 변경
테이블 메타데이터 또는 CREATE/ALTER 동작을 변경할 때 사용합니다.
- `src/pg_schema_sync/__main__.py`의 `fetch_tables_metadata`, `generate_create_table_ddl`, `is_safe_type_change`를 업데이트합니다.
- 테이블 관련 테스트(`tests/test_compare_tables.py`)를 업데이트합니다.
- 변경 사항을 `SPEC.md`에 문서화합니다.

## 3. 데이터 마이그레이션 변경
데이터 마이그레이션, FK 처리, 시퀀스 동기화를 조정할 때 사용합니다.
- `src/pg_schema_sync/dataMig.py`와 관련 스크립트를 업데이트합니다.
- FK 삭제/재생성 및 `validate_fks.sql` 생성 로직을 점검합니다.
- `MIGRATION_GUIDE.md`와 `SPEC.md`를 업데이트합니다.

## 4. MCP 래퍼 변경
MCP 서버 동작 또는 도구 스키마를 변경할 때 사용합니다.
- `mcp_server/index.py`를 업데이트하고 도구 계약을 일관되게 유지합니다.
- 필요 시 `SPEC.md`와 `README.md`의 MCP 섹션을 업데이트합니다.

## 5. 운영 스크립트 변경
운영 헬퍼 스크립트를 수정할 때 사용합니다.
- `check_connections.py`, `kill_*`, `migrate_clean.sh`, `migrate_single_table.py`를 업데이트합니다.
- 운영 절차와 일치하도록 `MIGRATION_GUIDE.md`를 갱신합니다.

# Testing `pg-schema-sync`

이 문서에서는 `pg-schema-sync` 프로젝트의 유닛 테스트에 대해 설명합니다. 테스트는 `pytest` 프레임워크를 사용하여 작성되었으며, `tests/` 디렉토리에 위치합니다.

## 테스트 실행

테스트를 실행하기 전에 개발 의존성을 설치해야 합니다.

```bash
pip install -e ".[dev]"
```

설치 후, 프로젝트 루트 디렉토리에서 다음 명령어를 실행하여 모든 테스트를 수행합니다.

```bash
pytest
```

## 스모크 테스트 (실제 DB 접속)

실제 DB에 접속하는 스모크 테스트는 기본적으로 skip됩니다.
환경 변수를 설정한 경우에만 실행됩니다.

필수:
- `PG_SCHEMA_SYNC_SMOKE_CONFIG`: 사용할 config YAML 경로

선택:
- `PG_SCHEMA_SYNC_SMOKE_TARGET`: 타겟 이름 (예: `gcp_test`) 지정 시 타겟 연결 테스트 추가
- `PG_SCHEMA_SYNC_SMOKE_INCLUDE_SEQUENCES=1`: 시퀀스 조회 포함

실행 예시:

```bash
PG_SCHEMA_SYNC_SMOKE_CONFIG=SupabaseDB1ToLocal.yaml pytest -m smoke
```

타겟까지 포함:

```bash
PG_SCHEMA_SYNC_SMOKE_CONFIG=SupabaseDB1ToLocal.yaml \
PG_SCHEMA_SYNC_SMOKE_TARGET=gcp_test \
pytest -m smoke
```

## 테스트 파일 설명

*   **`tests/test_compare_tables.py`**: 테이블 스키마 비교 로직을 테스트합니다. 특히 `--use-alter` 옵션 사용 시 컬럼 추가, 삭제, 안전한 타입 변경, Null 제약 조건 변경 등에 대한 `ALTER TABLE` 문 생성 및 `DROP/CREATE` 로직 전환을 검증합니다.
*   **`tests/test_compare_enums.py`**: Enum 타입 비교 로직을 테스트합니다. 값 추가/삭제 시 `DROP/CREATE` 동작 및 변경 없을 시 스킵 동작을 검증합니다.
*   **`tests/test_compare_views.py`**: View 정의 비교 로직을 테스트합니다. View 정의 변경 시 `CREATE OR REPLACE VIEW` 생성 및 변경 없을 시 스킵 동작을 검증합니다.
*   **`tests/test_compare_functions.py`**: Function 정의 비교 로직을 테스트합니다. Function 정의 변경 시 `DROP/CREATE` 생성 및 변경 없을 시 스킵 동작을 검증합니다.
*   **`tests/test_compare_indexes.py`**: Index 정의 비교 로직을 테스트합니다. Index 정의 변경 시 `DROP/CREATE` 생성 및 변경 없을 시 스킵 동작을 검증합니다. (TODO: `EXCLUDE_INDEXES` 기능 테스트 추가 필요)

## 기여

새로운 기능 추가 또는 버그 수정 시 관련 유닛 테스트를 함께 추가하거나 업데이트하는 것을 권장합니다.

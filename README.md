# PostgreSQL Schema Migration Helper

이 스크립트는 두 개의 PostgreSQL 데이터베이스 스키마(소스와 타겟)를 비교하고, 소스 스키마를 기준으로 타겟 스키마를 업데이트하는 마이그레이션 SQL을 생성하거나 직접 실행하는 데 도움을 줍니다. 또한, 스키마 간의 차이점을 검증하는 기능도 제공합니다.

## 주요 기능

*   소스와 타겟 데이터베이스 스키마 비교 (Enum, Sequence, Table, View, Function, Index)
*   스키마 객체 타입별 비교 로직 적용:
    *   Table: 컬럼 메타데이터 (이름, 타입, Null 여부, 기본값) 비교
    *   Enum: 값 목록 비교
    *   Function: 원본 DDL 직접 비교 (달러 인용 문자열 보호)
    *   View, Index, Sequence: 정규화된 DDL 비교
*   특정 객체(예: 마이그레이션 도구 테이블, 기본 키 인덱스) 비교 제외 기능
*   마이그레이션 SQL 생성 및 파일 저장 (`history/migrate.*.sql`)
*   건너뛴(동일한) 객체 정보 파일 저장 (`history/skip.*.sql`)
*   마이그레이션 SQL 자동 실행 및 롤백 기능 (`--commit` 옵션, 각 DDL 블록 단위 실행)
*   스키마 차이점 검증 및 보고 기능 (`--verify` 옵션)

## 설치

1.  **소스 코드에서 직접 설치**:
    ```bash
    pip install .
    ```
    (이 명령은 `pyproject.toml` 파일을 사용하여 패키지를 빌드하고 설치합니다. 필요한 의존성도 함께 설치됩니다.)

2.  **(선택 사항) PyPI에서 설치**:
    패키지가 PyPI에 배포된 경우 다음 명령으로 설치할 수 있습니다.
    ```bash
    pip install pg-schema-sync
    ```

## 설정

**`config.yaml` 파일 설정**:

이 도구를 실행하는 **현재 작업 디렉토리**에 `config.yaml` 파일을 생성하고 다음과 같은 형식으로 소스 및 타겟 데이터베이스 연결 정보를 입력합니다.

```yaml
source:
  host: SOURCE_DB_HOST

    ```yaml
    source:
      host: SOURCE_DB_HOST
      port: SOURCE_DB_PORT
      db: SOURCE_DB_NAME
      username: SOURCE_DB_USER
      password: SOURCE_DB_PASSWORD

    targets:
      TARGET_NAME_1: # 예: gcp_test
        host: TARGET_DB_HOST_1
        port: TARGET_DB_PORT_1
        db: TARGET_DB_NAME_1
        username: TARGET_DB_USER_1
        password: TARGET_DB_PASSWORD_1
      # TARGET_NAME_2: ... # 필요시 다른 타겟 추가 가능 (현재 스크립트는 첫 번째 타겟만 사용)
    ```

## 사용법

패키지를 설치하면 `pg-schema-sync` 명령어를 사용할 수 있습니다.

```bash
pg-schema-sync [OPTIONS]
```

**(참고)** 패키지를 설치하지 않고 개발 중 소스 코드 디렉토리(프로젝트 루트)에서 직접 실행하려면 다음 명령을 사용할 수 있습니다:
```bash
python -m pg_schema_sync [OPTIONS]
```
(편집 가능한 모드(`pip install -e .`)로 설치한 경우에도 위 명령은 작동하며, `pg-schema-sync` 명령어와 동일하게 동작합니다.)


**옵션:** (`pg-schema-sync` 또는 `python -m pg_schema_sync` 사용)

*   `--verify`: 스키마 차이점만 검증하고 보고합니다. SQL 파일을 생성하거나 데이터베이스를 변경하지 않습니다.
*   `--commit` (기본값): 스키마를 비교하고, 변경이 필요한 SQL을 생성하여 `history/migrate.*.sql` 파일에 저장한 후, 타겟 데이터베이스에 해당 SQL을 **실행하고 커밋**합니다. 건너뛴 객체 정보는 `history/skip.*.sql`에 저장됩니다.
*   `--no-commit`: `--commit`과 동일하게 SQL 파일을 생성하지만, 타겟 데이터베이스에 **실행하지는 않습니다**. 생성된 SQL 파일을 검토한 후 수동으로 적용할 때 유용합니다.
*   `--use-alter` (실험적): 테이블 컬럼 추가/삭제 시 `DROP/CREATE` 대신 `ALTER TABLE` 문 생성을 시도합니다. 컬럼 타입 변경 등 복잡한 변경은 여전히 `DROP/CREATE`로 처리될 수 있습니다. **데이터 손실 위험이 있으므로 주의해서 사용하세요.**

**실행 예시:**

1.  **스키마 차이 검증만 수행:**
    ```bash
    pg-schema-sync --verify
    # 또는 모듈로 실행:
    # python -m pg_schema_sync --verify
    ```

2.  **마이그레이션 SQL 생성 및 자동 적용:**
    ```bash
    pg-schema-sync
    # 또는 명시적으로
    pg-schema-sync --commit
    # 또는 모듈로 실행:
    # python -m pg_schema_sync --commit
    ```

3.  **마이그레이션 SQL 파일만 생성 (자동 적용 안 함):**
    ```bash
    pg-schema-sync --no-commit
    # 또는 모듈로 실행:
    # python -m pg_schema_sync --no-commit
    ```

## 출력 파일

스크립트 실행 시 `history` 디렉토리에 다음과 같은 파일이 생성됩니다 (타겟 이름이 `gcp_test`이고 타임스탬프가 `20250405143000`인 경우 예시):

*   `history/migrate.gcp_test.20250405143000.sql`: 타겟 데이터베이스에 적용해야 할 마이그레이션 SQL 문 (DROP/CREATE 등)이 포함됩니다. `--commit` 옵션 사용 시 이 파일의 내용이 실행됩니다.
*   `history/skip.gcp_test.20250405143000.sql`: 소스와 타겟 간에 동일하여 변경이 필요 없는 객체들의 정보 (주석 및 주석 처리된 DDL)가 포함됩니다.

## 주의사항

*   `--commit` 옵션은 타겟 데이터베이스를 직접 변경하므로 주의해서 사용해야 합니다. 특히 `DROP ... CASCADE`와 같은 명령어가 포함될 수 있으므로, 중요한 데이터베이스에서는 `--no-commit` 옵션으로 생성된 SQL을 먼저 검토하는 것을 강력히 권장합니다.
*   현재 스키마 비교는 주로 객체의 존재 여부 및 정의의 유사성을 기반으로 합니다. 복잡한 의존성이나 데이터 마이그레이션은 처리하지 않습니다.
*   테이블 비교 시 컬럼 순서 변경은 감지하지 못할 수 있습니다. 컬럼 기본값 비교는 표현 방식 차이로 인해 완벽하지 않을 수 있습니다.
*   함수 DDL 비교는 `pg_get_functiondef` 결과의 원본 문자열을 기준으로 하므로, 사소한 공백이나 주석 차이도 다르게 인식될 수 있었으나, 현재는 원본 DDL을 직접 비교하여 이러한 문제를 최소화했습니다. (단, `normalize_sql` 함수는 여전히 다른 객체 타입에 사용됩니다.)

## MCP 서버 (Wrapper)

이 프로젝트는 Model Context Protocol (MCP) 서버 Wrapper를 포함하여, 기존 `pg-schema-sync` 기능을 MCP 클라이언트(예: Cline)를 통해 사용할 수 있도록 합니다. Wrapper 서버는 `mcp_server` 디렉토리에 위치합니다.

**설치 및 실행:**

1.  **기본 패키지 설치:** 먼저 위 "설치" 섹션에 따라 `pg-schema-sync` 패키지를 설치합니다.
2.  **Wrapper 의존성 설치:** MCP 서버 Wrapper를 실행하기 위해 추가 의존성을 설치해야 합니다.
    ```bash
    pip install -r mcp_server/requirements.txt
    ```
3.  **설정 파일 경로 지정:** Wrapper 서버는 데이터베이스 연결 정보가 포함된 `config.yaml` 파일의 경로를 알아야 합니다. `PG_SYNC_CONFIG_PATH` 환경 변수를 설정하여 파일의 전체 경로를 지정합니다.
    ```bash
    # 리눅스/맥
    export PG_SYNC_CONFIG_PATH="/path/to/your/config.yaml"
    # 윈도우
    # set PG_SYNC_CONFIG_PATH="C:\path\to\your\config.yaml"
    ```
4.  **서버 실행:** 환경 변수가 설정된 상태에서 Wrapper 서버를 실행합니다.
    ```bash
    python mcp_server/index.py
    ```
    (또는 MCP 클라이언트 설정 파일에 이 명령어를 등록하여 자동으로 실행되도록 할 수 있습니다.)

**제공되는 도구:**

MCP 클라이언트를 통해 다음 도구들을 사용할 수 있습니다. 각 도구는 `PG_SYNC_CONFIG_PATH` 환경 변수에 지정된 `config.yaml` 파일을 사용합니다.

*   `verify_schema`: 소스와 타겟 데이터베이스 스키마의 차이점을 검증하고 보고서를 반환합니다. (`--verify` 옵션과 유사)
    *   입력: `{ "target_name": "TARGET_KEY_IN_CONFIG", "exclude_tables": [...], "exclude_indexes": [...] }`
*   `generate_migration_sql`: 타겟 스키마를 업데이트하기 위한 마이그레이션 SQL을 생성하여 반환합니다 (적용 안 함). (`--no-commit` 옵션과 유사)
    *   입력: `{ "target_name": "TARGET_KEY_IN_CONFIG", "exclude_tables": [...], "exclude_indexes": [...], "use_alter": false }`
*   `apply_schema_migration`: 마이그레이션 SQL을 생성하고 지정된 타겟 데이터베이스에 직접 적용합니다. (`--commit` 옵션과 유사)
    *   입력: `{ "target_name": "TARGET_KEY_IN_CONFIG", "exclude_tables": [...], "exclude_indexes": [...], "use_alter": false }`
    *   **주의:** `use_alter: true` 사용 시 생성된 `ALTER TABLE DROP COLUMN` 문은 데이터 손실을 유발할 수 있습니다.

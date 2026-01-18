import os

import pytest
import yaml

from pg_schema_sync.__main__ import (
    fetch_enums,
    fetch_enums_values,
    fetch_functions,
    fetch_indexes,
    fetch_sequences,
    fetch_tables_metadata,
    fetch_views,
    get_connection,
)


def _normalize_config(db_config):
    normalized = db_config.copy()
    if "db" in normalized:
        normalized["dbname"] = normalized.pop("db")
    if "username" in normalized:
        normalized["user"] = normalized.pop("username")
    return normalized


def _load_smoke_config():
    config_path = os.getenv("PG_SCHEMA_SYNC_SMOKE_CONFIG")
    if not config_path:
        pytest.skip("PG_SCHEMA_SYNC_SMOKE_CONFIG is not set.")
    with open(config_path, "r", encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    if not config:
        pytest.skip(f"{config_path} is empty or invalid.")
    return config


@pytest.mark.smoke
def test_live_source_introspection():
    config = _load_smoke_config()
    source = config.get("source")
    if not source:
        pytest.skip("No source config found.")

    conn = get_connection(_normalize_config(source))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

        assert isinstance(fetch_enums(conn), dict)
        assert isinstance(fetch_enums_values(conn), dict)
        tables, composite_uniques, composite_primaries, composite_fks = fetch_tables_metadata(conn)
        assert isinstance(tables, dict)
        assert isinstance(composite_uniques, dict)
        assert isinstance(composite_primaries, dict)
        assert isinstance(composite_fks, dict)
        assert isinstance(fetch_views(conn), dict)
        assert isinstance(fetch_functions(conn), dict)
        indexes, pkey_indexes = fetch_indexes(conn)
        assert isinstance(indexes, dict)
        assert isinstance(pkey_indexes, dict)

        if os.getenv("PG_SCHEMA_SYNC_SMOKE_INCLUDE_SEQUENCES") == "1":
            assert isinstance(fetch_sequences(conn), dict)
    finally:
        conn.close()


@pytest.mark.smoke
def test_live_target_introspection():
    config = _load_smoke_config()
    target_name = os.getenv("PG_SCHEMA_SYNC_SMOKE_TARGET")
    if not target_name:
        pytest.skip("PG_SCHEMA_SYNC_SMOKE_TARGET is not set.")
    target = config.get("targets", {}).get(target_name)
    if not target:
        pytest.skip(f"No target config found for {target_name}.")

    conn = get_connection(_normalize_config(target))
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1

        assert isinstance(fetch_enums(conn), dict)
        assert isinstance(fetch_enums_values(conn), dict)
        tables, composite_uniques, composite_primaries, composite_fks = fetch_tables_metadata(conn)
        assert isinstance(tables, dict)
        assert isinstance(composite_uniques, dict)
        assert isinstance(composite_primaries, dict)
        assert isinstance(composite_fks, dict)
        assert isinstance(fetch_views(conn), dict)
        assert isinstance(fetch_functions(conn), dict)
        indexes, pkey_indexes = fetch_indexes(conn)
        assert isinstance(indexes, dict)
        assert isinstance(pkey_indexes, dict)

        if os.getenv("PG_SCHEMA_SYNC_SMOKE_INCLUDE_SEQUENCES") == "1":
            assert isinstance(fetch_sequences(conn), dict)
    finally:
        conn.close()

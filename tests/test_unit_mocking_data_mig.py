from unittest.mock import patch

import yaml

from pg_schema_sync import dataMig


class DummyConn:
    def close(self):
        return None


def test_run_data_migration_parallel_normalizes_config(tmp_path):
    config = {
        "source": {
            "host": "source-host",
            "port": 5432,
            "db": "postgres",
            "username": "source-user",
            "password": "source-pass",
        },
        "targets": {
            "gcp_test": {
                "host": "target-host",
                "port": 5432,
                "db": "postgres",
                "username": "target-user",
                "password": "target-pass",
            }
        },
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    calls = []

    def fake_get_connection(cfg):
        calls.append(cfg)
        return DummyConn()

    with patch.object(dataMig, "get_connection", side_effect=fake_get_connection), patch.object(
        dataMig, "drop_all_foreign_keys", return_value=[]
    ), patch.object(dataMig, "recreate_foreign_keys_not_valid", return_value=None), patch.object(
        dataMig, "generate_validate_script", return_value=None
    ):
        dataMig.run_data_migration_parallel(None, {}, config_path=str(config_path))

    assert calls, "Expected run_data_migration_parallel to create connections."
    for cfg in calls:
        assert "dbname" in cfg
        assert "user" in cfg
        assert "db" not in cfg
        assert "username" not in cfg

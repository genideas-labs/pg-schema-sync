from pg_schema_sync.__main__ import load_config


def test_load_config_success(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        "source:\n  host: example\n",
        encoding="utf-8",
    )

    config = load_config(str(config_path))
    assert config["source"]["host"] == "example"


def test_load_config_missing_file(tmp_path):
    missing_path = tmp_path / "missing.yaml"
    config = load_config(str(missing_path))
    assert config is None


def test_load_config_invalid_yaml(tmp_path):
    config_path = tmp_path / "bad.yaml"
    config_path.write_text("source: [", encoding="utf-8")

    config = load_config(str(config_path))
    assert config is None

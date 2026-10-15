"""Tests for the configuration admin CLI: python -m maas_collector.rawdata.cli.config"""

import json
import os
import shutil
from unittest.mock import patch

import pytest

from maas_collector.rawdata.cli.config import config_main
from maas_collector.rawdata.configstore import ConfigStore

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

INDEX = "maas-collector-config"

ENV = (
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
    "CONFIG_INDEX",
    "USER",
    "VAULT_ADDR",
    "VAULT_AUTH_METHOD",
    "VAULT_TOKEN_FILE",
    "VAULT_TOKEN",
)


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def write_json(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="UTF-8") as conf_fd:
        json.dump(content, conf_fd)


@pytest.fixture
def opensearch_class(fake_opensearch):
    """the OpenSearch class of the CLI, answering the fake"""
    with patch(
        "maas_collector.rawdata.cli.config.OpenSearch", return_value=fake_opensearch
    ) as opensearch:
        yield opensearch


@pytest.fixture
def env(monkeypatch, opensearch_class):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ES_URL", "http://opensearch.example:9200")
    monkeypatch.setenv("USER", "alice")
    return monkeypatch


@pytest.fixture
def store(fake_opensearch):
    return ConfigStore(fake_opensearch, INDEX)


@pytest.fixture
def deploy_dir(tmp_path):
    """<dir>/<service>/... like the collector directory of a deploy tree"""
    root = tmp_path / "collector"
    conf1 = read_json(os.path.join(CONF_DIR, "collector.d", "conf1.json"))
    write_json(root / "odata" / "shared" / "conf1.json", conf1)
    write_json(root / "sftp" / "shared" / "conf1.json", conf1)
    write_json(root / "sftp" / ".hidden" / "conf1.json", conf1)
    for name in ("models.json", "conf1.json"):
        write_json(
            root / "odata" / "resolve" / name,
            read_json(os.path.join(CONF_DIR, "model_resolve", name)),
        )
    os.makedirs(root / "odata" / "credentials")
    shutil.copy(
        os.path.join(CONF_DIR, "test-maas-collector-credentials.json"),
        root / "odata" / "credentials",
    )
    return root


def run(*argv) -> int:
    return config_main([str(arg) for arg in argv])


def test_init_creates_the_index_once(env, fake_opensearch, capsys):
    assert run("init") == 0
    assert run("init") == 0

    assert capsys.readouterr().out.splitlines() == [
        f"created index {INDEX}",
        f"index {INDEX} exists already",
    ]
    assert fake_opensearch.mappings[INDEX]["dynamic"] == "strict"


def test_import_merges_the_services_of_a_shared_path(env, store, deploy_dir, capsys):
    assert run("import", deploy_dir) == 0

    out = capsys.readouterr().out
    assert "skipped odata/credentials/test-maas-collector-credentials.json" in out
    assert "created shared/conf1.json v1 (odata, sftp)" in out
    assert [doc["path"] for doc in store.latest()] == [
        "resolve/conf1.json",
        "resolve/models.json",
        "shared/conf1.json",
    ]
    assert store.get("shared/conf1.json")["services"] == ["odata", "sftp"]
    assert store.get("shared/conf1.json")["author"] == "alice"


def test_import_dry_run_writes_nothing(env, store, deploy_dir, capsys):
    assert run("import", deploy_dir, "--dry-run") == 0

    assert "would create shared/conf1.json v1" in capsys.readouterr().out
    assert store.latest() == []


def test_import_again_leaves_equal_paths_alone(env, store, deploy_dir, capsys):
    run("import", deploy_dir)

    assert run("import", deploy_dir) == 0

    assert "unchanged shared/conf1.json v1" in capsys.readouterr().out
    assert store.get("shared/conf1.json")["version"] == 1


def test_import_reports_drift_and_leaves_the_index_untouched(
    env, store, deploy_dir, capsys
):
    run("import", deploy_dir)
    models = deploy_dir / "odata" / "resolve" / "models.json"
    write_json(models, {"models": []})

    assert run("import", deploy_dir) == 0

    assert "drift resolve/models.json: v1 in the index" in capsys.readouterr().out
    assert store.get("resolve/models.json")["version"] == 1
    assert store.get("resolve/models.json")["content"]["models"] != []


def test_import_refuses_a_path_whose_copies_differ(env, store, deploy_dir, capsys):
    write_json(deploy_dir / "sftp" / "shared" / "conf1.json", {"amqp": {}})

    assert run("import", deploy_dir) == 1

    assert "refused shared/conf1.json: the copies" in capsys.readouterr().out
    assert store.get("shared/conf1.json") is None
    assert store.get("resolve/models.json")["version"] == 1


def test_import_refuses_an_invalid_file(env, store, deploy_dir, capsys):
    write_json(
        deploy_dir / "odata" / "bad.json",
        {"collectors": [{"extractor": {"class": "NoSuchExtractor"}}]},
    )

    assert run("import", deploy_dir) == 1

    assert "refused bad.json: bad.json: AttributeError" in capsys.readouterr().out
    assert store.get("bad.json") is None


def test_export_after_import_reproduces_the_tree(env, deploy_dir, tmp_path):
    run("import", deploy_dir)
    out_dir = tmp_path / "out"

    assert run("export", out_dir) == 0

    def tree(root):
        return {
            os.path.relpath(path, root): read_json(path)
            for path in (
                os.path.join(directory, name)
                for directory, dirs, names in os.walk(root)
                if "/." not in directory
                for name in names
            )
            if "credentials" not in path
        }

    assert tree(out_dir) == tree(deploy_dir)


def test_export_of_one_service(env, deploy_dir, tmp_path):
    run("import", deploy_dir)

    run("export", tmp_path / "out", "--service", "sftp")

    assert os.listdir(tmp_path / "out") == ["sftp"]
    assert os.path.isfile(tmp_path / "out" / "sftp" / "shared" / "conf1.json")


def test_put_a_new_path_needs_its_services(env, store, tmp_path, capsys):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {}})

    assert run("put", "odata/new.json", conf, "-m", "new") == 1
    assert "give its services with --services" in capsys.readouterr().err

    assert (
        run("put", "odata/new.json", conf, "-m", "new", "--services", "odata,sftp") == 0
    )
    assert store.get("odata/new.json")["services"] == ["odata", "sftp"]


def test_put_keeps_the_services_and_checks_the_base_version(
    env, store, tmp_path, capsys
):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {}})
    run("put", "odata/a.json", conf, "-m", "v1", "--services", "odata,sftp")
    write_json(conf, {"amqp": {"rk": {"chunk_size": 5}}})

    assert run("put", "odata/a.json", conf, "-m", "v2", "--base-version", "1") == 0
    assert run("put", "odata/a.json", conf, "-m", "v3", "--base-version", "1") == 1

    assert "version 2 was published after version 1" in capsys.readouterr().err
    assert store.get("odata/a.json")["services"] == ["odata", "sftp"]
    assert store.get("odata/a.json")["version"] == 2


def test_put_refuses_a_credential_file(env, store, capsys):
    credentials = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")

    assert (
        run("put", "odata/c.json", credentials, "-m", "x", "--services", "odata") == 1
    )

    assert "not a collector configuration" in capsys.readouterr().err
    assert store.get("odata/c.json") is None


def test_get_history_diff_rollback_and_delete(env, store, tmp_path, capsys):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {"rk": {"chunk_size": 5}}})
    run("put", "odata/a.json", conf, "-m", "first", "--services", "odata")
    write_json(conf, {"amqp": {"rk": {"chunk_size": 50}}})
    env.setenv("USER", "bob")
    run("put", "odata/a.json", conf, "-m", "bigger chunks")
    capsys.readouterr()

    assert run("get", "odata/a.json", "--version", "1") == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {"amqp": {"rk": {"chunk_size": 5}}}
    assert "odata/a.json@1 services=odata author=alice" in output.err

    assert run("diff", "odata/a.json") == 0
    diff = capsys.readouterr().out
    assert "--- odata/a.json@1" in diff
    assert '-                "chunk_size": 5' in diff
    assert '+                "chunk_size": 50' in diff

    assert run("rollback", "odata/a.json", "--to", "1", "-m", "too big") == 0
    assert store.get("odata/a.json")["content"] == {"amqp": {"rk": {"chunk_size": 5}}}

    assert run("delete", "odata/a.json", "-m", "decommissioned") == 0
    capsys.readouterr()

    assert run("history", "odata/a.json") == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split("\t")[0] for line in lines] == ["v1", "v2", "v3", "v4"]
    assert lines[1].split("\t")[2] == "bob"
    assert lines[2].endswith("too big (based on v1)")
    assert lines[3].split("\t")[3] == "latest,deleted"

    assert run("list") == 0
    assert capsys.readouterr().out == ""


def test_list_filters_by_service_and_interface(env, store, capsys):
    store.publish(
        "odata/a.json",
        read_json(os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")),
        ["odata"],
        "alice",
        "v1",
    )
    store.publish("sftp/b.json", {"amqp": {}}, ["sftp"], "alice", "v1")

    run("list", "--service", "sftp")
    assert capsys.readouterr().out.startswith("sftp/b.json\tv1\tsftp\talice\t")

    run("list", "--interface", "basic_test")
    assert capsys.readouterr().out.startswith("odata/a.json\tv1\todata\talice\t")


def test_unknown_path_is_reported(env, capsys):
    assert run("get", "odata/none.json") == 1

    assert "odata/none.json does not exist" in capsys.readouterr().err


def test_no_connection_is_reported(env, capsys):
    env.delenv("ES_URL")

    assert run("list") == 1

    assert "set ES_URL or ES_VAULT_PATH" in capsys.readouterr().err


def test_connection_from_the_vault(env, fake_vault, opensearch_class, tmp_path):
    """the admin CLI reads its OpenSearch connection like the collectors"""
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")
    fake_vault.put(
        "secret",
        "maas/cds/opensearch-admin",
        {
            "url": "https://opensearch.example:9200",
            "username": "config-admin",
            "password": "s3cr3t/admin",
        },
    )
    env.delenv("ES_URL")
    env.setenv("VAULT_ADDR", fake_vault.url)
    env.setenv("VAULT_AUTH_METHOD", "token")
    env.setenv("VAULT_TOKEN_FILE", str(token_file))
    env.setenv("ES_VAULT_PATH", "maas/cds/opensearch-admin")

    assert run("init") == 0

    assert opensearch_class.call_args.kwargs["hosts"] == [
        "https://config-admin:s3cr3t%2Fadmin@opensearch.example:9200"
    ]

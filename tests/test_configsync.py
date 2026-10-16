"""Tests for the synchronisation of the configuration index into a folder"""

import json

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import MANIFEST_NAME, sync_configs


@pytest.fixture
def store(fake_opensearch):
    add_config_document(fake_opensearch, "a.json", 1, ["odata"], {"a": "é"})
    add_config_document(
        fake_opensearch, "shared/b.json", 1, ["odata", "sftp"], {"b": 1}
    )
    return ConfigStore(fake_opensearch)


def test_first_sync_writes_every_current_path(store, tmp_path):
    result = sync_configs(store, tmp_path)

    assert sorted(result.written) == [
        "odata/a.json",
        "odata/shared/b.json",
        "sftp/shared/b.json",
    ]
    assert result.unchanged == result.removed == result.skipped == []
    assert (tmp_path / "odata" / "a.json").read_text(
        encoding="UTF-8"
    ) == json.dumps({"a": "é"}, indent=4, ensure_ascii=False) + "\n"


def test_second_sync_leaves_unchanged_files_untouched(store, tmp_path):
    sync_configs(store, tmp_path)
    target = tmp_path / "sftp" / "shared" / "b.json"
    mtime = target.stat().st_mtime_ns

    result = sync_configs(store, tmp_path)

    assert result.written == []
    assert len(result.unchanged) == 3
    assert target.stat().st_mtime_ns == mtime


def test_changed_content_is_rewritten(store, fake_opensearch, tmp_path):
    sync_configs(store, tmp_path)
    add_config_document(fake_opensearch, "a.json", 1, ["odata"], {}, latest=False)
    add_config_document(fake_opensearch, "a.json", 2, ["odata"], {"a": 2})

    result = sync_configs(store, tmp_path)

    assert result.written == ["odata/a.json"]
    assert json.loads((tmp_path / "odata" / "a.json").read_text()) == {"a": 2}


def test_deleted_path_is_removed(store, fake_opensearch, tmp_path):
    sync_configs(store, tmp_path)
    add_config_document(fake_opensearch, "a.json", 1, ["odata"], {}, latest=False)
    add_config_document(fake_opensearch, "a.json", 2, ["odata"], {}, deleted=True)

    result = sync_configs(store, tmp_path)

    assert result.removed == ["odata/a.json"]
    assert not (tmp_path / "odata" / "a.json").exists()
    assert (tmp_path / "odata" / "shared" / "b.json").exists()


def test_path_leaving_a_service_is_removed_from_it(store, fake_opensearch, tmp_path):
    sync_configs(store, tmp_path)
    add_config_document(
        fake_opensearch, "shared/b.json", 1, ["odata", "sftp"], {}, latest=False
    )
    add_config_document(fake_opensearch, "shared/b.json", 2, ["odata"], {"b": 1})

    result = sync_configs(store, tmp_path)

    assert result.removed == ["sftp/shared/b.json"]


def test_files_sync_did_not_write_are_never_removed(store, tmp_path):
    (tmp_path / "odata").mkdir()
    (tmp_path / "odata" / "credentials.json").write_text("{}")

    sync_configs(store, tmp_path)
    sync_configs(store, tmp_path)

    assert (tmp_path / "odata" / "credentials.json").exists()
    assert (tmp_path / "odata" / MANIFEST_NAME).exists()


def test_sync_of_one_service_touches_only_its_folder(store, tmp_path):
    result = sync_configs(store, tmp_path, ["sftp"])

    assert result.written == ["sftp/shared/b.json"]
    assert not (tmp_path / "odata").exists()


def test_sync_never_writes_outside_its_directory(fake_opensearch, tmp_path):
    add_config_document(fake_opensearch, "../x.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "ok.json", 1, ["../evil"], {})
    out = tmp_path / "out"

    result = sync_configs(ConfigStore(fake_opensearch), out)

    assert sorted(result.skipped) == ["../evil", "odata/../x.json"]
    assert result.written == []
    assert not (tmp_path / "x.json").exists()
    assert not (tmp_path / "evil").exists()


def test_tampered_manifest_cannot_remove_outside_files(store, tmp_path):
    out = tmp_path / "out"
    sync_configs(store, out)
    victim = tmp_path / "victim.json"
    victim.write_text("{}")
    (out / "odata" / MANIFEST_NAME).write_text(json.dumps(["../../victim.json"]))

    result = sync_configs(store, out)

    assert victim.exists()
    assert result.skipped == ["odata/../../victim.json"]

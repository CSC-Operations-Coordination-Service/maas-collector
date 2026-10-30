import logging

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata.configprovider import ConfigSource
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.transfer.backups import (
    BackupUnavailable,
    backup_keys,
    build_sink,
    resolve_request_backup,
)

S3 = {
    "name": "archive-a",
    "type": "BackupS3",
    "directory": "archive",
    "buckets": ["b1", "b2"],
    "credentials": "backup-archive-a",
}
KEYS = {"s3_access_key": "ACCESS-1", "s3_secret_key": "SECRET-1"}


class FakeProvider:
    def __init__(self, entries, unreadable=()):
        self.entries = entries
        self.unreadable = frozenset(unreadable)
        self.loaded = []

    def load(self, names):
        self.loaded.append(list(names))
        return {name: dict(self.entries[name]) for name in names if name in self.entries}


def test_latest_definition_comes_from_the_loaded_sources():
    backup = resolve_request_backup(
        [ConfigSource("backups/archive-a.json", {"backups": [S3]})],
        None,
        "svc",
        "archive-a",
        None,
    )
    assert (backup.block["buckets"], backup.credentials, "credentials" in backup.block) == (
        ["b1", "b2"],
        "backup-archive-a",
        False,
    )
    assert "name" not in backup.block


def test_pinned_version_is_read_from_the_store(fake_opensearch):
    add_config_document(
        fake_opensearch, "backups/archive-a.json", 1, ["svc"],
        {"backups": [S3 | {"directory": "v1"}]}, latest=False,
    )
    add_config_document(fake_opensearch, "backups/archive-a.json", 2, ["svc"], {"backups": [S3]})
    backup = resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)
    assert backup.block["directory"] == "v1" and backup.version == 1


def test_pinned_version_is_found_by_name_whatever_the_path(fake_opensearch):
    add_config_document(
        fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"],
        {"backups": [S3 | {"directory": "v1"}]}, latest=False,
    )
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 2, ["svc"], {"backups": [S3]})
    assert (
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1).block["directory"]
        == "v1"
    )


def test_backup_defined_in_two_current_documents_is_refused(fake_opensearch):
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": [S3]})
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"], {"backups": [S3]})
    with pytest.raises(BackupUnavailable, match="several"):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)


def test_definition_of_another_service_is_not_a_candidate(fake_opensearch):
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["other"], {"backups": [S3]})
    add_config_document(
        fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"],
        {"backups": [S3 | {"directory": "mine"}]},
    )
    assert (
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1).block["directory"]
        == "mine"
    )


@pytest.mark.parametrize(
    "latest_services, latest_deleted, version, word",
    [(["other"], False, 1, "tagged svc"), (["svc"], True, 1, "tagged svc"), (["svc"], False, 9, "version 9")],
)
def test_pinned_version_that_cannot_serve_is_refused(
    fake_opensearch, latest_services, latest_deleted, version, word
):
    add_config_document(
        fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": [S3]}, latest=False
    )
    add_config_document(
        fake_opensearch, "backups/archive-a.json", 2, latest_services,
        {"backups": [S3]}, deleted=latest_deleted,
    )
    with pytest.raises(BackupUnavailable, match=word):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", version)


def test_pinned_version_that_does_not_define_the_backup_is_refused(fake_opensearch):
    add_config_document(
        fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": []}, latest=False
    )
    add_config_document(fake_opensearch, "backups/archive-a.json", 2, ["svc"], {"backups": [S3]})
    with pytest.raises(BackupUnavailable, match="version 1"):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)


def test_backup_without_any_document_is_refused_when_pinned(fake_opensearch):
    with pytest.raises(BackupUnavailable, match="archive-a"):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)


def test_pinned_version_without_the_store_is_refused():
    with pytest.raises(BackupUnavailable, match="opensearch"):
        resolve_request_backup([], None, "svc", "archive-a", 1)


def test_unknown_backup_is_refused():
    with pytest.raises(BackupUnavailable, match="archive-b"):
        resolve_request_backup([], None, "svc", "archive-b", None)


def test_keys_are_the_two_s3_keys():
    assert backup_keys(FakeProvider({"backup-archive-a": KEYS}), "backup-archive-a") == KEYS


def test_without_an_entry_there_are_no_keys():
    assert backup_keys(None, None) == {}


def test_entry_with_another_field_is_refused_naming_it():
    with pytest.raises(BackupUnavailable, match="token") as error:
        backup_keys(FakeProvider({"backup-archive-a": KEYS | {"token": "x"}}), "backup-archive-a")
    assert "SECRET-1" not in str(error.value)


def test_entry_missing_a_key_is_refused_naming_it():
    with pytest.raises(BackupUnavailable, match="s3_secret_key"):
        backup_keys(
            FakeProvider({"backup-archive-a": {"s3_access_key": "ACCESS-1"}}),
            "backup-archive-a",
        )


@pytest.mark.parametrize(
    "make_provider, word",
    [
        (lambda: FakeProvider({}, unreadable={"backup-archive-a"}), "vault policy"),
        (lambda: FakeProvider({}), "backup-archive-a"),
        (lambda: None, "no credential source"),
    ],
)
def test_unreadable_or_absent_entry_is_refused(make_provider, word):
    with pytest.raises(BackupUnavailable, match=word):
        backup_keys(make_provider(), "backup-archive-a")


def test_sink_gets_the_keys_and_the_narrowed_buckets_on_a_copy():
    blocks = []
    backup = resolve_request_backup(
        [ConfigSource("b.json", {"backups": [S3]})], None, "svc", "archive-a", None
    )
    build_sink(backup, KEYS, ["b2"], factory=blocks.append)
    assert blocks[0]["buckets"] == ["b2"] and blocks[0]["s3_secret_key"] == "SECRET-1"
    assert blocks[0]["backup_name"] == "archive-a"
    assert "s3_secret_key" not in backup.block and backup.block["buckets"] == ["b1", "b2"]


def test_keys_are_never_logged(caplog):
    caplog.set_level(logging.DEBUG)
    backup = resolve_request_backup(
        [ConfigSource("b.json", {"backups": [S3]})], None, "svc", "archive-a", None
    )
    build_sink(backup, KEYS, factory=lambda block: None)
    assert "SECRET-1" not in caplog.text and "SECRET-1" not in repr(backup)

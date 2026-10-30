"""Tests for the S3 keys of a backup, read from a credential entry"""

import json
from unittest.mock import patch

import pytest

from backup_samples import KEYS, LOCAL, S3, backups_file, entry
from maas_collector.backup.named import SECRET_FIELDS
from maas_collector.rawdata.collector import credentialmixin
from maas_collector.rawdata.collector.credentialprovider import VaultArgs
from maas_collector.rawdata.collector.filecollector import CollectorArgs, FileCollector

ENTRY_NAME = "backup-archive-b"

NEW_KEYS = {"s3_access_key": "new-access", "s3_secret_key": "new-secret"}


class FakeClock:
    """replace the time module of credentialmixin to move time forward"""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        """seconds"""
        return self.now


@pytest.fixture
def clock(monkeypatch):
    fake_clock = FakeClock()
    monkeypatch.setattr(credentialmixin, "time", fake_clock)
    return fake_clock


@pytest.fixture(autouse=True)
def no_database_nor_bus():
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection",
        return_value=True,
    ), patch("maas_collector.rawdata.messenger.Messenger.setup", return_value=True):
        yield


@pytest.fixture
def vault_args(fake_vault, tmp_path):
    secret_id_file = tmp_path / "secret_id"
    secret_id_file.write_text(fake_vault.secret_id, encoding="UTF-8")

    return VaultArgs(
        url=fake_vault.url,
        role_id=fake_vault.role_id,
        secret_id_file=str(secret_id_file),
    )


def write_credentials(tmp_path, entries: dict) -> None:
    """write the credential file: entry name -> fields"""
    content = {"interfaces": [{"name": n, **fields} for n, fields in entries.items()]}
    (tmp_path / "credentials.json").write_text(json.dumps(content), encoding="UTF-8")


def rewrite_entry(tmp_path, fields: dict, name: str = ENTRY_NAME) -> None:
    """rewrite an entry of tmp_path / credentials.json"""
    path = tmp_path / "credentials.json"
    content = json.loads(path.read_text(encoding="UTF-8"))
    content["interfaces"] = [
        {"name": name, **fields} if item["name"] == name else item
        for item in content["interfaces"]
    ]
    path.write_text(json.dumps(content), encoding="UTF-8")


def start(tmp_path, entry_fields=KEYS, files=None, extra=None, **args) -> FileCollector:
    """a started collector whose entry uses the S3 backup archive-b, with its keys in
    the entry backup-archive-b (absent when entry_fields is None)"""
    conf = tmp_path / "conf"
    conf.mkdir()
    files = files or {
        "backups.json": backups_file(S3),
        "entries.json": {"collectors": [entry(backup="archive-b", **(extra or {}))]},
    }
    for name, content in files.items():
        (conf / name).write_text(json.dumps(content), encoding="UTF-8")

    if "credential_backend" not in args:
        entries = {} if entry_fields is None else {ENTRY_NAME: entry_fields}
        write_credentials(tmp_path, entries)
        args["credential_file"] = str(tmp_path / "credentials.json")

    collector = FileCollector(CollectorArgs(rawdata_config_dir=str(conf), **args))
    collector.load_config()
    return collector


def test_the_keys_go_to_backup_credentials_not_to_the_block(tmp_path):
    config = start(tmp_path).configs[0]

    assert config.backup_credentials == KEYS
    assert "s3_secret_key" not in config.backup


def test_the_credential_names_include_the_backup_entry(tmp_path):
    assert ENTRY_NAME in start(tmp_path).credential_names()


def test_an_entry_with_another_field_fails_the_load_naming_it_but_never_its_value(
    tmp_path,
):
    with pytest.raises(ValueError, match="token") as error:
        start(tmp_path, entry_fields=KEYS | {"token": "test-token-value"})

    assert "test-token-value" not in str(error.value)
    assert "test-secret-key" not in str(error.value)


def test_the_block_is_built_on_a_copy_with_the_keys(tmp_path):
    collector = start(tmp_path)
    config = collector.configs[0]

    block = collector.build_backup_block(config)

    assert block["s3_secret_key"] == "test-secret-key"
    assert block["backup_name"] == "archive-b"
    assert "s3_secret_key" not in config.backup
    assert "backup_name" not in config.backup


def test_a_block_without_entry_is_a_copy_with_the_name(tmp_path):
    collector = start(
        tmp_path,
        files={
            "b.json": backups_file(LOCAL),
            "e.json": {"collectors": [entry(backup="archive-a")]},
        },
    )

    assert collector.build_backup_block(collector.configs[0]) == {
        "type": "BackupLocal",
        "directory": "/data/archive-a",
        "backup_name": "archive-a",
    }


def test_an_absent_entry_fails_the_backup_not_the_load(tmp_path, caplog):
    collector = start(tmp_path, entry_fields=None)

    with pytest.raises(ValueError, match=ENTRY_NAME):
        collector.build_backup_block(collector.configs[0])

    assert ENTRY_NAME in caplog.text


@pytest.mark.parametrize("missing", SECRET_FIELDS)
def test_entry_with_one_key_only_fails_the_backup(tmp_path, missing, caplog):
    # Review Focus 1
    collector = start(
        tmp_path, entry_fields={k: v for k, v in KEYS.items() if k != missing}
    )
    path = tmp_path / "f.log"
    path.write_text("x")

    with patch("boto3.client") as client:
        collector.backup_and_report(collector.configs[0], str(path))

    # no client built, so no fallback on the ambient credentials
    client.assert_not_called()
    assert "Cannot backup" in caplog.text


def test_the_s3_client_gets_the_keys_of_the_entry_at_each_file(tmp_path):
    collector = start(tmp_path)
    path = tmp_path / "f.log"
    path.write_text("x")

    with patch("boto3.client") as client:
        collector.backup_and_report(collector.configs[0], str(path))

    assert client.call_args.kwargs["aws_access_key_id"] == "test-access-key"
    assert client.call_args.kwargs["aws_secret_access_key"] == "test-secret-key"


def test_rotation_reaches_the_keys(tmp_path, clock):
    collector = start(tmp_path)
    rewrite_entry(tmp_path, NEW_KEYS)
    clock.now += 300

    collector.refresh_credentials()

    assert (
        collector.build_backup_block(collector.configs[0])["s3_secret_key"]
        == "new-secret"
    )


def test_rotation_reaches_an_entry_without_any_interface_credentials(
    tmp_path, clock, caplog
):
    collector = start(tmp_path)
    caplog.clear()
    rewrite_entry(tmp_path, NEW_KEYS)
    clock.now += 300

    collector.refresh_credentials()

    assert collector.configs[0].backup_credentials == NEW_KEYS
    assert "No credential configuration found" not in caplog.text


def test_a_change_of_another_entry_leaves_the_keys_alone(tmp_path, clock, caplog):
    collector = start(tmp_path, extra={"interface_credentials": "shared"})
    write_credentials(tmp_path, {ENTRY_NAME: KEYS, "shared": {"exchange_name": "a"}})
    collector.load_credentials()
    rewrite_entry(tmp_path, {"exchange_name": "b"}, name="shared")
    clock.now += 300
    caplog.clear()

    collector.refresh_credentials()

    assert collector.configs[0].exchange_name == "b"
    assert collector.configs[0].backup_credentials == KEYS
    assert f"Credentials changed for {ENTRY_NAME}" not in caplog.text


def test_rotation_logs_no_key_value(tmp_path, clock, caplog):
    collector = start(tmp_path)
    rewrite_entry(tmp_path, NEW_KEYS)
    clock.now += 300

    with caplog.at_level("DEBUG"):
        collector.refresh_credentials()

    assert "new-secret" not in caplog.text
    assert "test-secret-key" not in caplog.text
    assert ENTRY_NAME in caplog.text


def test_an_inline_s3_block_naming_credentials_gets_its_keys(tmp_path):
    inline = {
        "type": "BackupS3",
        "directory": "d",
        "buckets": ["b"],
        "credentials": "backup-inline",
    }
    collector = start(
        tmp_path,
        files={"e.json": {"collectors": [entry(backup=inline)]}},
    )
    write_credentials(tmp_path, {"backup-inline": KEYS})
    collector.load_credentials()

    config = collector.configs[0]

    assert config.backup_name == ""
    assert config.backup_credentials_entry == "backup-inline"
    assert config.backup_credentials == KEYS


def test_vault_entries_are_read_and_rotated(clock, fake_vault, vault_args, tmp_path):
    fake_vault.put("secret", f"maas-collector/{ENTRY_NAME}", KEYS)

    collector = start(
        tmp_path, entry_fields=None, credential_backend="vault", vault=vault_args
    )

    assert collector.configs[0].backup_credentials == KEYS

    fake_vault.put("secret", f"maas-collector/{ENTRY_NAME}", NEW_KEYS)
    clock.now += 300
    collector.refresh_credentials()

    assert (
        collector.build_backup_block(collector.configs[0])["s3_secret_key"]
        == "new-secret"
    )


def test_a_vault_policy_denying_the_entry_stops_the_load(
    fake_vault, vault_args, tmp_path
):
    fake_vault.put("secret", f"maas-collector/{ENTRY_NAME}", KEYS)
    fake_vault.denied.add(f"secret/maas-collector/{ENTRY_NAME}")

    with pytest.raises(PermissionError, match=ENTRY_NAME):
        start(tmp_path, entry_fields=None, credential_backend="vault", vault=vault_args)


def test_the_configuration_repr_masks_the_keys(tmp_path):
    assert "test-secret-key" not in repr(start(tmp_path).configs[0])


def test_a_failing_backup_logs_no_key(tmp_path, caplog):
    collector = start(tmp_path)
    path = tmp_path / "f.log"
    path.write_text("x")

    with patch("boto3.client", side_effect=RuntimeError("boom")):
        collector.backup_and_report(collector.configs[0], str(path))

    assert "Cannot backup" in caplog.text
    assert "test-secret-key" not in caplog.text
    assert "test-access-key" not in caplog.text

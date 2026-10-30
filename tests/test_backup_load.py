"""Tests for the resolution of the named backups when a collector loads"""

import json
import shutil
from unittest.mock import Mock, patch

import pytest
from opensearchpy.connection.connections import connections as es_connections

from backup_samples import LOCAL, S3, backups_file, entry, models_file
from maas_collector.backup.named import SECRET_FIELDS
from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
    FileCollectorConfiguration,
)
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import sync_configs


def write(tmp_path, files: dict):
    """write the JSON files under tmp_path, return tmp_path"""
    for name, content in files.items():
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(content), encoding="UTF-8")

    return tmp_path


def load(path) -> FileCollector:
    """build a collector on a configuration directory and load it"""
    collector = FileCollector(CollectorArgs(rawdata_config_dir=str(path)))
    collector.load_config()
    return collector


FILES = {
    "backups/archive-a.json": backups_file(LOCAL),
    "entries.json": {"collectors": [entry(backup="archive-a")]},
}


@pytest.fixture(name="default_connection")
def fixture_default_connection():
    client = Mock()
    es_connections.add_connection("default", client)
    yield client
    es_connections._conn = {}
    es_connections._kwargs = {}


def test_a_name_is_resolved_for_the_entry(tmp_path):
    config = load(write(tmp_path, FILES)).configs[0]

    assert config.backup == {"type": "BackupLocal", "directory": "/data/archive-a"}
    assert (config.backup_name, config.backup_credentials_entry) == ("archive-a", "")


def test_the_report_model_of_the_definition_is_resolved(tmp_path):
    files = FILES | {
        "backups/archive-a.json": backups_file(
            LOCAL | {"report_model": "ReportBackup"}
        ),
        "models.json": models_file("ReportBackup"),
    }

    config = load(write(tmp_path, files)).configs[0]

    assert config.backup_report_model.__name__ == "ReportBackup"


def test_an_s3_name_gives_the_credentials_entry_and_no_secret(tmp_path):
    config = load(
        write(
            tmp_path,
            {
                "b.json": backups_file(S3),
                "e.json": {"collectors": [entry(backup="archive-b")]},
            },
        )
    ).configs[0]

    assert config.backup_credentials_entry == "backup-archive-b"
    assert "credentials" not in config.backup
    assert config.backup["buckets"] == ["bucket-a", "bucket-b"]


@pytest.mark.parametrize("enabled", [True, False])
def test_an_unknown_name_is_a_load_error_for_enabled_and_disabled_entries(
    tmp_path, enabled
):
    with pytest.raises(ValueError, match="nope"):
        load(
            write(
                tmp_path,
                {"e.json": {"collectors": [entry(backup="nope", enabled=enabled)]}},
            )
        )


def test_a_name_defined_twice_fails_the_load(tmp_path):
    with pytest.raises(ValueError, match="archive-a"):
        load(write(tmp_path, FILES | {"backups/copy.json": backups_file(LOCAL)}))


def test_an_inline_block_is_kept_and_has_no_name(tmp_path):
    block = {"type": "BackupLocal", "directory": "/d"}

    config = load(
        write(tmp_path, {"e.json": {"collectors": [entry(backup=block)]}})
    ).configs[0]

    assert config.backup_name == ""
    assert config.backup == block


@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_an_inline_block_with_a_secret_fails_at_load_without_validation(
    tmp_path, field
):
    inline = {
        "type": "BackupS3",
        "directory": "d",
        "buckets": ["b"],
        "credentials": "backup-x",
        field: "test-secret-key",
    }

    with pytest.raises(ValueError, match=field) as error:
        load(write(tmp_path, {"e.json": {"collectors": [entry(backup=inline)]}}))

    assert "test-secret-key" not in str(error.value)


def test_a_definition_with_a_secret_fails_at_load(tmp_path):
    with pytest.raises(ValueError, match="s3_access_key"):
        load(
            write(
                tmp_path,
                FILES
                | {"backups/archive-a.json": backups_file(S3 | {"s3_access_key": "x"})},
            )
        )


@pytest.mark.parametrize(
    "field", ["backup_name", "backup_credentials", "backup_credentials_entry"]
)
def test_the_resolved_fields_are_not_settable_from_json(tmp_path, field):
    assert not FileCollectorConfiguration.__dataclass_fields__[field].init

    with pytest.raises(TypeError):
        load(write(tmp_path, {"e.json": {"collectors": [entry() | {field: "x"}]}}))


def test_a_configuration_synced_from_the_index_resolves_like_the_files(
    tmp_path, fake_opensearch
):
    store = ConfigStore(fake_opensearch)
    store.publish("backups/archive-a.json", backups_file(LOCAL), ["svc"], "a", "c")
    store.publish(
        "entries.json", {"collectors": [entry(backup="archive-a")]}, ["svc"], "a", "c"
    )
    sync_configs(store, str(tmp_path / "copy"), ["svc"])

    assert load(tmp_path / "copy" / "svc").configs[0].backup_name == "archive-a"


def run_ingestion(collector, tmp_path):
    """ingest a file as test_extract_from_file_stores_the_reports does"""
    ingested = tmp_path / "some-report.log"
    ingested.write_text("a line\n")

    with patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk", return_value=[]
    ):
        with patch(
            "maas_collector.rawdata.backup_report.bulk", return_value=(1, [])
        ) as bulk_mock:
            collector.extract_from_file(
                str(ingested),
                collector.configs[0],
                report_name="some-report.log",
                report_folder=str(tmp_path),
            )

    return bulk_mock


def test_a_file_backed_up_through_a_name_writes_the_name_in_its_report(
    default_connection, tmp_path
):
    shutil.copy("tests/conf/backup_report/models.json", tmp_path / "models.json")
    (tmp_path / "backup").mkdir()
    definition = LOCAL | {
        "directory": str(tmp_path / "backup"),
        "report_model": "Backup",
    }
    collector = load(
        write(
            tmp_path,
            {
                "backups/archive-a.json": backups_file(definition),
                "entries.json": {"collectors": [entry(backup="archive-a")]},
            },
        )
    )

    bulk_mock = run_ingestion(collector, tmp_path)

    source = bulk_mock.call_args.args[1][0]["_source"]
    assert (source["backup_name"], source["origin"], source["status"]) == (
        "archive-a",
        "collector",
        "OK",
    )


def test_a_name_without_report_model_backs_up_and_reports_nothing(
    default_connection, tmp_path
):
    (tmp_path / "backup").mkdir()
    definition = LOCAL | {"directory": str(tmp_path / "backup")}
    collector = load(
        write(
            tmp_path,
            {
                "backups/archive-a.json": backups_file(definition),
                "entries.json": {"collectors": [entry(backup="archive-a")]},
            },
        )
    )

    bulk_mock = run_ingestion(collector, tmp_path)

    assert bulk_mock.call_count == 0
    assert (tmp_path / "backup" / "ITF_A" / "some-report.log").is_file()

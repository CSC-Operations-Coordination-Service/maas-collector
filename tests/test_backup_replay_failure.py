"""Tests for the replay that fails when its files could not be backed up"""

import json
import shutil
from types import SimpleNamespace
from unittest.mock import Mock, patch

import botocore.exceptions
import pytest
from opensearchpy.connection.connections import connections as es_connections

from backup_samples import KEYS, LOCAL, S3, backups_file, entry
from maas_collector.rawdata.cli.jirax import jirax_collector_main
from maas_collector.rawdata.cli.odata import odata_collector_main
from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.replay import ReplayArgs


@pytest.fixture(name="default_connection")
def fixture_default_connection():
    client = Mock()
    es_connections.add_connection("default", client)
    yield client
    es_connections._conn = {}
    es_connections._kwargs = {}


def replay_collector(
    tmp_path, backup: dict, report_model: bool = False, replay: bool = True
) -> FileCollector:
    """a loaded collector whose entry uses the backup archive-a"""
    conf = tmp_path / "conf"
    conf.mkdir()

    definition = backup | {"name": "archive-a"}
    if report_model:
        definition["report_model"] = "Backup"
        shutil.copy("tests/conf/backup_report/models.json", conf / "models.json")

    files = {
        "backups.json": backups_file(definition),
        "entries.json": {"collectors": [entry(backup="archive-a")]},
    }
    for name, content in files.items():
        (conf / name).write_text(json.dumps(content), encoding="UTF-8")

    args = {"rawdata_config_dir": str(conf)}
    if backup["type"] == "BackupS3":
        credentials = tmp_path / "credentials.json"
        credentials.write_text(
            json.dumps({"interfaces": [{"name": backup["credentials"], **KEYS}]}),
            encoding="UTF-8",
        )
        args["credential_file"] = str(credentials)

    collector = FileCollector(CollectorArgs(**args))
    collector.load_config()

    if replay:
        collector.args.replay = ReplayArgs(
            ["ITF_A"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z"
        )

    return collector


def ingest(collector, tmp_path, bulk_error=None, sidecar=False) -> None:
    """ingest a file, with the database and the report index patched"""
    path = tmp_path / "some-report.log"
    path.write_text("a line\n")

    if sidecar:
        (tmp_path / "some-report.log-maas-meta.json").write_text("{}")

    with patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk", return_value=[]
    ):
        with patch(
            "maas_collector.rawdata.backup_report.bulk",
            return_value=(1, []),
            side_effect=bulk_error,
        ):
            collector.extract_from_file(str(path), collector.configs[0])


def working_local(tmp_path) -> dict:
    (tmp_path / "backup").mkdir()
    return LOCAL | {"directory": str(tmp_path / "backup")}


def upload_failing_for(bucket):
    """boto3.client() patch whose upload fails for one bucket"""

    def upload_file(_path, name, _key):
        if name == bucket:
            raise botocore.exceptions.ClientError(
                {"Error": {"Code": "500"}}, "PutObject"
            )

    client = Mock()
    client.upload_file.side_effect = upload_file
    return patch("boto3.client", return_value=client)


def test_a_failed_transfer_is_counted_in_replay(default_connection, tmp_path):
    # BackupLocal on a missing directory: a KO report
    collector = replay_collector(tmp_path, LOCAL | {"directory": str(tmp_path / "x")})

    ingest(collector, tmp_path)

    assert collector.backup_failures == {"archive-a": 1}


def test_one_ko_bucket_out_of_two_counts_the_file_once(default_connection, tmp_path):
    collector = replay_collector(tmp_path, S3)

    with upload_failing_for("bucket-b"):
        ingest(collector, tmp_path)

    assert collector.backup_failures == {"archive-a": 1}


def test_a_failure_without_any_report_is_counted(default_connection, tmp_path):
    collector = replay_collector(tmp_path, working_local(tmp_path))

    with patch(
        "maas_collector.rawdata.collector.filecollector.instanciate_collector_backup",
        side_effect=ValueError("bad backup configuration"),
    ):
        ingest(collector, tmp_path)

    assert collector.backup_failures == {"archive-a": 1}


def test_a_sidecar_that_fails_counts_its_file_once(default_connection, tmp_path):
    collector = replay_collector(tmp_path, LOCAL | {"directory": str(tmp_path / "x")})

    ingest(collector, tmp_path, sidecar=True)

    assert collector.backup_failures == {"archive-a": 1}


def test_a_sidecar_that_fails_alone_counts_its_file(default_connection, tmp_path):
    collector = replay_collector(tmp_path, working_local(tmp_path))

    with patch("shutil.copy2", side_effect=[None, PermissionError("denied")]):
        ingest(collector, tmp_path, sidecar=True)

    assert collector.backup_failures == {"archive-a": 1}


def test_a_successful_backup_is_not_counted(default_connection, tmp_path):
    collector = replay_collector(tmp_path, working_local(tmp_path))

    ingest(collector, tmp_path)

    assert collector.backup_failures == {}


def test_live_mode_counts_nothing(default_connection, tmp_path):
    collector = replay_collector(
        tmp_path, LOCAL | {"directory": str(tmp_path / "x")}, replay=False
    )

    ingest(collector, tmp_path)

    assert collector.backup_failures == {}


def test_failing_to_store_the_reports_is_not_a_backup_failure(
    default_connection, tmp_path
):
    collector = replay_collector(tmp_path, working_local(tmp_path), report_model=True)

    ingest(collector, tmp_path, bulk_error=ConnectionError("database is down"))

    assert collector.backup_failures == {}


def test_the_failure_line(tmp_path):
    collector = replay_collector(tmp_path, LOCAL)

    collector.backup_failures = {"archive-b": 1, "archive-a": 2}

    assert (
        collector.backup_failure_line()
        == "backup failed: 3 files to archive-a, archive-b"
    )


def test_an_inline_backup_is_counted_under_its_own_label(tmp_path):
    collector = replay_collector(tmp_path, LOCAL)

    collector.count_backup_failure(SimpleNamespace(backup_name=""))

    assert collector.backup_failures == {"(inline)": 1}


def test_the_exit_codes_are_distinct():
    assert len({1, 75, BACKUP_FAILED_EXIT_CODE}) == 3
    assert BACKUP_FAILED_EXIT_CODE == 74


REPLAY_ARGV = [
    "--replay-interface-name",
    "A",
    "--replay-start-date",
    "2026-01-01T00:00:00Z",
    "--replay-end-date",
    "2026-01-02T00:00:00Z",
]

CLIS = [
    (odata_collector_main, "odata", "ODataCollector"),
    (jirax_collector_main, "jirax", "JIRAExtendedCollector"),
]


@pytest.mark.parametrize("cli, module, collector_class", CLIS)
def test_the_cli_exits_with_the_backup_code_and_prints_the_line_last(
    cli, module, collector_class, capsys
):
    with patch(f"maas_collector.rawdata.cli.{module}.{collector_class}") as klass:
        collector = klass.return_value
        collector.replay_failures = []
        collector.backup_failures = {"archive-a": 3}
        collector.backup_failure_line.return_value = (
            "backup failed: 3 files to archive-a"
        )

        with pytest.raises(SystemExit) as exit_info:
            cli(REPLAY_ARGV)

    assert exit_info.value.code == BACKUP_FAILED_EXIT_CODE
    assert (
        capsys.readouterr().out.splitlines()[-1]
        == "backup failed: 3 files to archive-a"
    )


@pytest.mark.parametrize("cli, module, collector_class", CLIS)
def test_replay_failure_wins_but_the_backup_line_is_printed(
    cli, module, collector_class, capsys
):
    # Review Focus 3
    with patch(f"maas_collector.rawdata.cli.{module}.{collector_class}") as klass:
        collector = klass.return_value
        collector.replay_failures = ["A"]
        collector.backup_failures = {"archive-a": 3}
        collector.backup_failure_line.return_value = (
            "backup failed: 3 files to archive-a"
        )

        with pytest.raises(SystemExit) as exit_info:
            cli(REPLAY_ARGV)

    assert exit_info.value.code == 1
    assert (
        capsys.readouterr().out.splitlines()[-1]
        == "backup failed: 3 files to archive-a"
    )


@pytest.mark.parametrize("cli, module, collector_class", CLIS)
def test_without_failures_the_cli_returns(cli, module, collector_class, capsys):
    with patch(f"maas_collector.rawdata.cli.{module}.{collector_class}") as klass:
        collector = klass.return_value
        collector.replay_failures = []
        collector.backup_failures = {}

        cli(REPLAY_ARGV)

    assert "backup failed" not in capsys.readouterr().out

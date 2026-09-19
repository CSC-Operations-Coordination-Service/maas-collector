"""Tests wiring backup reporting into the file collector.

Covers the two halves the unit tests cannot reach: resolving the report model at
configuration load time, and actually storing the reports from the finally block
of extract_from_file.
"""

import os
from unittest.mock import Mock, patch

import pytest

from opensearchpy.connection.connections import connections as es_connections

from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

WITH_REPORT = os.path.join(CONF_DIR, "backup_report")

WITHOUT_REPORT = os.path.join(CONF_DIR, "backup_report_off")

UNKNOWN_REPORT = os.path.join(CONF_DIR, "backup_report_unknown")


@pytest.fixture(name="default_connection")
def fixture_default_connection():
    """register a mock under the alias extract_from_file actually uses.

    The mock_client fixture of conftest registers "mock", but the collector calls
    es_connections.get_connection() with no argument, which is "default".
    """
    client = Mock()
    es_connections.add_connection("default", client)
    yield client
    es_connections._conn = {}
    es_connections._kwargs = {}


def load(conf_dir):
    """build a collector from a configuration directory"""
    collector = FileCollector(CollectorArgs(rawdata_config_dir=conf_dir))
    collector.load_config()
    return collector


def test_load_config_resolves_report_model():
    """the model name in the backup block becomes a class on the config"""
    collector = load(WITH_REPORT)

    config = collector.configs[0]

    assert config.backup_report_model is not None
    assert config.backup_report_model.__name__ == "Backup"
    assert config.backup_report_model.Index.name == "raw-data-backup"


def test_load_config_without_report_model():
    """no report_model means reporting is off, not an error"""
    collector = load(WITHOUT_REPORT)

    assert collector.configs[0].backup_report_model is None


def test_load_config_unknown_report_model_fails_at_startup():
    """a typo costs five seconds, not a stack trace in a running pod"""
    with pytest.raises(KeyError):
        load(UNKNOWN_REPORT)


def test_backup_report_model_is_not_settable_from_json():
    """init=False keeps the resolved class out of the configuration file"""
    from maas_collector.rawdata.collector.filecollector import (
        FileCollectorConfiguration,
    )

    assert not FileCollectorConfiguration.__dataclass_fields__[
        "backup_report_model"
    ].init


def test_extract_from_file_stores_the_reports(default_connection, tmp_path):
    """the whole path: ingest a file, back it up, store one report"""
    collector = load(WITH_REPORT)

    config = collector.configs[0]
    config.backup = dict(config.backup, directory=str(tmp_path / "backup"))

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
                config,
                report_name="some-report.log",
                report_folder=str(tmp_path),
            )

    assert bulk_mock.call_count == 1

    actions = bulk_mock.call_args.args[1]

    assert len(actions) == 1

    action = actions[0]

    assert action["_op_type"] == "create"
    # the partitionned index, never the bare alias
    assert action["_index"].startswith("raw-data-backup-")
    assert action["_index"] != "raw-data-backup"

    source = action["_source"]

    assert source["status"] == "OK"
    assert source["filename"] == "some-report.log"
    assert source["backup_type"] == "BackupLocal"
    assert source["interface_name"] == "BACKED_UP"
    assert source["reportName"] == "some-report.log"

    # the file really reached the backup directory
    assert os.path.exists(source["destination_path"])

    # and extract_from_file left no state behind
    assert collector._action_iterator is None


def test_extract_from_file_without_report_model_writes_nothing(
    default_connection, tmp_path
):
    """reporting off: the backup still happens, no document is produced"""
    collector = load(WITHOUT_REPORT)

    config = collector.configs[0]
    config.backup = dict(config.backup, directory=str(tmp_path / "backup"))

    ingested = tmp_path / "some-report.log"
    ingested.write_text("a line\n")

    with patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk", return_value=[]
    ):
        with patch("maas_collector.rawdata.backup_report.bulk") as bulk_mock:
            collector.extract_from_file(str(ingested), config)

    bulk_mock.assert_not_called()

    # the backup itself still ran
    assert os.path.isdir(tmp_path / "backup")


def test_backup_failure_does_not_break_ingestion(default_connection, tmp_path, caplog):
    """backup_and_report runs in a finally block, so it must never raise"""
    collector = load(WITH_REPORT)

    config = collector.configs[0]
    config.backup = dict(config.backup, directory=str(tmp_path / "backup"))

    ingested = tmp_path / "some-report.log"
    ingested.write_text("a line\n")

    with patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk", return_value=[]
    ):
        with patch(
            "maas_collector.rawdata.collector.filecollector"
            ".instanciate_collector_backup",
            side_effect=ValueError("bad backup configuration"),
        ):
            # no exception escapes
            collector.extract_from_file(str(ingested), config)

    assert "Cannot backup" in caplog.text
    # the state of the collector is still clean for the next file
    assert collector._action_iterator is None


def test_report_write_failure_does_not_break_ingestion(default_connection, tmp_path):
    """failing to report is not failing to ingest"""
    collector = load(WITH_REPORT)

    config = collector.configs[0]
    config.backup = dict(config.backup, directory=str(tmp_path / "backup"))

    ingested = tmp_path / "some-report.log"
    ingested.write_text("a line\n")

    with patch(
        "maas_collector.rawdata.collector.filecollector.parallel_bulk", return_value=[]
    ):
        with patch(
            "maas_collector.rawdata.backup_report.bulk",
            side_effect=ConnectionError("database is down"),
        ):
            collector.extract_from_file(str(ingested), config)

    assert collector._action_iterator is None

"""Tests for the storage of backup transfer reports.

This is where the risk of the whole feature sits: the reports are written outside
ActionIterator, so the identifier, ingestionTime and above all the partitionned
index name have to be produced by hand.
"""

import datetime
import logging
import re
from unittest.mock import Mock, patch

import pytest

from maas_collector.backup.backup import BackupReport
from maas_collector.rawdata.backup_report import BackupReportWriter, get_report_id
from maas_collector.rawdata.configuration import build_model_class

MODEL_META = {
    "index": "raw-data-backup",
    "name": "Backup",
    "fields": [
        {"name": "filename", "type": "Keyword"},
        {"name": "source_filename", "type": "Keyword"},
        {"name": "source_path", "type": "Keyword"},
        {"name": "size_bytes", "type": "Long"},
        {"name": "compressed", "type": "Boolean"},
        {"name": "backup_type", "type": "Keyword"},
        {"name": "destination", "type": "Keyword"},
        {"name": "destination_path", "type": "Keyword"},
        {"name": "interface_name", "type": "Keyword"},
        {"name": "model_name", "type": "Keyword"},
        {"name": "start_time", "type": "Date"},
        {"name": "end_time", "type": "Date"},
        {"name": "duration", "type": "Float"},
        {"name": "status", "type": "Keyword"},
        {"name": "error_type", "type": "Keyword"},
        {"name": "details", "type": "Text"},
    ],
    "partition_field": "ingestionTime",
    "partition_format": "%Y",
}

ZULU_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z$")


@pytest.fixture(name="model")
def fixture_model():
    """the runtime Backup class, as the collector builds it from JSON"""
    return build_model_class(MODEL_META)


@pytest.fixture(name="report")
def fixture_report():
    """a successful transfer"""
    start = datetime.datetime(2026, 8, 1, 10, 0, 0, tzinfo=datetime.UTC)

    return BackupReport(
        filename="S1A_IW_OCN.SAFE",
        source_filename="S1A_IW_OCN.SAFE",
        source_path="/tmp/S1A_IW_OCN.SAFE",
        size_bytes=4194304,
        backup_type="BackupS3",
        destination="cdse-backup",
        destination_path="cdse/sentinel-1/ocn/2026/08/01/CDSE_S1_OCN/S1A_IW_OCN.SAFE",
        interface_name="CDSE_S1_OCN_FILES",
        model_name="S1OcnBackup",
        start_time=start,
        end_time=start + datetime.timedelta(seconds=2, milliseconds=500),
        duration=2.5,
        status="OK",
    )


def test_report_id_is_stable(report):
    """the same transfer always hashes to the same identifier"""
    assert get_report_id(report.to_dict()) == get_report_id(report.to_dict())
    assert len(get_report_id(report.to_dict())) == 32


@pytest.mark.parametrize(
    "field,value",
    [
        ("destination", "other-bucket"),
        ("destination_path", "elsewhere/S1A_IW_OCN.SAFE"),
        ("filename", "other.SAFE"),
        ("interface_name", "OTHER"),
        ("start_time", datetime.datetime(2026, 8, 1, 10, 0, 1, tzinfo=datetime.UTC)),
    ],
)
def test_report_id_differs_per_transfer(report, field, value):
    """one document per transfer: each coordinate of a transfer changes the id"""
    reference = get_report_id(report.to_dict())

    setattr(report, field, value)

    assert get_report_id(report.to_dict()) != reference


def test_build_action_targets_the_partitionned_index(model, report):
    """The regression test of the whole feature.

    Document.save() resolves the index through opensearchpy, which knows nothing
    about partition_index_name, and would write to the bare alias raw-data-backup.
    That works only until a second yearly partition exists, then every write fails
    with "no write index is defined for alias". Only to_bulk_action(op_type=create)
    applies the partitionning rule.
    """
    action = BackupReportWriter(model).build_action(report)

    year = datetime.datetime.now(tz=datetime.UTC).year

    assert action["_index"] == f"raw-data-backup-{year}"
    # the alias is exactly what must never be written to
    assert action["_index"] != "raw-data-backup"


def test_build_action_is_a_create(model, report):
    """create makes a replayed transfer a 409 instead of a silent overwrite"""
    action = BackupReportWriter(model).build_action(report)

    assert action["_op_type"] == "create"
    assert action["_id"] == get_report_id(report.to_dict())


def test_build_action_source(model, report):
    """everything ActionIterator would normally fill has to be filled by hand"""
    action = BackupReportWriter(model).build_action(
        report, report_name="CDSE_S1_OCN.json", report_folder="/tmp/work"
    )

    source = action["_source"]

    assert source["status"] == "OK"
    assert source["destination"] == "cdse-backup"
    assert source["size_bytes"] == 4194304
    assert source["duration"] == 2.5
    assert source["reportName"] == "CDSE_S1_OCN.json"
    assert source["reportFolder"] == "/tmp/work"

    assert ZULU_RE.match(source["start_time"])
    assert ZULU_RE.match(source["end_time"])
    assert ZULU_RE.match(source["ingestionTime"])


def test_build_action_report_name_falls_back_to_the_file(model, report):
    """a collector that does not pass a report name still produces joinable docs"""
    source = BackupReportWriter(model).build_action(report)["_source"]

    assert source["reportName"] == "S1A_IW_OCN.SAFE"
    assert source["reportFolder"] == "/tmp"


def test_build_action_keeps_ko_details(model, report):
    """the failure fields survive the round trip through the model"""
    report.status = "KO"
    report.error_type = "ClientError"
    report.details = "An error occurred (500) when calling the PutObject operation"

    source = BackupReportWriter(model).build_action(report)["_source"]

    assert source["status"] == "KO"
    assert source["error_type"] == "ClientError"
    assert "PutObject" in source["details"]


def test_writer_warns_on_model_drift(caplog):
    """a template that lost a field would otherwise create dynamic fields"""
    meta = dict(MODEL_META, name="DriftedBackup")
    meta["fields"] = [
        field for field in MODEL_META["fields"] if field["name"] != "error_type"
    ]

    with caplog.at_level(logging.WARNING):
        BackupReportWriter(build_model_class(meta))

    assert "error_type" in caplog.text


def test_write_uses_a_single_bulk_call(model, report):
    """one bulk per ingested file, not one round trip per destination"""
    reports = [report, report]

    with patch(
        "maas_collector.rawdata.backup_report.bulk", return_value=(2, [])
    ) as bulk_mock:
        with patch(
            "maas_collector.rawdata.backup_report.es_connections.get_connection",
            return_value=Mock(),
        ):
            success, errors = BackupReportWriter(model).write(reports)

    assert bulk_mock.call_count == 1
    assert len(bulk_mock.call_args.args[1]) == 2
    assert bulk_mock.call_args.kwargs["raise_on_error"] is False
    assert success == 2
    assert errors == []


def test_write_filters_conflicts(model, report, caplog):
    """a 409 is another pod reporting the same transfer: dedup, not an error"""
    with patch(
        "maas_collector.rawdata.backup_report.bulk",
        return_value=(0, [{"create": {"status": 409}}]),
    ):
        with patch(
            "maas_collector.rawdata.backup_report.es_connections.get_connection",
            return_value=Mock(),
        ):
            with caplog.at_level(logging.ERROR):
                _, errors = BackupReportWriter(model).write([report])

    assert errors == []
    assert caplog.text == ""


def test_write_reports_real_errors(model, report, caplog):
    """anything that is not a conflict is worth an ERROR line"""
    with patch(
        "maas_collector.rawdata.backup_report.bulk",
        return_value=(0, [{"create": {"status": 400, "error": "mapping"}}]),
    ):
        with patch(
            "maas_collector.rawdata.backup_report.es_connections.get_connection",
            return_value=Mock(),
        ):
            with caplog.at_level(logging.ERROR):
                _, errors = BackupReportWriter(model).write([report])

    assert len(errors) == 1
    assert "could not be stored" in caplog.text

import datetime
import itertools
from unittest.mock import patch

import pytest

from maas_collector.backup.backup import BackupReport
from maas_collector.rawdata.transfer.contract import TransferRequest
from maas_collector.rawdata.transfer.reports import (
    ArchiveReportError,
    REPORT_INDEX,
    archive_report,
    archived_state,
    missing_product,
    read_products,
    report_model,
    requested_reports,
    write_reports,
)
from maas_collector.rawdata.transfer.rule import ArchiveRule

REQUEST = TransferRequest(
    "svc",
    "ITF_A",
    (("d1", "i1"), ("d2", "i1")),
    "archive-a",
    2,
    "skip",
    False,
    "op-1",
    "alice",
    "recollect",
)
P1 = {
    "document_id": "d1",
    "document_index": "i1",
    "product_id": "id-1",
    "product_name": "P1.zip",
    "size": 3,
    "checksum": "abc",
    "publication_date": None,
}

COUNTER = itertools.count(1)


def report(**fields):
    """a report source as stored in the index"""
    return {
        "origin": "archive",
        "backup_name": "archive-a",
        "interface_name": "ITF_A",
        "product_name": "P1.zip",
        "destination": "b1",
        "status": "OK",
        "start_time": "2026-10-01T00:00:00.000Z",
        **fields,
    }


def add(client, source):
    client.index(index=REPORT_INDEX, id=f"r{next(COUNTER)}", body=source)


def test_requested_reports_one_per_product():
    [first, second] = requested_reports(
        REQUEST, [P1, P1 | {"product_name": "P2.zip", "product_id": "id-2"}]
    )
    assert (
        first.status,
        first.origin,
        first.backup_name,
        first.operation_id,
        first.interface_name,
        first.product_id,
        first.product_name,
        first.catalogue_checksum,
        first.destination,
    ) == ("REQUESTED", "archive", "archive-a", "op-1", "ITF_A", "id-1", "P1.zip", "abc", "")
    assert second.product_name == "P2.zip"


def test_archive_report_is_stamped_with_the_request_and_the_time():
    report_ = archive_report(
        REQUEST, P1 | {"checksum": None}, "KO", destination="b1", details="boom"
    )
    assert (
        report_.status,
        report_.destination,
        report_.details,
        report_.catalogue_checksum,
        report_.filename,
        report_.source_filename,
    ) == ("KO", "b1", "boom", "", "P1.zip", "P1.zip")
    assert report_.start_time == report_.end_time
    assert report_.start_time.tzinfo is not None


def test_missing_product_has_an_empty_name_and_names_the_document():
    product = missing_product(("d9", "i1"))
    assert (product["product_name"], product["product_id"], product["details"]) == (
        "",
        "",
        "document d9 not found in i1",
    )
    [report_] = requested_reports(REQUEST, [product])
    assert (report_.status, report_.product_name, report_.details) == (
        "REQUESTED",
        "",
        "document d9 not found in i1",
    )


def test_archived_state_keeps_the_last_report_per_destination(fake_opensearch):
    add(fake_opensearch, report(destination="b1", status="OK"))
    add(
        fake_opensearch,
        report(destination="b1", status="KO", start_time="2026-10-02T00:00:00.000Z"),
    )
    add(fake_opensearch, report(destination="b2", status="REPLACED"))
    add(fake_opensearch, report(destination="b3", status="OK", origin="collector"))
    add(fake_opensearch, report(destination="b4", status="OK", product_name="P2.zip"))
    add(fake_opensearch, report(destination="b5", status="OK", backup_name="archive-b"))
    add(fake_opensearch, report(destination="b6", status="REQUESTED"))
    state = archived_state(fake_opensearch, "archive-a", "ITF_A", "P1.zip")
    assert {name: value["status"] for name, value in state.items()} == {
        "b1": "KO",
        "b2": "REPLACED",
    }


def test_archived_state_without_index_is_empty(fake_opensearch):
    assert archived_state(fake_opensearch, "archive-a", "ITF_A", "P1.zip") == {}


def test_write_reports_creates_documents_in_the_yearly_index(fake_opensearch):
    with patch(
        "maas_collector.rawdata.backup_report.bulk", return_value=(1, [])
    ) as bulk:
        write_reports(requested_reports(REQUEST, [P1]))
    [action] = bulk.call_args.args[1]
    year = datetime.datetime.now(datetime.UTC).year
    assert action["_op_type"] == "create" and action["_index"] == f"raw-data-backup-{year}"
    assert (action["_source"]["origin"], action["_source"]["status"]) == (
        "archive",
        "REQUESTED",
    )
    assert bulk.call_args.kwargs["refresh"] == "wait_for"


def test_write_reports_raises_when_a_report_is_not_stored(fake_opensearch):
    with patch(
        "maas_collector.rawdata.backup_report.bulk",
        return_value=(0, [{"create": {"status": 500}}]),
    ):
        with pytest.raises(ArchiveReportError):
            write_reports(requested_reports(REQUEST, [P1]))


def test_write_reports_raises_when_the_bulk_call_fails(fake_opensearch):
    with patch(
        "maas_collector.rawdata.backup_report.bulk", side_effect=OSError("down")
    ):
        with pytest.raises(ArchiveReportError, match="down"):
            write_reports(requested_reports(REQUEST, [P1]))


def test_report_model_declares_every_report_field():
    assert set(BackupReport().to_dict()) <= set(report_model()._INITIAL_FIELDS)
    assert report_model().Index.name == "raw-data-backup"
    assert report_model() is report_model()


def test_read_products_maps_the_rule_fields_and_lists_the_missing(fake_opensearch):
    fake_opensearch.index(
        index="i1",
        id="d1",
        body={
            "product_id": "id-1",
            "product_name": "P1.zip",
            "content_length": 3,
            "checksum": "abc",
        },
    )
    products, missing = read_products(
        fake_opensearch, (("d1", "i1"), ("d9", "i1")), ArchiveRule.default("archive-a")
    )
    assert products == [P1] and missing == [("d9", "i1")]

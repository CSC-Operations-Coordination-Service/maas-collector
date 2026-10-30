import datetime
import itertools

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata.transfer.gaps import find_gaps
from maas_collector.rawdata.transfer.reports import REPORT_INDEX

NOW = datetime.datetime(2026, 10, 29, 12, tzinfo=datetime.UTC)
DELAY = datetime.timedelta(hours=6)
START = datetime.datetime(2026, 10, 28, tzinfo=datetime.UTC)
END = NOW
MODEL = {
    "name": "ProductA",
    "index": "raw-data-a",
    "fields": [
        {"name": "interface_name", "type": "Keyword"},
        {"name": "product_name", "type": "Keyword"},
    ],
}
ENTRY = {
    "interface_name": "ITF_A",
    "model": MODEL,
    "archive": {"backup": "archive-a"},
}
COUNTER = itertools.count(1)


def raw(client, document_id, name, indexed, interface="ITF_A", **fields):
    client.index(
        index="raw-data-a",
        id=document_id,
        body={
            "interface_name": interface,
            "product_id": f"id-{document_id}",
            "product_name": name,
            "ingestionTime": indexed,
        }
        | fields,
    )


def report(client, name, status="OK", backup="archive-a", **fields):
    client.index(
        index=REPORT_INDEX,
        id=f"r{next(COUNTER)}",
        body={
            "origin": "archive",
            "backup_name": backup,
            "interface_name": "ITF_A",
            "product_name": name,
            "status": status,
        }
        | fields,
    )


def gaps(client, entry=ENTRY, backup="archive-a", **kwargs):
    return find_gaps(client, entry, backup, START, END, DELAY, now=NOW, **kwargs)


def test_product_without_outcome_is_a_gap(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    assert gaps(fake_opensearch) == [
        {
            "product_id": "id-d1",
            "product_name": "P1.zip",
            "document_index": "raw-data-a",
            "indexed_at": "2026-10-28T10:00:00.000Z",
        }
    ]


@pytest.mark.parametrize("status", ["OK", "REPLACED", "KO", "SKIPPED"])
def test_product_with_an_outcome_is_not_a_gap(fake_opensearch, status):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    report(fake_opensearch, "P1.zip", status)
    assert gaps(fake_opensearch) == []


def test_requested_only_is_still_a_gap(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    report(fake_opensearch, "P1.zip", "REQUESTED")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P1.zip"]


def test_product_indexed_within_the_delay_is_not_yet_a_gap(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-29T08:00:00.000Z")
    assert gaps(fake_opensearch) == []


def test_the_end_of_the_window_bounds_the_products_too(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    raw(fake_opensearch, "d2", "P2.zip", "2026-10-28T20:00:00.000Z")
    found = find_gaps(
        fake_opensearch,
        ENTRY,
        "archive-a",
        START,
        datetime.datetime(2026, 10, 28, 12, tzinfo=datetime.UTC),
        DELAY,
        now=NOW,
    )
    assert [gap["product_name"] for gap in found] == ["P1.zip"]


def test_window_bounds_the_products(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-27T23:59:00.000Z")
    raw(fake_opensearch, "d2", "P2.zip", "2026-10-28T00:00:00.000Z")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P2.zip"]


def test_gaps_are_sorted_by_indexed_time(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T12:00:00.000Z")
    raw(fake_opensearch, "d2", "P2.zip", "2026-10-28T09:00:00.000Z")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P2.zip", "P1.zip"]


def test_documents_of_another_interface_are_ignored(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z", interface="ITF_B")
    assert gaps(fake_opensearch) == []


def test_report_of_another_backup_does_not_count(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    report(fake_opensearch, "P1.zip", backup="archive-b")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P1.zip"]


def test_report_of_a_file_backup_does_not_count(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    report(fake_opensearch, "P1.zip", origin="collector")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P1.zip"]


def test_many_products_are_looked_up_in_chunks(fake_opensearch):
    for number in range(1030):
        raw(fake_opensearch, f"d{number}", f"P{number}.zip", "2026-10-28T10:00:00.000Z")
    for number in range(1029):
        report(fake_opensearch, f"P{number}.zip")
    assert [gap["product_name"] for gap in gaps(fake_opensearch)] == ["P1029.zip"]


def test_the_fields_of_the_rule_are_used(fake_opensearch):
    entry = ENTRY | {"archive": {"backup": "archive-a", "fields": {"product_name": "pname"}}}
    raw(fake_opensearch, "d1", "ignored", "2026-10-28T10:00:00.000Z", pname="P9.zip")
    assert [gap["product_name"] for gap in gaps(fake_opensearch, entry)] == ["P9.zip"]


def test_an_entry_without_rule_reads_the_default_fields(fake_opensearch):
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    entry = {key: value for key, value in ENTRY.items() if key != "archive"}
    assert [gap["product_name"] for gap in gaps(fake_opensearch, entry)] == ["P1.zip"]


def test_model_without_interface_name_field_is_refused(fake_opensearch):
    entry = ENTRY | {
        "model": {
            "name": "ProductA",
            "index": "raw-data-a",
            "fields": [{"name": "product_name", "type": "Keyword"}],
        }
    }
    with pytest.raises(ValueError, match="ITF_A.*interface_name"):
        gaps(fake_opensearch, entry)


def test_model_name_is_resolved_through_the_configuration_index(fake_opensearch):
    add_config_document(
        fake_opensearch, "models.json", 1, ["svc"],
        {"models": [{"name": "ProductA", "index": "raw-data-a",
                     "fields": [{"name": "interface_name", "type": "Keyword"}]}]},
    )
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    assert len(gaps(fake_opensearch, ENTRY | {"model": "ProductA"})) == 1


def test_named_model_without_interface_name_field_is_refused(fake_opensearch):
    add_config_document(
        fake_opensearch, "models.json", 1, ["svc"],
        {"models": [{"name": "ProductA", "index": "raw-data-a", "fields": []}]},
    )
    with pytest.raises(ValueError, match="interface_name"):
        gaps(fake_opensearch, ENTRY | {"model": "ProductA"})


def test_unknown_named_model_is_refused(fake_opensearch):
    add_config_document(fake_opensearch, "models.json", 1, ["svc"], {"models": []})
    with pytest.raises(ValueError, match="ProductB"):
        gaps(fake_opensearch, ENTRY | {"model": "ProductB"})


def test_a_model_defined_with_two_indexes_is_refused(fake_opensearch):
    model = {"name": "ProductA", "fields": [{"name": "interface_name", "type": "Keyword"}]}
    add_config_document(
        fake_opensearch, "a.json", 1, ["svc"], {"models": [model | {"index": "raw-a-1"}]}
    )
    add_config_document(
        fake_opensearch, "b.json", 1, ["svc"], {"models": [model | {"index": "raw-a-2"}]}
    )
    with pytest.raises(ValueError, match="two indexes"):
        gaps(fake_opensearch, ENTRY | {"model": "ProductA"})


def test_a_deleted_model_document_is_not_a_definition(fake_opensearch):
    add_config_document(
        fake_opensearch, "models.json", 1, ["svc"],
        {"models": [MODEL]}, deleted=True,
    )
    with pytest.raises(ValueError, match="ProductA"):
        gaps(fake_opensearch, ENTRY | {"model": "ProductA"})


def test_a_model_defined_inline_by_another_entry_is_found(fake_opensearch):
    add_config_document(
        fake_opensearch, "other.json", 1, ["svc"],
        {"collectors": [{"interface_name": "ITF_X", "model": MODEL}]},
    )
    raw(fake_opensearch, "d1", "P1.zip", "2026-10-28T10:00:00.000Z")
    assert len(gaps(fake_opensearch, ENTRY | {"model": "ProductA"})) == 1

import datetime

from maas_collector.rawdata.transfer.conflicts import (
    CONFLICT_INDEX,
    conflict_id,
    open_conflict,
    resolve_conflict,
)

OK1 = {
    "status": "OK",
    "catalogue_checksum": "aa",
    "written_md5": "m1",
    "start_time": "2026-10-01T00:00:00.000Z",
    "end_time": "2026-10-01T00:01:00.000Z",
}
PRODUCT = {
    "product_id": "id-1",
    "product_name": "P1.zip",
    "document_index": "i1",
    "publication_date": "2026-03-04T10:00:00.000Z",
}
T1 = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
T2 = T1 + datetime.timedelta(days=1)


def stored(client):
    [document] = client.documents[CONFLICT_INDEX].values()
    return document


def test_conflict_id_is_stable_and_distinct():
    assert (
        conflict_id("archive-a", "ITF_A", "P1.zip")
        == conflict_id("archive-a", "ITF_A", "P1.zip")
        != conflict_id("archive-a", "ITF_A", "P2.zip")
    )


def test_open_conflict_writes_the_document(fake_opensearch):
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    assert stored(fake_opensearch) == {
        "backup_name": "archive-a",
        "interface_name": "ITF_A",
        "product_name": "P1.zip",
        "product_id": "id-1",
        "document_index": "i1",
        "status": "OPEN",
        "archived_md5": "m1",
        "archived_at": "2026-10-01T00:01:00.000Z",
        "catalogue_checksum": "bb",
        "publication_date": "2026-03-04T10:00:00.000Z",
        "first_seen": "2026-10-01T00:00:00.000Z",
        "last_seen": "2026-10-01T00:00:00.000Z",
    }


def test_a_conflict_seen_again_keeps_its_first_seen(fake_opensearch):
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "dd", T2)
    assert (
        stored(fake_opensearch)["first_seen"],
        stored(fake_opensearch)["last_seen"],
        stored(fake_opensearch)["catalogue_checksum"],
    ) == ("2026-10-01T00:00:00.000Z", "2026-10-02T00:00:00.000Z", "dd")


def test_a_resolved_conflict_seen_again_reopens_as_new(fake_opensearch):
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip")
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T2)
    assert (stored(fake_opensearch)["status"], stored(fake_opensearch)["first_seen"]) == (
        "OPEN",
        "2026-10-02T00:00:00.000Z",
    )


def test_resolve_answers_whether_it_resolved(fake_opensearch):
    assert resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip") is False
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    assert (
        resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip") is True
        and stored(fake_opensearch)["status"] == "RESOLVED"
    )
    assert resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip") is False

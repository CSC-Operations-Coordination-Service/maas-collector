"""Tests of the extra fields stamped on every record of a file

These fields are what makes a re-upload of an attachment visible: without them a
record dropped from a re-uploaded file is byte identical to the indexed one, so
the collector skips it and nothing downstream can tell it was removed.
"""

from unittest.mock import Mock

from maas_model import MAASRawDocument
from opensearchpy import Keyword

from maas_collector.rawdata.model import ActionIterator


class FakeExtractor:
    """Yield a fixed list of records, like a CSV extractor would"""

    def __init__(self, extracts):
        self.extracts = extracts

    def extract(self, path, report_folder=""):
        yield from (dict(extract) for extract in self.extracts)

    def stop(self):
        pass


def make_config(extracts):
    config = Mock()
    config.extractor = FakeExtractor(extracts)
    config.get_id_func.return_value = lambda extract: extract["product_name"]
    config.store_meta = False
    return config


def build_iterator(extracts, **kwargs):
    iterator = ActionIterator(
        "/tmp/OMCS-1234_LTA_Werum_DelList.csv",
        make_config(extracts),
        report_name="OMCS-1234_LTA_Werum_DelList.csv",
        **kwargs,
    )
    # bypass the database round trip: keep the extracted records as they are
    iterator.process_chunk = lambda chunk: iter(chunk)
    iterator.has_meta_file = False
    return iterator


def test_extra_fields_are_stamped_on_every_record():
    iterator = build_iterator(
        [{"product_name": "PRODUCT_A"}, {"product_name": "PRODUCT_B"}],
        extra_fields={"attachment_id": "10501"},
    )

    records = list(iterator)

    assert [record["product_name"] for record in records] == [
        "PRODUCT_A",
        "PRODUCT_B",
    ]
    assert all(record["attachment_id"] == "10501" for record in records)
    assert all(
        record["reportName"] == "OMCS-1234_LTA_Werum_DelList.csv" for record in records
    )


def test_without_extra_fields_records_are_unchanged():
    """Backward compatibility: every other interface must be untouched"""
    iterator = build_iterator([{"product_name": "PRODUCT_A"}])

    assert list(iterator) == [
        {
            "product_name": "PRODUCT_A",
            "reportName": "OMCS-1234_LTA_Werum_DelList.csv",
        }
    ]


class DeletionRow(MAASRawDocument):
    """A stand-in for the ProductDeletion raw model"""

    class Index:
        name = "raw-data-product-deletion"

    _PARTITION_FIELD = "ingestionTime"
    _PARTITION_FIELD_FORMAT = "static"

    product_name = Keyword()
    attachment_id = Keyword()


def existing_row(product_name, attachment_id):
    """A row as already stored in the index"""
    document = DeletionRow(
        product_name=product_name,
        attachment_id=attachment_id,
        reportName="OMCS-1234_LTA_Werum_DelList.csv",
    )
    document.ingestionTime = "2024-05-01T00:00:00.000Z"
    document.meta.id = product_name
    document.meta.index = "raw-data-product-deletion-static"
    return document


def process(existing, extract):
    iterator = build_iterator([])
    iterator.config.model = DeletionRow
    return iterator.process_document(extract["product_name"], extract, existing)


def test_row_is_rewritten_when_the_attachment_changes():
    """The heart of the feature: a re-upload refreshes the surviving rows.

    Their attachment identity changes, so the record no longer matches the indexed
    one and the collector stops skipping it. The rows dropped from the file are
    never re-extracted and keep the previous identity, which is what makes them
    identifiable as stale.
    """
    document = process(
        existing_row("PRODUCT_A", "10400"),
        {
            "product_name": "PRODUCT_A",
            "attachment_id": "10501",
            "reportName": "OMCS-1234_LTA_Werum_DelList.csv",
        },
    )

    assert document is not None
    assert document["_source"]["attachment_id"] == "10501"
    # the ingestion time is refreshed, proving the row is really written again
    assert document["_source"]["ingestionTime"] != "2024-05-01T00:00:00.000Z"


def test_row_is_skipped_when_the_attachment_is_unchanged():
    """No spurious rewrite between two collects of the same attachment"""
    iterator = build_iterator([])
    iterator.config.model = DeletionRow

    extract = {
        "product_name": "PRODUCT_A",
        "attachment_id": "10400",
        "reportName": "OMCS-1234_LTA_Werum_DelList.csv",
    }

    document = iterator.process_document(
        "PRODUCT_A", extract, existing_row("PRODUCT_A", "10400")
    )

    assert document is None
    assert iterator.unmodified_ids == ["PRODUCT_A"]

"""Tests of the JIRA attachment identity stamping"""

import logging
from unittest.mock import Mock, patch

import jira.resources
import pytest

from maas_collector.rawdata.collector.jiraxcollector import (
    JIRAExtendedCollector,
    JiraExtendedCollectorConfiguration,
)


def make_config(**kwargs):
    """Build a JIRA interface configuration"""
    return JiraExtendedCollectorConfiguration(
        id_field="key",
        routing_key="new.raw.data.deletion-issue",
        interface_name="Jira_OMCS_Deletion_LTA",
        model=None,
        extractor=None,
        model_meta=None,
        **kwargs,
    )


def make_attachment(
    attachment_id="10501",
    filename="OMCS-1234_LTA_Werum_DelList.csv",
    created="2024-05-13T09:12:33.000+0200",
):
    """Build an attachment from a raw REST payload.

    jira.resources.Attachment declares no attribute: they all come from the
    payload through dict2resource, so there is nothing to stub.
    """
    return jira.resources.Attachment(
        options={"server": "https://jira.test", "headers": {}},
        session=Mock(),
        raw={
            "self": f"https://jira.test/rest/api/2/attachment/{attachment_id}",
            "id": attachment_id,
            "filename": filename,
            "created": created,
            "size": 4096,
            "mimeType": "text/csv",
        },
    )


@pytest.fixture(name="collector")
def collector_fixture():
    """A collector instance without running its constructor"""
    collector = JIRAExtendedCollector.__new__(JIRAExtendedCollector)
    collector.logger = logging.getLogger("JIRAExtendedCollector")
    return collector


def test_attachment_raw_attributes():
    """Pin the JIRA resource contract against a dependency bump"""
    attachment = make_attachment()

    assert attachment.id == "10501"
    assert attachment.filename == "OMCS-1234_LTA_Werum_DelList.csv"
    # a basic format offset, that the opensearch date_time format rejects
    assert attachment.created == "2024-05-13T09:12:33.000+0200"


def test_build_attachment_extra_fields_normalises_date(collector):
    """The offset must be converted to ZULU or every row is rejected on ingestion"""
    config = make_config(
        attachment_extra_fields={
            "attachment_id": "id",
            "attachment_created": "created",
        },
        attachment_date_fields=["attachment_created"],
    )

    assert collector.build_attachment_extra_fields(make_attachment(), config) == {
        "attachment_id": "10501",
        # 09:12:33+02:00 is 07:12:33 UTC
        "attachment_created": "2024-05-13T07:12:33.000Z",
    }


def test_build_attachment_extra_fields_disabled_by_default(collector):
    """Interfaces that do not opt in, like EDRS, must be left untouched"""
    assert collector.build_attachment_extra_fields(make_attachment(), make_config()) == {}
    assert collector.build_attachment_extra_fields(make_attachment(), None) == {}


def test_build_attachment_extra_fields_ignores_unknown_attribute(collector):
    config = make_config(attachment_extra_fields={"attachment_author": "nonexistent"})

    assert collector.build_attachment_extra_fields(make_attachment(), config) == {}


def test_deduplicate_attachments_keeps_newest():
    """Two attachments sharing a file name would overwrite each other forever"""
    older = make_attachment("10400", created="2024-05-01T09:00:00.000+0200")
    newer = make_attachment("10501", created="2024-05-13T09:12:33.000+0200")

    kept = JIRAExtendedCollector.deduplicate_attachments([older, newer])

    assert [attachment.id for attachment in kept] == ["10501"]

    # order of arrival does not matter
    kept = JIRAExtendedCollector.deduplicate_attachments([newer, older])

    assert [attachment.id for attachment in kept] == ["10501"]


def test_deduplicate_attachments_keeps_distinct_names():
    werum = make_attachment("10400", filename="OMCS-1234_LTA_Werum_DelList.csv")
    acri = make_attachment("10501", filename="OMCS-1234_LTA_Acri_DelList.csv")

    kept = JIRAExtendedCollector.deduplicate_attachments([werum, acri])

    assert sorted(attachment.id for attachment in kept) == ["10400", "10501"]


def test_ingest_attachement_passes_extra_fields(collector, tmp_path):
    """The stamped fields must reach the extraction of the attachment"""
    config = make_config(
        attachment_extra_fields={
            "attachment_id": "id",
            "attachment_created": "created",
        },
        attachment_date_fields=["attachment_created"],
    )

    attachment = make_attachment()
    attachment.get = Mock(return_value=b"PRODUCT_A\nPRODUCT_B\n")

    collector.args = Mock(working_directory=str(tmp_path), force=False)
    collector.get_configurations = Mock(return_value=[make_config()])

    with patch.object(JIRAExtendedCollector, "extract_from_file") as extract:
        collector.ingest_attachement(attachment, "OMCS-1234_", config)

    extract.assert_called_once()

    assert extract.call_args.kwargs["extra_fields"] == {
        "attachment_id": "10501",
        "attachment_created": "2024-05-13T07:12:33.000Z",
    }
    assert (
        extract.call_args.kwargs["report_name"]
        == "OMCS-1234_OMCS-1234_LTA_Werum_DelList.csv"
    )


def test_ingest_attachement_without_opt_in(collector, tmp_path):
    """An interface that did not opt in extracts exactly as before"""
    attachment = make_attachment()
    attachment.get = Mock(return_value=b"PRODUCT_A\n")

    collector.args = Mock(working_directory=str(tmp_path), force=False)
    collector.get_configurations = Mock(return_value=[make_config()])

    with patch.object(JIRAExtendedCollector, "extract_from_file") as extract:
        collector.ingest_attachement(attachment, "", make_config())

    assert extract.call_args.kwargs["extra_fields"] == {}

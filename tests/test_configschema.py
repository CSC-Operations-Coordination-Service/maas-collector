import dataclasses

import pytest

from maas_collector.rawdata.collector.odatacollector import (
    ODataCollectorConfiguration,
)
from maas_collector.rawdata.configschema import (
    GROUPS,
    NOT_SECRET_KEYS,
    SECRET_KEYS,
    archive_rule,
    backup_types,
    collector_classes,
    config_classes,
    entry_class_name,
    service_class_of,
    unclassified_credential_fields,
)
from maas_collector.rawdata.operator.overrides import DUPLICATES, OVERRIDABLE_KEYS


def test_every_sensitive_field_is_a_secret_key():
    for cls in config_classes().values():
        # a runtime field (init=False) is not a key of a file
        sensitive = {
            f.name
            for f in dataclasses.fields(cls)
            if f.init and f.metadata.get("sensitive")
        }
        assert sensitive <= SECRET_KEYS


def test_the_unmarked_credential_fields_are_secret_keys():
    assert "oauth_basic_credential" in SECRET_KEYS
    assert {"s3_access_key", "s3_secret_key"} <= SECRET_KEYS


def test_no_field_looks_like_a_credential_without_being_classified():
    assert unclassified_credential_fields() == []

    @dataclasses.dataclass
    class Fresh:
        api_token_value: str = ""

    assert unclassified_credential_fields([Fresh]) == ["Fresh.api_token_value"]


def test_runtime_fields_are_not_keys_and_not_classified():
    keys = collector_classes()["ODataCollectorConfiguration"]["keys"]
    assert {
        "backup_credentials",
        "backup_credentials_entry",
        "backup_name",
        "backup_report_model",
    }.isdisjoint(keys)
    assert {"backup_credentials", "backup_credentials_entry"}.isdisjoint(
        SECRET_KEYS | NOT_SECRET_KEYS
    )


def test_a_name_is_secret_or_not_secret_never_both():
    assert not SECRET_KEYS & NOT_SECRET_KEYS


def test_a_secret_name_is_sensitive_in_every_class_that_has_it():
    # marks neither field in the dataclass
    keys = collector_classes()["MpipCollectorConfiguration"]["keys"]
    assert keys["client_password"]["sensitive"] and keys["client_secret"]["sensitive"]


def test_key_descriptors():
    odata = collector_classes()["ODataCollectorConfiguration"]
    assert odata["replayable"] is True
    assert collector_classes()["FTPCollectorConfiguration"]["replayable"] is False
    keys = odata["keys"]
    assert keys["odata_query_filter"]["overridable"]
    assert keys["odata_query_filter"]["type"] == "string"
    assert (keys["product_per_page"]["type"], keys["product_per_page"]["default"]) == (
        "integer",
        1000,
    )
    # annotated with a bare `list`: TYPE_OVERRIDES
    assert (
        keys["download_file_pattern"]["type"],
        keys["download_file_pattern"]["default"],
    ) == ("string_list", [])
    # the pseudo-key
    assert (
        keys["class"]["type"],
        keys["class"]["required"],
        keys["class"]["default"],
    ) == (
        "string",
        False,
        None,
    )
    assert keys["token"]["sensitive"] and not keys["interface_credentials"]["sensitive"]
    assert {k for k, v in keys.items() if v["required"]} == {"id_field", "routing_key"}
    # runtime fields are not JSON keys
    assert {"backup_report_model", "model_meta", "backup_credentials"}.isdisjoint(keys)
    assert {k for k, v in keys.items() if v["overridable"]} == OVERRIDABLE_KEYS[
        "ODataCollectorConfiguration"
    ]


def test_archivable_follows_the_class_attribute(monkeypatch):
    monkeypatch.setattr(ODataCollectorConfiguration, "ARCHIVABLE", True, raising=False)
    assert collector_classes()["ODataCollectorConfiguration"]["archivable"] is True
    assert (
        collector_classes()["JiraExtendedCollectorConfiguration"]["archivable"] is False
    )


def test_an_entry_builds_with_the_class_of_its_service():
    assert (
        entry_class_name({"interface_name": "SUB"}, "ODataCollectorConfiguration")
        == "ODataCollectorConfiguration"
    )
    assert (
        entry_class_name(
            {"class": "JiraExtendedCollectorConfiguration"},
            "ODataCollectorConfiguration",
        )
        == "ODataCollectorConfiguration"
    )
    assert (
        entry_class_name({"class": "JiraExtendedCollectorConfiguration"}, None)
        == "JiraExtendedCollectorConfiguration"
    )
    assert entry_class_name({}, None) == "FileCollectorConfiguration"
    assert (
        service_class_of(
            [
                {},
                {"class": "ODataCollectorConfiguration"},
                {"class": "ODataCollectorConfiguration"},
            ]
        )
        == "ODataCollectorConfiguration"
    )
    assert service_class_of([{}]) is None
    with pytest.raises(ValueError, match="several"):
        service_class_of([{"class": "A"}, {"class": "B"}])


@pytest.mark.parametrize(
    "cls",
    [
        "ODataCollectorConfiguration",
        "JiraExtendedCollectorConfiguration",
        "FileCollectorConfiguration",
    ],
)
def test_replayable_and_file_entry_keys_are_described_and_grouped(cls):
    for key, descriptor in collector_classes()[cls]["keys"].items():
        assert descriptor["description"] and descriptor["group"] in GROUPS, key


def test_groups_of_the_keys_that_matter():
    keys = collector_classes()["ODataCollectorConfiguration"]["keys"]
    assert [
        keys[k]["group"]
        for k in (
            "interface_name",
            "odata_query_filter",
            "download_file_pattern",
            "extractor",
            "backup",
        )
    ] == ["identity", "remote_query", "download", "extraction", "backup"]


def test_backup_types():
    s3, local = (
        backup_types()["BackupS3"]["keys"],
        backup_types()["BackupLocal"]["keys"],
    )
    assert set(s3) == {
        "name",
        "description",
        "type",
        "directory",
        "calendar_tree",
        "enable_gzip",
        "report_model",
        "buckets",
        "s3_endpoint_url",
        "s3_region",
        "s3_signature_version",
        "credentials",
    }
    assert set(local) == {
        "name",
        "description",
        "type",
        "directory",
        "calendar_tree",
        "enable_gzip",
        "report_model",
    }
    assert s3["credentials"]["required"] and s3["buckets"]["required"]
    assert s3["name"]["required"] and local["directory"]["required"]
    assert s3["buckets"]["type"] == "string_list"
    assert s3["calendar_tree"]["default"] is False
    # secrets are in no definition
    assert not ({"s3_access_key", "s3_secret_key"} & (set(s3) | set(local)))


def test_archive_rule_keys():
    keys = archive_rule()["keys"]
    # the four editable keys: backup_version and force are written by prepare
    assert set(keys) == {"backup", "duplicates", "fields", "download"}
    assert keys["duplicates"]["default"] == "skip"
    assert keys["duplicates"]["choices"] == ["skip", "replace_if_changed"]
    assert keys["fields"]["default"] == {
        "product_id": "product_id",
        "product_name": "product_name",
        "size": "content_length",
        "checksum": "checksum",
        "checksum_algorithm": "MD5",
        "publication_date": "publication_date",
    }
    assert keys["download"]["default"] == {"url": None, "quote_id": True}
    assert keys["duplicates"]["choices"] == list(DUPLICATES)

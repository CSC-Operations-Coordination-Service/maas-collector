import datetime
import re

import pytest

from maas_collector.rawdata.collector.odatacollector import ODataCollectorConfiguration
from maas_collector.rawdata.configprovider import (
    ConfigValidationError,
    validate_config_sources,
)
from maas_collector.rawdata.configstore import ConfigSource, config_classes_for
from maas_collector.rawdata.configuration import build_collector_configuration
from maas_collector.rawdata.transfer.rule import (
    DEFAULT_FIELDS,
    ArchiveRule,
    product_of,
    utc_datetime,
)

ENTRY = {
    "class": "ODataCollectorConfiguration",
    "interface_name": "ITF_A",
    "id_field": "product_id",
    "routing_key": "",
    "archive": {"backup": "archive-a"},
}


def entry(**archive):
    return {**ENTRY, "archive": {"backup": "archive-a", **archive}}


def test_entry_without_archive_has_no_rule():
    assert ArchiveRule.from_entry({"interface_name": "ITF_A"}) is None
    assert ArchiveRule.from_entry({"interface_name": "ITF_A", "archive": None}) is None


def test_defaults():
    assert ArchiveRule.from_entry(ENTRY) == ArchiveRule(
        "archive-a", "skip", DEFAULT_FIELDS, None, True, None, False
    )
    assert ArchiveRule.default("archive-a") == ArchiveRule.from_entry(ENTRY)


def test_fields_are_merged_over_the_defaults():
    rule = ArchiveRule.from_entry(entry(fields={"checksum": "origin_checksum_md5"}))
    assert rule.fields == DEFAULT_FIELDS | {"checksum": "origin_checksum_md5"}


def test_download_template_with_an_unquoted_id():
    rule = ArchiveRule.from_entry(
        entry(
            download={
                "url": "https://bytes.example/odata/v1/Products({id})/$value",
                "quote_id": False,
            }
        )
    )
    assert (rule.download_url, rule.quote_id) == (
        "https://bytes.example/odata/v1/Products({id})/$value",
        False,
    )


def test_recollect_choice_is_read():
    rule = ArchiveRule.from_entry(
        entry(duplicates="replace_if_changed", backup_version=4, force=True)
    )
    assert (rule.duplicates, rule.backup_version, rule.force) == (
        "replace_if_changed",
        4,
        True,
    )


@pytest.mark.parametrize(
    "archive, word",
    [
        ("archive-a", "archive"),
        ({}, "backup"),
        ({"backup": "Archive_A"}, "backup"),
        ({"backup": "a" * 65}, "backup"),
        ({"backup": "archive-a", "duplicates": "keep"}, "duplicates"),
        ({"backup": "archive-a", "fields": {"name": "x"}}, "name"),
        ({"backup": "archive-a", "fields": {"checksum": ""}}, "checksum"),
        (
            {"backup": "archive-a", "fields": {"checksum_algorithm": "CRC99"}},
            "checksum_algorithm",
        ),
        (
            {"backup": "archive-a", "fields": {"checksum_algorithm": "BLAKE3"}},
            "checksum_algorithm",
        ),
        (
            {
                "backup": "archive-a",
                "download": {"url": "https://bytes.example/Products/$value"},
            },
            "{id}",
        ),
        (
            {"backup": "archive-a", "download": {"url": "ftp://bytes.example/{id}"}},
            "download.url",
        ),
        ({"backup": "archive-a", "download": {"quote_id": "no"}}, "quote_id"),
        ({"backup": "archive-a", "backup_version": 0}, "backup_version"),
        ({"backup": "archive-a", "force": 1}, "force"),
        ({"backup": "archive-a", "buckets": ["b1"]}, "buckets"),
    ],
)
def test_bad_rule_is_refused_naming_the_key(archive, word):
    with pytest.raises(ValueError, match=re.escape(word)):
        ArchiveRule.from_entry({**ENTRY, "archive": archive})


def test_message_starts_with_the_interface_name():
    with pytest.raises(ValueError, match="^ITF_A: "):
        ArchiveRule.from_entry({**ENTRY, "archive": {"backup": "archive-a", "force": 1}})


@pytest.mark.parametrize(
    "algorithm, name",
    [
        ("MD5", "md5"),
        ("SHA-256", "sha256"),
        ("sha256", "sha256"),
        ("SHA3-256", "sha3_256"),
    ],
)
def test_new_checksum_follows_the_algorithm(algorithm, name):
    rule = ArchiveRule.from_entry(entry(fields={"checksum_algorithm": algorithm}))
    assert rule.new_checksum().name == name


def test_configuration_parses_its_rule_when_built():
    config = build_collector_configuration(
        ENTRY, ODataCollectorConfiguration, register=False
    )
    assert config.archive_rule == ArchiveRule.default("archive-a")
    assert (
        build_collector_configuration(
            {**ENTRY, "archive": None}, ODataCollectorConfiguration, register=False
        ).archive_rule
        is None
    )


@pytest.mark.parametrize("evaluate", [True, False])
def test_validation_refuses_a_bad_rule_in_both_modes(evaluate):
    with pytest.raises(ConfigValidationError, match="duplicates"):
        validate_config_sources(
            [ConfigSource("a.json", {"collectors": [entry(duplicates="keep")]})],
            config_classes_for,
            resolve_models=False,
            evaluate=evaluate,
        )


def test_validation_refuses_a_rule_on_an_entry_that_cannot_archive():
    # class "" builds as FileCollectorConfiguration
    with pytest.raises(ConfigValidationError, match="odata"):
        validate_config_sources(
            [ConfigSource("a.json", {"collectors": [{**ENTRY, "class": ""}]})],
            config_classes_for,
            resolve_models=False,
        )


def test_validation_refuses_the_same_backup_for_files_and_archive():
    content = {
        "collectors": [{**ENTRY, "backup": "archive-a"}],
        "backups": [{"name": "archive-a", "type": "BackupLocal", "directory": "/archive"}],
    }
    with pytest.raises(ConfigValidationError, match="archive-a"):
        validate_config_sources(
            [ConfigSource("a.json", content)], config_classes_for, resolve_models=False
        )


def test_product_of_reads_the_rule_fields():
    rule = ArchiveRule.from_entry(entry(fields={"checksum": "md5"}))
    source = {
        "product_id": "id-1",
        "product_name": "P1.zip",
        "content_length": "3000",
        "md5": " ABC ",
        "publication_date": "2026-03-04T10:00:00.000Z",
    }
    assert product_of(source, rule, "d1", "raw-data-a-2026") == {
        "document_id": "d1",
        "document_index": "raw-data-a-2026",
        "product_id": "id-1",
        "product_name": "P1.zip",
        "size": 3000,
        "checksum": "ABC",
        "publication_date": "2026-03-04T10:00:00.000Z",
    }


def test_product_of_leaves_unknown_values_empty():
    product = product_of(
        {"product_name": "P1.zip", "content_length": 0, "checksum": "  "},
        ArchiveRule.default("archive-a"),
        "d1",
        "i",
    )
    assert (
        product["size"],
        product["checksum"],
        product["product_id"],
        product["publication_date"],
    ) == (None, None, None, None)


def test_utc_datetime():
    assert utc_datetime("2026-03-04T23:30:00-02:00") == datetime.datetime(
        2026, 3, 5, 1, 30, tzinfo=datetime.UTC
    )
    assert utc_datetime("2026-03-04T10:00:00") == datetime.datetime(
        2026, 3, 4, 10, tzinfo=datetime.UTC
    )
    assert utc_datetime(None) is None
    with pytest.raises(ValueError):
        utc_datetime("yesterday")

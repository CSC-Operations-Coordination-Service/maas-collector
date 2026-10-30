"""Tests for the definition and the resolution of named backups"""

import pytest

from backup_samples import LOCAL, S3, backups_file
from maas_collector.backup.named import (
    DESTINATION_KEYS,
    SECRET_FIELDS,
    backup_definitions,
    check_backup_block,
    check_definition,
    destination_changes,
    resolve_backup,
)


def test_definitions_are_indexed_by_name_across_contents():
    found = backup_definitions([backups_file(LOCAL), {"collectors": []}, backups_file(S3)])
    assert set(found) == {"archive-a", "archive-b"} and found["archive-a"] == LOCAL


def test_a_name_defined_twice_is_refused():
    with pytest.raises(ValueError, match="archive-a"):
        backup_definitions([backups_file(LOCAL), backups_file(LOCAL)])


@pytest.mark.parametrize("name", ["", "A", "-a", "a_b", "a b", "a" * 65, "ab\n", "é", 7, None])
def test_bad_names_are_refused(name):
    with pytest.raises(ValueError, match="name"):
        backup_definitions([backups_file(LOCAL | {"name": name})])


def test_a_name_of_64_characters_is_accepted():
    assert "a" * 64 in backup_definitions([backups_file(LOCAL | {"name": "a" * 64})])


@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_a_secret_field_is_refused_in_a_definition_and_in_an_inline_block(field):
    with pytest.raises(ValueError, match=field) as error:
        backup_definitions([backups_file(S3 | {field: "test-secret-key"})])
    assert "test-secret-key" not in str(error.value)
    with pytest.raises(ValueError, match=field):
        check_backup_block({"type": "BackupLocal", "directory": "/d", field: "x"}, "ITF_A backup")


def test_the_message_names_where():
    with pytest.raises(ValueError, match="entry ITF_A"):
        check_backup_block({"type": "BackupLocal", "directory": "/d", "s3_secret_key": "x"}, "entry ITF_A")


@pytest.mark.parametrize("credentials", ["archive-b", "backup-", "backup-a/b", "Backup-a", 7])
def test_a_bad_credentials_name_is_refused(credentials):
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block(S3 | {"credentials": credentials}, "x")


def test_an_s3_block_must_name_credentials_and_a_local_one_must_not():
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block({k: v for k, v in S3.items() if k != "credentials"}, "x")
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block(LOCAL | {"credentials": "backup-x"}, "x")


@pytest.mark.parametrize(
    "base, change, word",
    [
        (LOCAL, {"type": "BackupSFTP"}, "type"),
        (LOCAL, {"directory": ""}, "directory"),
        (LOCAL, {"buckets": ["b"]}, "buckets"),
        (S3, {"buckets": []}, "buckets"),
        (S3, {"buckets": "bucket-a"}, "buckets"),
        (LOCAL, {"bucket": "x"}, "bucket"),
        (LOCAL, {"calendar_tree": "yes"}, "calendar_tree"),
        (LOCAL, {"enable_gzip": 1}, "enable_gzip"),
        (LOCAL, {"report_model": 3}, "report_model"),
        (LOCAL, {"description": 3}, "description"),
    ],
)
def test_a_definition_that_breaks_a_rule_is_refused(base, change, word):
    with pytest.raises(ValueError, match=word):
        check_definition(base | change, "x")


def test_an_s3_definition_without_buckets_is_refused():
    with pytest.raises(ValueError, match="buckets"):
        check_definition({k: v for k, v in S3.items() if k != "buckets"}, "x")


def test_local_directory_must_be_absolute_without_dot_dot_segments():  # Review Focus 5
    for directory in ("data", "./data", "/data/../etc", "/data/.."):
        with pytest.raises(ValueError, match="directory"):
            check_definition(LOCAL | {"directory": directory}, "x")
    check_definition(LOCAL | {"directory": "/data/archive.v2"}, "x")


def test_resolving_a_name_drops_name_description_and_credentials():
    block, credentials = resolve_backup("archive-b", {"archive-b": S3 | {"description": "d"}})
    assert credentials == "backup-archive-b"
    assert set(block) == {
        "type",
        "directory",
        "buckets",
        "s3_endpoint_url",
        "s3_region",
        "s3_signature_version",
    }


def test_the_resolved_block_does_not_alias_the_definition():
    definitions = {"archive-b": dict(S3)}
    block, _ = resolve_backup("archive-b", definitions)
    block["buckets"].append("other")
    assert definitions["archive-b"]["buckets"] == ["bucket-a", "bucket-b"]


def test_an_unknown_name_raises_key_error():
    with pytest.raises(KeyError, match="nope"):
        resolve_backup("nope", {})


def test_an_inline_block_is_returned_without_its_credentials_entry():
    block, credentials = resolve_backup(
        {"type": "BackupS3", "directory": "d", "buckets": ["b"], "credentials": "backup-x"}, {}
    )
    assert (credentials, "credentials" in block) == ("backup-x", False)
    assert resolve_backup({"type": "BackupLocal", "directory": "d"}, {}) == (
        {"type": "BackupLocal", "directory": "d"},
        None,
    )


def test_an_inline_block_with_a_secret_is_refused_when_resolved():
    with pytest.raises(ValueError, match="s3_access_key"):
        resolve_backup(
            {"type": "BackupS3", "directory": "d", "s3_access_key": "x", "credentials": "backup-x"},
            {},
        )


@pytest.mark.parametrize("reference", [[], 3, None])
def test_a_reference_that_is_neither_a_name_nor_a_block_is_refused(reference):
    with pytest.raises(ValueError):
        resolve_backup(reference, {})


@pytest.mark.parametrize(
    "change, expected",
    [
        ({"directory": "/other"}, ["directory"]),
        ({"calendar_tree": True}, ["calendar_tree"]),
        ({"enable_gzip": True}, ["enable_gzip"]),
        ({"type": "BackupLocal"}, ["type"]),
        ({"s3_endpoint_url": "https://other.example"}, ["s3_endpoint_url"]),
        ({"buckets": ["bucket-a"]}, ["buckets"]),  # a removed bucket
        ({"buckets": ["bucket-b", "bucket-a", "bucket-c"]}, []),  # an added bucket, another order
        ({"description": "x"}, []),
        ({"report_model": "M"}, []),
        ({"credentials": "backup-other"}, []),
        ({"s3_region": "region-2"}, []),
    ],
)
def test_destination_changes(change, expected):
    assert destination_changes(S3, S3 | change) == expected


def test_destination_defaults_are_normalized():
    assert destination_changes(LOCAL, LOCAL | {"calendar_tree": False, "enable_gzip": False}) == []


def test_destination_keys_are_the_contract_ones():
    assert DESTINATION_KEYS == (
        "type",
        "s3_endpoint_url",
        "directory",
        "calendar_tree",
        "enable_gzip",
        "buckets",
    )

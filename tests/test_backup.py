"""Tests for the backup sinks and the transfer reports they produce.

The backup package had no coverage at all before backup reporting was added.
These tests stay away from OpenSearch: the sinks are pure data producers, storing
what they return is tested in test_backup_report.py.
"""

import dataclasses
import datetime
import gzip
import os
import pathlib
from unittest.mock import Mock, patch

import botocore.exceptions
import pytest

from maas_collector.backup import instanciate_collector_backup
from maas_collector.backup.backup import (
    KO,
    OK,
    REPLACED,
    REQUESTED,
    SKIPPED,
    BackupReport,
    CollectorBackup,
)
from maas_collector.backup.local_backup import CollectorBackupLocal
from maas_collector.backup.s3_backup import (
    CollectorBackupS3,
    CollectorBackupS3Configuration,
)


@dataclasses.dataclass
class FakeConfig:
    """the handful of attributes a backup implementation reads from a config"""

    interface_name: str = "ITF_A_FILES"

    model_name: str = "S1OcnBackup"


@pytest.fixture(name="source_file")
def fixture_source_file(tmp_path):
    """an ingested file to back up"""
    path = tmp_path / "S1A_IW_OCN__2SDV_20260801T054321.SAFE"
    path.write_bytes(b"product payload" * 100)
    return str(path)


@pytest.fixture(name="local_backup")
def fixture_local_backup(tmp_path):
    """a local sink writing under tmp_path/backup"""

    # a local backup no longer creates its directory
    (tmp_path / "backup").mkdir()

    def _build(**kwargs):
        args = {"type": "BackupLocal", "directory": str(tmp_path / "backup")}
        args.update(kwargs)
        return instanciate_collector_backup(args)

    return _build


def build_s3_backup(**kwargs):
    """an s3 sink with a mocked boto3 client"""
    args = {
        "type": "BackupS3",
        "directory": "archive/ocn",
        "buckets": ["bucket-a"],
    }
    args.update(kwargs)

    with patch("boto3.client") as client_mock:
        backup = CollectorBackupS3(CollectorBackupS3Configuration(**args))

    return backup, client_mock


def test_local_backup_report_ok(local_backup, source_file):
    """a successful transfer yields one OK report carrying its timing"""
    backup = local_backup()

    reports = backup.backup_file(FakeConfig(), source_file)

    assert len(reports) == 1

    report = reports[0]

    assert report.status == "OK"
    assert report.error_type == ""
    assert report.details == ""
    assert report.filename == os.path.basename(source_file)
    assert report.source_filename == os.path.basename(source_file)
    assert report.compressed is False
    assert report.backup_type == "BackupLocal"
    assert report.destination == backup.args.directory
    assert report.destination_path.endswith(os.path.basename(source_file))
    assert report.interface_name == "ITF_A_FILES"
    assert report.model_name == "S1OcnBackup"
    assert report.size_bytes == os.path.getsize(source_file)
    assert report.duration >= 0
    assert report.start_time <= report.end_time

    # and the file really landed there
    assert os.path.exists(report.destination_path)


def test_local_backup_report_ko(local_backup, source_file):
    """a failing transfer is reported, not raised: the ingestion loop goes on"""
    backup = local_backup()

    with patch("shutil.copy2", side_effect=PermissionError("denied")):
        reports = backup.backup_file(FakeConfig(), source_file)

    assert len(reports) == 1

    report = reports[0]

    assert report.status == "KO"
    assert report.error_type == "PermissionError"
    assert "denied" in report.details
    assert report.end_time is not None


def test_local_backup_uses_config_interface_name(local_backup, source_file):
    """the backup block rarely declares an interface, the collector config does"""
    backup = local_backup(interface_name=None)

    reports = backup.backup_file(FakeConfig(interface_name="ITF_A"), source_file)

    assert reports[0].interface_name == "ITF_A"


def test_local_backup_model_name_nonetype_is_normalized(local_backup, source_file):
    """FileCollectorConfiguration.model_name is the literal "NoneType" with no model"""
    backup = local_backup()

    reports = backup.backup_file(FakeConfig(model_name="NoneType"), source_file)

    assert reports[0].model_name == ""


def test_s3_backup_report_per_bucket(source_file):
    """one report per bucket, and one bucket failing does not skip the next"""
    backup, _ = build_s3_backup(buckets=["bucket-a", "bucket-b", "bucket-c"])

    backup.s3_client = Mock()
    backup.s3_client.upload_file.side_effect = [
        None,
        botocore.exceptions.ClientError({"Error": {"Code": "500"}}, "PutObject"),
        None,
    ]

    reports = backup.backup_file(FakeConfig(), source_file)

    assert [report.status for report in reports] == ["OK", "KO", "OK"]
    assert [report.destination for report in reports] == [
        "bucket-a",
        "bucket-b",
        "bucket-c",
    ]
    # the loop really continued past the failure
    assert backup.s3_client.upload_file.call_count == 3
    assert reports[1].error_type == "ClientError"

    # the remote path is computed once, so the copies cannot end up in two
    # different calendar directories when a run straddles midnight
    assert len({report.destination_path for report in reports}) == 1


def test_s3_backup_unexpected_error_propagates(source_file):
    """the catch stays narrow on purpose: only transport errors are reported"""
    backup, _ = build_s3_backup()

    backup.s3_client = Mock()
    backup.s3_client.upload_file.side_effect = ZeroDivisionError("boom")

    with pytest.raises(ZeroDivisionError):
        backup.backup_file(FakeConfig(), source_file)


def test_s3_backup_destinations(source_file):
    """destinations makes the one-to-many asymmetry of s3 explicit"""
    backup, _ = build_s3_backup(buckets=["bucket-a", "bucket-b"])

    assert backup.destinations == ["bucket-a", "bucket-b"]


def test_backup_gzip_report(local_backup, source_file):
    """the report distinguishes the transferred file from the ingested one"""
    backup = local_backup(enable_gzip=True)

    reports = backup.backup_file(FakeConfig(), source_file)

    report = reports[0]

    assert report.compressed is True
    assert report.filename == os.path.basename(source_file) + ".gz"
    assert report.source_filename == os.path.basename(source_file)

    # the temporary .gz is cleaned up, and the archived copy is really gzip
    assert not os.path.exists(source_file + ".gz")
    with gzip.open(report.destination_path, "rb") as fileobj:
        assert fileobj.read() == open(source_file, "rb").read()


def test_backup_gzip_cleanup_on_failure(local_backup, source_file):
    """the .gz used to leak whenever the implementation raised"""
    backup = local_backup(enable_gzip=True)

    with patch.object(
        CollectorBackupLocal,
        "backup_file_implementation",
        side_effect=ZeroDivisionError("boom"),
    ):
        with pytest.raises(ZeroDivisionError):
            backup.backup_file(FakeConfig(), source_file)

    assert not os.path.exists(source_file + ".gz")


def test_backup_do_not_compress_extension(local_backup, tmp_path):
    """already compressed types are transferred as-is"""
    path = tmp_path / "report.zip"
    path.write_bytes(b"PK\x03\x04")

    backup = local_backup(enable_gzip=True)

    reports = backup.backup_file(FakeConfig(), str(path))

    assert reports[0].compressed is False
    assert reports[0].filename == "report.zip"


def test_backup_report_to_dict_serializes_dates(local_backup, source_file):
    """dates leave as zulu strings, which is what the raw database expects"""
    backup = local_backup()

    data = backup.backup_file(FakeConfig(), source_file)[0].to_dict()

    assert data["start_time"].endswith("Z")
    assert data["end_time"].endswith("Z")
    # a ClassVar must not become a field
    assert "DETAILS_MAX_LENGTH" not in data


def test_backup_report_details_are_truncated(local_backup, source_file):
    """an unbounded text field fed by exception messages can blow up a shard"""
    backup = local_backup()

    with patch("shutil.copy2", side_effect=PermissionError("x" * 5000)):
        reports = backup.backup_file(FakeConfig(), source_file)

    assert len(reports[0].details) == BackupReport.DETAILS_MAX_LENGTH


def test_report_model_is_an_accepted_backup_key(tmp_path):
    """an undeclared key in the backup block is a TypeError, so pin this one"""
    backup = instanciate_collector_backup(
        {
            "type": "BackupLocal",
            "directory": str(tmp_path),
            "report_model": "Backup",
        }
    )

    assert backup.args.report_model == "Backup"


def test_backup_file_implementation_is_abstract(tmp_path):
    """the base class still refuses to be used directly"""
    backup = CollectorBackup(
        CollectorBackup.CONFIGURATION_CLASS(directory=str(tmp_path))
    )

    with pytest.raises(NotImplementedError):
        backup.backup_file_implementation(FakeConfig(), "whatever")


def test_a_local_backup_fails_when_its_directory_does_not_exist(tmp_path, source_file):
    backup = instanciate_collector_backup(
        {"type": "BackupLocal", "directory": str(tmp_path / "missing")}
    )

    [report] = backup.backup_file(FakeConfig(), source_file)

    assert (report.status, report.error_type) == ("KO", "FileNotFoundError")
    assert "missing" in report.details
    assert not (tmp_path / "missing").exists()


def test_a_local_backup_whose_directory_is_a_file_fails(tmp_path, source_file):
    backup = instanciate_collector_backup(
        {"type": "BackupLocal", "directory": source_file}
    )

    [report] = backup.backup_file(FakeConfig(), source_file)

    assert report.status == "KO"


def test_a_local_backup_still_creates_the_folders_below_its_directory(
    local_backup, source_file, tmp_path
):
    [report] = local_backup(calendar_tree=True).backup_file(FakeConfig(), source_file)

    assert report.status == "OK" and os.path.isfile(report.destination_path)
    assert report.destination_path.startswith(str(tmp_path / "backup") + "/20")


def test_the_report_carries_the_backup_name_and_the_collector_origin(
    local_backup, source_file
):
    [report] = local_backup(backup_name="archive-a").backup_file(
        FakeConfig(), source_file
    )

    assert (report.backup_name, report.origin) == ("archive-a", "collector")
    assert local_backup().backup_file(FakeConfig(), source_file)[0].backup_name == ""


def test_every_s3_report_carries_the_backup_name(source_file):
    backup, _ = build_s3_backup(
        backup_name="archive-b", buckets=["bucket-a", "bucket-b"]
    )
    backup.s3_client = Mock()

    reports = backup.backup_file(FakeConfig(), source_file)

    assert [report.backup_name for report in reports] == ["archive-b", "archive-b"]


def test_new_report_fields_default_to_empty():
    report = BackupReport()

    assert (
        report.origin,
        report.backup_name,
        report.operation_id,
        report.product_id,
        report.product_name,
        report.written_md5,
        report.catalogue_checksum,
    ) == ("collector", "", "", "", "", "", "")
    assert (OK, KO, REPLACED, SKIPPED, REQUESTED) == (
        "OK",
        "KO",
        "REPLACED",
        "SKIPPED",
        "REQUESTED",
    )


def test_report_to_dict_has_the_new_fields():
    data = BackupReport(
        origin="archive",
        backup_name="archive-a",
        operation_id="op1",
        product_id="p1",
        product_name="P1.zip",
        written_md5="m",
        catalogue_checksum="c",
    ).to_dict()

    assert data["origin"] == "archive"
    assert {
        "backup_name",
        "operation_id",
        "product_id",
        "product_name",
        "written_md5",
        "catalogue_checksum",
    } <= set(data)


def test_backup_name_is_an_accepted_backup_key(tmp_path):
    backup = instanciate_collector_backup(
        {"type": "BackupLocal", "directory": str(tmp_path), "backup_name": "archive-a"}
    )

    assert backup.args.backup_name == "archive-a"


def test_the_s3_configuration_masks_both_keys_in_its_repr():
    config = CollectorBackupS3Configuration(
        directory="d", s3_access_key="test-access-key", s3_secret_key="test-secret-key"
    )

    assert "test-access-key" not in repr(config)
    assert "test-secret-key" not in repr(config)

    fields = {f.name: f for f in dataclasses.fields(config)}

    assert fields["s3_access_key"].metadata["sensitive"]
    assert fields["s3_secret_key"].metadata["sensitive"]


CONFIG = FakeConfig(interface_name="ITF_A", model_name="ProductA")
DAY = datetime.datetime(2026, 3, 4, 23, 30, tzinfo=datetime.UTC)


def test_object_key_without_calendar_tree(local_backup):
    assert local_backup().object_key("ITF_A", "P1.zip", DAY) == "ITF_A/P1.zip"


def test_object_key_files_by_the_given_day(local_backup):
    assert (
        local_backup(calendar_tree=True).object_key("ITF_A", "P1.zip", DAY)
        == "ITF_A/2026/03/04/P1.zip"
    )


def test_object_key_uses_the_utc_day_of_an_offset_date(local_backup):
    late = datetime.datetime(
        2026, 3, 4, 23, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=-2))
    )
    assert (
        local_backup(calendar_tree=True).object_key("ITF_A", "P1.zip", late)
        == "ITF_A/2026/03/05/P1.zip"
    )


def test_object_key_without_a_date_is_today_utc(local_backup):
    today = datetime.datetime.now(tz=datetime.UTC)
    assert local_backup(calendar_tree=True).object_key("ITF_A", "P1.zip") == (
        f"ITF_A/{today:%Y/%m/%d}/P1.zip"
    )


def test_local_backup_writes_at_the_key(local_backup, source_file, tmp_path):
    [report] = local_backup().backup_file(CONFIG, source_file, key="ITF_A/2026/03/04/P1.zip")
    target = tmp_path / "backup" / "ITF_A/2026/03/04/P1.zip"
    assert report.destination_path == str(target)
    assert target.read_bytes() == pathlib.Path(source_file).read_bytes()


def test_s3_backup_writes_every_bucket_at_the_key(source_file):
    backup, client_mock = build_s3_backup(buckets=["b1", "b2"], directory="archive")
    backup.backup_file(CONFIG, source_file, key="ITF_A/P1.zip")
    upload = client_mock.return_value.upload_file
    assert [call.args[1:] for call in upload.call_args_list] == [
        ("b1", "archive/ITF_A/P1.zip"),
        ("b2", "archive/ITF_A/P1.zip"),
    ]


def test_compressed_object_gets_gz_after_the_key(local_backup, tmp_path):
    dat_file = tmp_path / "P1.dat"
    dat_file.write_bytes(b"payload" * 10)
    [report] = local_backup(enable_gzip=True).backup_file(
        CONFIG, str(dat_file), key="ITF_A/P1.dat"
    )
    assert report.destination_path.endswith("ITF_A/P1.dat.gz") and report.compressed


def test_without_key_the_layout_is_unchanged(local_backup, source_file, tmp_path):
    [report] = local_backup().backup_file(CONFIG, source_file)
    assert report.destination_path == str(
        tmp_path / "backup" / "ITF_A" / os.path.basename(source_file)
    )

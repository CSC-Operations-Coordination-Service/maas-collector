import datetime
import hashlib
import json
import logging
import os
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import botocore.exceptions
import pytest
from opensearchpy.exceptions import (
    AuthorizationException,
    ConnectionError as OpenSearchConnectionError,
    RequestError,
    TransportError,
)

from maas_model import datetime_to_zulu

from maas_collector.backup import instanciate_collector_backup
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.transfer.contract import TransferRequest
from maas_collector.rawdata.transfer.conflicts import CONFLICT_INDEX
from maas_collector.rawdata.transfer.download import (
    Downloaded,
    PermanentTransferError,
    TransferStopped,
    TransientTransferError,
)
from maas_collector.rawdata.transfer.reports import (
    REPORT_INDEX,
    ArchiveReportError,
    read_products,
)
from maas_collector.rawdata.transfer.stage import (
    RequestRefused,
    TransferSettings,
    TransferStage,
)

CONTENT = b"x" * 3000
NEW_CONTENT = b"y" * 3000
MD5 = hashlib.md5(CONTENT).hexdigest()
NEW_MD5 = hashlib.md5(NEW_CONTENT).hexdigest()
KEYS = {"s3_access_key": "ACCESS-1", "s3_secret_key": "SECRET-1"}

MODEL = {
    "name": "ProductA",
    "index": "raw-data-a",
    "fields": [
        {"name": "product_id", "type": "Keyword"},
        {"name": "product_name", "type": "Keyword"},
        {"name": "content_length", "type": "Long"},
        {"name": "checksum", "type": "Keyword"},
        {"name": "publication_date", "type": "Date"},
        {"name": "interface_name", "type": "Keyword"},
    ],
    "partition_field": "ingestionTime",
    "partition_format": "%Y",
}
ENTRY = {
    "class": "ODataCollectorConfiguration",
    "interface_name": "ITF_A",
    "id_field": "product_id",
    "routing_key": "",
    "model": "ProductA",
    "odata_product_url": "https://catalogue.example",
    "archive": {"backup": "archive-a"},
    "extractor": {
        "class": "JSONExtractor",
        "args": {"attr_map": {"product_id": "Id", "product_name": "Name"}},
    },
}


class FakeDownloads:
    """the downloader factory of the stage, and the downloaders it builds"""

    def __init__(self, tmp):
        self.tmp = tmp
        self.factory_calls = []
        self.calls = []
        self.directories = []
        self.errors = []
        self.content = CONTENT
        self.after = None

    def __call__(self, config, rule, **kwargs):
        self.factory_calls.append((config, rule, kwargs))
        return SimpleNamespace(download=self.download)

    def download(self, product, directory):
        self.calls.append(product)
        self.directories.append(directory)
        if self.errors:
            error = self.errors.pop(0)
            if isinstance(error, Exception):
                raise error
        path = os.path.join(directory, product["product_name"])
        with open(path, "wb") as output:
            output.write(self.content)
        digest = hashlib.md5(self.content).hexdigest()
        if self.after:
            self.after(product)
        return Downloaded(path, len(self.content), digest, digest)


class FakeSinks:
    """the S3 client behind the sinks: records the uploads, fails the scripted ones"""

    def __init__(self):
        self.uploads = []
        self.calls = []
        self.failing = []

    def client(self, **_kwargs):
        sinks = self

        def upload_file(_path, bucket, key):
            if bucket in sinks.failing:
                sinks.failing.remove(bucket)
                raise botocore.exceptions.ClientError(
                    {"Error": {"Code": "500"}}, "PutObject"
                )
            sinks.uploads.append((bucket, key))

        return Mock(upload_file=upload_file)

    def factory(self, block):
        sink = instanciate_collector_backup(block)
        original = sink.backup_file

        def backup_file(config, path, key=None):
            self.calls.append(list(sink.destinations))
            return original(config, path, key=key)

        sink.backup_file = backup_file
        return sink


class Env:
    """a loaded transfer stage and the fakes around it"""

    def __init__(self, tmp, es, monkeypatch, backup):
        self.tmp = tmp
        self.es = es
        self.sleeps = []
        self.written = 0
        self.downloads = FakeDownloads(tmp)
        self.sinks = FakeSinks()
        monkeypatch.setattr("boto3.client", self.sinks.client)
        conf = tmp / "conf"
        (conf / "backups").mkdir(parents=True)
        (conf / "itf.json").write_text(
            json.dumps({"collectors": [ENTRY], "models": [MODEL]}), encoding="UTF-8"
        )
        (conf / "backups" / "archive-a.json").write_text(
            json.dumps({"backups": [backup]}), encoding="UTF-8"
        )
        credentials = tmp / "credentials.json"
        credentials.write_text(
            json.dumps({"interfaces": [{"name": "backup-archive-a", **KEYS}]}),
            encoding="UTF-8",
        )
        self.publisher = SimpleNamespace(requests=[], publish=lambda r: None)
        self.stage = TransferStage(
            CollectorArgs(
                rawdata_config_dir=str(conf),
                working_directory=str(tmp / "work"),
                credential_file=str(credentials),
                config_store=ConfigStoreArgs(service="svc"),
            ),
            TransferSettings(retry_delay=1.0),
            self.publisher,
            sleep=self.sleeps.append,
            sink_factory=self.sinks.factory,
            downloader_factory=self.downloads,
            report_writer=self.index_reports,
        )
        self.stage.load_config()
        self.stage._healthcheck = Mock()

    def index_reports(self, reports):
        for report in reports:
            source = report.to_dict()
            # one second apart: the last report of a destination is well defined
            source["start_time"] = datetime_to_zulu(
                datetime.datetime(2026, 10, 30, tzinfo=datetime.UTC)
                + datetime.timedelta(seconds=self.written)
            )
            self.written += 1
            self.es.index(index=REPORT_INDEX, id=f"report-{self.written}", body=source)

    def reports(self):
        return list(self.es.documents.get(REPORT_INDEX, {}).values())

    def conflicts(self):
        return list(self.es.documents.get(CONFLICT_INDEX, {}).values())

    def raw(self, document_id, **fields):
        self.es.index(
            index="raw-data-a-2026",
            id=document_id,
            body={
                "product_id": "id-1",
                "product_name": "P1.zip",
                "content_length": 3000,
                "checksum": MD5,
                "publication_date": "2026-03-04T10:00:00.000Z",
                "interface_name": "ITF_A",
            }
            | fields,
        )

    def transfer(self, request):
        """the product flow without the request loop"""
        context = self.stage.request_context(request)
        products, _missing = read_products(self.es, request.documents, context.rule)
        for product in products:
            self.index_reports(self.stage.transfer_product(context, product))

    def seed(self, **fields):
        """an archive report, as an earlier transfer left it"""
        self.written += 1
        source = {
            "origin": "archive",
            "backup_name": "archive-a",
            "interface_name": "ITF_A",
            "product_name": "P1.zip",
            "status": "OK",
            "destination": "b1",
            "catalogue_checksum": MD5,
            "written_md5": MD5,
            "start_time": "2026-10-01T00:00:00.000Z",
            "end_time": "2026-10-01T00:01:00.000Z",
            "operation_id": "",
            "error_type": "",
        } | fields
        self.es.index(index=REPORT_INDEX, id=f"seed-{self.written}", body=source)


LOCAL = {"name": "archive-a", "type": "BackupLocal"}
S3 = {
    "name": "archive-a",
    "type": "BackupS3",
    "directory": "archive",
    "buckets": ["b1", "b2"],
    "credentials": "backup-archive-a",
}


@pytest.fixture(name="env")
def fixture_env(request, tmp_path, fake_opensearch, monkeypatch):
    options = getattr(request, "param", {})
    if options.get("s3"):
        backup = S3
    else:
        (tmp_path / "archive").mkdir()
        backup = LOCAL | {
            "directory": str(tmp_path / "archive"),
            "calendar_tree": options.get("calendar_tree", False),
        }
    env = Env(tmp_path, fake_opensearch, monkeypatch, backup)
    env.raw("d1")
    return env


TREE = pytest.mark.parametrize("env", [{"calendar_tree": True}], indirect=True)
S3_ENV = pytest.mark.parametrize("env", [{"s3": True}], indirect=True)


def request(**changes):
    return replace(
        TransferRequest(
            "svc", "ITF_A", (("d1", "raw-data-a-2026"),), "archive-a", None, "skip"
        ),
        **changes,
    )


OPERATION = dict(operation_id="op-1", origin="download")


def test_new_product_is_transferred_and_reported_ok(env):
    env.transfer(request())
    assert (env.tmp / "archive/ITF_A/P1.zip").read_bytes() == CONTENT
    [report] = env.reports()
    assert {
        key: report[key]
        for key in (
            "status",
            "origin",
            "backup_name",
            "interface_name",
            "product_id",
            "product_name",
            "written_md5",
            "catalogue_checksum",
            "operation_id",
            "source_path",
        )
    } == {
        "status": "OK",
        "origin": "archive",
        "backup_name": "archive-a",
        "interface_name": "ITF_A",
        "product_id": "id-1",
        "product_name": "P1.zip",
        "written_md5": MD5,
        "catalogue_checksum": MD5,
        "operation_id": "",
        "source_path": "",
    }
    assert report["destination"] == str(env.tmp / "archive")
    assert "SECRET-1" not in json.dumps(report)


@TREE
def test_stable_key_by_publication_date(env):
    env.transfer(request())
    assert (env.tmp / "archive/ITF_A/2026/03/04/P1.zip").exists()


@TREE
def test_object_key_uses_the_utc_day_of_an_offset_publication_date(env):
    env.raw("d1", publication_date="2026-03-04T23:30:00-02:00")
    env.transfer(request())
    assert (env.tmp / "archive/ITF_A/2026/03/05/P1.zip").exists()


@TREE
def test_missing_publication_date_with_calendar_tree_is_ko(env):
    env.raw("d1", publication_date=None)
    env.transfer(request())
    assert [(r["status"], "publication_date" in r["details"]) for r in env.reports()] == [
        ("KO", True)
    ]
    assert env.downloads.calls == []


@TREE
def test_unparsable_publication_date_with_calendar_tree_is_ko(env):
    env.raw("d1", publication_date="yesterday")
    env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["KO"] and env.downloads.calls == []


def test_already_archived_product_is_skipped_live_without_report(env):
    env.transfer(request())
    env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["OK"]
    assert len(env.downloads.calls) == 1


def test_already_archived_product_of_an_operation_is_reported_skipped(env):
    env.transfer(request())
    env.transfer(request(**OPERATION))
    assert [(r["status"], r["operation_id"]) for r in env.reports()] == [
        ("OK", ""),
        ("SKIPPED", "op-1"),
    ]


def test_uppercase_catalogue_checksum_matches_the_archived_one(env):
    env.transfer(request())
    env.raw("d1", checksum=MD5.upper())
    env.transfer(request(**OPERATION))
    assert [r["status"] for r in env.reports()] == ["OK", "SKIPPED"]
    assert env.conflicts() == []


def test_changed_checksum_under_skip_opens_a_conflict(env):
    env.transfer(request())
    env.raw("d1", checksum="0" * 32)
    env.transfer(request(**OPERATION))
    assert env.conflicts()[0]["status"] == "OPEN"
    assert len(env.downloads.calls) == 1
    assert (env.reports()[-1]["status"], env.reports()[-1]["error_type"]) == (
        "SKIPPED",
        "ArchiveConflict",
    )


def test_a_live_conflict_writes_no_report(env):
    env.transfer(request())
    env.raw("d1", checksum="0" * 32)
    env.transfer(request())
    assert env.conflicts()[0]["status"] == "OPEN" and len(env.reports()) == 1


def test_changed_checksum_under_replace_if_changed_replaces(env):
    env.transfer(request())
    env.raw("d1", checksum=NEW_MD5)
    env.downloads.content = NEW_CONTENT
    env.transfer(request(duplicates="replace_if_changed"))
    assert [r["status"] for r in env.reports()] == ["OK", "REPLACED"]
    assert (env.tmp / "archive/ITF_A/P1.zip").read_bytes() == NEW_CONTENT


def test_forced_transfer_replaces_and_resolves_the_conflict(env):
    env.transfer(request())
    env.raw("d1", checksum="0" * 32)
    env.transfer(request(**OPERATION))
    assert env.conflicts()[0]["status"] == "OPEN"
    env.downloads.content = NEW_CONTENT
    env.transfer(request(force=True))
    assert env.reports()[-1]["status"] == "REPLACED"
    assert env.conflicts()[0]["status"] == "RESOLVED"


@S3_ENV
def test_mirror_is_filled_when_the_bytes_match(env):
    env.seed()
    env.transfer(request())
    assert env.sinks.uploads == [("b2", "archive/ITF_A/P1.zip")]
    assert env.reports()[-1]["destination"] == "b2"


@S3_ENV
def test_mirror_is_not_filled_with_other_bytes(env):
    env.seed(written_md5="1" * 32)
    env.transfer(request(**OPERATION))
    assert env.sinks.uploads == [] and env.conflicts()[0]["catalogue_checksum"] == MD5
    assert [
        (r["destination"], r["status"], r["error_type"], r["operation_id"])
        for r in env.reports()[-2:]
    ] == [("b1", "SKIPPED", "", "op-1"), ("b2", "SKIPPED", "ArchiveConflict", "op-1")]


@S3_ENV
def test_live_mirror_conflict_writes_no_report(env):
    env.seed(written_md5="1" * 32)
    env.transfer(request())
    assert env.sinks.uploads == [] and env.conflicts()[0]["status"] == "OPEN"
    assert [r["destination"] for r in env.reports()] == ["b1"]


@S3_ENV
def test_failed_bucket_is_retried_alone(env):
    env.sinks.failing = ["b2"]
    env.transfer(request())
    assert env.sinks.calls == [["b1", "b2"], ["b2"]]
    assert [r["status"] for r in env.reports()] == ["OK", "OK"]
    assert env.sleeps == [1.0]


@S3_ENV
def test_a_bucket_that_keeps_failing_ends_ko_after_bounded_attempts(env):
    env.sinks.failing = ["b2", "b2", "b2", "b2"]
    env.transfer(request())
    assert env.sinks.calls == [["b1", "b2"], ["b2"], ["b2"]]
    assert sorted((r["destination"], r["status"]) for r in env.reports()) == [
        ("b1", "OK"),
        ("b2", "KO"),
    ]


def test_download_failure_ends_ko_after_bounded_retries(env):
    env.downloads.errors = [TransientTransferError("down")] * 3
    env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["KO"]
    assert env.sleeps == [1.0, 2.0] and len(env.downloads.calls) == 3
    assert env.reports()[0]["error_type"] == "TransientTransferError"


def test_permanent_download_error_is_ko_at_once(env):
    env.downloads.errors = [PermanentTransferError("gone")]
    env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["KO"]
    assert env.sleeps == [] and len(env.downloads.calls) == 1


def test_unsafe_product_name_is_ko_and_writes_nothing(env):
    env.raw("d1", product_name="../P1.zip")
    env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["KO"]
    assert list((env.tmp / "archive").rglob("*")) == [] and env.downloads.calls == []


def test_working_directory_is_never_named_after_the_product_and_is_removed(env):
    env.transfer(request())
    [directory] = env.downloads.directories
    assert os.path.basename(directory).startswith("transfer-")
    assert "P1" not in directory and "id-1" not in directory
    assert os.listdir(env.tmp / "work") == []


def test_working_directory_is_removed_after_a_failure(env):
    env.downloads.errors = [PermanentTransferError("gone")]
    env.transfer(request())
    assert os.listdir(env.tmp / "work") == []


def test_a_stop_during_the_download_propagates_and_reports_nothing(env):
    env.downloads.errors = [TransferStopped("stop")]
    with pytest.raises(TransferStopped):
        env.transfer(request())
    assert env.reports() == [] and os.listdir(env.tmp / "work") == []


def test_unknown_interface_is_refused(env):
    with pytest.raises(RequestRefused, match="ITF_B"):
        env.stage.request_context(request(interface_name="ITF_B"))


def test_unknown_backup_is_refused(env):
    with pytest.raises(RequestRefused, match="archive-b"):
        env.stage.request_context(request(backup="archive-b"))


def test_request_context_takes_the_choice_of_the_request(env):
    context = env.stage.request_context(
        request(backup_version=None, duplicates="replace_if_changed", force=True)
    )
    assert (context.rule.duplicates, context.rule.force, context.destinations) == (
        "replace_if_changed",
        True,
        (str(env.tmp / "archive"),),
    )
    assert "SECRET" not in repr(context)


def test_entries_are_built_without_their_extractor(env):
    assert env.stage.configs[0].extractor is None
    assert env.stage.configs[0].archive_rule.backup == "archive-a"


@S3_ENV
def test_the_keys_are_read_from_the_credential_entry_and_never_reported(env):
    env.transfer(request())
    assert "SECRET-1" not in json.dumps(env.reports())
    assert "SECRET-1" not in repr(env.stage.request_context(request()))


# the request loop

DOCUMENTS = (("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2026"))


def test_request_of_one_product_returns_nothing_left(env):
    assert env.stage.handle_request(request()) == ()
    assert [r["status"] for r in env.reports()] == ["OK"]


def test_unknown_interface_ends_every_product_ko(env):
    env.stage.handle_request(request(interface_name="ITF_B"))
    assert [(r["status"], "ITF_B" in r["details"]) for r in env.reports()] == [
        ("KO", True)
    ]
    assert env.downloads.calls == []


def test_unknown_backup_ends_every_product_ko(env):
    env.stage.handle_request(request(backup="archive-b"))
    assert [
        (r["status"], r["destination"], "archive-b" in r["details"])
        for r in env.reports()
    ] == [("KO", "", True)]


def test_missing_document_is_ko_naming_it(env):
    env.stage.handle_request(request(documents=(("d9", "raw-data-a-2026"),)))
    [report] = env.reports()
    assert (report["status"], report["product_name"]) == ("KO", "")
    assert "d9" in report["details"] and "raw-data-a-2026" in report["details"]


def test_missing_documents_get_distinct_reports(env):
    env.stage.handle_request(
        request(documents=(("d8", "raw-data-a-2026"), ("d9", "raw-data-a-2026")))
    )
    assert len(env.reports()) == 2


def test_request_of_another_service_is_dropped(env, caplog):
    assert env.stage.handle_request(request(service="other")) == ()
    assert env.reports() == [] and env.downloads.calls == []
    assert "other" in caplog.text


def test_stop_returns_the_documents_left(env):
    env.raw("d2", product_name="P2.zip", product_id="id-2")
    env.downloads.errors = [TransferStopped("stop")]
    assert env.stage.handle_request(request(documents=DOCUMENTS)) == DOCUMENTS
    assert env.reports() == []


def test_stop_between_products_keeps_the_first_report(env):
    env.raw("d2", product_name="P2.zip", product_id="id-2")

    def stop(_product):
        env.stage.should_stop_loop = True

    env.downloads.after = stop
    assert env.stage.handle_request(request(documents=DOCUMENTS)) == (DOCUMENTS[1],)
    assert [r["product_name"] for r in env.reports()] == ["P1.zip"]


def test_a_stop_before_the_first_product_returns_every_document(env):
    env.raw("d2", product_name="P2.zip", product_id="id-2")
    env.stage.should_stop_loop = True
    assert env.stage.handle_request(request(documents=DOCUMENTS)) == DOCUMENTS


def test_same_product_twice_in_a_request_is_transferred_once(env):
    env.raw("d2")
    env.stage.handle_request(request(documents=DOCUMENTS))
    assert len(env.downloads.calls) == 1
    assert [r["status"] for r in env.reports()] == ["OK"]


def test_report_write_is_retried_then_logged_critical(env, caplog):
    def failing(_reports):
        raise ArchiveReportError("down")

    env.stage.report_writer = failing
    with caplog.at_level(logging.CRITICAL):
        env.stage.handle_request(request())
    assert env.sleeps == [1.0, 2.0]
    assert any(record.levelname == "CRITICAL" for record in caplog.records)
    assert "P1.zip" in caplog.text
    assert env.stage._healthcheck.tick.called


def test_an_unexpected_error_ends_the_product_ko_and_the_loop_goes_on(env):
    env.raw("d2", product_name="P2.zip", product_id="id-2")
    env.downloads.errors = [RuntimeError("boom")]
    assert env.stage.handle_request(request(documents=DOCUMENTS)) == ()
    assert [(r["product_name"], r["status"]) for r in env.reports()] == [
        ("P1.zip", "KO"),
        ("P2.zip", "OK"),
    ]


def test_changed_credentials_rebuild_the_downloader(env):
    env.stage.handle_request(request())
    env.stage.on_credentials_changed(["ITF_A"])
    env.stage.handle_request(request(force=True))
    assert len(env.downloads.factory_calls) == 2


def test_the_downloader_is_kept_between_requests(env):
    env.stage.handle_request(request())
    env.stage.handle_request(request(force=True))
    assert len(env.downloads.factory_calls) == 1


def raising(error):
    def fail(*_args, **_kwargs):
        raise error

    return fail


@pytest.mark.parametrize(
    "error",
    [
        OpenSearchConnectionError("N/A", "refused", Exception("refused")),
        TransportError(503, "unavailable", {}),
        TransportError(429, "too_many_requests", {}),
    ],
)
def test_a_transient_failure_resolving_the_backup_republishes_the_documents(
    env, monkeypatch, error
):
    monkeypatch.setattr(
        "maas_collector.rawdata.transfer.stage.resolve_request_backup", raising(error)
    )
    assert env.stage.handle_request(request()) == (("d1", "raw-data-a-2026"),)
    assert env.sleeps == [1.0] and env.reports() == []
    assert env.downloads.calls == []


def test_a_permanent_failure_resolving_the_backup_ends_ko_without_the_value(
    env, monkeypatch
):
    monkeypatch.setattr(
        "maas_collector.rawdata.transfer.stage.build_sink",
        raising(RuntimeError("secret-value")),
    )
    assert env.stage.handle_request(request()) == ()
    [report] = env.reports()
    assert report["status"] == "KO" and "RuntimeError" in report["details"]
    assert "secret-value" not in json.dumps(report)
    assert env.sleeps == [] and env.downloads.calls == []


def test_a_client_error_resolving_the_backup_ends_ko(env, monkeypatch):
    monkeypatch.setattr(
        "maas_collector.rawdata.transfer.stage.resolve_request_backup",
        raising(AuthorizationException(403, "forbidden", {})),
    )
    assert env.stage.handle_request(request()) == ()
    assert [r["status"] for r in env.reports()] == ["KO"]


@pytest.mark.parametrize(
    "error",
    [
        OpenSearchConnectionError("N/A", "refused", Exception("refused")),
        TransportError(502, "bad_gateway", {}),
        TransportError(429, "too_many_requests", {}),
    ],
)
def test_a_transient_read_failure_republishes_the_documents(env, monkeypatch, error):
    monkeypatch.setattr(env.es, "get", raising(error))
    assert env.stage.handle_request(request()) == (("d1", "raw-data-a-2026"),)
    assert env.sleeps == [1.0] and env.reports() == []


@pytest.mark.parametrize(
    "error",
    [
        AuthorizationException(403, "forbidden", {}),
        RequestError(400, "invalid_index_name_exception", {}),
    ],
)
def test_an_unreadable_document_ends_ko_instead_of_waiting_forever(
    env, monkeypatch, error
):
    monkeypatch.setattr(env.es, "get", raising(error))
    assert env.stage.handle_request(request()) == ()
    [report] = env.reports()
    assert (report["status"], report["error_type"]) == ("KO", "DocumentNotReadable")
    assert "d1" in report["details"] and "raw-data-a-2026" in report["details"]
    assert type(error).__name__ in report["details"]
    assert env.sleeps == [] and env.downloads.calls == []

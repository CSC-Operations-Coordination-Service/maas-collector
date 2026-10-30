import logging
from dataclasses import replace
from types import SimpleNamespace

import pytest

from maas_collector.rawdata.replay import ReplayArgs
from maas_collector.rawdata.transfer import requester as requester_module
from maas_collector.rawdata.transfer.amqp import TransferPublishError
from maas_collector.rawdata.transfer.contract import TransferRequest
from maas_collector.rawdata.transfer.reports import ArchiveReportError
from maas_collector.rawdata.transfer.requester import TransferRequester
from maas_collector.rawdata.transfer.rule import ArchiveRule

REPLAY = ReplayArgs(
    ["ITF_A"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", "op-1", 3, requested_by="alice"
)


class FakePublisher:
    def __init__(self, events):
        self.events = events
        self.requests = []
        self.fail = []
        self.closed = False

    def publish(self, request):
        self.events.append("publish")
        if self.fail:
            raise self.fail.pop(0)
        self.requests.append(request)

    def close(self):
        self.closed = True


class FakeWriter:
    def __init__(self, events):
        self.events = events
        self.reports = []
        self.fail = None

    def __call__(self, reports):
        self.events.append("reports")
        if self.fail:
            raise self.fail
        self.reports.extend(reports)


@pytest.fixture(name="config")
def fixture_config():
    return SimpleNamespace(
        interface_name="ITF_A",
        model_name="ProductA",
        archive_rule=ArchiveRule.default("archive-a"),
    )


@pytest.fixture(name="env")
def fixture_env(fake_opensearch):
    events = []
    publisher = FakePublisher(events)
    writer = FakeWriter(events)

    def build(replay):
        return TransferRequester(
            publisher, "svc", replay, client_factory=lambda: fake_opensearch, report_writer=writer
        )

    return SimpleNamespace(
        events=events,
        publisher=publisher,
        writer=writer,
        live=build(None),
        replay=build(REPLAY),
        es=fake_opensearch,
    )


def raw(env, document_id, name, index="i1", **fields):
    env.es.index(
        index=index,
        id=document_id,
        body={
            "product_id": f"id-{document_id}",
            "product_name": name,
            "content_length": 3,
            "checksum": "abc",
        }
        | fields,
    )


def test_live_requests_name_the_created_and_updated_documents(env, config):
    env.live.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [("d3", "i0")])
    assert env.publisher.requests == [
        TransferRequest(
            "svc", "ITF_A", (("d1", "i1"), ("d2", "i1")), "archive-a", None, "skip",
            False, "", "", "live",
        )
    ]


def test_replay_requests_name_every_extracted_document(env, config):
    env.replay.documents_indexed(config, [("d1", "i1")], [("d3", "i0")])
    [request] = env.publisher.requests
    assert (
        request.documents,
        request.origin,
        request.operation_id,
        request.requested_by,
    ) == ((("d1", "i1"), ("d3", "i0")), "recollect", "op-1", "alice")


def test_requests_carry_at_most_ten_documents(env, config):
    env.live.documents_indexed(config, [(f"d{n}", "i1") for n in range(25)], [])
    assert [len(request.documents) for request in env.publisher.requests] == [10, 10, 5]


def test_a_document_is_requested_once(env, config):
    env.replay.documents_indexed(config, [("d1", "i1")], [("d1", "i1")])
    assert env.publisher.requests[0].documents == (("d1", "i1"),)


def test_entry_without_rule_requests_nothing(env, config):
    env.live.documents_indexed(replace_rule(config, None), [("d1", "i1")], [])
    assert env.publisher.requests == []


def replace_rule(config, rule):
    return SimpleNamespace(**vars(config) | {"archive_rule": rule})


def test_recollect_choice_is_carried(env, config):
    rule = replace(
        ArchiveRule.default("archive-a"),
        duplicates="replace_if_changed",
        backup_version=4,
        force=True,
    )
    env.live.documents_indexed(replace_rule(config, rule), [("d1", "i1")], [])
    [request] = env.publisher.requests
    assert (request.duplicates, request.backup_version, request.force) == (
        "replace_if_changed",
        4,
        True,
    )


def test_operation_writes_one_requested_report_per_product_before_publishing(env, config):
    raw(env, "d1", "P1.zip")
    raw(env, "d3", "P3.zip", index="i0")
    env.replay.documents_indexed(config, [("d1", "i1")], [("d3", "i0")])
    assert env.events == ["reports", "publish"]
    assert [(r.status, r.product_name, r.operation_id) for r in env.writer.reports] == [
        ("REQUESTED", "P1.zip", "op-1"),
        ("REQUESTED", "P3.zip", "op-1"),
    ]


def test_operation_reports_a_missing_document_as_requested_too(env, config):
    raw(env, "d1", "P1.zip")
    env.replay.documents_indexed(config, [("d1", "i1"), ("d9", "i1")], [])
    assert env.events == ["reports", "publish"] and len(env.publisher.requests) == 1
    assert [(r.status, r.product_name, r.details) for r in env.writer.reports] == [
        ("REQUESTED", "P1.zip", ""),
        ("REQUESTED", "", "document d9 not found in i1"),
    ]


def test_replay_without_operation_id_writes_no_report(env, config):
    requester = TransferRequester(
        env.publisher,
        "svc",
        replace(REPLAY, suffix=""),
        client_factory=lambda: env.es,
        report_writer=env.writer,
    )
    requester.documents_indexed(config, [("d1", "i1"), ("d9", "i1")], [])
    assert env.writer.reports == [] and len(env.publisher.requests) == 1


def test_failed_publish_in_replay_is_counted(env, config):
    env.publisher.fail = [TransferPublishError("no queue")]
    env.replay.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [])
    assert (env.replay.failures, env.replay.pending) == (2, [])


def test_failed_report_write_counts_and_publishes_nothing(env, config):
    env.writer.fail = ArchiveReportError("down")
    env.replay.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [])
    assert (env.replay.failures, env.publisher.requests) == (2, [])


def test_failed_live_publish_is_kept_for_the_next_loop(env, config):
    env.publisher.fail = [TransferPublishError("down")]
    env.live.documents_indexed(config, [("d1", "i1")], [])
    assert len(env.live.pending) == 1 and env.publisher.requests == []
    env.live.retry_pending()
    assert env.live.pending == [] and len(env.publisher.requests) == 1


def test_retry_stops_at_the_first_failure_and_keeps_the_order(env, config):
    env.publisher.fail = [TransferPublishError("down")] * 2
    env.live.documents_indexed(config, [("d1", "i1")], [])
    env.live.documents_indexed(config, [("d2", "i1")], [])
    env.publisher.fail = [TransferPublishError("still down")]
    env.live.retry_pending()
    assert [request.documents for request in env.live.pending] == [
        (("d1", "i1"),),
        (("d2", "i1"),),
    ]
    env.live.retry_pending()
    assert env.live.pending == []
    assert [request.documents for request in env.publisher.requests] == [
        (("d1", "i1"),),
        (("d2", "i1"),),
    ]


def test_pending_requests_are_bounded(env, config, monkeypatch, caplog):
    monkeypatch.setattr(requester_module, "MAX_PENDING_REQUESTS", 2)
    env.publisher.fail = [TransferPublishError("down")] * 3
    with caplog.at_level(logging.CRITICAL):
        for name in ("d1", "d2", "d3"):
            env.live.documents_indexed(config, [(name, "i1")], [])
    assert [request.documents for request in env.live.pending] == [
        (("d2", "i1"),),
        (("d3", "i1"),),
    ]
    assert "dropped" in caplog.text


def test_close_closes_the_publisher(env):
    env.live.close()
    assert env.publisher.closed

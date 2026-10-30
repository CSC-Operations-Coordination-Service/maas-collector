import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

import kombu
import pytest

from maas_collector.rawdata.cli import transfer as transfer_cli
from maas_collector.rawdata.cli.transfer import transfer_main
from maas_collector.rawdata.collector.filecollector import CollectorArgs, FileCollector
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.transfer import stage as stage_module
from maas_collector.rawdata.transfer.amqp import (
    TransferPublisher,
    take_body,
)
from maas_collector.rawdata.transfer.contract import TransferRequest, transfer_queue
from maas_collector.rawdata.transfer.stage import TransferSettings, TransferStage

REQUEST = TransferRequest("svc", "ITF_A", (("d1", "i1"),), "archive-a", None, "skip")
BODY_1 = json.dumps(REQUEST.to_body()).encode()
BODY_2 = json.dumps(replace(REQUEST, documents=(("d2", "i1"),)).to_body()).encode()


def memory():
    return kombu.Connection("memory://")


class FakePublisher:
    def __init__(self):
        self.requests = []
        self.fail = None

    def publish(self, request):
        if self.fail:
            raise self.fail
        self.requests.append(request)


def make_stage(bodies):
    """a stage whose queue is the list of bodies, and whose requests are recorded"""
    handled = []
    pending = iter(bodies)

    def take():
        return next(pending)

    stage = TransferStage(
        CollectorArgs(
            amqp_url="memory://", config_store=ConfigStoreArgs(service="svc")
        ),
        TransferSettings(),
        FakePublisher(),
        take=take,
    )
    stage._healthcheck = SimpleNamespace(tick=lambda: None)
    stage.left = ()

    def handle_request(request):
        handled.append(request)
        return stage.left

    stage.handle_request = handle_request
    return stage, handled


@pytest.fixture(name="queue")
def fixture_queue():
    with memory() as connection:
        bound = transfer_queue("svc")(connection.default_channel)
        bound.declare()
        bound.purge()
    yield
    with memory() as connection:
        transfer_queue("svc")(connection.default_channel).purge()


def test_stage_setup_declares_its_queue(monkeypatch):
    # a queue name no other test uses: the memory broker is shared by the process
    stage, _ = make_stage([])
    stage.args.config_store = ConfigStoreArgs(service="svc-declared")
    monkeypatch.setattr(FileCollector, "setup", lambda self: None)
    stage.setup()
    request = replace(REQUEST, service="svc-declared")
    TransferPublisher(memory).publish(request)
    assert TransferRequest.from_body(take_body(memory, "svc-declared")) == request


def test_cli_requires_a_service(monkeypatch):
    monkeypatch.delenv("CONFIG_SERVICE", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        transfer_main([])
    assert exit_info.value.code == 2


def test_cli_requires_opensearch_and_amqp(monkeypatch, capsys):
    for name in ("ES_URL", "ES_VAULT_PATH", "AMQP_URL", "AMQP_VAULT_PATH"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(SystemExit) as exit_info:
        transfer_main(["--config-service", "svc"])
    assert exit_info.value.code == 2 and "ES_URL" in capsys.readouterr().err


def test_cli_builds_the_stage_from_the_collector_options(monkeypatch):
    built = []
    run_calls = []

    def run(self, path_list):
        built.append(self)
        run_calls.append(tuple(path_list))

    monkeypatch.setattr(TransferStage, "setup", lambda self: None)
    monkeypatch.setattr(TransferStage, "run", run)
    code = transfer_main(
        [
            "--config-service",
            "svc",
            "--transfer-max-attempts",
            "5",
            "--es-url",
            "http://os:9200",
            "--amqp-url",
            "amqp://broker//",
        ]
    )
    [stage] = built
    assert code == 0
    assert (stage.settings.max_attempts, stage.args.config_store.service) == (5, "svc")
    assert stage.settings.retry_delay == 5.0 and stage.settings.max_download_seconds == 900.0
    assert run_calls == [("transfer",)]


def test_cli_settings_come_from_the_environment(monkeypatch):
    built = []
    monkeypatch.setenv("TRANSFER_RETRY_DELAY", "2.5")
    monkeypatch.setattr(TransferStage, "setup", lambda self: None)
    monkeypatch.setattr(TransferStage, "run", lambda self, path_list: built.append(self))
    transfer_main(
        ["--config-service", "svc", "--es-url", "http://os:9200", "--amqp-url", "amqp://b//"]
    )
    assert built[0].settings.retry_delay == 2.5


def test_loop_body_drains_the_queue():
    stage, handled = make_stage([BODY_1, BODY_2, None])
    stage.loop_body(["transfer"])
    assert [request.documents for request in handled] == [
        (("d1", "i1"),),
        (("d2", "i1"),),
    ]


def test_loop_body_takes_from_the_real_queue(queue):
    publisher = TransferPublisher(memory)
    publisher.publish(REQUEST)
    stage, handled = make_stage([])
    stage.take = stage.take_from_queue
    stage.loop_body(["transfer"])
    assert handled == [REQUEST]


def test_malformed_request_is_dropped_and_the_loop_goes_on(caplog):
    stage, handled = make_stage([b"{", BODY_1, None])
    stage.loop_body(["transfer"])
    assert len(handled) == 1 and "malformed" in caplog.text


def test_an_unreachable_broker_ends_the_loop_without_raising(caplog):
    def take():
        raise OSError("broker gone")

    stage, handled = make_stage([])
    stage.take = take
    stage.loop_body(["transfer"])
    assert handled == [] and "broker gone" in caplog.text


def test_documents_left_by_a_stop_are_republished():
    stage, _ = make_stage([BODY_1, None])
    stage.left = (("d2", "i1"),)
    stage.loop_body(["transfer"])
    assert stage.publisher.requests == [replace(REQUEST, documents=(("d2", "i1"),))]


def test_documents_that_cannot_be_republished_are_logged_critical(caplog):
    stage, _ = make_stage([BODY_1, None])
    stage.left = (("d2", "i1"),)
    stage.publisher.fail = OSError("down")
    stage.loop_body(["transfer"])
    assert "CRITICAL" in caplog.text and "d2" in caplog.text


def test_loop_body_returns_after_max_loop_seconds(monkeypatch):
    stage, handled = make_stage([])
    stage.take = lambda: BODY_1

    monkeypatch.setattr(
        stage_module.time, "monotonic", lambda: 61 if handled else 0
    )
    stage.loop_body(["transfer"])
    assert len(handled) == 1


def test_loop_body_returns_when_the_stage_stops():
    stage, handled = make_stage([BODY_1, BODY_2, None])
    stage.should_stop_loop = True
    stage.loop_body(["transfer"])
    assert handled == []

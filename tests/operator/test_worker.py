import datetime
import logging
import sys
import threading
import time
import uuid

import kombu
import pytest

from maas_collector.rawdata.cli.operator_worker import (
    build_settings,
    build_parser,
    operator_worker_main,
)
from maas_collector.rawdata.operator.contract import (
    LANES,
    CheckReply,
    CheckRequest,
    ReplayRequest,
    check_queue_name,
    check_routing_key,
    exchange,
    replay_queue_name,
    replay_routing_key,
)
from maas_collector.rawdata.operator.records import FAILED
from maas_collector.rawdata.operator.replay import ChildRunner
from maas_collector.rawdata.operator.worker import OperatorSettings, OperatorWorker

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)

ENVIRONMENT = (
    "CONFIG_SERVICE",
    "CONFIG_INDEX",
    "OPERATOR_OPERATION_INDEX",
    "OPERATOR_SHORT_TIMEOUT",
    "OPERATOR_LONG_TIMEOUT_PER_DAY",
    "OPERATOR_POLL_INTERVAL",
    "OPERATOR_HEALTHCHECK_PORT",
    "OPERATOR_WORKDIR",
    "OPERATOR_CHECK_TIMEOUT",
)

REQ_LONG = ReplayRequest(
    "op-long", "ITF_A", START, START + datetime.timedelta(days=3), "bob", "long"
)
REQ_SHORT = ReplayRequest(
    "op-short", "ITF_A", START, START + datetime.timedelta(hours=2), "bob", "short"
)


def connect():
    return kombu.Connection("memory://")


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


class FakeRecorder:
    def __init__(self):
        self.updates = []

    def update(self, operation_id, status, exit_code=None, details=None):
        self.updates.append((operation_id, status, exit_code, details))
        return True


class FakeRunner:
    def __init__(self):
        self.terminated = 0

    def terminate(self):
        self.terminated += 1


class FakeLane:
    def __init__(self, block=None, runner=None):
        self.handled = []
        self.started = threading.Event()
        self.block = block
        self.recorder = FakeRecorder()
        self.runner = runner or FakeRunner()

    def handle(self, request):
        self.handled.append(request)
        self.started.set()
        if self.block is not None:
            self.block.wait(5)


class SleepingLane(FakeLane):
    """runs a child that sleeps a minute with a real ChildRunner"""

    def __init__(self):
        super().__init__(runner=ChildRunner())
        self.results = []

    def handle(self, request):
        self.handled.append(request)
        self.started.set()
        self.results.append(
            self.runner.run(
                [sys.executable, "-c", "import time; time.sleep(60)"],
                {},
                60,
                lambda lines: None,
            )
        )


class FakeChecker:
    def __init__(self):
        self.requests = []

    def check(self, request):
        self.requests.append(request)
        return CheckReply("ok", remote={"name": request.product_name})


class Harness:
    """an OperatorWorker on the memory transport, run in a thread"""

    def __init__(self, lanes=None, checker=None):
        self.service = "svc" + uuid.uuid4().hex[:8]
        self.lanes = lanes or {lane: FakeLane() for lane in LANES}
        self.checker = checker or FakeChecker()
        self.settings = OperatorSettings(service=self.service, poll_interval=0.01)
        self.worker = OperatorWorker(
            connect, self.settings, self.lanes, self.checker, sleep=time.sleep
        )
        self.thread = threading.Thread(target=self.worker.run, daemon=True)
        self.declare()

    def declare(self):
        """the queues the backend relies on, so that nothing published is lost"""
        with connect() as connection:
            channel = connection.default_channel
            for lane in LANES:
                kombu.Queue(
                    replay_queue_name(self.service, lane),
                    exchange=exchange(),
                    routing_key=replay_routing_key(self.service, lane),
                    durable=True,
                )(channel).declare()
            kombu.Queue(
                check_queue_name(self.service),
                exchange=exchange(),
                routing_key=check_routing_key(self.service),
                durable=True,
            )(channel).declare()

    def publish(self, routing_key, body, **properties):
        with connect() as connection:
            connection.Producer().publish(
                body,
                exchange=exchange(),
                routing_key=routing_key,
                serializer="json" if isinstance(body, dict) else None,
                **properties,
            )

    def queue_size(self, name):
        with connect() as connection:
            return (
                kombu.Queue(name)(connection.default_channel)
                .queue_declare(passive=True)
                .message_count
            )

    def start(self):
        self.thread.start()

    def stop(self):
        self.worker.stop()
        self.thread.join(10)
        assert not self.thread.is_alive()


@pytest.fixture
def harness():
    harness = Harness()
    yield harness
    if harness.thread.is_alive():
        harness.stop()


def reply_queue():
    name = "reply-" + uuid.uuid4().hex[:8]
    with connect() as connection:
        kombu.Queue(name, durable=False)(connection.default_channel).declare()
    return name


def get_reply(name, timeout=5.0):
    deadline = time.monotonic() + timeout
    with connect() as connection:
        queue = kombu.Queue(name)(connection.default_channel)
        while time.monotonic() < deadline:
            message = queue.get(no_ack=True)
            if message is not None:
                return message
            time.sleep(0.01)
    raise AssertionError(f"no reply on {name}")


def test_replay_message_reaches_its_lane(harness):
    harness.start()
    harness.publish(replay_routing_key(harness.service, "long"), REQ_LONG.to_body())

    wait_for(lambda: harness.lanes["long"].handled)
    harness.stop()

    assert harness.lanes["long"].handled == [REQ_LONG]
    assert harness.lanes["short"].handled == []


def test_replay_request_is_acked_on_receipt(harness):
    block = threading.Event()
    harness.lanes["short"].block = block
    harness.start()
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())

    wait_for(harness.lanes["short"].started.is_set)
    # still running, already gone from the queue
    assert harness.queue_size(replay_queue_name(harness.service, "short")) == 0
    block.set()
    harness.stop()


def test_malformed_replay_body_is_skipped_and_worker_keeps_running(harness, caplog):
    harness.start()
    with caplog.at_level(logging.ERROR, logger="maas_collector.operator"):
        harness.publish(replay_routing_key(harness.service, "short"), "not json")
        harness.publish(
            replay_routing_key(harness.service, "short"), REQ_SHORT.to_body()
        )
        wait_for(lambda: harness.lanes["short"].handled)
    harness.stop()

    assert harness.lanes["short"].handled == [REQ_SHORT]
    assert harness.lanes["short"].recorder.updates == []
    assert harness.queue_size(replay_queue_name(harness.service, "short")) == 0
    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "malformed replay request" in errors[0].getMessage()


def test_malformed_body_with_operation_id_records_failed(harness):
    harness.start()
    harness.publish(
        replay_routing_key(harness.service, "long"),
        {"operation_id": "op9", "interface_name": "ITF_A"},
    )

    recorder = harness.lanes["long"].recorder
    wait_for(lambda: recorder.updates)
    harness.stop()

    assert harness.lanes["long"].handled == []
    [(operation_id, status, exit_code, details)] = recorder.updates
    assert (operation_id, status, exit_code) == ("op9", FAILED, None)
    assert "malformed replay request" in details
    assert "start_date" in details


def test_check_reply_goes_to_reply_to_with_correlation_id(harness):
    harness.start()
    reply_to = reply_queue()
    request = CheckRequest("op-check", "ITF_A", product_name="S1A_X.SAFE")
    harness.publish(
        check_routing_key(harness.service),
        request.to_body(),
        reply_to=reply_to,
        correlation_id="corr-1",
    )

    message = get_reply(reply_to)
    harness.stop()

    assert harness.checker.requests == [request]
    assert message.payload == CheckReply("ok", remote={"name": "S1A_X.SAFE"}).to_body()
    assert message.properties["correlation_id"] == "corr-1"
    # milliseconds on the wire
    assert message.properties["expiration"] == "60000"
    assert message.delivery_info["exchange"] == ""
    assert message.delivery_info["routing_key"] == reply_to


def test_check_without_reply_to_is_dropped(harness, caplog):
    harness.start()
    request = CheckRequest("op-check", "ITF_A", product_name="S1A_X.SAFE")
    with caplog.at_level(logging.WARNING, logger="maas_collector.operator"):
        harness.publish(check_routing_key(harness.service), request.to_body())
        wait_for(
            lambda: harness.queue_size(check_queue_name(harness.service)) == 0
            and any("reply_to" in r.getMessage() for r in caplog.records)
        )
    harness.stop()

    assert harness.checker.requests == []


def test_malformed_check_gets_an_error_reply(harness):
    harness.start()
    reply_to = reply_queue()
    harness.publish(
        check_routing_key(harness.service),
        {"operation_id": "op-check", "interface_name": "ITF_A"},
        reply_to=reply_to,
        correlation_id="corr-2",
    )

    message = get_reply(reply_to)
    harness.stop()

    assert harness.checker.requests == []
    assert message.payload["status"] == "error"
    assert "product_name" in message.payload["error"]
    assert message.properties["correlation_id"] == "corr-2"


def test_check_is_answered_while_a_long_replay_runs(harness):
    block = threading.Event()
    harness.lanes["long"].block = block
    harness.start()
    harness.publish(replay_routing_key(harness.service, "long"), REQ_LONG.to_body())
    wait_for(harness.lanes["long"].started.is_set)

    reply_to = reply_queue()
    harness.publish(
        check_routing_key(harness.service),
        CheckRequest("op-check", "ITF_A", product_id="abc").to_body(),
        reply_to=reply_to,
        correlation_id="corr-3",
    )
    message = get_reply(reply_to)

    # the replay is still running
    assert not block.is_set()
    assert message.properties["correlation_id"] == "corr-3"
    block.set()
    harness.stop()


def test_stop_terminates_a_running_child():
    lane = SleepingLane()
    harness = Harness(lanes={"short": lane, "long": FakeLane()})
    harness.start()
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())
    wait_for(lane.started.is_set)
    # let the child start
    wait_for(lambda: lane.runner._child is not None)

    started = time.monotonic()
    harness.stop()

    assert time.monotonic() - started < 5
    [(code, _tail)] = lane.results
    assert code != 0


def test_stop_calls_terminate_on_every_lane_runner(harness):
    harness.worker.stop()

    assert [lane.runner.terminated for lane in harness.lanes.values()] == [1, 1]


def test_no_request_is_taken_after_stop(harness):
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())
    harness.worker.stop()

    harness.worker.run()

    assert harness.lanes["short"].handled == []
    assert harness.queue_size(replay_queue_name(harness.service, "short")) == 1


def test_cli_requires_a_service(monkeypatch):
    monkeypatch.delenv("CONFIG_SERVICE", raising=False)
    with pytest.raises(SystemExit) as exit_info:
        operator_worker_main([])
    # argparse: CONFIG_SERVICE / --config-service missing
    assert exit_info.value.code == 2


def test_cli_settings_from_environment(monkeypatch):
    monkeypatch.setenv("CONFIG_SERVICE", "odata")
    monkeypatch.setenv("CONFIG_INDEX", "my-config")
    monkeypatch.setenv("OPERATOR_SHORT_TIMEOUT", "60")
    monkeypatch.setenv("OPERATOR_LONG_TIMEOUT_PER_DAY", "120.5")
    monkeypatch.setenv("OPERATOR_POLL_INTERVAL", "2")
    monkeypatch.setenv("OPERATOR_HEALTHCHECK_PORT", "19000")
    monkeypatch.setenv("OPERATOR_WORKDIR", "/work")
    monkeypatch.setenv("OPERATOR_CHECK_TIMEOUT", "15")
    monkeypatch.setenv("OPERATOR_OPERATION_INDEX", "my-operation")

    namespace = build_parser().parse_args([])

    assert build_settings(namespace) == OperatorSettings(
        service="odata",
        config_index="my-config",
        operation_index="my-operation",
        short_timeout=60.0,
        long_timeout_per_day=120.5,
        poll_interval=2.0,
        healthcheck_port=19000,
        workdir="/work",
        check_timeout=15.0,
    )


def test_cli_settings_defaults(monkeypatch):
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)

    namespace = build_parser().parse_args(["--config-service", "odata"])

    assert build_settings(namespace) == OperatorSettings(service="odata")

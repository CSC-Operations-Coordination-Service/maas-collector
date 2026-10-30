import datetime
import logging
import sys
import threading
import time
import uuid
from unittest.mock import patch

import kombu
import pytest
from maas_engine.replay import ChildRunner, Lane, LaneTimeout, ReplayWorker

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
from maas_collector.rawdata.operator.worker import (
    CheckWorker,
    OperatorSettings,
    OperatorWorker,
)

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
    "OPERATOR_REPLAY_RETRY",
    "OPERATOR_SINGLE_LANE",
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


class FakeHandler:
    """the collector's handler, replaced: records the requests, runs a child"""

    def __init__(self, code="print('replayed')"):
        self.code = code
        self.requests = []
        self.started = threading.Event()
        self.block = None

    def prepare(self, request, lane, workdir):
        self.requests.append((lane, request.operation_id))
        self.started.set()
        if self.block is not None:
            self.block.wait(5)
        return None

    def command(self, prepared, request, lane):
        return [sys.executable, "-u", "-c", self.code], {}

    def cleanup(self, prepared):
        pass


class FakeChecker:
    def __init__(self):
        self.requests = []

    def check(self, request):
        self.requests.append(request)
        return CheckReply("ok", remote={"name": request.product_name})


class Harness:
    """an OperatorWorker on the memory transport, run in a thread"""

    def __init__(self, handler=None, checker=None, service=None):
        self.service = service or "svc" + uuid.uuid4().hex[:8]
        self.handler = handler or FakeHandler()
        self.checker = checker or FakeChecker()
        self.recorder = FakeRecorder()
        self.settings = OperatorSettings(service=self.service, poll_interval=0.01)
        lanes = [
            Lane(
                lane,
                replay_queue_name(self.service, lane),
                replay_routing_key(self.service, lane),
                LaneTimeout(base=60),
            )
            for lane in LANES
        ]
        self.replays = ReplayWorker(
            connect,
            self.recorder,
            self.handler,
            lanes,
            exchange(),
            "/tmp",
            poll_interval=0.01,
        )
        self.worker = OperatorWorker(
            self.replays,
            CheckWorker(connect, self.settings, self.checker, sleep=time.sleep),
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


def test_replay_message_reaches_the_lane_of_its_queue(harness):
    harness.start()
    harness.publish(replay_routing_key(harness.service, "long"), REQ_LONG.to_body())
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())

    wait_for(
        lambda: {u[0] for u in harness.recorder.updates if u[1] == "DONE"}
        == {"op-long", "op-short"}
    )
    harness.stop()

    assert sorted(harness.handler.requests) == [
        ("long", "op-long"),
        ("short", "op-short"),
    ]


def test_replay_request_is_acked_on_receipt(harness):
    block = threading.Event()
    harness.handler.block = block
    harness.start()
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())

    wait_for(harness.handler.started.is_set)
    # still running, already gone from the queue
    assert harness.queue_size(replay_queue_name(harness.service, "short")) == 0
    block.set()
    harness.stop()


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
    harness.handler.block = block
    harness.start()
    harness.publish(replay_routing_key(harness.service, "long"), REQ_LONG.to_body())
    wait_for(harness.handler.started.is_set)

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
    handler = FakeHandler("import time; time.sleep(60)")
    harness = Harness(handler=handler)
    harness.start()
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())
    wait_for(handler.started.is_set)
    # let the child start
    wait_for(lambda: harness.replays.runners["short"]._child is not None)

    started = time.monotonic()
    harness.stop()

    assert time.monotonic() - started < 5
    assert harness.recorder.updates[-1][1] == "FAILED"


def test_stop_stops_the_checks_and_the_replays(harness):
    harness.worker.stop()

    assert harness.worker.checks.should_stop
    assert harness.replays.should_stop


def test_no_request_is_taken_after_stop(harness):
    harness.publish(replay_routing_key(harness.service, "short"), REQ_SHORT.to_body())
    harness.worker.stop()

    harness.worker.run()

    assert harness.handler.requests == []
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
    monkeypatch.setenv("OPERATOR_REPLAY_RETRY", "7")
    monkeypatch.setenv("OPERATOR_SINGLE_LANE", "true")

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
        replay_retry=7,
        single_lane=True,
    )


def test_cli_settings_defaults(monkeypatch):
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)

    namespace = build_parser().parse_args(["--config-service", "odata"])

    assert build_settings(namespace) == OperatorSettings(service="odata")
    assert OperatorSettings(service="odata").replay_retry == 3


def test_cli_wires_the_collector_handler_and_the_lanes(monkeypatch, fake_opensearch):
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    module = "maas_collector.rawdata.cli.operator_worker"

    with patch(f"{module}.OperatorReplayWorker") as worker_class, patch(
        f"{module}.OperatorWorker"
    ), patch(f"{module}.ProductChecker") as checker_class, patch(
        f"{module}.build_credential_provider", side_effect=lambda _args: object()
    ), patch(
        f"{module}.signal.signal"
    ), patch(
        f"{module}.es_connections"
    ) as connections:
        connections.get_connection.return_value = fake_opensearch
        operator_worker_main(
            [
                "--config-service",
                "odata",
                "--es-url",
                "http://localhost:9200",
                "--amqp-url",
                "memory://",
                "--operator-replay-retry",
                "5",
                "--operator-healthcheck-port",
                "19000",
                "--operator-short-timeout",
                "60",
                "--operator-long-timeout-per-day",
                "20",
            ]
        )

    _connect, _recorder, handler, lanes, _exchange, _workdir = (
        worker_class.call_args.args
    )
    # its own provider: a load rewrites `unreadable`, which the checker reads
    assert handler.credential_provider is not checker_class.call_args.args[2]
    assert handler.credential_provider is not None
    assert (handler.replay_retry, handler.healthcheck_port, handler.service) == (
        5,
        19000,
        "odata",
    )
    assert handler.lanes == ("short", "long")
    assert [(lane.name, lane.queue, lane.routing_key) for lane in lanes] == [
        (
            "short",
            "maas-operator-odata-replay-short",
            "maas.operator.odata.replay.short",
        ),
        ("long", "maas-operator-odata-replay-long", "maas.operator.odata.replay.long"),
        (
            "custom-short",
            "maas-operator-odata-replay-custom-short",
            "maas.operator.odata.replay-custom.short",
        ),
        (
            "custom-long",
            "maas-operator-odata-replay-custom-long",
            "maas.operator.odata.replay-custom.long",
        ),
    ]
    assert [(lane.timeout.base, lane.timeout.per_day) for lane in lanes] == [
        (60, 0),
        (60, 20),
        (60, 0),
        (60, 20),
    ]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("true", True),
        ("True", True),
        ("1", True),
        ("false", False),
        ("0", False),
        ("", False),
    ],
)
def test_the_single_lane_setting_is_read_from_the_environment(
    monkeypatch, value, expected
):
    monkeypatch.setenv("OPERATOR_SINGLE_LANE", value)
    namespace = build_parser().parse_args(["--config-service", "svc"])
    assert build_settings(namespace).single_lane is expected


def test_the_single_lane_setting_is_off_by_default(monkeypatch):
    monkeypatch.delenv("OPERATOR_SINGLE_LANE", raising=False)
    namespace = build_parser().parse_args(["--config-service", "svc"])
    assert build_settings(namespace).single_lane is False


def test_a_mistyped_single_lane_value_stops_the_worker(monkeypatch):
    monkeypatch.setenv("OPERATOR_SINGLE_LANE", "ture")
    with pytest.raises(SystemExit):
        build_parser().parse_args(["--config-service", "svc"])


def test_cli_single_lane_wiring(monkeypatch, fake_opensearch):
    for name in ENVIRONMENT:
        monkeypatch.delenv(name, raising=False)
    module = "maas_collector.rawdata.cli.operator_worker"

    with patch(f"{module}.OperatorReplayWorker") as worker_class, patch(
        f"{module}.OperatorWorker"
    ), patch(f"{module}.signal.signal"), patch(
        f"{module}.es_connections"
    ) as connections:
        connections.get_connection.return_value = fake_opensearch
        operator_worker_main(
            [
                "--config-service",
                "odata",
                "--es-url",
                "http://localhost:9200",
                "--amqp-url",
                "memory://",
                "--operator-single-lane",
                "true",
                "--operator-short-timeout",
                "60",
                "--operator-long-timeout-per-day",
                "20",
            ]
        )

    _connect, _recorder, handler, lanes, _exchange, _workdir = (
        worker_class.call_args.args
    )
    assert handler.lanes == ("single",)
    assert [(lane.name, lane.queue, lane.routing_key) for lane in lanes] == [
        (
            "single",
            "maas-operator-odata-replay-single",
            "maas.operator.odata.replay.short",
        )
    ]
    assert lanes[0].extra_routing_keys == (
        "maas.operator.odata.replay.long",
        "maas.operator.odata.replay-custom.short",
        "maas.operator.odata.replay-custom.long",
    )
    assert (lanes[0].timeout.base, lanes[0].timeout.per_day) == (60, 20)

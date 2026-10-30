import kombu
import pytest
from maas_engine.replay import ReplayWorker

from maas_collector.rawdata.operator.contract import (
    check_routing_key,
    exchange,
    replay_custom_routing_key,
    replay_routing_key,
)
from maas_collector.rawdata.operator.lanes import (
    SINGLE_LANE,
    OperatorLane,
    OperatorReplayWorker,
    accepts_custom,
    build_lanes,
    custom_lane_name,
    slot_of,
)
from maas_collector.rawdata.operator.worker import OperatorSettings


def test_names():
    assert SINGLE_LANE == "single"
    assert custom_lane_name("short") == "custom-short"
    assert custom_lane_name("long") == "custom-long"


@pytest.mark.parametrize(
    "lane, slot",
    [
        ("short", "short"),
        ("long", "long"),
        ("custom-short", "short"),
        ("custom-long", "long"),
        ("single", "single"),
    ],
)
def test_a_custom_lane_shares_the_slot_of_its_plain_lane(lane, slot):
    assert slot_of(lane) == slot


@pytest.mark.parametrize(
    "lane, accepted",
    [
        ("custom-short", True),
        ("custom-long", True),
        ("single", True),
        ("short", False),
        ("long", False),
    ],
)
def test_only_custom_and_single_lanes_accept_a_custom_request(lane, accepted):
    assert accepts_custom(lane) is accepted


def test_build_lanes_declares_a_custom_lane_beside_each_plain_one():
    lanes = build_lanes(
        OperatorSettings(service="svc", short_timeout=60, long_timeout_per_day=20)
    )
    assert [
        (l.name, l.queue, l.routing_key, l.timeout.base, l.timeout.per_day)
        for l in lanes
    ] == [
        (
            "short",
            "maas-operator-svc-replay-short",
            "maas.operator.svc.replay.short",
            60,
            0,
        ),
        (
            "long",
            "maas-operator-svc-replay-long",
            "maas.operator.svc.replay.long",
            60,
            20,
        ),
        (
            "custom-short",
            "maas-operator-svc-replay-custom-short",
            "maas.operator.svc.replay-custom.short",
            60,
            0,
        ),
        (
            "custom-long",
            "maas-operator-svc-replay-custom-long",
            "maas.operator.svc.replay-custom.long",
            60,
            20,
        ),
    ]


def test_custom_queues_receive_only_the_custom_keys():
    lanes = build_lanes(OperatorSettings(service="svc"))
    worker = ReplayWorker(
        lambda: None, object(), object(), lanes, exchange(), "/unused"
    )
    with kombu.Connection("memory://") as connection:
        channel = connection.default_channel
        queues = {lane.name: worker.queue_of(lane)(channel) for lane in lanes}
        for queue in queues.values():
            queue.declare()
        producer = connection.Producer()
        producer.publish(
            {"body": "plain"},
            exchange=exchange(),
            routing_key=replay_routing_key("svc", "short"),
            declare=[exchange()],
        )
        producer.publish(
            {"body": "custom"},
            exchange=exchange(),
            routing_key=replay_custom_routing_key("svc", "short"),
        )
        received = {}
        for name, queue in queues.items():
            bodies = []
            while (message := queue.get()) is not None:
                bodies.append(message.payload["body"])
            received[name] = bodies
    assert received == {
        "short": ["plain"],
        "long": [],
        "custom-short": ["custom"],
        "custom-long": [],
    }


SERVICE_KEYS = (
    "maas.operator.svc.replay.short",
    "maas.operator.svc.replay.long",
    "maas.operator.svc.replay-custom.short",
    "maas.operator.svc.replay-custom.long",
)


def single_lane(**settings):
    return build_lanes(
        OperatorSettings(
            service="svc",
            short_timeout=60,
            long_timeout_per_day=20,
            single_lane=True,
            **settings,
        )
    )


def test_single_lane_mode_has_one_lane_with_every_key_on_its_queue():
    [lane] = single_lane()
    assert isinstance(lane, OperatorLane)
    assert (
        lane.name,
        lane.queue,
        lane.routing_key,
        lane.timeout.base,
        lane.timeout.per_day,
    ) == (
        "single",
        "maas-operator-svc-replay-single",
        "maas.operator.svc.replay.short",
        60,
        20,
    )
    assert lane.extra_routing_keys == SERVICE_KEYS[1:]


def test_every_key_of_the_service_reaches_the_single_queue_and_no_other():
    lanes = single_lane()
    worker = OperatorReplayWorker(
        lambda: None, object(), object(), lanes, exchange(), "/unused"
    )
    with kombu.Connection("memory://") as connection:
        queue = worker.queue_of(lanes[0])(connection.default_channel)
        queue.declare()
        producer = connection.Producer()
        keys = (
            *SERVICE_KEYS,
            check_routing_key("svc"),
            "maas.operator.other.replay.short",
        )
        for key in keys:
            producer.publish(
                {"key": key}, exchange=exchange(), routing_key=key, declare=[exchange()]
            )
        received = []
        while (message := queue.get()) is not None:
            received.append(message.payload["key"])
    assert sorted(received) == sorted(SERVICE_KEYS)
    # one lane: one thread
    worker.should_stop = True
    worker.run()
    assert len(worker.lanes) == 1 and len(worker.threads) == 1


def test_a_lane_without_extra_keys_binds_its_one_key():
    lanes = build_lanes(OperatorSettings(service="svc"))
    operator = OperatorReplayWorker(
        lambda: None, object(), object(), lanes, exchange(), "/unused"
    )
    engine = ReplayWorker(
        lambda: None, object(), object(), lanes, exchange(), "/unused"
    )
    for lane in lanes:
        ours, theirs = operator.queue_of(lane), engine.queue_of(lane)
        assert (ours.name, ours.routing_key, ours.exchange, ours.durable) == (
            theirs.name,
            theirs.routing_key,
            theirs.exchange,
            theirs.durable,
        )
        assert not ours.bindings

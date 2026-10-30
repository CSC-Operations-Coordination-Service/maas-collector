import kombu
import pytest
from maas_engine.replay import ReplayWorker

from maas_collector.rawdata.operator.contract import (
    exchange,
    replay_custom_routing_key,
    replay_routing_key,
)
from maas_collector.rawdata.operator.lanes import (
    SINGLE_LANE,
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

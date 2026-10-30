"""
Lanes of the operator worker.

A plain lane (`short`, `long`) replays an interface as the index describes it. A
custom lane (`custom-short`, `custom-long`) has a queue of its own that only custom
requests reach, and replays an interface with the overrides of the request. A custom
lane shares the slot (the lock, and the health check port) of its plain lane, so a
service never runs more children than it has plain lanes. In single-lane mode there
is one lane, `single`, which serves plain and custom requests one at a time.
"""

from maas_engine.replay import Lane, LaneTimeout

from maas_collector.rawdata.operator.contract import (
    LANES,
    replay_custom_queue_name,
    replay_custom_routing_key,
    replay_queue_name,
    replay_routing_key,
)
from maas_collector.rawdata.operator.worker import OperatorSettings

SINGLE_LANE = "single"
CUSTOM_LANE_PREFIX = "custom-"


def custom_lane_name(lane: str) -> str:
    """Name of the custom lane beside a plain lane"""
    return CUSTOM_LANE_PREFIX + lane


def slot_of(lane: str) -> str:
    """The plain lane whose slot a lane uses: a custom lane takes its plain lane's"""
    if lane.startswith(CUSTOM_LANE_PREFIX):
        return lane[len(CUSTOM_LANE_PREFIX) :]
    return lane


def accepts_custom(lane: str) -> bool:
    """Whether a lane may run a custom request: custom lanes and the single lane"""
    return lane.startswith(CUSTOM_LANE_PREFIX) or lane == SINGLE_LANE


def build_lanes(settings: OperatorSettings) -> list[Lane]:
    """The lanes of the replay worker: `short`, `long`, then a custom lane beside each

    A custom lane has a queue of its own, bound to the custom key only, and the
    timeout of its plain lane: the short lane has a fixed timeout, the long one scales
    with the window.
    """
    plain = [
        Lane(
            lane,
            replay_queue_name(settings.service, lane),
            replay_routing_key(settings.service, lane),
            LaneTimeout(
                base=settings.short_timeout,
                per_day=settings.long_timeout_per_day if lane == "long" else 0,
            ),
        )
        for lane in LANES
    ]
    custom = [
        Lane(
            custom_lane_name(lane.name),
            replay_custom_queue_name(settings.service, lane.name),
            replay_custom_routing_key(settings.service, lane.name),
            lane.timeout,
        )
        for lane in plain
    ]
    return plain + custom

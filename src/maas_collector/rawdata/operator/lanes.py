"""
Lanes of the operator worker.

A plain lane (`short`, `long`) replays an interface as the index describes it. A
custom lane (`custom-short`, `custom-long`) has a queue of its own that only custom
requests reach, and replays an interface with the overrides of the request. A custom
lane shares the slot (the lock, and the health check port) of its plain lane, so a
service never runs more children than it has plain lanes. In single-lane mode there
is one lane, `single`, which serves plain and custom requests one at a time.
"""

from dataclasses import dataclass

import kombu
from maas_engine.replay import Lane, LaneTimeout, ReplayWorker

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


@dataclass(frozen=True)
class OperatorLane(Lane):
    """A lane whose queue is bound to other routing keys than its own"""

    extra_routing_keys: tuple[str, ...] = ()


class OperatorReplayWorker(ReplayWorker):
    """maas-engine's replay worker, with lanes that take several routing keys"""

    def queue_of(self, lane) -> kombu.Queue:
        """the kombu queue of a lane, bound to every key of an `OperatorLane`"""
        extra = getattr(lane, "extra_routing_keys", ())
        if not extra:
            return super().queue_of(lane)
        # with bindings, kombu declares each of them and ignores `exchange=`
        return kombu.Queue(
            lane.queue,
            bindings=[
                kombu.binding(self.exchange, routing_key=key)
                for key in (lane.routing_key, *extra)
            ],
            durable=True,
        )


def build_lanes(settings: OperatorSettings) -> list[Lane]:
    """The lanes of the replay worker: `short`, `long`, then a custom lane beside each

    In single-lane mode there is one lane, `single`, whose queue takes the four keys of
    the service: its one thread runs plain and custom requests, one at a time, with the
    timeout of the long lane (a short replay still gets the base timeout).

    A custom lane has a queue of its own, bound to the custom key only, and the
    timeout of its plain lane: the short lane has a fixed timeout, the long one scales
    with the window.
    """
    if settings.single_lane:
        service = settings.service
        return [
            OperatorLane(
                SINGLE_LANE,
                replay_queue_name(service, SINGLE_LANE),
                replay_routing_key(service, "short"),
                LaneTimeout(
                    base=settings.short_timeout,
                    per_day=settings.long_timeout_per_day,
                ),
                (
                    replay_routing_key(service, "long"),
                    replay_custom_routing_key(service, "short"),
                    replay_custom_routing_key(service, "long"),
                ),
            )
        ]
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

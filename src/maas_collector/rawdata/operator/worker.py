"""
Operator worker: take the replay and check requests of one collector service from
RabbitMQ and hand them to the replay lanes and the product checker.

Each replay lane polls its queue with basic_get, acknowledges the request on
receipt, closes its connection and runs the replay: no connection is held while a
replay runs, so none misses its heartbeats. A replay is never redelivered, a
request lost to a crash keeps its record RUNNING.

The check thread keeps its connection, a check being short, and publishes each
reply on the default exchange to the reply_to queue of the request.
"""

import json
import logging
import tempfile
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

import kombu

from maas_collector.rawdata.configprovider import DEFAULT_CONFIG_INDEX
from maas_collector.rawdata.operator.check import DEFAULT_TIMEOUT, ProductChecker
from maas_collector.rawdata.operator.contract import (
    CheckReply,
    CheckRequest,
    ReplayRequest,
    check_queue_name,
    check_routing_key,
    exchange,
    replay_queue_name,
    replay_routing_key,
)
from maas_collector.rawdata.operator.records import FAILED, OPERATION_INDEX
from maas_collector.rawdata.operator.replay import ReplayLane

LOGGER = logging.getLogger("maas_collector.operator")

# seconds a check reply waits for the backend; it gives up after 30 s anyway
REPLY_EXPIRATION = 60


@dataclass
class OperatorSettings:
    """Settings of the operator worker"""

    # collector service whose requests are taken
    service: str
    config_index: str = DEFAULT_CONFIG_INDEX
    operation_index: str = OPERATION_INDEX
    # seconds a short replay may run
    short_timeout: float = 6 * 3600
    # seconds a long replay may run per day of its window
    long_timeout_per_day: float = 3 * 3600
    # seconds between two polls of an empty queue
    poll_interval: float = 5
    # health check port of the short lane child, the long lane child uses +1
    healthcheck_port: int = 18081
    # folder of the configuration copies and of the check payloads
    workdir: str = tempfile.gettempdir()
    # seconds a check may wait for the interface
    check_timeout: float = DEFAULT_TIMEOUT
    # retries of a failing replay, 10 s apart, before it fails: a dead interface
    # must not hold its lane for hours
    replay_retry: int = 3


def _operation_id(body) -> str:
    """operation_id of a malformed body, empty when it has none"""
    try:
        if isinstance(body, (bytes, bytearray)):
            body = bytes(body).decode("utf-8")
        if isinstance(body, str):
            body = json.loads(body)
    except ValueError:
        return ""
    if isinstance(body, dict) and isinstance(body.get("operation_id"), str):
        return body["operation_id"]
    return ""


class OperatorWorker:
    """One thread per replay lane, one for the checks"""

    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        connection_factory: Callable[[], kombu.Connection],
        settings: OperatorSettings,
        lanes: dict[str, ReplayLane],
        checker: ProductChecker,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.connection_factory = connection_factory
        self.settings = settings
        self.lanes = lanes
        self.checker = checker
        self.sleep = sleep
        self.should_stop = False

    def replay_queue(self, lane: str) -> kombu.Queue:
        """Queue of the replay requests of a lane"""
        return kombu.Queue(
            replay_queue_name(self.settings.service, lane),
            exchange=exchange(),
            routing_key=replay_routing_key(self.settings.service, lane),
            durable=True,
        )

    def check_queue(self) -> kombu.Queue:
        """Queue of the check requests"""
        return kombu.Queue(
            check_queue_name(self.settings.service),
            exchange=exchange(),
            routing_key=check_routing_key(self.settings.service),
            durable=True,
        )

    def run(self) -> None:
        """Serve the lanes and the checks until stop()"""
        threads = [
            threading.Thread(
                target=self.run_lane,
                args=(name, lane),
                name=f"replay-{name}",
                daemon=True,
            )
            for name, lane in self.lanes.items()
        ]
        threads.append(
            threading.Thread(target=self.run_checks, name="check", daemon=True)
        )
        LOGGER.info(
            "Operator worker of service %s: lanes %s and checks",
            self.settings.service,
            ", ".join(self.lanes),
        )
        for thread in threads:
            thread.start()
        for thread in threads:
            # a join with a timeout lets the main thread run the signal handlers
            while thread.is_alive():
                thread.join(1)

    def stop(self, *_args) -> None:
        """Stop taking requests, terminate the running children and start no other"""
        self.should_stop = True
        for lane in self.lanes.values():
            lane.runner.terminate()

    def run_lane(self, name: str, lane: ReplayLane) -> None:
        """Take the replay requests of a lane, one at a time"""
        queue = self.replay_queue(name)
        while not self.should_stop:
            try:
                body = self.next_body(queue)
            # broker restarting, network blip: try again after a pause
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.warning("Cannot fetch a %s replay request: %s", name, error)
                self.sleep(self.settings.poll_interval)
                continue
            if body is None:
                continue
            try:
                self.on_replay(name, lane, body)
            # a lane that dies takes no more request: keep it alive
            except Exception:  # pylint: disable=broad-except
                LOGGER.exception("Replay request of lane %s failed", name)

    def next_body(self, queue: kombu.Queue):
        """Wait for a message, acknowledge it and close the connection

        Returns:
            the raw message body, None when stopped while waiting
        """
        with self.connection_factory() as connection:
            bound = queue(connection.default_channel)
            bound.declare()
            while not self.should_stop:
                message = bound.get(no_ack=False)
                if message is not None:
                    message.ack()
                    return message.body
                # also sends the heartbeats and notices a broker gone away
                connection.heartbeat_check()
                self.sleep(self.settings.poll_interval)
        return None

    def on_replay(self, name: str, lane: ReplayLane, body) -> None:
        """Parse an acknowledged replay request and run it"""
        try:
            request = ReplayRequest.from_body(body)
        except ValueError as error:
            LOGGER.error(
                "Skipping a malformed replay request of lane %s: %s (%r)",
                name,
                error,
                body,
            )
            operation_id = _operation_id(body)
            if operation_id:
                lane.recorder.update(
                    operation_id, FAILED, details=f"malformed replay request: {error}"
                )
            return

        if self.should_stop:
            # taken between the stop and the end of the poll: already acknowledged
            lane.recorder.update(
                request.operation_id,
                FAILED,
                details="the operator worker stopped before the replay started",
            )
            return

        LOGGER.info(
            "Replaying %s from %s to %s in lane %s for %s (operation %s)",
            request.interface_name,
            request.start_date,
            request.end_date,
            name,
            request.requested_by or "?",
            request.operation_id,
        )
        lane.handle(request)

    def run_checks(self) -> None:
        """Answer the check requests, one at a time"""
        queue = self.check_queue()
        while not self.should_stop:
            try:
                with self.connection_factory() as connection:
                    bound = queue(connection.default_channel)
                    bound.declare()
                    producer = connection.Producer()
                    while not self.should_stop:
                        message = bound.get(no_ack=False)
                        if message is None:
                            connection.heartbeat_check()
                            self.sleep(self.settings.poll_interval)
                            continue
                        # a check redelivered after a crash would answer too late
                        message.ack()
                        self.on_check(producer, message)
            # broker restarting, network blip: try again after a pause
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.warning("Cannot serve the check requests: %s", error)
                self.sleep(self.settings.poll_interval)

    def on_check(self, producer: kombu.Producer, message: kombu.Message) -> None:
        """Run a check and publish its reply to reply_to"""
        reply_to = message.properties.get("reply_to")
        if not reply_to:
            LOGGER.warning(
                "Dropping a check request without reply_to: %r", message.body
            )
            return

        try:
            request = CheckRequest.from_body(message.body)
        except ValueError as error:
            LOGGER.error("Malformed check request: %s (%r)", error, message.body)
            reply = CheckReply("error", error=f"malformed check request: {error}")
        else:
            reply = self.checker.check(request)

        properties = {}
        correlation_id = message.properties.get("correlation_id")
        if correlation_id:
            properties["correlation_id"] = correlation_id

        try:
            producer.publish(
                reply.to_body(),
                exchange="",
                routing_key=reply_to,
                serializer="json",
                expiration=REPLY_EXPIRATION,
                **properties,
            )
        except Exception:  # pylint: disable=broad-except
            LOGGER.warning(
                "Cannot publish the check reply %s to %s", correlation_id, reply_to
            )
            # the connection is likely gone: run_checks reconnects
            raise

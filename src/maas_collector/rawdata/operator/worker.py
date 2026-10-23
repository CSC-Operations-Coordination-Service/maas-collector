"""
Operator worker of one collector service: the replays and the product checks asked by
the operator backend over RabbitMQ.

The replays are served by the replay worker of maas-engine (maas_engine.replay): one
lane per queue, each polling with basic_get, acknowledging a request on receipt,
closing its connection and running the replay as a child: no connection is held while
a replay runs, so none misses its heartbeats. What is specific to the collector is
handler.py. This module adds the check thread beside it.

The check thread keeps its connection, a check being short, and publishes each
reply on the default exchange to the reply_to queue of the request.
"""

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
    OPERATION_INDEX,
    CheckReply,
    CheckRequest,
    check_queue_name,
    check_routing_key,
    exchange,
)

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


class CheckWorker:
    """Answer the check requests of one service, one at a time"""

    def __init__(
        self,
        connection_factory: Callable[[], kombu.Connection],
        settings: OperatorSettings,
        checker: ProductChecker,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.connection_factory = connection_factory
        self.settings = settings
        self.checker = checker
        self.sleep = sleep
        self.should_stop = False

    def stop(self, *_args) -> None:
        """Stop answering"""
        self.should_stop = True

    def check_queue(self) -> kombu.Queue:
        """Queue of the check requests"""
        return kombu.Queue(
            check_queue_name(self.settings.service),
            exchange=exchange(),
            routing_key=check_routing_key(self.settings.service),
            durable=True,
        )

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


class OperatorWorker:
    """The replay worker of maas-engine, and the check thread beside it"""

    def __init__(self, replays, checks: CheckWorker):
        self.replays = replays
        self.checks = checks

    def run(self) -> None:
        """Serve the replays and the checks until stop()"""
        checking = threading.Thread(
            target=self.checks.run_checks, name="check", daemon=True
        )
        checking.start()
        # blocks until stop(); its lanes are the replay threads
        self.replays.run()
        checking.join(timeout=5)

    def stop(self, *_args) -> None:
        """Stop taking requests, terminate the running children and start no other"""
        self.checks.stop()
        self.replays.stop()

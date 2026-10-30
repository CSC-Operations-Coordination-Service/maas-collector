"""Confirmed, mandatory publishing of transfer requests, and taking them."""

import logging
import socket
from typing import Callable

import kombu

from maas_collector.rawdata.transfer.contract import (
    TRANSFER_EXCHANGE,
    TransferRequest,
    transfer_exchange,
    transfer_queue,
    transfer_routing_key,
)

LOGGER = logging.getLogger(__name__)

AMQP_HEARTBEAT = 60


class TransferPublishError(Exception):
    """A transfer request was not accepted by a queue"""


def transfer_connection(amqp_url: str) -> kombu.Connection:
    """Connection with publisher confirms"""
    return kombu.Connection(
        amqp_url,
        heartbeat=AMQP_HEARTBEAT,
        connect_timeout=10,
        transport_options={
            "confirm_publish": True,
            "client_properties": {
                "connection_name": f"transfer@{socket.gethostname()}"
            },
        },
    )


class TransferPublisher:
    """Publish transfer requests, mandatory and confirmed

    One connection and one producer are kept until an error closes them: the
    next publish reconnects.
    """

    def __init__(
        self, connection_factory: Callable[[], kombu.Connection], max_retries: int = 3
    ):
        self.connection_factory = connection_factory
        self.max_retries = max_retries
        self._connection = None
        self._producer = None
        self._returned: list = []

    def _on_return(self, *args) -> None:
        self._returned.append(args)

    def _get_producer(self) -> kombu.Producer:
        if self._producer is None:
            self._connection = self.connection_factory()
            self._producer = kombu.Producer(
                self._connection, on_return=self._on_return
            )
        return self._producer

    def publish(self, request: TransferRequest) -> None:
        """Publish a request

        Raises:
            TransferPublishError: returned (no queue bound), nacked or broker
                unreachable
        """
        routing_key = transfer_routing_key(request.service)
        self._returned.clear()
        try:
            self._get_producer().publish(
                request.to_body(),
                exchange=transfer_exchange(),
                routing_key=routing_key,
                serializer="json",
                delivery_mode="persistent",
                mandatory=True,
                declare=[transfer_exchange()],
                retry=True,
                retry_policy={
                    "max_retries": self.max_retries,
                    "interval_start": 0,
                    "interval_step": 2,
                    "interval_max": 10,
                },
            )
        except Exception as error:  # pylint: disable=broad-except
            self.close()
            raise TransferPublishError(
                f"cannot publish to {routing_key} on {TRANSFER_EXCHANGE}: {error}"
            ) from error
        # with publisher confirms the broker's answer, a return included, has
        # been read when publish returns
        if self._returned:
            self.close()
            raise TransferPublishError(
                f"no queue bound to {routing_key} on {TRANSFER_EXCHANGE}: "
                f"is the transfer stage of {request.service} running?"
            )

    def close(self) -> None:
        """Close the connection, if any"""
        connection, self._connection, self._producer = self._connection, None, None
        if connection is not None:
            try:
                connection.close()
            except Exception:  # pylint: disable=broad-except
                LOGGER.debug("Cannot close the transfer connection", exc_info=True)


def take_body(connection_factory: Callable[[], kombu.Connection], service: str):
    """Take one request of the queue of a service

    The message is acknowledged on receipt and the connection closed, so that
    no connection is held during a transfer.

    Returns:
        the message body, None when the queue is empty
    """
    with connection_factory() as connection:
        bound = transfer_queue(service)(connection.default_channel)
        message = bound.get(no_ack=False)
        if message is None:
            return None
        message.ack()
        return message.body

"""Turn the documents a collector indexed into transfer requests."""

import logging

from opensearchpy.connection.connections import connections as es_connections

from maas_collector.rawdata.replay import ReplayArgs
from maas_collector.rawdata.transfer.amqp import TransferPublishError
from maas_collector.rawdata.transfer.contract import TransferRequest
from maas_collector.rawdata.transfer.reports import (
    missing_product,
    read_products,
    requested_reports,
    write_reports,
)

LOGGER = logging.getLogger(__name__)

REQUEST_BATCH_SIZE = 10
"""documents of one request"""

MAX_PENDING_REQUESTS = 10_000
"""requests of the live flow kept for the next loop when the broker refuses them"""


class TransferRequester:
    """Publishes the transfer requests of the documents an archived entry indexed

    Live (no replay), the created and updated documents are requested, and a request
    the broker refuses is kept in ``pending`` for the next loop. In replay, every
    extracted document is requested, and a request that cannot be sent is counted in
    ``failures`` and never retried. Nothing here raises: the collection goes on.
    """

    def __init__(
        self,
        publisher,
        service: str,
        replay: ReplayArgs | None,
        client_factory=es_connections.get_connection,
        report_writer=write_reports,
    ):
        self.publisher = publisher
        self.service = service
        self.replay = replay
        self.client_factory = client_factory
        self.report_writer = report_writer
        self.failures = 0
        self.pending: list[TransferRequest] = []

    def documents_indexed(
        self,
        config,
        indexed: list[tuple[str, str]],
        unchanged: list[tuple[str, str]],
    ) -> None:
        """Request the archive of the documents of one extracted file

        Args:
            config: configuration of the entry, with an ``archive_rule``
            indexed: (id, index) of the documents created or updated
            unchanged: (id, stored index) of the documents found unchanged
        """
        rule = getattr(config, "archive_rule", None)
        if rule is None:
            return

        documents = list(indexed) + (list(unchanged) if self.replay else [])
        # in order, each document once
        documents = list(dict.fromkeys(documents))

        for start in range(0, len(documents), REQUEST_BATCH_SIZE):
            batch = tuple(documents[start : start + REQUEST_BATCH_SIZE])
            request = self.build_request(config, rule, batch)
            try:
                self.send(config, rule, request)
            except Exception as error:  # pylint: disable=broad-except
                self.failed(request, error)

    def build_request(self, config, rule, batch) -> TransferRequest:
        """The request of a batch of documents"""
        replay = self.replay
        return TransferRequest(
            service=self.service,
            interface_name=config.interface_name,
            documents=batch,
            backup=rule.backup,
            backup_version=rule.backup_version,
            duplicates=rule.duplicates,
            force=rule.force,
            operation_id=(replay.suffix or "") if replay else "",
            requested_by=(replay.requested_by or "") if replay else "",
            origin="recollect" if replay else "live",
        )

    def send(self, config, rule, request: TransferRequest) -> None:
        """Write the REQUESTED reports of an operation, then publish"""
        if request.operation_id:
            products, missing = read_products(
                self.client_factory(), request.documents, rule
            )
            self.report_writer(
                requested_reports(
                    request, products + [missing_product(document) for document in missing]
                )
            )
        self.publisher.publish(request)

    def failed(self, request: TransferRequest, error: Exception) -> None:
        """A request was not sent"""
        if self.replay:
            self.failures += len(request.documents)
            LOGGER.error(
                "Cannot send a transfer request of %d documents of %s: %s",
                len(request.documents),
                request.interface_name,
                error,
            )
            return
        LOGGER.error(
            "Cannot publish a transfer request of %d documents of %s, it will be"
            " retried: %s",
            len(request.documents),
            request.interface_name,
            error,
        )
        self.pending.append(request)
        excess = len(self.pending) - MAX_PENDING_REQUESTS
        if excess > 0:
            del self.pending[:excess]
            LOGGER.critical(
                "dropped %d transfer requests of %s: their products will show as gaps",
                excess,
                request.interface_name,
            )

    def retry_pending(self) -> None:
        """Publish again the requests of the live flow the broker refused"""
        while self.pending:
            request = self.pending[0]
            try:
                self.publisher.publish(request)
            except TransferPublishError as error:
                LOGGER.error(
                    "Transfer requests still cannot be published (%d waiting): %s",
                    len(self.pending),
                    error,
                )
                return
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.error("Cannot publish a pending transfer request: %s", error)
                return
            self.pending.pop(0)

    def close(self) -> None:
        """Close the publisher"""
        try:
            self.publisher.close()
        except Exception:  # pylint: disable=broad-except
            LOGGER.debug("Cannot close the transfer publisher", exc_info=True)

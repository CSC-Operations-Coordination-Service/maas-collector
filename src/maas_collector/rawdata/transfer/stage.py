"""The transfer stage: the second stage of the archive.

``TransferStage`` is a ``FileCollector`` that loads its service's configuration and
credentials, restarts on a new configuration and stops on SIGTERM as the collectors
do. It transfers the products of the requests one at a time into the backup the
request names. The error classification, the one report per destination and the
bounded retries follow the download manager's engine.
"""

import dataclasses
import datetime
import logging
import os
import shutil
import tempfile
import time
import typing
from dataclasses import dataclass, field

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from opensearchpy.exceptions import TransportError

from maas_collector.backup import instanciate_collector_backup
from maas_collector.backup.backup import KO, OK, REPLACED, SKIPPED, BackupReport
from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.collector.odatacollector import ODataCollectorConfiguration
from maas_collector.rawdata.configuration import load_json_dict
from maas_collector.rawdata.configprovider import ConfigSource
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.transfer.amqp import take_body, transfer_connection
from maas_collector.rawdata.transfer.backups import (
    BackupUnavailable,
    ResolvedBackup,
    backup_keys,
    build_sink,
    resolve_request_backup,
)
from maas_collector.rawdata.transfer.conflicts import open_conflict, resolve_conflict
from maas_collector.rawdata.transfer.contract import TransferRequest, transfer_queue
from maas_collector.rawdata.transfer.download import (
    MAX_RETRY_DELAY,
    ProductDownloader,
    TransferError,
    TransferStopped,
    TransientTransferError,
    safe_file_name,
    with_retries,
)
from maas_collector.rawdata.transfer.duplicates import decide
from maas_collector.rawdata.transfer.reports import (
    CONFLICT_ERROR,
    archive_fields,
    archive_report,
    archived_state,
    missing_product,
    read_products,
    unreadable_product,
    write_reports,
)
from maas_collector.rawdata.transfer.rule import ArchiveRule, utc_datetime

LOGGER = logging.getLogger(__name__)

MAX_LOOP_SECONDS = 60
"""``loop_body`` returns to ``run`` at least this often, for the checks of the
configuration and of the credentials"""


@dataclass
class TransferSettings:
    """Bounds of the transfer stage"""

    max_attempts: int = 3
    """download, upload per destination, report write"""

    retry_delay: float = 5.0
    """doubled per attempt, at most 60 s"""

    max_download_seconds: float = 900.0


def is_transient_store_error(error: Exception) -> bool:
    """Whether a failure of the index is worth waiting for: the connection, a server
    error (5xx) or a throttling (429). Any other failure is permanent."""
    if isinstance(error, OpenSearchConnectionError):
        return True
    if isinstance(error, TransportError):
        status = error.status_code
        return status == "N/A" or (
            isinstance(status, int) and (status >= 500 or status == 429)
        )
    return False


class RequestRefused(ValueError):
    """The request cannot be served: every product ends KO"""


@dataclass
class RequestContext:
    """What serving a request needs, resolved once"""

    request: TransferRequest
    config: ODataCollectorConfiguration
    rule: ArchiveRule
    """the rule of the entry (the default one without), with the backup, backup
    version, duplicates and force of the request"""

    backup: ResolvedBackup
    keys: dict = field(repr=False)
    destinations: tuple[str, ...]
    sink: typing.Any = field(default=None, repr=False, compare=False)
    """a sink on every destination, for the object keys"""


class TransferStage(FileCollector):
    """Archive the products of the transfer requests of one collector service"""

    CONFIG_CLASS = ODataCollectorConfiguration

    # pylint: disable-next=too-many-arguments
    def __init__(
        self,
        args: CollectorArgs,
        settings: TransferSettings,
        publisher=None,
        *,
        sleep=time.sleep,
        sink_factory=instanciate_collector_backup,
        downloader_factory=ProductDownloader,
        report_writer=write_reports,
        take=None,
    ):
        super().__init__(args)
        self.settings = settings
        self.publisher = publisher
        self.sleep = sleep
        self.sink_factory = sink_factory
        self.downloader_factory = downloader_factory
        self.report_writer = report_writer
        self.take = take or self.take_from_queue
        self._downloaders: dict[str, ProductDownloader] = {}

    @property
    def service(self) -> str:
        """the service whose requests the stage serves"""
        return self.transfer_service

    @property
    def config_store(self) -> ConfigStore | None:
        """the configuration store, for the pinned versions of a backup: None with
        the file backend, which has no versions"""
        if self.args.config_backend != "opensearch":
            return None
        return ConfigStore(es_connections.get_connection(), self.args.config_store.index)

    def take_from_queue(self):
        """The body of the next request of the queue of the service, None when empty"""
        return take_body(lambda: transfer_connection(self.args.amqp_url), self.service)

    def declare_queue(self) -> None:
        """Declare the exchange and the queue of the service, so that a request
        published before the first one is taken is routed"""
        with transfer_connection(self.args.amqp_url) as connection:
            transfer_queue(self.service)(connection.default_channel).declare()

    def setup(self) -> None:
        """Load the configuration and the credentials, then declare the queue of the
        service, before any request is awaited"""
        super().setup()
        self.declare_queue()

    def loop_body(self, path_list) -> None:
        """Take requests until the queue is empty, the stage stops or
        ``MAX_LOOP_SECONDS`` have passed

        A request is acknowledged when it is taken, and no connection is held while
        it is served. A malformed body is logged and dropped. The documents a stop
        left are published again as one request.
        """
        deadline = time.monotonic() + MAX_LOOP_SECONDS
        while not self.should_stop_loop and time.monotonic() < deadline:
            self._healthcheck.tick()
            try:
                body = self.take()
            # the broker is restarting, or the network blinks: next loop
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.warning("Cannot take a transfer request: %s", error)
                return
            if body is None:
                return

            try:
                request = TransferRequest.from_body(body)
            except ValueError as error:
                LOGGER.error("Dropping a malformed transfer request: %s (%r)", error, body)
                continue

            left = self.handle_request(request)
            if left:
                self.republish(request, left)

    def republish(self, request: TransferRequest, documents) -> None:
        """Publish again the documents a stop left"""
        again = dataclasses.replace(request, documents=tuple(documents))
        try:
            self.publisher.publish(again)
        except Exception as error:  # pylint: disable=broad-except
            LOGGER.critical(
                "Cannot publish again the %d documents a stop left of %s: %s: %s",
                len(documents),
                request.interface_name,
                error,
                list(documents),
            )

    def get_configs(self, source: ConfigSource) -> list:
        """The entries of a source, built without their extractor: the stage never
        extracts, and a lambda is never evaluated for it"""
        content = dict(source.content)
        content["collectors"] = [
            {key: value for key, value in meta.items() if key != "extractor"}
            for meta in content.get("collectors", [])
        ]
        return list(load_json_dict(content, self.CONFIG_CLASS, source.name))

    def request_context(self, request: TransferRequest) -> RequestContext:
        """Resolve the entry, the rule and the backup of a request

        Raises:
            RequestRefused: the entry is absent, disabled or ambiguous, or the
                backup cannot serve
        """
        name = request.interface_name
        try:
            config = self.get_configuration_by_interface_name(name)
        except ValueError as error:
            raise RequestRefused(str(error)) from error
        if config is None:
            state = "is disabled" if name in self.disabled_interface_names else "is unknown"
            raise RequestRefused(f"interface {name} {state} in the configuration of {self.service}")

        rule = dataclasses.replace(
            config.archive_rule or ArchiveRule.default(request.backup),
            backup=request.backup,
            backup_version=request.backup_version,
            duplicates=request.duplicates,
            force=request.force,
        )

        try:
            backup = resolve_request_backup(
                self.config_sources,
                self.config_store,
                self.service,
                request.backup,
                request.backup_version,
            )
            keys = backup_keys(self.credential_provider, backup.credentials)
            sink = build_sink(backup, keys, factory=self.sink_factory)
            destinations = tuple(sink.destinations)
        except BackupUnavailable as error:
            raise RequestRefused(str(error)) from error

        return RequestContext(request, config, rule, backup, keys, destinations, sink)

    def downloader_for(self, context: RequestContext) -> ProductDownloader:
        """The downloader of an interface, built on first use: it keeps the token"""
        name = context.request.interface_name
        if name not in self._downloaders:
            self._downloaders[name] = self.downloader_factory(
                context.config,
                context.rule,
                should_stop=lambda: self.should_stop_loop,
                tick=self._healthcheck.tick,
                max_seconds=self.settings.max_download_seconds,
            )
        return self._downloaders[name]

    def on_credentials_changed(self, interface_names) -> None:
        """New credentials: the downloaders of these interfaces are built again, so
        that they take the new ones"""
        for name in interface_names:
            self._downloaders.pop(name, None)
        super().on_credentials_changed(interface_names)

    # a request

    def handle_request(
        self, request: TransferRequest
    ) -> tuple[tuple[str, str], ...]:
        """Serve a request: transfer its products, one at a time, and report each

        Never raises.

        Returns:
            the (id, index) documents a stop left, in order: the caller publishes
            them again, so that nothing is lost
        """
        if request.service != self.service:
            LOGGER.error(
                "Dropping a transfer request for the service %s: this stage serves %s",
                request.service,
                self.service,
            )
            return ()

        refusal = None
        try:
            context = self.request_context(request)
            rule = context.rule
        except RequestRefused as error:
            LOGGER.error("Transfer request of %s refused: %s", request.interface_name, error)
            context, refusal, rule = None, error, ArchiveRule.default(request.backup)
        # the backup versions are in the index, the sink may be built from anything:
        # the request is acknowledged, so it is never lost nor left to kill the stage
        except Exception as error:  # pylint: disable=broad-except
            kind = type(error).__name__
            if is_transient_store_error(error):
                LOGGER.error(
                    "Cannot resolve a transfer request of %s (%s): it is tried again",
                    request.interface_name,
                    kind,
                )
                self.sleep(self.settings.retry_delay)
                return tuple(request.documents)
            # never the message of the error: it may carry a value
            LOGGER.error(
                "Transfer request of %s cannot be resolved (%s)",
                request.interface_name,
                kind,
            )
            context, rule = None, ArchiveRule.default(request.backup)
            refusal = RequestRefused(f"the request cannot be resolved ({kind})")

        try:
            products, missing = read_products(
                es_connections.get_connection(), request.documents, rule
            )
        except Exception as error:  # pylint: disable=broad-except
            kind = type(error).__name__
            # the index cannot be reached: nothing is lost if the documents wait
            if is_transient_store_error(error):
                LOGGER.error("Cannot read the documents of a transfer request: %s", kind)
                self.sleep(self.settings.retry_delay)
                return tuple(request.documents)
            # a refusal or a bad index never changes: the documents end KO, or they
            # would be published again forever
            LOGGER.error("The documents of a transfer request are unreadable: %s", kind)
            self.store(
                [
                    archive_report(
                        request,
                        stand_in,
                        KO,
                        error_type="DocumentNotReadable",
                        details=stand_in["details"],
                    )
                    for stand_in in (
                        unreadable_product(document, kind)
                        for document in request.documents
                    )
                ]
            )
            return ()

        if missing:
            self.store(
                [
                    archive_report(
                        request,
                        missing_product(document),
                        KO,
                        error_type="DocumentNotFound",
                        details=missing_product(document)["details"],
                    )
                    for document in missing
                ]
            )

        seen = set()
        for position, product in enumerate(products):
            left = tuple(
                (item["document_id"], item["document_index"])
                for item in products[position:]
            )
            if self.should_stop_loop:
                return left

            # one transfer for a product two documents of the request describe
            if product["product_name"] in seen:
                continue
            seen.add(product["product_name"])

            try:
                if context is None:
                    reports = self.failure(request, product, [""], refusal)
                else:
                    reports = self.transfer_product(context, product)
            except TransferStopped:
                return left
            # one product never takes the stage down: it ends as a KO
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.exception("Transfer of %s failed", product["product_name"])
                reports = self.failure(
                    request, product, context.destinations if context else [""], error
                )

            self.store(reports)
            self._healthcheck.tick()
        return ()

    def store(self, reports: list[BackupReport]) -> None:
        """Write reports, with bounded retries. When they cannot be stored they are
        logged at CRITICAL, and the stage goes on."""
        if not reports:
            return

        def write():
            try:
                self.report_writer(reports)
            except Exception as error:  # pylint: disable=broad-except
                raise TransientTransferError(str(error)) from error

        try:
            with_retries(
                write, self.settings.max_attempts, self.settings.retry_delay, self.sleep
            )
        except TransientTransferError as error:
            LOGGER.critical(
                "Cannot store %d archive reports (%s): %s",
                len(reports),
                error,
                [
                    f"{report.product_name or report.filename}"
                    f" {report.destination} {report.status}"
                    for report in reports
                ],
            )

    # reports

    @staticmethod
    def failure(
        request: TransferRequest, product, destinations, error: Exception
    ) -> list[BackupReport]:
        """One KO report per destination"""
        return [
            archive_report(
                request,
                product,
                KO,
                destination=destination,
                error_type=type(error).__name__,
                details=str(error)[: BackupReport.DETAILS_MAX_LENGTH],
            )
            for destination in destinations
        ]

    @staticmethod
    def skipped(
        request: TransferRequest, product, destinations, **fields
    ) -> list[BackupReport]:
        """SKIPPED reports, of an operation only: a live skip is not reported"""
        if not request.operation_id:
            return []
        return [
            archive_report(request, product, SKIPPED, destination=destination, **fields)
            for destination in destinations
        ]

    def transfer_product(
        self, context: RequestContext, product: dict
    ) -> list[BackupReport]:
        """Transfer one product into the backup of the request

        Returns:
            the reports to store, one per destination at most

        Raises:
            TransferStopped: the stage is stopping, nothing is to be reported
        """
        request, rule, config = context.request, context.rule, context.config
        destinations = context.destinations
        client = es_connections.get_connection()

        try:
            name = safe_file_name(product.get("product_name"))
        except TransferError as error:
            return self.failure(request, product, destinations, error)

        decision = decide(
            destinations,
            archived_state(client, rule.backup, request.interface_name, name),
            rule.duplicates,
            rule.force,
            product.get("checksum"),
        )

        if decision.conflict:
            open_conflict(
                client,
                rule.backup,
                request.interface_name,
                product,
                decision.reference,
                product.get("checksum") or "",
                datetime.datetime.now(tz=datetime.UTC),
            )
            return self.skipped(
                request,
                product,
                destinations,
                error_type=CONFLICT_ERROR,
                details="the archived product has another checksum",
            )

        if not decision.transfer:
            return self.skipped(request, product, decision.skip)

        try:
            key = self.object_key(context, name, product)
        except ValueError as error:
            return self.failure(request, product, decision.transfer, error)

        os.makedirs(self.args.working_directory, exist_ok=True)
        directory = tempfile.mkdtemp(dir=self.args.working_directory, prefix="transfer-")
        try:
            return self.download_and_upload(
                context, product, decision, key, directory, client
            )
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def object_key(self, context: RequestContext, name: str, product: dict) -> str:
        """The stable key of a product in the backup

        Raises:
            ValueError: the backup files by date and the product has no usable
                publication date. Never the date of today.
        """
        date = None
        if context.backup.block.get("calendar_tree"):
            try:
                date = utc_datetime(product.get("publication_date"))
            except ValueError as error:
                raise ValueError(f"unusable publication_date: {error}") from error
            if date is None:
                raise ValueError(
                    "the backup files by date and the product has no publication_date"
                )
        return context.sink.object_key(context.request.interface_name, name, date)

    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def download_and_upload(
        self, context, product, decision, key, directory, client
    ) -> list[BackupReport]:
        """Download a product into ``directory``, then upload it"""
        request, rule = context.request, context.rule
        settings = self.settings
        downloader = self.downloader_for(context)

        try:
            downloaded = with_retries(
                lambda: downloader.download(product, directory),
                settings.max_attempts,
                settings.retry_delay,
                self.sleep,
            )
        except TransferStopped:
            raise
        except TransferError as error:
            return self.failure(request, product, decision.transfer, error)

        if decision.same_bytes_as and decision.same_bytes_as != downloaded.md5:
            # a mirror cannot be filled with other bytes than the archived ones
            open_conflict(
                client,
                rule.backup,
                request.interface_name,
                product,
                decision.reference,
                downloaded.md5,
                datetime.datetime.now(tz=datetime.UTC),
            )
            return self.skipped(request, product, decision.skip) + self.skipped(
                request,
                product,
                decision.transfer,
                error_type=CONFLICT_ERROR,
                details="the downloaded bytes differ from the archived ones",
            )

        finals = self.upload(context, decision.transfer, downloaded.path, key)

        reports = []
        for destination in decision.transfer:
            report = finals[destination]
            for name, value in archive_fields(request, product).items():
                setattr(report, name, value)
            report.written_md5 = downloaded.md5
            report.source_path = ""
            if report.status == OK and destination in decision.archived:
                report.status = REPLACED
            reports.append(report)

        reports.extend(self.skipped(request, product, decision.skip))

        if all(report.status in (OK, REPLACED) for report in finals.values()):
            resolve_conflict(
                client, rule.backup, request.interface_name, product["product_name"]
            )
        return reports

    def upload(self, context, destinations, path: str, key: str) -> dict:
        """Upload a file to destinations, trying again the failed ones alone

        Returns:
            destination -> its last report
        """
        finals: dict[str, BackupReport] = {}
        pending = list(destinations)
        settings = self.settings
        for attempt in range(1, settings.max_attempts + 1):
            try:
                sink = build_sink(
                    context.backup, context.keys, pending, factory=self.sink_factory
                )
                reports = sink.backup_file(context.config, path, key=key)
            # whatever the sink raises, the request goes on with a KO
            except Exception as error:  # pylint: disable=broad-except
                LOGGER.error("Cannot upload to %s: %s", pending, type(error).__name__)
                reports = [
                    self.sink_failure(
                        context, destination, path, type(error).__name__, str(error)
                    )
                    for destination in pending
                ]
            for report in reports:
                finals[report.destination] = report
            pending = [
                destination
                for destination in pending
                if finals.get(destination) is None or finals[destination].status != OK
            ]
            if not pending:
                break
            if attempt < settings.max_attempts:
                self.sleep(min(settings.retry_delay * 2 ** (attempt - 1), MAX_RETRY_DELAY))
        # a destination that never answered is a KO too
        for destination in pending:
            if destination not in finals:
                finals[destination] = self.sink_failure(
                    context,
                    destination,
                    path,
                    "NoReport",
                    "the sink returned no report for this destination",
                )
        return finals

    @staticmethod
    def sink_failure(context, destination, path, error_type, details) -> BackupReport:
        """The KO report of an upload that raised, or that the sink did not report"""
        now = datetime.datetime.now(tz=datetime.UTC)
        return BackupReport(
            filename=os.path.basename(path),
            source_filename=os.path.basename(path),
            destination=destination,
            interface_name=context.request.interface_name,
            start_time=now,
            end_time=now,
            status=KO,
            error_type=error_type,
            details=details[: BackupReport.DETAILS_MAX_LENGTH],
        )

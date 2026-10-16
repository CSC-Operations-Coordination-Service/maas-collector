"""Telemetry helpers of the collectors: configuration and topology declaration."""

import contextlib
import os
import time

import maas_collector
from maas_collector.queues.queues import DEFAULT_PUBLISH_EXCHANGE_NAME
from maas_model import telemetry
from maas_model.telemetry import metrics
from maas_model.telemetry import semconv
from maas_model.telemetry import topology

# most span contexts linked to one message: the files of an aggregated message,
# or the collected files kept for the file_routing_key message
MAX_GROUP_LINKS = 128


def node_name(config) -> str:
    """name of a configuration in the topology and the metric labels"""
    return getattr(config, "interface_name", "") or config.model_name


def declare_topology(collector) -> None:
    """declare the nodes and the published routes of the loaded configurations"""
    collector_class = type(collector)
    impl = f"{collector_class.__module__}.{collector_class.__qualname__}"
    summary = topology.node_summary(collector_class)
    messenger_exchange = collector._messenger.exchange_name  # pylint: disable=W0212

    for config in collector.configs:
        node = node_name(config)
        topology.declare_node(
            node_type="collector", node=node, impl=impl, summary=summary
        )
        if config.routing_key:
            topology.declare_route(
                direction="out",
                node_type="collector",
                node=node,
                exchange=config.exchange_name
                or messenger_exchange
                or DEFAULT_PUBLISH_EXCHANGE_NAME,
                routing_key=config.routing_key,
                document_class=config.model_name,
            )
        if config.file_routing_key:
            topology.declare_route(
                direction="out",
                node_type="collector",
                node=node,
                exchange=messenger_exchange or DEFAULT_PUBLISH_EXCHANGE_NAME,
                routing_key=config.file_routing_key,
                document_class=config.model_name,
            )


def setup_telemetry(collector, declare: bool = True) -> bool:
    """configure telemetry, off unless --telemetry or MAAS_TELEMETRY, and declare
    the topology. Returns whether telemetry is enabled."""
    enabled = telemetry.configure(
        "collector",
        collector_type=type(collector).__name__,
        project="maas-collector",
        project_version=maas_collector.__version__,
        enabled=True if collector.args.telemetry else None,
    )
    if declare:
        declare_topology(collector)
    return enabled


class _FileScope:  # pylint: disable=too-few-public-methods
    """counters of one collected file, fed by FileCollector.extract_from_file"""

    def __init__(self, node: str, document_class: str):
        self.node = node
        self.document_class = document_class
        self.document_count = None
        self.counts = {"created": 0, "updated": 0, "error": 0, "unmodified": 0}

    def count(self, result: str) -> None:
        """count one document by result: created, updated, error or unmodified"""
        self.counts[result] += 1
        metrics.COLLECTOR_DOCUMENTS.add(
            1,
            {"node": self.node, "document_class": self.document_class, "result": result},
        )


@contextlib.contextmanager
def collector_file_scope(collector, config, path: str):
    """span, activity and baggage of one collected file, with its document counts,
    its duration and its outcome measured on exit, including on exception"""
    node = node_name(config)
    scope = _FileScope(node, config.model_name)
    start = time.perf_counter()
    failed = True
    with telemetry.span(
        "collect.file",
        attributes={
            "maas.collector.type": type(collector).__name__,
            "maas.interface": node,
            "maas.document.class": config.model_name,
            "maas.file.name": os.path.basename(path),
        },
    ) as span, telemetry.activity("collector", node), telemetry.baggage(
        {semconv.ATTR_ORIGIN_INTERFACE: node}
    ):
        try:
            yield scope
            failed = False
        finally:
            counts = scope.counts
            span.set_attributes(
                {
                    "maas.documents.created": counts["created"],
                    "maas.documents.updated": counts["updated"],
                    "maas.documents.errors": counts["error"],
                    "maas.documents.unmodified": counts["unmodified"],
                }
            )
            if failed or counts["error"] > 0:
                outcome = "error"
            elif scope.document_count == 0:
                outcome = "empty"
            else:
                outcome = "ok"
            labels = {"node": node, "document_class": config.model_name}
            metrics.COLLECTOR_FILE_DURATION.record(time.perf_counter() - start, labels)
            metrics.COLLECTOR_FILES.add(1, {**labels, "outcome": outcome})
            # every file until on_ingest_success or on_ingest_error, which
            # clear the list: an archive extracts several files. Collectors
            # that call neither keep only the most recent ones.
            contexts = collector._collect_span_contexts  # pylint: disable=W0212
            context = telemetry.current_span_context()
            if context is not None:
                contexts.append(context)
                del contexts[:-MAX_GROUP_LINKS]

"""Telemetry helpers of the collectors: configuration and topology declaration."""

import maas_collector
from maas_collector.queues.queues import DEFAULT_PUBLISH_EXCHANGE_NAME
from maas_model import telemetry
from maas_model.telemetry import topology


def node_name(config) -> str:
    """name of a configuration in the topology and the metric labels"""
    return config.interface_name or config.model_name


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


def setup_telemetry(collector) -> bool:
    """configure telemetry, off unless --telemetry or MAAS_TELEMETRY, and declare
    the topology. Returns whether telemetry is enabled."""
    enabled = telemetry.configure(
        "collector",
        collector_type=type(collector).__name__,
        project="maas-collector",
        project_version=maas_collector.__version__,
        enabled=True if collector.args.telemetry else None,
    )
    declare_topology(collector)
    return enabled

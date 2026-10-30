"""
Operator worker of one collector service: replays and product checks asked by the
operator backend over RabbitMQ.

    python -m maas_collector.rawdata.cli.operator_worker --config-service odata
"""

import argparse
import signal
import socket
import sys

import kombu
from maas_engine.replay import OperationRecorder, ReplayWorker
from opensearchpy.connection.connections import connections as es_connections

import maas_collector.rawdata.cli.lib.log as maas_log
from maas_collector.rawdata.cli.lib.args import (
    EnvDefault,
    common_parser,
    get_collector_args,
)
from maas_collector.rawdata.collector.credentialprovider import (
    build_credential_provider,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator.check import ProductChecker
from maas_collector.rawdata.operator.contract import LANES, exchange
from maas_collector.rawdata.operator.handler import CollectorReplayHandler
from maas_collector.rawdata.operator.lanes import build_lanes
from maas_collector.rawdata.operator.worker import (
    CheckWorker,
    OperatorSettings,
    OperatorWorker,
)

# seconds between two AMQP heartbeats
AMQP_HEARTBEAT = 60


def operator_parser() -> argparse.ArgumentParser:
    """--operator-* options"""
    parser = argparse.ArgumentParser(add_help=False)

    options = (
        (
            "operation-index",
            str,
            "OpenSearch index of the operation records",
        ),
        ("short-timeout", float, "seconds a short lane replay may run"),
        (
            "long-timeout-per-day",
            float,
            "seconds a long lane replay may run per day of its window",
        ),
        ("poll-interval", float, "seconds between two polls of an empty queue"),
        (
            "healthcheck-port",
            int,
            "health check port of the short lane collector, the long lane one uses"
            " the next port",
        ),
        ("workdir", str, "folder of the configuration copies and check payloads"),
        ("check-timeout", float, "seconds a check may wait for the interface"),
        (
            "replay-retry",
            int,
            "retries of a failing replay, 10 s apart, before it fails",
        ),
    )

    for name, kind, text in options:
        dest = name.replace("-", "_")
        parser.add_argument(
            f"--operator-{name}",
            dest=f"operator_{dest}",
            help=f"{text} (default: %(default)s)",
            action=EnvDefault,
            envvar=f"OPERATOR_{dest.upper()}",
            required=False,
            default=getattr(OperatorSettings, dest),
            type=kind,
        )

    return parser


def build_parser() -> argparse.ArgumentParser:
    """command line parser of the operator worker"""
    return argparse.ArgumentParser(
        prog="python -m maas_collector.rawdata.cli.operator_worker",
        description="Run the replays and product checks of a collector service",
        parents=[common_parser(), operator_parser()],
    )


def build_settings(namespace: argparse.Namespace) -> OperatorSettings:
    """OperatorSettings of parsed arguments"""
    return OperatorSettings(
        service=namespace.config_service,
        config_index=namespace.config_index,
        operation_index=namespace.operator_operation_index,
        short_timeout=namespace.operator_short_timeout,
        long_timeout_per_day=namespace.operator_long_timeout_per_day,
        poll_interval=namespace.operator_poll_interval,
        healthcheck_port=namespace.operator_healthcheck_port,
        workdir=namespace.operator_workdir,
        check_timeout=namespace.operator_check_timeout,
        replay_retry=namespace.operator_replay_retry,
    )


def operator_worker_main(argv: list[str]) -> int:
    """entry point"""
    parser = build_parser()
    namespace = parser.parse_args(argv)

    if not namespace.config_service:
        parser.error(
            "the service is required: set CONFIG_SERVICE or --config-service,"
            " one worker per collector service"
        )

    maas_log.setup_logging(namespace.loglevel)

    settings = build_settings(namespace)
    args = get_collector_args(CollectorArgs, namespace)

    if not args.es_url:
        parser.error("no OpenSearch connection: set ES_URL or ES_VAULT_PATH")
    if not args.amqp_url:
        parser.error("no AMQP connection: set AMQP_URL or AMQP_VAULT_PATH")

    # the default connection: the product checker and the models use it too
    es_connections.create_connection(
        hosts=[args.es_url],
        retry_on_timeout=True,
        max_retries=args.es_retries,
        timeout=args.es_timeout,
        verify_certs=not args.es_ignore_certs_verification,
        ssl_show_warn=not args.es_ignore_certs_verification,
    )
    client = es_connections.get_connection()

    store = ConfigStore(client, settings.config_index)
    recorder = OperationRecorder(client, settings.operation_index)

    lanes = build_lanes(settings)
    handler = CollectorReplayHandler(
        store,
        settings.service,
        LANES,
        settings.healthcheck_port,
        settings.replay_retry,
        # its own provider: the checker reads `unreadable` after each of its loads,
        # and every load rewrites it
        credential_provider=build_credential_provider(args),
    )

    checker = ProductChecker(
        settings.service,
        settings.config_index,
        build_credential_provider(args),
        settings.workdir,
        timeout=settings.check_timeout,
    )

    def connect() -> kombu.Connection:
        return kombu.Connection(
            args.amqp_url,
            heartbeat=AMQP_HEARTBEAT,
            connect_timeout=10,
            transport_options={
                "client_properties": {
                    "connection_name": f"operator-worker@{socket.gethostname()}"
                }
            },
        )

    replays = ReplayWorker(
        connect,
        recorder,
        handler,
        lanes,
        exchange(),
        settings.workdir,
        poll_interval=settings.poll_interval,
    )
    worker = OperatorWorker(replays, CheckWorker(connect, settings, checker))
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    worker.run()
    return 0


if __name__ == "__main__":
    sys.exit(operator_worker_main(sys.argv[1:]))

"""
Transfer stage of one collector service: archives the products of the transfer
requests the collectors publish.

    python -m maas_collector.rawdata.cli.transfer --config-service odata
"""

import argparse
import sys

import maas_collector.rawdata.cli.lib.log as maas_log
from maas_collector.rawdata.cli.lib.args import (
    EnvDefault,
    common_parser,
    get_collector_args,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.transfer.amqp import TransferPublisher, transfer_connection
from maas_collector.rawdata.transfer.stage import TransferSettings, TransferStage


def transfer_parser() -> argparse.ArgumentParser:
    """--transfer-* options"""
    parser = argparse.ArgumentParser(add_help=False)

    options = (
        (
            "max-attempts",
            int,
            "attempts of a download, of an upload per destination and of a report"
            " write",
        ),
        ("retry-delay", float, "seconds before the second attempt, doubled after"),
        (
            "max-download-seconds",
            float,
            "seconds a download may take",
        ),
    )

    for name, kind, text in options:
        dest = name.replace("-", "_")
        parser.add_argument(
            f"--transfer-{name}",
            dest=f"transfer_{dest}",
            help=f"{text} (default: %(default)s)",
            action=EnvDefault,
            envvar=f"TRANSFER_{dest.upper()}",
            required=False,
            default=getattr(TransferSettings, dest),
            type=kind,
        )

    return parser


def build_parser() -> argparse.ArgumentParser:
    """command line parser of the transfer stage"""
    return argparse.ArgumentParser(
        prog="python -m maas_collector.rawdata.cli.transfer",
        description="Archive the products of the transfer requests of a collector"
        " service",
        parents=[common_parser(), transfer_parser()],
    )


def transfer_main(argv: list[str]) -> int:
    """entry point"""
    parser = build_parser()
    namespace = parser.parse_args(argv)

    if not namespace.config_service:
        parser.error(
            "the service is required: set CONFIG_SERVICE or --config-service,"
            " one transfer stage per collector service"
        )

    maas_log.setup_logging(namespace.loglevel)

    settings = TransferSettings(
        max_attempts=namespace.transfer_max_attempts,
        retry_delay=namespace.transfer_retry_delay,
        max_download_seconds=namespace.transfer_max_download_seconds,
    )
    args = get_collector_args(CollectorArgs, namespace)

    if not args.es_url:
        parser.error("no OpenSearch connection: set ES_URL or ES_VAULT_PATH")
    if not args.amqp_url:
        parser.error("no AMQP connection: set AMQP_URL or AMQP_VAULT_PATH")

    publisher = TransferPublisher(lambda: transfer_connection(args.amqp_url))
    stage = TransferStage(args, settings, publisher)

    try:
        stage.setup()
        stage.run(("transfer",))
    except KeyboardInterrupt:
        print("exited by keyboard interruption")
    finally:
        publisher.close()

    return 0


if __name__ == "__main__":
    sys.exit(transfer_main(sys.argv[1:]))

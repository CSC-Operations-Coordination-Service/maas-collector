"""
Collector side of a replay: find the interface, build the command that replays it, and
the environment of the child. The generic part (queue, child process, record) is the
replay worker of maas-engine, see handler.py.
"""

import socket
import sys
from collections.abc import Mapping

from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator.contract import ReplayRequest

REPLAY_MODULES = {
    "ODataCollectorConfiguration": "odata",
    "JiraExtendedCollectorConfiguration": "jirax",
}

DROPPED_ENVIRONMENT = (
    "CONFIG_BACKEND",
    "CONFIG_SERVICE",
    "CONFIG_INDEX",
    "RAWDATA_CONFIG",
    "RAWDATA_CONFIG_DIR",
)
DROPPED_PREFIX = "REPLAY_"


class ReplayNotPossible(ValueError):
    """The requested interface cannot be replayed"""


def _declares(document: dict, interface_name: str) -> bool:
    return any(
        entry.get("interface_name") == interface_name
        for entry in (document.get("content") or {}).get("collectors", [])
    )


def _was_deleted(
    store: ConfigStore, tombstones: list[str], interface_name: str
) -> bool:
    """a tombstone has no content: its older versions tell what it declared"""
    return any(
        not version.get("deleted") and _declares(version, interface_name)
        for path in tombstones
        for version in store.history(path)
    )


def find_entry(store: ConfigStore, service: str, interface_name: str) -> dict:
    """Return the raw collector entry of an interface of the service

    Raises:
        ReplayNotPossible: absent, deleted, only under another service, disabled
            or of a class that cannot be replayed
    """
    live, elsewhere, tombstones = [], [], []
    for document in store.latest():
        services = document.get("services", [])
        if document.get("deleted"):
            if service in services:
                tombstones.append(document["path"])
            continue
        for entry in (document.get("content") or {}).get("collectors", []):
            if entry.get("interface_name") != interface_name:
                continue
            if service in services:
                live.append(entry)
            else:
                elsewhere.extend(services)

    if not live:
        if _was_deleted(store, tombstones, interface_name):
            raise ReplayNotPossible(f"{interface_name} is deleted")
        if elsewhere:
            raise ReplayNotPossible(
                f"{interface_name} does not belong to service {service}, "
                f"only to {', '.join(sorted(set(elsewhere)))}"
            )
        raise ReplayNotPossible(f"{interface_name} not found in service {service}")

    enabled = [entry for entry in live if entry.get("enabled", True) is not False]
    if not enabled:
        raise ReplayNotPossible(f"{interface_name} is disabled")
    entry = enabled[0]
    if entry.get("class") not in REPLAY_MODULES:
        raise ReplayNotPossible(
            f"{interface_name} of class {entry.get('class')} cannot be replayed"
        )
    return entry


def _format_date(value) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_command(
    module: str,
    conf_dir: str,
    request: ReplayRequest,
    healthcheck_port: int,
    replay_retry: int,
    service: str = "",
) -> list[str]:
    """Collector invocation, as an argument list

    With a service, the child is told it (`--config-service`): the child still reads
    the copy of the configuration, the service only names the transfer stage its
    archive requests are published for.
    """
    command = [
        sys.executable,
        "-m",
        f"maas_collector.rawdata.cli.{module}",
        "-v",
        "-d",
        conf_dir,
        "--healthcheck-port",
        str(healthcheck_port),
        "--replay-interface-name",
        request.interface_name,
        "--replay-start-date",
        _format_date(request.start_date),
        "--replay-end-date",
        _format_date(request.end_date),
        "--replay-retry",
        str(replay_retry),
        # one item: argparse would take an id starting with - for an option
        f"--replay-suffix={request.operation_id}",
        f"--replay-requested-by={request.requested_by}",
    ]
    if service:
        command.append(f"--config-service={service}")
    return command


def child_environment(environ: Mapping[str, str], lane: str = "") -> dict[str, str]:
    """Environment of the child: without what makes it read another configuration

    With a lane, the child gets an instance id of its own, so its telemetry does not
    merge with the collector it was started from.
    """
    env = {
        name: value
        for name, value in environ.items()
        if name not in DROPPED_ENVIRONMENT and not name.startswith(DROPPED_PREFIX)
    }
    if lane:
        base = environ.get("MAAS_INSTANCE_ID") or socket.gethostname()
        env["MAAS_INSTANCE_ID"] = f"{base}-replay-{lane}"
    return env

"""
The replay handler of the collector: what maas-engine's replay worker asks of a
caller (maas_engine.replay.ReplayHandler).

For a request, `prepare` finds the interface in the configuration index, copies the
configuration of the service into a folder of its own, and `command` is the collector
of that interface in replay mode on that copy. Needs maas-engine:
`pip install maas-collector[operator]`.
"""

import logging
import os
import shutil
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass

from maas_engine.replay import ReplayNotPossible as EngineReplayNotPossible

from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    BACKUP_FAILURE_LINE,
    REPLAY_FAILED_EXIT_CODE,
)
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import sync_configs
from maas_collector.rawdata.operator.replay import (
    REPLAY_MODULES,
    ReplayNotPossible,
    build_command,
    child_environment,
    find_entry,
)

LOGGER = logging.getLogger("maas_collector.operator")


@dataclass
class Prepared:
    """what `prepare` made: the copy of the configuration and the collector to run"""

    copy: str
    module: str


class CollectorReplayHandler:
    """Replay an interface of one collector service, from the configuration index"""

    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        store: ConfigStore,
        service: str,
        lanes: tuple[str, ...] = ("short", "long"),
        healthcheck_port: int = 18081,
        replay_retry: int = 3,
        environ: Mapping[str, str] | None = None,
    ):
        self.store = store
        self.service = service
        # each lane has a child of its own, so each gets its own health check port
        self.lanes = tuple(lanes)
        self.healthcheck_port = healthcheck_port
        self.replay_retry = replay_retry
        self.environ = environ

    def prepare(self, request, lane: str, workdir: str) -> Prepared:
        """Find the interface and copy the configuration of the service

        Raises:
            maas_engine.replay.ReplayNotPossible: absent, deleted, disabled, of
                another service or class, or a configuration file the copy skipped
        """
        os.makedirs(workdir, exist_ok=True)
        # never built from the request: operation_id is not a safe file name
        copy = tempfile.mkdtemp(dir=workdir, prefix="replay-")
        try:
            entry = find_entry(self.store, self.service, request.interface_name)
            synced = sync_configs(self.store, copy, [self.service])
            if synced.skipped:
                # the child would run without them, maybe without the interface
                raise ReplayNotPossible(
                    "configuration files the copy skipped: " + ", ".join(synced.skipped)
                )
        except ReplayNotPossible as error:
            shutil.rmtree(copy, ignore_errors=True)
            raise EngineReplayNotPossible(str(error)) from error
        except BaseException:
            shutil.rmtree(copy, ignore_errors=True)
            raise
        return Prepared(copy, REPLAY_MODULES[entry["class"]])

    def command(self, prepared: Prepared, request, lane: str):
        """The collector in replay mode on the copy, and its environment"""
        return (
            build_command(
                prepared.module,
                os.path.join(prepared.copy, self.service),
                request,
                self.healthcheck_port + self.lanes.index(lane),
                self.replay_retry,
            ),
            child_environment(self.environ or os.environ, lane),
        )

    def cleanup(self, prepared: Prepared) -> None:
        """Remove the copy of the configuration"""
        shutil.rmtree(prepared.copy, ignore_errors=True)

    @staticmethod
    def describe_failure(code: int, tail: str) -> str:
        """Why a replay failed: the collector's own exit code says what it found"""
        if code == REPLAY_FAILED_EXIT_CODE:
            reason = "an interface failed during the replay"
        elif code == BACKUP_FAILED_EXIT_CODE:
            reason = "files could not be backed up"
            # the line is the last one the collector printed, other output may follow
            for line in reversed(tail.splitlines()):
                found = BACKUP_FAILURE_LINE.fullmatch(line.strip())
                if found:
                    reason = (
                        f"{found['count']} files could not be backed up"
                        f" to {found['backup']}"
                    )
                    break
        else:
            reason = f"exit code {code}"
        return f"{reason}\n{tail}"

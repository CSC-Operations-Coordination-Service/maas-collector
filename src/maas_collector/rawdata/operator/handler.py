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
from dataclasses import dataclass, replace

from maas_engine.replay import ReplayNotPossible as EngineReplayNotPossible

from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    BACKUP_FAILURE_LINE,
    REPLAY_FAILED_EXIT_CODE,
)
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import sync_configs
from maas_collector.rawdata.operator.contract import CUSTOM_SUFFIX
from maas_collector.rawdata.operator.lanes import accepts_custom, slot_of
from maas_collector.rawdata.operator.replay import (
    REPLAY_MODULES,
    ReplayNotPossible,
    build_command,
    child_environment,
    find_entry,
)

LOGGER = logging.getLogger("maas_collector.operator")


@dataclass(frozen=True)
class CustomPart:
    """What a custom request asks: the real interface and the fields it carries"""

    interface_name: str
    overrides: dict | None
    configuration: str
    archive: dict | None


def custom_part(request, lane: str) -> CustomPart | None:
    """The custom part of a request, None for a plain request

    A request is custom when its interface name ends with `@custom` and at least one
    of `overrides`, `archive` (objects, `{}` counts) and `configuration` (a non-empty
    string) is set. The real name is `interface` in the body.

    Raises:
        maas_engine.replay.ReplayNotPossible: custom fields without the name, a name
            without fields or with another interface, or a custom request on a plain
            lane. It is the engine's class, not the local one: this runs before the
            copy exists, and the engine records only its own class as a plain refusal.
    """
    extra = request.extra or {}
    overrides = extra.get("overrides")
    archive = extra.get("archive")
    configuration = extra.get("configuration") or ""
    is_set = overrides is not None or archive is not None or bool(configuration)
    name = request.interface_name
    if not name.endswith(CUSTOM_SUFFIX):
        if is_set:
            raise EngineReplayNotPossible(
                f"overrides, configuration or archive without the {CUSTOM_SUFFIX} "
                "interface name"
            )
        return None
    real = name[: -len(CUSTOM_SUFFIX)]
    if not real or "@" in real or extra.get("interface") != real:
        raise EngineReplayNotPossible(
            f"interface must be the interface name without {CUSTOM_SUFFIX}, "
            "which cannot contain @"
        )
    if not is_set:
        raise EngineReplayNotPossible(
            f"{CUSTOM_SUFFIX} without overrides, configuration or archive"
        )
    for field_name, value in (("overrides", overrides), ("archive", archive)):
        if value is not None and not isinstance(value, dict):
            raise EngineReplayNotPossible(f"{field_name} must be an object")
    if not isinstance(configuration, str):
        raise EngineReplayNotPossible("configuration must be a string")
    if not accepts_custom(lane):
        raise EngineReplayNotPossible(
            f"custom recollect on a plain lane ({lane}): this worker serves it "
            "on its custom lanes only"
        )
    return CustomPart(real, overrides, configuration, archive)


@dataclass
class Prepared:
    """what `prepare` made: the copy of the configuration and the collector to run"""

    copy: str
    module: str
    # the name the child is started with: the real one, whatever the request's
    interface_name: str = ""


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
        # the plain lanes (the slots): each has a child of its own, so each gets its own
        # health check port, and a custom lane takes the port of its plain lane
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
        part = custom_part(request, lane)
        interface_name = part.interface_name if part else request.interface_name
        os.makedirs(workdir, exist_ok=True)
        # never built from the request: operation_id is not a safe file name
        copy = tempfile.mkdtemp(dir=workdir, prefix="replay-")
        try:
            entry = find_entry(self.store, self.service, interface_name)
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
        return Prepared(copy, REPLAY_MODULES[entry["class"]], interface_name)

    def command(self, prepared: Prepared, request, lane: str):
        """The collector in replay mode on the copy, and its environment"""
        return (
            build_command(
                prepared.module,
                os.path.join(prepared.copy, self.service),
                replace(
                    request,
                    interface_name=prepared.interface_name or request.interface_name,
                ),
                self.healthcheck_port + self.lanes.index(slot_of(lane)),
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

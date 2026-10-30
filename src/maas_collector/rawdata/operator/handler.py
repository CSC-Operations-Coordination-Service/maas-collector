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
import threading
from collections.abc import Mapping
from dataclasses import dataclass, replace

from maas_engine.replay import ReplayNotPossible as EngineReplayNotPossible

from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    BACKUP_FAILURE_LINE,
    REPLAY_FAILED_EXIT_CODE,
    TRANSFER_FAILED_EXIT_CODE,
    TRANSFER_FAILURE_LINE,
)
from maas_collector.rawdata.configprovider import (
    ConfigValidationError,
    FileConfigProvider,
    validate_config_sources,
)
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import serialize, sync_configs
from maas_collector.rawdata.implementation import (
    get_collector_class_by_config_classname,
)
from maas_collector.rawdata.operator.contract import CUSTOM_SUFFIX
from maas_collector.rawdata.operator.lanes import accepts_custom, slot_of
from maas_collector.rawdata.operator.overrides import (
    OverrideRefused,
    check_archive_choice,
    merge_overrides,
    normalize_archive_choice,
)
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


def entry_in_copy(service_dir: str, interface_name: str) -> tuple[str, dict, int]:
    """The one enabled entry of an interface in a copy of the configuration

    Returns:
        (path of the file, its content, index of the entry in its `collectors`)

    Raises:
        ReplayNotPossible: not exactly one enabled entry has this name across the
            files of the copy (a disabled twin is left alone)
    """
    found = []
    for source in FileConfigProvider(config_dir=service_dir).load():
        collectors = source.content.get("collectors", [])
        for index, meta in enumerate(collectors):
            if (
                meta.get("interface_name") == interface_name
                and meta.get("enabled", True) is not False
            ):
                found.append((source.name, source.content, index))
    if len(found) != 1:
        raise ReplayNotPossible(
            f"{len(found)} enabled entries are named {interface_name} in the "
            "configuration of the service, a custom recollect needs exactly one"
        )
    return found[0]


def _defines_for(document: dict | None, service: str, name: str) -> bool:
    """whether a configuration document is a live definition of a backup for a service"""
    if not document or document.get("deleted"):
        return False
    if service not in document.get("services", []):
        return False
    content = document.get("content") or {}
    return any(
        isinstance(definition, dict) and definition.get("name") == name
        for definition in content.get("backups", [])
    )


def resolve_backup_version(
    store: ConfigStore, service: str, name: str, version: int | None
) -> int:
    """The version of a backup definition a recollect archives with

    Exactly one current, live file of the service defines the backup `name`. Its
    version is the answer for `None`; a named version must still define it, for the
    service.

    Raises:
        ReplayNotPossible: no such file, several, or a version that cannot serve
    """
    current = [
        document for document in store.latest() if _defines_for(document, service, name)
    ]
    if not current:
        raise ReplayNotPossible(
            f"backup {name} is not defined by a current file of service {service}"
        )
    if len(current) > 1:
        paths = ", ".join(document["path"] for document in current)
        raise ReplayNotPossible(f"backup {name} is defined by several files: {paths}")
    if version is None:
        return current[0]["version"]
    if not _defines_for(store.get(current[0]["path"], version), service, name):
        raise ReplayNotPossible(
            f"backup {name} has no live version {version} for service {service}"
        )
    return version


@dataclass
class Prepared:
    """what `prepare` made: the copy of the configuration and the collector to run"""

    copy: str
    module: str
    # the name the child is started with: the real one, whatever the request's
    interface_name: str = ""
    # the slot of the lane, held from `prepare` to `cleanup`
    slot: "threading.Lock | None" = None


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
        credential_provider=None,
    ):
        self.store = store
        self.service = service
        # the plain lanes (the slots): each has a child of its own, so each gets its own
        # health check port, and a custom lane takes the port of its plain lane
        self.lanes = tuple(lanes)
        self.healthcheck_port = healthcheck_port
        self.replay_retry = replay_retry
        self.environ = environ
        # its own instance, never the checker's: a load rewrites `unreadable`, which
        # the checker reads after each of its own loads
        self.credential_provider = credential_provider
        # one lock per plain lane, shared by the custom lane beside it: a service never
        # runs more children than it has plain lanes
        self._slots = {name: threading.Lock() for name in self.lanes}

    def prepare(self, request, lane: str, workdir: str) -> Prepared:
        """Find the interface and copy the configuration of the service

        Raises:
            maas_engine.replay.ReplayNotPossible: absent, deleted, disabled, of
                another service or class, or a configuration file the copy skipped
        """
        part = custom_part(request, lane)
        interface_name = part.interface_name if part else request.interface_name
        # a refusal above needs no slot; the record stays RUNNING while this waits
        slot = self._slots[slot_of(lane)]
        slot.acquire()  # pylint: disable=consider-using-with
        copy = None
        try:
            os.makedirs(workdir, exist_ok=True)
            # never built from the request: operation_id is not a safe file name
            copy = tempfile.mkdtemp(dir=workdir, prefix="replay-")
            entry = find_entry(self.store, self.service, interface_name)
            synced = sync_configs(self.store, copy, [self.service])
            if synced.skipped:
                # the child would run without them, maybe without the interface
                raise ReplayNotPossible(
                    "configuration files the copy skipped: " + ", ".join(synced.skipped)
                )
            if part:
                self._customize(part, os.path.join(copy, self.service), entry["class"])
        except (ReplayNotPossible, OverrideRefused, ConfigValidationError) as error:
            self._abandon(copy, slot)
            raise EngineReplayNotPossible(str(error)) from error
        except BaseException:
            self._abandon(copy, slot)
            raise
        return Prepared(copy, REPLAY_MODULES[entry["class"]], interface_name, slot=slot)

    def _customize(self, part: CustomPart, service_dir: str, class_name: str) -> None:
        """Apply the overrides of a custom request to the copy, then validate the copy

        Nothing is evaluated: the copy is validated statically, the lambdas of its
        extractors are only parsed.
        """
        path, content, index = entry_in_copy(service_dir, part.interface_name)
        overrides = part.overrides or {}
        name = overrides.get("interface_credentials")
        if name and self.credential_provider is not None:
            if name not in self.credential_provider.load([name]):
                raise ReplayNotPossible(
                    f"interface_credentials: no credential entry {name} for "
                    f"service {self.service}"
                )
        merged = merge_overrides(class_name, content["collectors"][index], overrides)
        if part.archive is not None:
            check_archive_choice(class_name, part.archive)
            choice = normalize_archive_choice(part.archive)
            choice["backup_version"] = resolve_backup_version(
                self.store, self.service, choice["backup"], choice["backup_version"]
            )
            rule = merged.get("archive")
            # the rule's fields and download stay: the choice says where, not what
            merged["archive"] = {**(rule if isinstance(rule, dict) else {}), **choice}
        content["collectors"][index] = merged
        with open(path, "wb") as conf_fd:
            conf_fd.write(serialize(content))
        configuration_class = get_collector_class_by_config_classname(
            class_name
        ).CONFIG_CLASS
        validate_config_sources(
            FileConfigProvider(config_dir=service_dir).load(),
            lambda _meta: [configuration_class],
            resolve_models=True,
            evaluate=False,
        )

    @staticmethod
    def _abandon(copy: "str | None", slot: "threading.Lock") -> None:
        """Remove a copy and free the slot of a `prepare` that failed"""
        if copy:
            shutil.rmtree(copy, ignore_errors=True)
        slot.release()

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
                service=self.service,
            ),
            child_environment(self.environ or os.environ, lane),
        )

    def cleanup(self, prepared: Prepared) -> None:
        """Remove the copy of the configuration and free the slot"""
        shutil.rmtree(prepared.copy, ignore_errors=True)
        slot, prepared.slot = prepared.slot, None
        if slot is not None:
            slot.release()

    @staticmethod
    def describe_failure(code: int, tail: str) -> str:
        """Why a replay failed: the collector's own exit code says what it found"""
        reasons = []
        if code == REPLAY_FAILED_EXIT_CODE:
            reasons.append("an interface failed during the replay")
        elif code == TRANSFER_FAILED_EXIT_CODE:
            reasons.append(
                _count_line(
                    tail,
                    TRANSFER_FAILURE_LINE,
                    "{count} documents could not be sent to the transfer stage",
                    "transfer requests could not all be sent",
                )
            )
            # a replay that failed both ways says so: neither is lost
            backup = _count_line(
                tail,
                BACKUP_FAILURE_LINE,
                "{count} files could not be backed up to {backup}",
                "",
            )
            if backup:
                reasons.append(backup)
        elif code == BACKUP_FAILED_EXIT_CODE:
            reasons.append(
                _count_line(
                    tail,
                    BACKUP_FAILURE_LINE,
                    "{count} files could not be backed up to {backup}",
                    "files could not be backed up",
                )
            )
        else:
            reasons.append(f"exit code {code}")
        return "\n".join([*reasons, tail])


def _count_line(tail: str, pattern, template: str, default: str) -> str:
    """The text for the last line of a tail that matches a failure line"""
    # the line is the last one the collector printed, other output may follow
    for line in reversed(tail.splitlines()):
        found = pattern.fullmatch(line.strip())
        if found:
            return template.format(**found.groupdict())
    return default

"""
Bring the configuration index into a local folder: <out>/<service>/<path>.

Only the files sync wrote are ever removed: each <out>/<service>/ holds a manifest
of the paths of the previous sync, any other file of the folder is left alone.
"""

import dataclasses
import json
import logging
import os
from typing import Sequence

from maas_collector.rawdata.configstore import (
    ConfigStore,
    check_path,
    check_service,
)

LOGGER = logging.getLogger("ConfigSync")

# per <out>/<service>/, lists the paths sync wrote
MANIFEST_NAME = ".maas-config-sync.json"


@dataclasses.dataclass
class SyncResult:
    """what a sync did, as <service>/<path>"""

    written: list = dataclasses.field(default_factory=list)
    unchanged: list = dataclasses.field(default_factory=list)
    removed: list = dataclasses.field(default_factory=list)
    skipped: list = dataclasses.field(default_factory=list)


def target_path(root: str, service: str, path: str) -> str:
    """file of a document path of a service under root, an already resolved
    directory (os.path.realpath)

    A document written by hand in the index can carry any path or service: never
    designate a file outside root.

    Raises:
        ValueError: on a bad path or service, or a file outside root
    """
    check_path(path)
    check_service(service)

    target = os.path.join(root, service, *path.split("/"))

    if not os.path.realpath(target).startswith(root + os.sep):
        raise ValueError(f"{target} is outside {root}")

    return target


def serialize(content: dict) -> bytes:
    """the bytes of a configuration file, as export writes it"""
    return (json.dumps(content, indent=4, ensure_ascii=False) + "\n").encode("UTF-8")


def read_manifest(service_dir: str) -> list:
    """paths listed by the previous sync of a service folder, [] if none usable"""
    try:
        with open(os.path.join(service_dir, MANIFEST_NAME), encoding="UTF-8") as fd:
            paths = json.load(fd)
    except (OSError, ValueError):
        return []

    if not isinstance(paths, list):
        return []

    return [path for path in paths if isinstance(path, str)]


def write_manifest(service_dir: str, paths: list):
    """record the paths of this sync"""
    os.makedirs(service_dir, exist_ok=True)

    with open(os.path.join(service_dir, MANIFEST_NAME), "w", encoding="UTF-8") as fd:
        json.dump(sorted(paths), fd, indent=4)
        fd.write("\n")


def sync_configs(
    store: ConfigStore, out_dir: str, services: Sequence[str] = ()
) -> SyncResult:
    """Make <out_dir>/<service>/ hold the current files of the index

    Args:
        store (ConfigStore): the configuration index
        out_dir (str): the folder to bring up to date
        services (Sequence[str]): services to sync, every service of the index
            (and every folder previously synced) if empty

    Returns:
        SyncResult: the files written, left as is, removed and skipped
    """
    result = SyncResult()
    root = os.path.realpath(out_dir)

    # service -> {path: content}
    current = {service: {} for service in services}

    for document in store.latest():
        for service in document["services"]:
            if services and service not in services:
                continue

            current.setdefault(service, {})

            if not document["deleted"]:
                current[service][document["path"]] = document["content"]

    if not services and os.path.isdir(root):
        for name in os.listdir(root):
            if os.path.isfile(os.path.join(root, name, MANIFEST_NAME)):
                current.setdefault(name, {})

    for service in sorted(current):
        sync_service(root, service, current[service], result)

    return result


def sync_service(root: str, service: str, files: dict, result: SyncResult):
    """sync one service folder, files being {path: content}"""
    try:
        check_service(service)
    except ValueError as error:
        LOGGER.error("skipped: %s", error)
        result.skipped.append(service)
        return

    service_dir = os.path.join(root, service)
    synced = []

    for path in sorted(files):
        name = f"{service}/{path}"

        try:
            if path == MANIFEST_NAME:
                raise ValueError(f"{path} is reserved to the sync manifest")

            target = target_path(root, service, path)
        except ValueError as error:
            LOGGER.error("skipped: %s", error)
            result.skipped.append(name)
            continue

        data = serialize(files[path])
        synced.append(path)

        try:
            with open(target, "rb") as fd:
                if fd.read() == data:
                    result.unchanged.append(name)
                    continue
        except OSError:
            pass

        os.makedirs(os.path.dirname(target), exist_ok=True)

        with open(target, "wb") as fd:
            fd.write(data)

        result.written.append(name)

    for path in read_manifest(service_dir):
        if path in files:
            continue

        name = f"{service}/{path}"

        try:
            target = target_path(root, service, path)
        except ValueError as error:
            LOGGER.error("skipped: %s", error)
            result.skipped.append(name)
            continue

        if os.path.isfile(target):
            os.remove(target)
            result.removed.append(name)

    # a path that could not be written stays out of the manifest
    if synced or os.path.exists(os.path.join(service_dir, MANIFEST_NAME)):
        write_manifest(service_dir, synced)

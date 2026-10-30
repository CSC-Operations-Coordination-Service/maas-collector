"""The backup a transfer request names: its definition, its keys, its sink."""

import copy
from dataclasses import dataclass
from typing import Sequence

from maas_collector.backup import instanciate_collector_backup
from maas_collector.backup.named import (
    SECRET_FIELDS,
    backup_definitions,
    resolve_backup,
)
from maas_collector.rawdata.configprovider import ConfigSource
from maas_collector.rawdata.configstore import ConfigStore


class BackupUnavailable(ValueError):
    """The backup of a request cannot serve it: permanent, every product of the
    request ends KO"""


@dataclass(frozen=True)
class ResolvedBackup:
    """A backup definition, ready to build a sink with"""

    name: str
    version: int | None
    block: dict
    """for instanciate_collector_backup, without any secret field"""

    credentials: str | None
    """name of the credential entry holding the S3 keys"""


def _defines(document: dict | None, service: str, name: str) -> bool:
    """whether a configuration document is a live definition of a backup for a
    service"""
    if not document or document.get("deleted"):
        return False
    if service not in document.get("services", []):
        return False
    content = document.get("content") or {}
    return any(
        isinstance(definition, dict) and definition.get("name") == name
        for definition in content.get("backups") or []
    )


def find_backup_document(store: ConfigStore, service: str, name: str) -> dict:
    """The one current document of a service that defines a backup, whatever its path

    Raises:
        BackupUnavailable: none, or several
    """
    current = [
        document for document in store.latest() if _defines(document, service, name)
    ]
    if not current:
        raise BackupUnavailable(
            f"backup {name} has no current definition tagged {service}"
            " (absent, deleted or untagged)"
        )
    if len(current) > 1:
        paths = ", ".join(document["path"] for document in current)
        raise BackupUnavailable(f"backup {name} is defined by several documents: {paths}")
    return current[0]


def resolve_request_backup(
    sources: list[ConfigSource],
    store: ConfigStore | None,
    service: str,
    name: str,
    version: int | None,
) -> ResolvedBackup:
    """The backup a request names, at its version

    Args:
        sources: the files the stage loaded, the latest definitions
        store: the configuration store, None with the file backend
        service: the service of the stage
        name: the backup
        version: the version of the file that defines it, None for the latest

    Raises:
        BackupUnavailable: unknown, deleted, untagged, ambiguous or without versions
    """
    try:
        if version is None:
            definitions = backup_definitions(source.content for source in sources)
            if name not in definitions:
                raise BackupUnavailable(
                    f"backup {name} is not defined by any file the service loads"
                )
        else:
            if store is None:
                raise BackupUnavailable(
                    f"backup {name} is pinned to version {version}: versions need"
                    " the opensearch configuration backend"
                )
            path = find_backup_document(store, service, name)["path"]
            document = store.get(path, version)
            if document is None:
                raise BackupUnavailable(f"version {version} of {name} does not exist")
            definitions = backup_definitions([document.get("content") or {}])
            if name not in definitions:
                raise BackupUnavailable(
                    f"version {version} of {name} does not define it"
                )
        block, credentials = resolve_backup(name, definitions)
    except BackupUnavailable:
        raise
    except (KeyError, ValueError) as error:
        raise BackupUnavailable(f"backup {name} cannot be used: {error}") from error
    return ResolvedBackup(name, version, block, credentials)


def backup_keys(provider, entry: str | None) -> dict:
    """The S3 keys of a backup, read from its credential entry

    Raises:
        BackupUnavailable: no credential source, an entry that is missing or that
            the vault policy does not allow reading, a field that is not an S3 key
            or a key that is missing. A message names fields, never a value.
    """
    if not entry:
        return {}
    if provider is None:
        raise BackupUnavailable(
            f"backup credential entry {entry} cannot be read: no credential source"
        )
    try:
        entries = provider.load([entry])
    except Exception as error:  # pylint: disable=broad-except
        raise BackupUnavailable(
            f"backup credential entry {entry} cannot be read: {type(error).__name__}"
        ) from error
    if entry in provider.unreadable:
        raise BackupUnavailable(
            f"the vault policy does not allow reading the credential entry {entry}"
        )
    if entry not in entries:
        raise BackupUnavailable(f"no credential entry {entry} for the backup")
    values = entries[entry]
    other = sorted(set(values) - set(SECRET_FIELDS))
    if other:
        raise BackupUnavailable(
            f"credential entry {entry} holds {', '.join(other)}: a backup entry"
            f" holds only {' and '.join(SECRET_FIELDS)}"
        )
    missing = [field for field in SECRET_FIELDS if not values.get(field)]
    if missing:
        raise BackupUnavailable(
            f"credential entry {entry} has no {', '.join(missing)}"
        )
    return {field: values[field] for field in SECRET_FIELDS}


def build_sink(
    backup: ResolvedBackup,
    keys: dict,
    destinations: Sequence[str] | None = None,
    factory=instanciate_collector_backup,
):
    """The sink of a backup, on a copy of its block with the keys merged in

    Args:
        backup: the resolved backup
        keys: the S3 keys, see ``backup_keys``
        destinations: write to these destinations only (the S3 buckets of the block
            are narrowed to them), None for all
        factory: builds the sink from the block
    """
    block = copy.deepcopy(backup.block)
    block["backup_name"] = backup.name
    block.update(keys)
    if destinations is not None and "buckets" in block:
        block["buckets"] = [bucket for bucket in block["buckets"] if bucket in destinations]
    return factory(block)

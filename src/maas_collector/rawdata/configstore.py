"""
Versioned collector configurations in OpenSearch.

Every version of a configuration file is a document <path>@<version> of the
maas-collector-config index, and the latest flag marks the current version of each
path. ConfigStore is the only writer: the admin CLI uses it, the collectors only read
the index (see configprovider.OpenSearchConfigProvider).
"""

import datetime
import logging
import re

from opensearchpy.exceptions import ConflictError, NotFoundError
from opensearchpy.helpers import scan

from maas_collector.backup.named import destination_changes
from maas_collector.rawdata.collector.filecollector import FileCollectorConfiguration
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitorCollectorConfiguration,
)
from maas_collector.rawdata.configprovider import (
    DEFAULT_CONFIG_INDEX,
    BackupDestinationError,
    ConfigIntegrityError,
    ConfigSource,
    current_documents,
    integrity_problems,
    validate_config_sources,
)
from maas_collector.rawdata.implementation import (
    get_collector_class_by_config_classname,
)

LOGGER = logging.getLogger("ConfigStore")

CONFIG_INDEX_BODY = {
    "settings": {"number_of_shards": 1},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "path": {"type": "keyword"},
            "version": {"type": "integer"},
            "latest": {"type": "boolean"},
            "deleted": {"type": "boolean"},
            "services": {"type": "keyword"},
            # the file JSON verbatim: stored, not indexed
            "content": {"type": "object", "enabled": False},
            "interface_names": {"type": "keyword"},
            "model_names": {"type": "keyword"},
            "author": {"type": "keyword"},
            "comment": {"type": "text"},
            "created_at": {"type": "date"},
            "based_on": {"type": "integer"},
        },
    },
}


class ConfigConflictError(Exception):
    """another version of the path was published in the meantime"""


class ConfigNotFoundError(LookupError):
    """no such path or version"""


def check_path(path: str):
    """A document path is a relative path to a .json file, with "/" separators,
    as under /conf/collector/ in a file deployment

    Raises:
        ValueError: on an empty, absolute or non-normalized path
    """
    segments = path.split("/") if path else []

    if (
        not segments
        or "\\" in path
        or any(segment in ("", ".", "..") for segment in segments)
        or not path.endswith(".json")
    ):
        raise ValueError(
            f"bad configuration path {path!r}: expected a relative path like"
            " odata/prip/conf.json, with / separators and no . or .. segment"
        )


# a service name is also a directory of export: a single name, no path
SERVICE_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def check_service(service: str):
    """A service name is a deploy service directory name: odata, monitor-main

    Raises:
        ValueError: on an empty name, or one that is not a single directory name
    """
    if not isinstance(service, str) or not SERVICE_NAME.fullmatch(service):
        raise ValueError(
            f"bad service name {service!r}: expected a single directory name like"
            " odata or monitor-main"
        )


def document_id(path: str, version: int) -> str:
    """identifier of a version document: odata/prip.json@3"""
    return f"{path}@{version}"


def config_class_for(meta: dict) -> type:
    """configuration class a collector entry builds with: the one its class key
    names, FileCollectorConfiguration without class

    Raises:
        ValueError: for an unknown configuration class
    """
    name = meta.get("class")

    if not name:
        return FileCollectorConfiguration

    # the monitor is not in the implementation registry, which it imports
    if name == InterfaceMonitorCollectorConfiguration.__name__:
        return InterfaceMonitorCollectorConfiguration

    try:
        return get_collector_class_by_config_classname(name).CONFIG_CLASS
    except KeyError as error:
        raise ValueError(f"unknown configuration class {name}") from error


def config_classes_for(meta: dict) -> list:
    """validate_config_sources callback: the class the entry names"""
    return [config_class_for(meta)]


def content_names(content: dict) -> tuple[list, list]:
    """interface names and model names of a configuration content, for searching
    in Dashboards"""
    interface_names = set()
    model_names = set()

    for meta in content.get("collectors", []):
        if meta.get("interface_name"):
            interface_names.add(meta["interface_name"])
        if isinstance(meta.get("model"), dict):
            model_names.add(meta["model"]["name"])
        elif meta.get("model"):
            model_names.add(meta["model"])

    for model_meta in content.get("models", []):
        model_names.add(model_meta["name"])

    return sorted(interface_names), sorted(model_names)


class ConfigStore:
    """Writer and reader of the configuration documents"""

    def __init__(
        self,
        client,
        index: str = DEFAULT_CONFIG_INDEX,
        evaluate: bool = True,
        report_index: str = "raw-data-backup",
    ):
        """
        Args:
            client: OpenSearch client
            index (str, optional): index of the configuration documents
            evaluate (bool, optional): False validates publications and rollbacks
                without evaluating any lambda or converter, for a service that must
                not execute what it validates. Defaults to True.
            report_index (str, optional): the backup reports, read with a trailing
                wildcard (the yearly partitions) to know whether a backup has reports.
                Defaults to raw-data-backup.
        """
        self.client = client

        self.index = index

        self.evaluate = evaluate

        self.report_index = report_index

        self._index_ready = False

    def init(self) -> bool:
        """Create the index with its mapping, when it does not exist

        Returns:
            bool: True if the index was created
        """
        if self.client.indices.exists(index=self.index):
            self._index_ready = True
            return False

        self.client.indices.create(index=self.index, body=CONFIG_INDEX_BODY)
        self._index_ready = True
        LOGGER.info("Created the configuration index %s", self.index)
        return True

    def _scan(self, query: dict) -> list:
        """every document matching a query, none without index"""
        try:
            return [
                hit["_source"]
                for hit in scan(self.client, index=self.index, query={"query": query})
            ]
        except NotFoundError:
            return []

    def history(self, path: str) -> list:
        """every version of a path, oldest first"""
        return sorted(
            self._scan({"term": {"path": path}}), key=lambda doc: doc["version"]
        )

    def get(self, path: str, version: int | None = None) -> dict | None:
        """a version of a path, the current one by default

        Returns:
            dict | None: document, None if there is no such version
        """
        if version is None:
            history = self.history(path)
            return history[-1] if history else None

        try:
            return self.client.get(index=self.index, id=document_id(path, version))[
                "_source"
            ]
        except NotFoundError:
            return None

    def latest(self, service: str = "", interface: str = "") -> list:
        """current version of every path, tombstones included, sorted by path

        Args:
            service (str, optional): only the paths tagged with this service
            interface (str, optional): only the paths declaring this interface
        """
        try:
            return current_documents(self.client, self.index, service, interface)
        except NotFoundError:
            return []

    # the audit fields make a long but explicit signature
    # pylint: disable=too-many-arguments
    def publish(
        self,
        path: str,
        content: dict,
        services: list,
        author: str,
        comment: str,
        base_version: int | None = None,
        deleted: bool = False,
        based_on: int | None = None,
    ) -> dict:
        """Write a new version of a path, and make it the current one

        Args:
            path (str): path relative to the service directory
            content (dict): configuration file content
            services (list): services loading the file
            author (str): who publishes
            comment (str): why
            base_version (int, optional): version the edit started from. Refused
                when another version was published since.
            deleted (bool, optional): publish a tombstone. Defaults to False.
            based_on (int, optional): version edited or rolled back to, for the
                audit. Defaults to the current version.

        Raises:
            ConfigConflictError: when base_version is not the current version, or
                when another publish wrote the same version first
            ConfigValidationError: when collectors cannot load the content
            ValueError: without service

        Returns:
            dict: the written document
        """
        check_path(path)

        if not self._index_ready:
            self.init()

        history = self.history(path)
        current = history[-1]["version"] if history else 0

        if base_version is not None and base_version != current:
            raise ConfigConflictError(
                f"{path}: version {current} was published after version {base_version}"
            )

        if not deleted:
            validate_config_sources(
                [ConfigSource(path, content)],
                config_classes_for,
                resolve_models=False,
                evaluate=self.evaluate,
            )

        services = sorted(set(services))
        if not services:
            raise ValueError(f"{path}: a configuration needs at least one service")

        for service in services:
            check_service(service)

        if not deleted:
            self._check_destinations(path, content, history)

        self._check_integrity(path, content, services, deleted, history)

        version = current + 1
        interface_names, model_names = content_names(content)

        document = {
            "path": path,
            "version": version,
            "latest": True,
            "deleted": deleted,
            "services": services,
            "content": content,
            "interface_names": interface_names,
            "model_names": model_names,
            "author": author,
            "comment": comment,
            "created_at": datetime.datetime.now(tz=datetime.UTC).isoformat(),
            "based_on": based_on if based_on is not None else (current or None),
        }

        try:
            self.client.index(
                index=self.index,
                id=document_id(path, version),
                body=document,
                op_type="create",
                refresh="wait_for",
            )
        except ConflictError as error:
            raise ConfigConflictError(
                f"{path}: version {version} was published concurrently"
            ) from error

        self._clear_latest(path, version)

        LOGGER.info("Published %s", document_id(path, version))

        return document

    def _has_reports(self, backup_name: str) -> bool:
        """True when a backup report names the backup, in any yearly partition of the
        report index. Reports are matched on the name alone.

        Raises:
            opensearchpy.exceptions.OpenSearchException: when the report index cannot
                be read (the change is refused), apart from a missing index, which
                means no reports
        """
        try:
            response = self.client.search(
                index=f"{self.report_index}*",
                body={
                    "size": 1,
                    "_source": False,
                    "query": {"term": {"backup_name": backup_name}},
                },
            )
        except NotFoundError:
            return False

        return bool(response["hits"]["hits"])

    def _check_destinations(self, path: str, content: dict, history: list) -> None:
        """refuse a backup that changes its destination once it has reports

        The previous definitions are those of the last version of the path that is not
        a tombstone, so that deleting then creating a backup again is compared with
        what the tombstone replaced.

        Raises:
            BackupDestinationError: a destination key changed, or a name that has
                reports is defined by another path
        """
        previous = {}

        for document in reversed(history):
            if not document.get("deleted"):
                for definition in document["content"].get("backups") or []:
                    if isinstance(definition, dict):
                        previous[definition.get("name")] = definition
                break

        for definition in content.get("backups") or []:
            name = definition.get("name")

            if name in previous:
                changes = destination_changes(previous[name], definition)

                if changes and self._has_reports(name):
                    raise BackupDestinationError(
                        path,
                        f"backup {name}: {', '.join(changes)} cannot change once the"
                        " backup has reports; derive a new backup instead",
                    )

            elif self._has_reports(name):
                raise BackupDestinationError(
                    path,
                    f"backup {name}: the name already has reports; a backup keeps"
                    " its file path, derive a new backup under a new name",
                )

    def _check_integrity(
        self, path: str, content: dict, services: list, deleted: bool, history: list
    ) -> None:
        """refuse a write that leaves a reference dangling in a service it affects

        The affected services are those of the new version (unless it is a tombstone)
        and those of the current version (unless it is one), so untagging, deleting
        and renaming are covered.

        Raises:
            ConfigIntegrityError: an entry or an archive rule of an affected service,
                enabled or not, names a backup the service does not load, or a backup
                names a report model the service does not define
        """
        affected = set()

        if not deleted:
            affected.update(services)

        if history and not history[-1].get("deleted"):
            affected.update(history[-1]["services"])

        problems = []

        for service in sorted(affected):
            sources = [
                ConfigSource(document["path"], document["content"])
                for document in self.latest(service=service)
                if not document["deleted"] and document["path"] != path
            ]

            if not deleted and service in services:
                sources.append(ConfigSource(path, content))

            problems.extend(
                f"service {service}: {line}" for line in integrity_problems(sources)
            )

        if problems:
            raise ConfigIntegrityError(path, problems)

    def _clear_latest(self, path: str, version: int):
        """clear the latest flag of the versions below version. Also repairs the
        flags a crashed publish left behind. A higher version belongs to a later
        publish and keeps its flag."""
        query = {
            "bool": {"filter": [{"term": {"path": path}}, {"term": {"latest": True}}]}
        }
        for doc in self._scan(query):
            if doc["version"] < version:
                self.client.update(
                    index=self.index,
                    id=document_id(path, doc["version"]),
                    body={"doc": {"latest": False}},
                    refresh="wait_for",
                )

    def rollback(
        self,
        path: str,
        to: int,
        author: str,
        comment: str,
        base_version: int | None = None,
    ) -> dict:
        """Publish the content and services of an older version as a new version

        Raises:
            ConfigNotFoundError: when the version does not exist
        """
        # pylint: disable=invalid-name
        target = self.get(path, to)
        if target is None:
            raise ConfigNotFoundError(f"{document_id(path, to)} does not exist")

        return self.publish(
            path,
            target["content"],
            target["services"],
            author,
            comment,
            base_version=base_version,
            deleted=target.get("deleted", False),
            based_on=to,
        )

    def delete(
        self, path: str, author: str, comment: str, base_version: int | None = None
    ) -> dict:
        """Publish a tombstone: the services stop loading the path

        Raises:
            ConfigNotFoundError: when the path does not exist or is deleted already
        """
        current = self.get(path)
        if current is None or current.get("deleted"):
            raise ConfigNotFoundError(f"{path} does not exist")

        return self.publish(
            path,
            {},
            current["services"],
            author,
            comment,
            base_version=base_version,
            deleted=True,
        )

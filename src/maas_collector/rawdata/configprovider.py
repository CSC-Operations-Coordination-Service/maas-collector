"""
Configuration providers: where the collectors read their configuration from.

A provider returns configuration sources. A source is the content of one
configuration file, {collectors?, models?, amqp?}, with its name: the file path for
the file backend, the document path for the OpenSearch backend.
"""

import abc
import dataclasses
import json
import logging
import typing

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import scan

from maas_collector.backup.named import (
    BACKUP_NAME,
    MAX_NAME_LENGTH,
    backup_definitions,
    check_backup_block,
)
from maas_collector.rawdata.configuration import (
    build_collector_configuration,
    build_model_class,
    find_configurations,
)
from maas_collector.rawdata.messenger import parse_chunk_config

LOGGER = logging.getLogger("ConfigProvider")

DEFAULT_CONFIG_INDEX = "maas-collector-config"

# top-level keys of a configuration file. A credential file, {"interfaces": ...},
# has none of them.
CONFIG_KEYS = frozenset({"collectors", "models", "amqp", "backups"})


class ConfigValidationError(ValueError):
    """a configuration source that a collector cannot load"""

    def __init__(self, label: str, reason: str):
        super().__init__(f"{label}: {reason}")

        self.label = label

        self.reason = reason


class ConfigIntegrityError(ConfigValidationError):
    """a write that would leave a reference dangling: a backup or a report model that
    a service does not define"""

    def __init__(self, label: str, problems: list):
        super().__init__(label, "; ".join(problems))

        self.problems = problems


class BackupDestinationError(ConfigValidationError):
    """a write that changes the destination of a backup which has reports"""


def is_config_content(content) -> bool:
    """True for the content of a configuration file: a JSON object whose top-level
    keys are within collectors, models, amqp and backups"""
    return isinstance(content, dict) and set(content) <= CONFIG_KEYS


@dataclasses.dataclass
class ConfigStoreArgs:
    """OpenSearch configuration settings of the collectors"""

    # index holding the configuration documents
    index: str = DEFAULT_CONFIG_INDEX

    # service tag of the documents the collector loads
    service: str = ""


@dataclasses.dataclass(frozen=True)
class ConfigSource:
    """content of one configuration file"""

    # file path, or document path
    name: str

    # {collectors?, models?, amqp?, backups?}
    content: dict

    # document version, None for a file
    version: int | None = None

    @property
    def label(self) -> str:
        """name and version, for the logs: odata/prip.json@3"""
        if self.version is None:
            return self.name
        return f"{self.name}@{self.version}"


class ConfigProvider(abc.ABC):
    """Source of collector configurations"""

    @abc.abstractmethod
    def load(self) -> list[ConfigSource]:
        """Read the configuration sources, in a deterministic order"""

    def fingerprint(self, sources: list[ConfigSource]) -> frozenset | None:
        """Identify a set of sources, to notice a change

        Args:
            sources (list[ConfigSource]): sources returned by load()

        Returns:
            frozenset | None: None when the sources never change while running
        """
        return None


class FileConfigProvider(ConfigProvider):
    """Configuration files: a single file and / or a directory tree"""

    def __init__(self, config_file: str = "", config_dir: str = ""):
        self.config_file = config_file

        self.config_dir = config_dir

    def paths(self) -> list[str]:
        """the configuration file, then the files of the directory tree, hidden
        files and directories excluded"""
        paths = []

        if self.config_file:
            paths.append(self.config_file)

        if self.config_dir:
            paths.extend(find_configurations(self.config_dir))

        return paths

    def load(self) -> list[ConfigSource]:
        """Read every configuration file"""
        sources = []

        for path in self.paths():
            LOGGER.info("Loading configuration file: %s", path)
            with open(path, encoding="UTF-8") as conf_fd:
                sources.append(ConfigSource(path, json.load(conf_fd)))

        return sources


# paths per terms query when reading the current versions of candidate paths
PATH_CHUNK_SIZE = 1024


def current_documents(client, index: str, service: str = "", interface: str = ""):
    """current version of every path, tombstones included, sorted by path.

    The current version of a path is its highest version among the documents
    flagged latest: two of them exist between the two steps of a publish. It is
    picked before the service and interface filters apply, so a newer version that
    drops the service hides the older one that still names it.

    Args:
        client: opensearch-py client
        index (str): configuration index
        service (str, optional): only the paths whose current version names it
        interface (str, optional): only the paths whose current version declares it

    Raises:
        opensearchpy.exceptions.NotFoundError: when the index does not exist

    Returns:
        list[dict]: documents
    """
    latest = {"term": {"latest": True}}

    candidate_filters = [latest]
    if service:
        candidate_filters.append({"term": {"services": service}})
    if interface:
        candidate_filters.append({"term": {"interface_names": interface}})

    candidates = sorted(
        {
            hit["_source"]["path"]
            for hit in scan(
                client,
                index=index,
                query={"query": {"bool": {"filter": candidate_filters}}},
            )
        }
    )

    newest = {}

    for start in range(0, len(candidates), PATH_CHUNK_SIZE):
        paths = candidates[start : start + PATH_CHUNK_SIZE]
        query = {"query": {"bool": {"filter": [latest, {"terms": {"path": paths}}]}}}

        for hit in scan(client, index=index, query=query):
            document = hit["_source"]
            current = newest.get(document["path"])
            if current is None or document["version"] > current["version"]:
                newest[document["path"]] = document

    return [
        newest[path]
        for path in sorted(newest)
        if (not service or service in newest[path]["services"])
        and (not interface or interface in newest[path].get("interface_names", []))
    ]


class OpenSearchConfigProvider(ConfigProvider):
    """Latest configuration documents of a service, in a maas-collector-config index.

    The provider uses the opensearch-py connection registered under the alias using:
    the one FileCollector.setup() creates from the collector arguments, after the
    vault has resolved ES_VAULT_PATH.
    """

    def __init__(self, index: str, service: str, using: str = "default"):
        self.index = index

        self.service = service

        self.using = using

    @property
    def client(self):
        """opensearch-py client of the collector"""
        return es_connections.get_connection(self.using)

    def load(self) -> list[ConfigSource]:
        """Read the current version of every path tagged with the service

        A path keeps its highest version among the documents flagged latest: two of
        them exist between the two steps of a publish. A path whose current version
        is a tombstone is left out.

        Raises:
            ValueError: when the index holds no configuration for the service
            opensearchpy.exceptions.OpenSearchException: when the index cannot be read

        Returns:
            list[ConfigSource]: sources sorted by path
        """
        try:
            documents = current_documents(self.client, self.index, self.service)
        except NotFoundError as error:
            raise ValueError(
                f"no configuration for service {self.service} in index {self.index}:"
                " the index does not exist"
            ) from error

        sources = [
            ConfigSource(document["path"], document["content"], document["version"])
            for document in documents
            if not document.get("deleted")
        ]

        if not sources:
            raise ValueError(
                f"no configuration for service {self.service} in index {self.index}"
            )

        return sources

    def fingerprint(self, sources: list[ConfigSource]) -> frozenset:
        """the (path, version) pairs"""
        return frozenset((source.name, source.version) for source in sources)


def build_config_provider(args) -> ConfigProvider:
    """Create the configuration provider of a collector

    Args:
        args (CollectorArgs): collector arguments

    Raises:
        ValueError: on an unknown backend or incomplete OpenSearch settings

    Returns:
        ConfigProvider: provider
    """
    backend = args.config_backend or "file"

    if backend == "file":
        return FileConfigProvider(args.rawdata_config, args.rawdata_config_dir)

    if backend == "opensearch":
        store_args = args.config_store or ConfigStoreArgs()

        if not store_args.service:
            raise ValueError(
                "CONFIG_BACKEND=opensearch needs the service to load:"
                " set CONFIG_SERVICE or --config-service"
            )

        if not args.es_url:
            raise ValueError(
                "CONFIG_BACKEND=opensearch needs the OpenSearch connection:"
                " set ES_URL or ES_VAULT_PATH"
            )

        if args.rawdata_config or args.rawdata_config_dir:
            LOGGER.warning(
                "RAWDATA_CONFIG / -c and RAWDATA_CONFIG_DIR / -d are ignored:"
                " the configuration comes from the OpenSearch index %s",
                store_args.index,
            )

        return OpenSearchConfigProvider(store_args.index, store_args.service)

    raise ValueError(
        f"Unknown configuration backend {backend}: expected file or opensearch"
    )


def integrity_problems(sources: list) -> list:
    """Find the references that the sources of ONE service leave dangling

    Entries are scanned enabled or not. A model is defined by a `models` item or by
    the inline `model` of an entry. The content may be malformed, as stored
    documents may predate a rule: it is read leniently.

    Args:
        sources (list[ConfigSource]): the files one service loads

    Returns:
        list[str]: one line per problem, each starting with the file path
    """
    problems = []
    backups = {}
    models = set()
    references = []
    report_models = []

    for source in sources:
        content = source.content if isinstance(source.content, dict) else {}

        for definition in content.get("backups") or []:
            if not isinstance(definition, dict):
                continue

            name = definition.get("name")

            if name in backups and backups[name] != source.name:
                problems.append(
                    f"backup {name} is defined in both {backups[name]}"
                    f" and {source.name}"
                )
            else:
                backups.setdefault(name, source.name)

            if definition.get("report_model"):
                report_models.append(
                    (source.name, name, definition["report_model"])
                )

        for model_meta in content.get("models") or []:
            if isinstance(model_meta, dict):
                models.add(model_meta.get("name"))

        for meta in content.get("collectors") or []:
            if not isinstance(meta, dict):
                continue

            if isinstance(meta.get("model"), dict):
                models.add(meta["model"].get("name"))

            interface = meta.get("interface_name")

            if isinstance(meta.get("backup"), str):
                references.append((source.name, interface, "uses", meta["backup"]))

            archive = meta.get("archive")
            if isinstance(archive, dict) and isinstance(archive.get("backup"), str):
                references.append(
                    (source.name, interface, "archives to", archive["backup"])
                )

    for path, interface, verb, backup in references:
        if backup not in backups:
            problems.append(
                f"{path}: interface {interface} {verb} backup {backup},"
                " which is not defined"
            )

    for path, backup, model in report_models:
        if model not in models:
            problems.append(
                f"{path}: backup {backup} names report model {model},"
                " which is not defined"
            )

    return problems


def check_entry_backups(meta: dict) -> None:
    """Check the backup keys of a collector entry, whatever the other files hold

    Raises:
        ValueError: `@` in the interface name, a `backup` that is neither a name nor
            a block, a secret field in an inline block, the same backup for the
            file backup and the archive
    """
    name = meta.get("interface_name")

    if isinstance(name, str) and "@" in name:
        raise ValueError(f"interface name {name} must not contain @")

    backup = meta.get("backup")

    if backup is None:
        return

    if isinstance(backup, str):
        if len(backup) > MAX_NAME_LENGTH or not BACKUP_NAME.fullmatch(backup):
            raise ValueError(
                f"interface {name}: backup name {backup!r} must match"
                f" [a-z0-9][a-z0-9-]* and be at most {MAX_NAME_LENGTH} characters"
            )
    elif isinstance(backup, dict):
        check_backup_block(backup, f"interface {name}: backup")
    else:
        raise ValueError(
            f"interface {name}: backup must be a backup name or a block,"
            f" got {backup!r}"
        )

    archive = meta.get("archive")
    if (
        isinstance(backup, str)
        and isinstance(archive, dict)
        and archive.get("backup") == backup
    ):
        raise ValueError(
            f"interface {name}: backup {backup} cannot be both the file backup"
            " and the archive backup"
        )


def validate_config_sources(
    sources: list,
    config_classes_for: typing.Callable[[dict], list],
    resolve_models: bool = True,
) -> None:
    """Check that collectors can load configuration sources, without changing the
    models the running process has registered

    Args:
        sources (list[ConfigSource]): sources to check
        config_classes_for (Callable[[dict], list]): configuration classes each
            collector entry must build with
        resolve_models (bool, optional): check that every model named by a collector
            entry or a backup report is defined by one of the sources, that every
            backup named by an entry or an archive rule is defined, and that the
            report model of a definition is defined. Off to check a single file,
            whose models and backups may live in another file. Defaults to True.

    Raises:
        ConfigValidationError: on the first source that cannot be loaded
        ConfigIntegrityError: when a reference of the sources is dangling
    """
    defined = set()
    referenced = []

    for source in sources:
        content = source.content

        if not is_config_content(content):
            keys = sorted(content) if isinstance(content, dict) else []
            raise ConfigValidationError(
                source.label,
                f"not a collector configuration: top-level keys {keys},"
                f" expected within {sorted(CONFIG_KEYS)}",
            )

        try:
            for key in ("collectors", "models", "backups"):
                if not isinstance(content.get(key, []), list):
                    raise TypeError(f"{key} must be a list")

            # the definition rules, and the names unique within the file
            backup_definitions([content])

            # the chunk sizes are read as setup() reads them
            parse_chunk_config(content)

            for meta in content.get("collectors", []):
                check_entry_backups(meta)

                for configuration_class in config_classes_for(meta):
                    build_collector_configuration(
                        meta, configuration_class, register=False
                    )

                if isinstance(meta.get("model"), dict):
                    defined.add(meta["model"]["name"])
                elif meta.get("model"):
                    referenced.append((source.label, meta["model"]))

                backup = meta.get("backup")
                if isinstance(backup, dict) and backup.get("report_model"):
                    referenced.append((source.label, backup["report_model"]))

            for model_meta in content.get("models", []):
                build_model_class(model_meta, register=False)
                defined.add(model_meta["name"])

        # any error makes the source unusable: report it with the source label
        # pylint: disable=W0703
        except Exception as error:
            raise ConfigValidationError(
                source.label, f"{error.__class__.__name__}: {error}"
            ) from error

    if resolve_models:
        for label, model_name in referenced:
            if model_name not in defined:
                raise ConfigValidationError(
                    label,
                    f"model {model_name} is defined in none of the configuration"
                    " files",
                )

        problems = integrity_problems(sources)
        if problems:
            raise ConfigIntegrityError("backups", problems)


def describe_config_changes(previous: list, current: list) -> str:
    """Summarize the difference between two sets of sources, for the logs

    Args:
        previous (list[ConfigSource]): sources the collector runs on
        current (list[ConfigSource]): sources read now

    Returns:
        str: like "odata/a.json v3→v4, odata/b.json added, odata/c.json removed"
    """
    before = {source.name: source.version for source in previous}
    after = {source.name: source.version for source in current}

    changes = []
    for name in sorted(before.keys() | after.keys()):
        if name not in before:
            changes.append(f"{name} added")
        elif name not in after:
            changes.append(f"{name} removed")
        elif before[name] != after[name]:
            changes.append(f"{name} v{before[name]}→v{after[name]}")

    return ", ".join(changes)

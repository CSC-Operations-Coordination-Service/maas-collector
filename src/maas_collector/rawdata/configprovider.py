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

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import scan

from maas_collector.rawdata.configuration import find_configurations

LOGGER = logging.getLogger("ConfigProvider")

DEFAULT_CONFIG_INDEX = "maas-collector-config"


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

    # {collectors?, models?, amqp?}
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
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"latest": True}},
                        {"term": {"services": self.service}},
                    ]
                }
            }
        }

        newest = {}

        try:
            for hit in scan(self.client, index=self.index, query=query):
                document = hit["_source"]
                current = newest.get(document["path"])
                if current is None or document["version"] > current["version"]:
                    newest[document["path"]] = document
        except NotFoundError as error:
            raise ValueError(
                f"no configuration for service {self.service} in index {self.index}:"
                " the index does not exist"
            ) from error

        sources = [
            ConfigSource(path, newest[path]["content"], newest[path]["version"])
            for path in sorted(newest)
            if not newest[path].get("deleted")
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

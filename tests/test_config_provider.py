"""Tests for the configuration providers: files and OpenSearch documents"""

import json
import logging
import os
from unittest.mock import patch

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.cli.lib.args import common_parser, get_collector_args
from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
    FileCollectorConfiguration,
)
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    ConfigStoreArgs,
    FileConfigProvider,
    OpenSearchConfigProvider,
    build_config_provider,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

COLLECTOR_D = os.path.join(CONF_DIR, "collector.d")

MODEL_RESOLVE = os.path.join(CONF_DIR, "model_resolve")

INDEX = "maas-collector-config"


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def collector_entry(model_name: str, interface_name: str = "") -> dict:
    """a collector entry building with FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": {"name": model_name, "index": "", "fields": []},
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
    }


def opensearch_args(**kwargs) -> CollectorArgs:
    return CollectorArgs(
        es_url="http://opensearch.example:9200",
        config_backend="opensearch",
        config_store=ConfigStoreArgs(index=INDEX, service="odata"),
        **kwargs,
    )


# ---------------------------------------------------------------- file backend


def test_file_provider_reads_the_visible_files_of_a_tree():
    provider = FileConfigProvider(config_dir=COLLECTOR_D)

    sources = provider.load()

    assert sorted(source.name for source in sources) == sorted(
        configuration.find_configurations(COLLECTOR_D)
    )
    assert len(sources) == 5
    assert all(".hidden" not in source.name for source in sources)
    for source in sources:
        assert source.content == read_json(source.name)
        assert source.version is None


def test_file_provider_reads_the_single_file_first():
    single = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")

    sources = FileConfigProvider(single, MODEL_RESOLVE).load()

    assert sources[0].name == single
    assert len(sources) == 4


def test_file_provider_never_changes():
    provider = FileConfigProvider(config_dir=COLLECTOR_D)

    assert provider.fingerprint(provider.load()) is None


@pytest.mark.parametrize("conf_dir", [COLLECTOR_D, MODEL_RESOLVE])
def test_file_backend_builds_the_same_configurations_as_before(conf_dir):
    collector = FileCollector(CollectorArgs(rawdata_config_dir=conf_dir))
    collector.load_config()

    expected = []
    for path in configuration.find_configurations(conf_dir):
        expected.extend(
            config.name
            for config in configuration.load_json(path, FileCollectorConfiguration)
        )

    assert sorted(config.name for config in collector.configs) == sorted(expected)
    assert all(not isinstance(config.model, str) for config in collector.configs)


def test_model_only_and_amqp_only_files_load(tmp_path):
    (tmp_path / "models.json").write_text(
        json.dumps({"models": [{"name": "OnlyAModel", "index": "", "fields": []}]}),
        encoding="UTF-8",
    )
    (tmp_path / "chunks.json").write_text(
        json.dumps({"amqp": {"new.raw.data.only": {"chunk_size": 64}}}),
        encoding="UTF-8",
    )

    collector = FileCollector(CollectorArgs(rawdata_config_dir=str(tmp_path)))
    collector.load_config()

    assert collector.configs == []
    assert collector._messenger.chunk_config == {"new.raw.data.only": 64}
    assert "OnlyAModel" in configuration.MODEL_DICT


# ---------------------------------------------------------- opensearch backend


def test_opensearch_provider_loads_the_latest_documents_of_its_service(
    fake_opensearch,
):
    add_config_document(fake_opensearch, "odata/a.json", 1, ["odata"], {}, latest=False)
    add_config_document(fake_opensearch, "odata/a.json", 2, ["odata"], {"amqp": {}})
    add_config_document(fake_opensearch, "sftp/b.json", 1, ["sftp"], {"amqp": {}})
    add_config_document(fake_opensearch, "shared/c.json", 4, ["sftp", "odata"], {})
    add_config_document(
        fake_opensearch, "odata/gone.json", 1, ["odata"], {}, latest=False
    )
    add_config_document(
        fake_opensearch, "odata/gone.json", 2, ["odata"], {}, deleted=True
    )

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert sources == [
        ConfigSource("odata/a.json", {"amqp": {}}, 2),
        ConfigSource("shared/c.json", {}, 4),
    ]


def test_opensearch_provider_keeps_the_highest_of_two_latest_versions(
    fake_opensearch,
):
    """a publish flags the new version latest before it clears the old one"""
    add_config_document(fake_opensearch, "odata/a.json", 2, ["odata"], {"amqp": {}})
    add_config_document(fake_opensearch, "odata/a.json", 3, ["odata"], {"models": []})

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert sources == [ConfigSource("odata/a.json", {"models": []}, 3)]


def test_opensearch_provider_hides_a_path_whose_tombstone_is_not_alone(
    fake_opensearch,
):
    """between the two steps of a delete, the previous version is still latest"""
    add_config_document(fake_opensearch, "odata/a.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "odata/b.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "odata/b.json", 2, ["odata"], {}, deleted=True)

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert [source.name for source in sources] == ["odata/a.json"]


def test_opensearch_provider_follows_a_newer_version_without_its_service(
    fake_opensearch,
):
    """between the two steps of a publish that drops the service, the previous
    version is still latest but no longer the current one"""
    add_config_document(fake_opensearch, "odata/keep.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "shared.json", 1, ["odata", "sftp"], {})
    add_config_document(fake_opensearch, "shared.json", 2, ["sftp"], {})

    odata = OpenSearchConfigProvider(INDEX, "odata").load()
    sftp = OpenSearchConfigProvider(INDEX, "sftp").load()

    assert [source.label for source in odata] == ["odata/keep.json@1"]
    assert [source.label for source in sftp] == ["shared.json@2"]


def test_opensearch_provider_order_is_stable(fake_opensearch):
    for path in ("odata/z.json", "odata/a.json", "odata/m/b.json"):
        add_config_document(fake_opensearch, path, 1, ["odata"], {})

    provider = OpenSearchConfigProvider(INDEX, "odata")

    names = [source.name for source in provider.load()]

    assert names == ["odata/a.json", "odata/m/b.json", "odata/z.json"]
    assert provider.fingerprint(provider.load()) == frozenset(
        {("odata/a.json", 1), ("odata/m/b.json", 1), ("odata/z.json", 1)}
    )


def test_opensearch_provider_raises_without_document(fake_opensearch):
    add_config_document(fake_opensearch, "sftp/b.json", 1, ["sftp"], {})

    with pytest.raises(ValueError, match="no configuration for service odata"):
        OpenSearchConfigProvider(INDEX, "odata").load()


def test_opensearch_provider_raises_without_index(fake_opensearch):
    with pytest.raises(ValueError, match="the index does not exist"):
        OpenSearchConfigProvider(INDEX, "odata").load()


# ------------------------------------------------------------------ settings


def test_opensearch_backend_needs_a_service():
    args = opensearch_args()
    args.config_store.service = ""

    with pytest.raises(ValueError, match="CONFIG_SERVICE"):
        build_config_provider(args)


def test_opensearch_backend_needs_the_opensearch_connection():
    args = opensearch_args()
    args.es_url = ""

    with pytest.raises(ValueError, match="ES_URL or ES_VAULT_PATH"):
        build_config_provider(args)


def test_opensearch_backend_ignores_the_configuration_files(caplog):
    args = opensearch_args(rawdata_config_dir=COLLECTOR_D)

    with caplog.at_level(logging.WARNING):
        provider = build_config_provider(args)

    assert isinstance(provider, OpenSearchConfigProvider)
    assert "RAWDATA_CONFIG_DIR" in caplog.text


def test_unknown_backend_is_refused():
    with pytest.raises(ValueError, match="expected file or opensearch"):
        build_config_provider(CollectorArgs(config_backend="consul"))


def test_file_backend_is_the_default():
    provider = build_config_provider(CollectorArgs(rawdata_config_dir=COLLECTOR_D))

    assert isinstance(provider, FileConfigProvider)


CONFIG_ENV = (
    "CONFIG_BACKEND",
    "CONFIG_INDEX",
    "CONFIG_SERVICE",
    "CONFIG_REFRESH_PERIOD",
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
)


@pytest.fixture
def clean_env(monkeypatch):
    for name in CONFIG_ENV:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_configuration_settings_default_to_the_files(clean_env):
    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))

    assert args.config_backend == "file"
    assert args.config_refresh_period == 300
    assert args.config_store == ConfigStoreArgs(index=INDEX, service="")


def test_configuration_settings_come_from_the_environment(clean_env):
    clean_env.setenv("CONFIG_BACKEND", "opensearch")
    clean_env.setenv("CONFIG_INDEX", "my-config")
    clean_env.setenv("CONFIG_SERVICE", "monitor-main")
    clean_env.setenv("CONFIG_REFRESH_PERIOD", "10")

    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))

    assert args.config_backend == "opensearch"
    assert args.config_refresh_period == 10
    assert args.config_store == ConfigStoreArgs(
        index="my-config", service="monitor-main"
    )


# ------------------------------------------------------- collector start-up


def test_collector_loads_its_configuration_from_opensearch(fake_opensearch):
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        1,
        ["odata"],
        {"collectors": [collector_entry("OpenSearchModelA", "ITF_A")]},
    )

    calls = []

    def create_connection(**kwargs):
        # the provider reads through the connection: it must exist by then
        assert collector.config_sources == []
        calls.append(kwargs)

    collector = FileCollector(opensearch_args())
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection",
        side_effect=create_connection,
    ):
        collector.setup()

    assert calls[0]["hosts"] == ["http://opensearch.example:9200"]
    assert collector.get_interface_names() == ["ITF_A"]
    assert collector.config_sources[0].label == "odata/a.json@1"


def test_collector_does_not_start_when_opensearch_is_down(fake_opensearch):
    fake_opensearch.outage = OpenSearchConnectionError("N/A", "unreachable", None)

    collector = FileCollector(opensearch_args())
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection"
    ), pytest.raises(OpenSearchConnectionError):
        collector.setup()


OPENSEARCH_SECRET = {
    "url": "https://opensearch.example:9200",
    "username": "maas",
    "password": "s3cr3t-opensearch",
}


def test_configuration_reads_through_the_vault_connection(
    fake_vault, fake_opensearch, clean_env, tmp_path
):
    """ES_VAULT_PATH alone gives the connection: no ES_URL is needed"""
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")
    fake_vault.put("secret", "maas/cds/opensearch", OPENSEARCH_SECRET)
    for name, value in {
        "VAULT_ADDR": fake_vault.url,
        "VAULT_AUTH_METHOD": "token",
        "VAULT_TOKEN_FILE": str(token_file),
        "ES_VAULT_PATH": "maas/cds/opensearch",
        "CONFIG_BACKEND": "opensearch",
        "CONFIG_SERVICE": "odata",
    }.items():
        clean_env.setenv(name, value)
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        1,
        ["odata"],
        {"collectors": [collector_entry("VaultConnectionModel", "ITF_VAULT")]},
    )

    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))
    collector = FileCollector(args)
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection"
    ) as create_connection:
        collector.setup()

    assert create_connection.call_args.kwargs["hosts"] == [
        "https://maas:s3cr3t-opensearch@opensearch.example:9200"
    ]
    assert collector.get_interface_names() == ["ITF_VAULT"]

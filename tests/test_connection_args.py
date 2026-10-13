"""Tests for the OpenSearch and AMQP connections read from OpenBao / Vault"""

import logging
from argparse import Namespace

import pytest
from kombu import Connection
from opensearchpy import OpenSearch

from maas_collector.rawdata.cli.lib.args import (
    common_parser,
    get_collector_args,
    get_es_credentials_url,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs

OPENSEARCH = {
    "url": "https://opensearch.example:9200",
    "username": "maas",
    "password": "s3cr3t-opensearch",
}

AMQP = {
    "url": "amqp://rabbit.example:5672//",
    "username": "maas",
    "password": "s3cr3t-amqp",
}

CONNECTION_ENV = (
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
    "AMQP_URL",
    "AMQP_USERNAME",
    "AMQP_PASSWORD",
    "AMQP_VAULT_PATH",
    "CREDENTIAL_BACKEND",
    "VAULT_ADDR",
    "VAULT_KV_MOUNT",
    "VAULT_AUTH_METHOD",
    "VAULT_ROLE_ID",
    "VAULT_SECRET_ID_FILE",
    "VAULT_TOKEN_FILE",
    "VAULT_TOKEN",
)


@pytest.fixture
def vault_env(fake_vault, monkeypatch, tmp_path):
    """environment of a collector reading its connections from the fake vault"""
    for name in CONNECTION_ENV:
        monkeypatch.delenv(name, raising=False)

    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token + "\n", encoding="UTF-8")

    monkeypatch.setenv("VAULT_ADDR", fake_vault.url)
    monkeypatch.setenv("VAULT_AUTH_METHOD", "token")
    monkeypatch.setenv("VAULT_TOKEN_FILE", str(token_file))

    fake_vault.put("secret", "maas/cds/opensearch", OPENSEARCH)
    fake_vault.put("secret", "maas/cds/amqp", AMQP)

    return monkeypatch


def parse(argv: list) -> CollectorArgs:
    return get_collector_args(CollectorArgs, common_parser().parse_args(argv))


def test_connections_come_from_the_vault(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    args = parse([])

    assert args.es_url == "https://maas:s3cr3t-opensearch@opensearch.example:9200"
    assert args.amqp_url == "amqp://maas:s3cr3t-amqp@rabbit.example:5672//"


def test_vault_paths_come_from_the_command_line(vault_env):
    args = parse(
        [
            "--es-vault-path",
            "maas/cds/opensearch",
            "--amqp-vault-path",
            "maas/cds/amqp",
        ]
    )

    assert args.es_url == "https://maas:s3cr3t-opensearch@opensearch.example:9200"
    assert args.amqp_url == "amqp://maas:s3cr3t-amqp@rabbit.example:5672//"


def test_one_connection_from_the_vault_the_other_from_the_environment(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_URL", "amqp://localhost:5672//")
    vault_env.setenv("AMQP_USERNAME", "guest")
    vault_env.setenv("AMQP_PASSWORD", "guest")

    args = parse([])

    assert args.es_url == "https://maas:s3cr3t-opensearch@opensearch.example:9200"
    assert args.amqp_url == "amqp://guest:guest@localhost:5672//"


def test_vault_connection_replaces_the_environment(vault_env, caplog):
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("ES_URL", "http://old-opensearch:9200")
    vault_env.setenv("ES_USERNAME", "admin")
    vault_env.setenv("ES_PASSWORD", "old-plain-text-password")

    with caplog.at_level(logging.WARNING):
        args = parse([])

    assert args.es_url == "https://maas:s3cr3t-opensearch@opensearch.example:9200"

    warnings = " ".join(
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.WARNING
    )
    for name in ("ES_URL", "ES_USERNAME", "ES_PASSWORD"):
        assert name in warnings

    assert "old-plain-text-password" not in caplog.text
    assert "s3cr3t-opensearch" not in caplog.text


def test_vault_credentials_reach_the_clients_unchanged(vault_env, fake_vault):
    """the vault holds raw values: a "/", "?" or "#" must not end the host part of
    the url, nor a "%41" turn into "A" """
    username = "maas@cds"
    password = "a/b#c%41?d:e@f"
    fake_vault.put(
        "secret",
        "maas/cds/opensearch",
        {**OPENSEARCH, "username": username, "password": password},
    )
    fake_vault.put(
        "secret", "maas/cds/amqp", {**AMQP, "username": username, "password": password}
    )
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    args = parse([])

    opensearch_host = OpenSearch(hosts=[args.es_url]).transport.hosts[0]
    assert opensearch_host["host"] == "opensearch.example"
    assert opensearch_host["port"] == 9200
    assert opensearch_host["http_auth"] == f"{username}:{password}"

    amqp = Connection(args.amqp_url)
    assert (amqp.hostname, amqp.port) == ("rabbit.example", 5672)
    assert amqp.virtual_host == "/"
    assert (amqp.userid, amqp.password) == (username, password)


def test_environment_credentials_are_used_as_they_are(vault_env):
    """ES_PASSWORD may hold an already encoded value: it is not encoded again"""
    vault_env.setenv("ES_URL", "http://localhost:9200")
    vault_env.setenv("ES_USERNAME", "admin")
    vault_env.setenv("ES_PASSWORD", "a%2Fb")

    args = parse([])

    assert args.es_url == "http://admin:a%2Fb@localhost:9200"


def test_vault_connection_without_authentication(vault_env, fake_vault):
    fake_vault.put("secret", "maas/cds/amqp", {"url": "amqp://rabbit.example:5672//"})
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    args = parse([])

    assert args.amqp_url == "amqp://rabbit.example:5672//"


def test_missing_vault_secret_stops_the_collector(vault_env):
    vault_env.setenv("ES_VAULT_PATH", "maas/other/opensearch")

    with pytest.raises(ValueError, match="OpenSearch.*maas/other/opensearch"):
        parse([])


def test_vault_secret_without_url_stops_the_collector(vault_env, fake_vault):
    fake_vault.put("secret", "maas/cds/amqp", {"username": "maas", "password": "pw"})
    vault_env.setenv("AMQP_VAULT_PATH", "maas/cds/amqp")

    with pytest.raises(ValueError, match="AMQP.*url"):
        parse([])


def test_vault_path_needs_the_vault_settings(vault_env):
    vault_env.delenv("VAULT_ADDR")
    vault_env.setenv("ES_VAULT_PATH", "maas/cds/opensearch")

    with pytest.raises(ValueError, match="VAULT_ADDR"):
        parse([])


def test_without_vault_path_the_environment_is_used(vault_env, fake_vault):
    vault_env.setenv("ES_URL", "http://localhost:9200")
    vault_env.setenv("ES_USERNAME", "admin")
    vault_env.setenv("ES_PASSWORD", "admin")

    args = parse([])

    assert args.es_url == "http://admin:admin@localhost:9200"
    assert fake_vault.logins == 0


def test_namespace_built_without_the_vault_options_uses_its_url():
    """callers that build their own namespace keep working"""
    namespace = Namespace(
        es_url="http://localhost:9200", es_username="admin", es_password="admin"
    )

    assert get_es_credentials_url(namespace) == "http://admin:admin@localhost:9200"

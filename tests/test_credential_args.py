"""Tests for the credential source command line options"""

import pytest

from maas_collector.rawdata.cli.lib.args import common_parser, get_collector_args
from maas_collector.rawdata.collector.credentialprovider import VaultArgs
from maas_collector.rawdata.collector.filecollector import CollectorArgs

CREDENTIAL_ENV = {
    "CREDENTIAL_FILE": "",
    "CREDENTIAL_BACKEND": "vault",
    "CREDENTIAL_REFRESH_PERIOD": "60",
    "VAULT_ADDR": "https://bao.example:8200",
    "VAULT_KV_MOUNT": "kv",
    "VAULT_PATH_PREFIX": "maas-collector/cds",
    "VAULT_AUTH_METHOD": "approle",
    "VAULT_ROLE_ID": "maas-collector-role",
    "VAULT_SECRET_ID_FILE": "/run/secrets/maas_collector_vault_secret_id",
    "VAULT_TOKEN_FILE": "",
}


@pytest.fixture
def clean_env(monkeypatch):
    for name in CREDENTIAL_ENV:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def parse(argv: list) -> CollectorArgs:
    return get_collector_args(CollectorArgs, common_parser().parse_args(argv))


def test_vault_settings_come_from_the_environment(clean_env):
    for name, value in CREDENTIAL_ENV.items():
        clean_env.setenv(name, value)

    args = parse([])

    assert args.credential_backend == "vault"
    assert args.credential_refresh_period == 60
    assert args.vault == VaultArgs(
        url="https://bao.example:8200",
        kv_mount="kv",
        path_prefix="maas-collector/cds",
        auth_method="approle",
        role_id="maas-collector-role",
        secret_id_file="/run/secrets/maas_collector_vault_secret_id",
        token_file="",
    )


def test_vault_settings_come_from_the_command_line(clean_env):
    args = parse(
        [
            "--credential-backend",
            "vault",
            "--credential-refresh-period",
            "0",
            "--vault-addr",
            "http://127.0.0.1:8200",
            "--vault-auth-method",
            "token",
            "--vault-token-file",
            "/run/secrets/token",
        ]
    )

    assert args.credential_backend == "vault"
    assert args.credential_refresh_period == 0
    assert args.vault.url == "http://127.0.0.1:8200"
    assert args.vault.auth_method == "token"
    assert args.vault.token_file == "/run/secrets/token"


def test_credential_file_stays_the_default_source(clean_env):
    args = parse(["--credential-file", "/conf/credentials.json"])

    assert args.credential_file == "/conf/credentials.json"
    assert args.credential_backend == "file"
    assert args.credential_refresh_period == 300
    assert args.vault == VaultArgs()

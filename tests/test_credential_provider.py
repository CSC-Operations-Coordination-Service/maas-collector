"""Tests for the credential providers: credential file and OpenBao / Vault"""

import os

import hvac
import pytest

from maas_collector.rawdata.collector.credentialprovider import (
    FileCredentialProvider,
    VaultArgs,
    VaultCredentialProvider,
    build_credential_provider,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs

CREDENTIAL_FILE = os.path.join(
    os.path.dirname(__file__), "conf", "test-maas-collector-credentials.json"
)

COMMON_CRED = {
    "token_field_header": "my_super_header",
    "auth_method": "Basic",
    "odata_product_url": "http://my_super_product_url",
    "client_username": "my_super_user",
    "client_password": "my_super_password",
}


@pytest.fixture
def vault_args(fake_vault, tmp_path):
    """AppRole settings for the fake vault, secret id in a file like a Swarm secret"""
    secret_id_file = tmp_path / "secret_id"
    # secret files usually end with a new line
    secret_id_file.write_text(fake_vault.secret_id + "\n", encoding="UTF-8")

    return VaultArgs(
        url=fake_vault.url,
        role_id=fake_vault.role_id,
        secret_id_file=str(secret_id_file),
    )


def test_file_provider_returns_every_entry_by_name():
    entries = FileCredentialProvider(CREDENTIAL_FILE).load(set())

    assert len(entries) == 10
    assert entries["common_cred"] == COMMON_CRED
    assert entries["interface_beta_commun_cred"] == {
        "odata_product_url": "http://my_super_product_url/custom_for_beta"
    }


def test_file_provider_raises_when_the_file_is_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        FileCredentialProvider(str(tmp_path / "missing.json")).load(set())


def test_vault_provider_reads_the_requested_entries(fake_vault, vault_args):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    fake_vault.put("secret", "maas-collector/basic_test", {"client_password": "other"})
    fake_vault.put("secret", "maas-collector/not_requested", {"client_password": "x"})

    entries = VaultCredentialProvider(vault_args).load({"common_cred", "basic_test"})

    assert entries == {
        "common_cred": COMMON_CRED,
        "basic_test": {"client_password": "other"},
    }


def test_vault_provider_leaves_out_names_without_secret(fake_vault, vault_args):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)

    entries = VaultCredentialProvider(vault_args).load(
        {"common_cred", "no_such_interface"}
    )

    assert entries == {"common_cred": COMMON_CRED}


def test_vault_provider_reads_under_the_configured_mount_and_prefix(
    fake_vault, vault_args
):
    fake_vault.put("kv", "maas-collector/cds/common_cred", COMMON_CRED)
    fake_vault.put("secret", "maas-collector/common_cred", {"client_password": "x"})
    vault_args.kv_mount = "kv"
    vault_args.path_prefix = "maas-collector/cds"

    entries = VaultCredentialProvider(vault_args).load({"common_cred"})

    assert entries == {"common_cred": COMMON_CRED}


def test_vault_provider_keeps_its_token_between_loads(fake_vault, vault_args):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    provider = VaultCredentialProvider(vault_args)

    provider.load({"common_cred"})
    provider.load({"common_cred"})

    assert fake_vault.logins == 1


def test_vault_provider_logs_in_again_when_its_token_expired(fake_vault, vault_args):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    provider = VaultCredentialProvider(vault_args)
    provider.load({"common_cred"})

    fake_vault.expire_tokens()
    fake_vault.put(
        "secret",
        "maas-collector/common_cred",
        {**COMMON_CRED, "client_password": "rotated"},
    )

    entries = provider.load({"common_cred"})

    assert entries["common_cred"]["client_password"] == "rotated"
    assert fake_vault.logins == 2


def test_vault_provider_gives_up_after_one_new_login(fake_vault, vault_args):
    """a policy that denies the read must not end up in a login loop"""
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    fake_vault.denied.add("secret/maas-collector/common_cred")

    with pytest.raises(hvac.exceptions.Forbidden):
        VaultCredentialProvider(vault_args).load({"common_cred"})

    assert fake_vault.logins == 2


def test_vault_provider_raises_when_the_secret_id_is_refused(fake_vault, vault_args):
    fake_vault.secret_id = "revoked"

    with pytest.raises(hvac.exceptions.InvalidRequest):
        VaultCredentialProvider(vault_args).load({"common_cred"})


def test_vault_provider_authenticates_with_a_token_file(fake_vault, tmp_path):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token + "\n", encoding="UTF-8")

    provider = VaultCredentialProvider(
        VaultArgs(url=fake_vault.url, auth_method="token", token_file=str(token_file))
    )

    assert provider.load({"common_cred"}) == {"common_cred": COMMON_CRED}


@pytest.mark.parametrize(
    "vault_args, setting",
    [
        (VaultArgs(url="", role_id="r", secret_id_file="f"), "VAULT_ADDR"),
        (VaultArgs(url="http://v", role_id="", secret_id_file="f"), "VAULT_ROLE_ID"),
        (
            VaultArgs(url="http://v", role_id="r", secret_id_file=""),
            "VAULT_SECRET_ID_FILE",
        ),
        (VaultArgs(url="http://v", auth_method="kerberos"), "VAULT_AUTH_METHOD"),
    ],
)
def test_vault_provider_names_the_missing_setting(vault_args, setting):
    with pytest.raises(ValueError, match=setting):
        VaultCredentialProvider(vault_args)


def test_no_provider_without_credential_file():
    assert build_credential_provider(CollectorArgs()) is None


def test_file_backend_reads_the_credential_file():
    provider = build_credential_provider(CollectorArgs(credential_file=CREDENTIAL_FILE))

    assert provider.load(set())["common_cred"] == COMMON_CRED


def test_vault_backend_reads_the_vault(fake_vault, vault_args):
    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)

    provider = build_credential_provider(
        CollectorArgs(credential_backend="vault", vault=vault_args)
    )

    assert provider.load({"common_cred"}) == {"common_cred": COMMON_CRED}


def test_vault_backend_requires_the_vault_settings():
    with pytest.raises(ValueError, match="VAULT_ADDR"):
        build_credential_provider(CollectorArgs(credential_backend="vault"))


def test_unknown_backend_is_rejected():
    with pytest.raises(ValueError, match="keyring"):
        build_credential_provider(CollectorArgs(credential_backend="keyring"))

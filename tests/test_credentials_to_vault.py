"""Tests for scripts/credentials_to_vault.py, moving a credential file to a vault"""

import os
import subprocess
import sys

from maas_collector.rawdata.collector.credentialprovider import (
    FileCredentialProvider,
    VaultArgs,
    VaultCredentialProvider,
)

SCRIPT = os.path.join(
    os.path.dirname(__file__), os.pardir, "scripts", "credentials_to_vault.py"
)

CREDENTIAL_FILE = os.path.join(
    os.path.dirname(__file__), "conf", "test-maas-collector-credentials.json"
)


def run_script(fake_vault, *argv) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, SCRIPT, CREDENTIAL_FILE, *argv],
        env={
            **os.environ,
            "VAULT_ADDR": fake_vault.url,
            "VAULT_TOKEN": fake_vault.root_token,
        },
        capture_output=True,
        text=True,
        check=True,
    )


def test_collector_reads_from_the_vault_what_the_file_held(fake_vault, tmp_path):
    result = run_script(fake_vault, "--path-prefix", "maas-collector/cds")

    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")
    provider = VaultCredentialProvider(
        VaultArgs(
            url=fake_vault.url,
            path_prefix="maas-collector/cds",
            auth_method="token",
            token_file=str(token_file),
        )
    )
    file_entries = FileCredentialProvider(CREDENTIAL_FILE).load(())

    assert provider.load(file_entries.keys()) == file_entries
    assert "my_super_password" not in result.stdout + result.stderr


def test_dry_run_writes_nothing_and_prints_no_value(fake_vault):
    result = run_script(fake_vault, "--dry-run")

    assert not fake_vault.secrets
    assert "secret/maas-collector/common_cred" in result.stdout
    assert "client_password" in result.stdout
    assert "my_super_password" not in result.stdout + result.stderr

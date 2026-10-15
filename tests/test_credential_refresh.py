"""Tests for picking up rotated credentials without restarting the collector"""

import json
import logging
import os
import shutil
from unittest.mock import patch

import hvac
import pytest

from maas_collector.rawdata.collector import credentialmixin
from maas_collector.rawdata.collector.credentialprovider import (
    FileCredentialProvider,
    VaultArgs,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.collector.odatacollector import (
    ODataCollector,
    ODataConfiguration,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

# two interfaces sharing the common_cred entry, the second one overriding the url
COMMON_CRED_CONF = os.path.join(
    CONF_DIR, "test-maas-collector-odata-oauth-common-cred.json"
)

MONITOR_CONF = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")

# basic_test interface, probed by the monitor
PROBED_CONF = os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")

CREDENTIAL_FILE = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")

COMMON_CRED = {
    "token_field_header": "my_super_header",
    "auth_method": "Basic",
    "odata_product_url": "http://my_super_product_url",
    "client_username": "my_super_user",
    "client_password": "my_super_password",
}

BETA_URL = "http://my_super_product_url/custom_for_beta"


class FakeClock:
    """replace the time module of credentialmixin to move time forward"""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        """seconds"""
        return self.now


class ProbeResponse:
    """an OData answer listing one product"""

    status_code = 200

    @staticmethod
    def json():
        """response body"""
        return {"Id": "3d5934fa-ca8a-4e74-a394-1eca3626e967"}


@pytest.fixture
def clock(monkeypatch):
    fake_clock = FakeClock()
    monkeypatch.setattr(credentialmixin, "time", fake_clock)
    return fake_clock


@pytest.fixture
def credential_file(tmp_path):
    path = tmp_path / "credentials.json"
    shutil.copy(CREDENTIAL_FILE, path)
    return path


@pytest.fixture(autouse=True)
def no_database_nor_bus():
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection",
        return_value=True,
    ), patch("maas_collector.rawdata.messenger.Messenger.setup", return_value=True):
        yield


def set_password(path, entry_name: str, password: str):
    """rotate the password of a credential file entry"""
    content = json.loads(path.read_text(encoding="UTF-8"))

    for entry in content["interfaces"]:
        if entry["name"] == entry_name:
            entry["client_password"] = password

    path.write_text(json.dumps(content), encoding="UTF-8")


def start_collector(**kwargs) -> ODataCollector:
    collector = ODataCollector(
        CollectorArgs(rawdata_config=COMMON_CRED_CONF, **kwargs), ODataConfiguration()
    )
    collector.setup()
    return collector


def passwords(collector) -> list:
    return [config.client_password for config in collector.configs]


def edit_entries(path, edit):
    """change the entries of a credential file: edit(list of entries)"""
    content = json.loads(path.read_text(encoding="UTF-8"))
    edit(content["interfaces"])
    path.write_text(json.dumps(content), encoding="UTF-8")


@pytest.fixture
def probed_dir(tmp_path):
    """configuration directory of the interfaces the monitor probes"""
    path = tmp_path / "probed"
    path.mkdir()
    shutil.copy(PROBED_CONF, path)
    return path


def start_monitor(credential_file, probed_dir) -> InterfaceMonitor:
    monitor = InterfaceMonitor(
        CollectorArgs(
            rawdata_config=MONITOR_CONF,
            rawdata_config_dir=str(probed_dir),
            credential_file=str(credential_file),
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    monitor.setup()
    return monitor


def test_rotated_password_reaches_every_interface_sharing_it(clock, credential_file):
    collector = start_collector(credential_file=str(credential_file))

    set_password(credential_file, "common_cred", "rotated")
    clock.now += 300
    collector.refresh_credentials()

    assert passwords(collector) == ["rotated", "rotated"]
    assert collector.configs[1].odata_product_url == BETA_URL


def test_credentials_are_not_read_again_before_the_refresh_period(
    clock, credential_file
):
    collector = start_collector(credential_file=str(credential_file))

    set_password(credential_file, "common_cred", "rotated")
    clock.now += 299
    collector.refresh_credentials()

    assert passwords(collector) == ["my_super_password", "my_super_password"]


def test_zero_refresh_period_keeps_the_startup_credentials(clock, credential_file):
    collector = start_collector(
        credential_file=str(credential_file), credential_refresh_period=0
    )

    set_password(credential_file, "common_cred", "rotated")
    clock.now += 100_000
    collector.refresh_credentials()

    assert passwords(collector) == ["my_super_password", "my_super_password"]


def test_failed_refresh_keeps_the_credentials_and_retries_at_next_call(
    clock, credential_file
):
    collector = start_collector(credential_file=str(credential_file))

    credential_file.write_text("{ broken json", encoding="UTF-8")
    clock.now += 300
    collector.refresh_credentials()

    assert passwords(collector) == ["my_super_password", "my_super_password"]

    shutil.copy(CREDENTIAL_FILE, credential_file)
    set_password(credential_file, "common_cred", "rotated")
    # next loop, long before another refresh period
    clock.now += 1
    collector.refresh_credentials()

    assert passwords(collector) == ["rotated", "rotated"]


def test_refresh_logs_no_secret_value(clock, credential_file, caplog):
    collector = start_collector(credential_file=str(credential_file))

    set_password(credential_file, "common_cred", "rotated")
    clock.now += 300
    with caplog.at_level(logging.DEBUG):
        collector.refresh_credentials()

    assert "common_cred" in caplog.text
    assert "rotated" not in caplog.text
    assert "my_super_password" not in caplog.text


def test_refresh_ignores_the_entries_the_collector_does_not_use(
    clock, credential_file, caplog
):
    """the credential file of a stack holds the entries of every collector"""
    collector = start_collector(credential_file=str(credential_file))

    set_password(credential_file, "basic_test", "rotated")
    clock.now += 300
    with caplog.at_level(logging.INFO):
        collector.refresh_credentials()

    assert "Credentials changed" not in caplog.text


def test_removed_attribute_keeps_its_value_with_a_warning(
    clock, credential_file, caplog
):
    collector = start_collector(credential_file=str(credential_file))

    def remove_header(entries):
        for entry in entries:
            if entry["name"] == "common_cred":
                del entry["token_field_header"]

    edit_entries(credential_file, remove_header)
    clock.now += 300
    collector.refresh_credentials()

    assert [config.token_field_header for config in collector.configs] == [
        "my_super_header",
        "my_super_header",
    ]
    assert "token_field_header" in caplog.text


def remove_entry(name: str):
    """edit_entries action deleting an entry"""
    return lambda entries: entries.remove(next(e for e in entries if e["name"] == name))


def test_deleted_entry_keeps_the_values_nothing_else_gives(
    clock, credential_file, caplog
):
    collector = start_collector(credential_file=str(credential_file))

    edit_entries(credential_file, remove_entry("common_cred"))
    clock.now += 300
    collector.refresh_credentials()

    assert passwords(collector) == ["my_super_password", "my_super_password"]
    assert "client_password" in caplog.text


def test_deleted_override_falls_back_to_the_shared_entry(clock, credential_file):
    """as a restart would"""
    collector = start_collector(credential_file=str(credential_file))

    edit_entries(credential_file, remove_entry("interface_beta_commun_cred"))
    clock.now += 300
    collector.refresh_credentials()

    assert collector.configs[1].odata_product_url == COMMON_CRED["odata_product_url"]


def test_failing_update_keeps_the_collector_running_and_is_retried(
    clock, credential_file, caplog
):
    """an entry attribute the configuration refuses must not stop the collector"""
    collector = start_collector(credential_file=str(credential_file))
    set_password(credential_file, "common_cred", "rotated")
    clock.now += 300

    with patch.object(
        collector,
        "set_credential_attributes",
        side_effect=AttributeError("can't set attribute"),
    ):
        collector.refresh_credentials()

    assert any(record.levelno == logging.ERROR for record in caplog.records)
    assert "rotated" not in caplog.text

    # next loop, long before another refresh period
    clock.now += 1
    collector.refresh_credentials()

    assert passwords(collector) == ["rotated", "rotated"]


def test_collector_loop_refreshes_the_credentials(clock, credential_file):
    collector = start_collector(credential_file=str(credential_file), watch_period=0)
    set_password(credential_file, "common_cred", "rotated")
    clock.now += 300

    with patch.object(collector, "start_healthcheck"), patch.object(
        collector, "stop_healthcheck"
    ), patch.object(collector, "_healthcheck"), patch.object(collector, "loop_body"):
        collector.run([])

    assert passwords(collector) == ["rotated", "rotated"]


@pytest.fixture
def vault_args(fake_vault, tmp_path):
    secret_id_file = tmp_path / "secret_id"
    secret_id_file.write_text(fake_vault.secret_id, encoding="UTF-8")

    fake_vault.put("secret", "maas-collector/common_cred", COMMON_CRED)
    fake_vault.put(
        "secret",
        "maas-collector/interface_beta_commun_cred",
        {"odata_product_url": BETA_URL},
    )

    return VaultArgs(
        url=fake_vault.url,
        role_id=fake_vault.role_id,
        secret_id_file=str(secret_id_file),
    )


def test_collector_follows_new_vault_secret_versions(clock, fake_vault, vault_args):
    collector = start_collector(credential_backend="vault", vault=vault_args)

    assert passwords(collector) == ["my_super_password", "my_super_password"]
    assert collector.configs[1].odata_product_url == BETA_URL

    fake_vault.put(
        "secret",
        "maas-collector/common_cred",
        {**COMMON_CRED, "client_password": "rotated"},
    )
    clock.now += 300
    collector.refresh_credentials()

    assert passwords(collector) == ["rotated", "rotated"]


def test_collector_does_not_start_when_the_vault_is_down(fake_vault, vault_args):
    fake_vault.available = False

    with pytest.raises(hvac.exceptions.VaultError):
        start_collector(credential_backend="vault", vault=vault_args)


def test_vault_outage_keeps_the_current_credentials(clock, fake_vault, vault_args):
    collector = start_collector(credential_backend="vault", vault=vault_args)

    fake_vault.available = False
    clock.now += 300
    collector.refresh_credentials()

    assert passwords(collector) == ["my_super_password", "my_super_password"]


def test_vault_outage_logs_one_traceback(clock, fake_vault, vault_args, caplog):
    """an outage lasts many loops: one traceback, then one line per loop"""
    collector = start_collector(credential_backend="vault", vault=vault_args)

    fake_vault.available = False
    for _ in range(3):
        clock.now += 300
        collector.refresh_credentials()

    errors = [record for record in caplog.records if record.levelno >= logging.ERROR]
    assert len(errors) == 3
    assert len([record for record in errors if record.exc_info]) == 1
    assert "my_super_password" not in caplog.text


def test_denied_entry_keeps_its_credentials_while_the_others_refresh(
    clock, fake_vault, vault_args
):
    collector = start_collector(credential_backend="vault", vault=vault_args)

    fake_vault.denied.add("secret/maas-collector/interface_beta_commun_cred")
    fake_vault.put(
        "secret",
        "maas-collector/common_cred",
        {**COMMON_CRED, "client_password": "rotated"},
    )
    for _ in range(3):
        clock.now += 300
        collector.refresh_credentials()

    assert passwords(collector) == ["rotated", "rotated"]
    assert collector.configs[1].odata_product_url == BETA_URL
    assert fake_vault.logins == 1


def test_collector_does_not_start_when_the_policy_denies_an_entry(
    fake_vault, vault_args
):
    fake_vault.denied.add("secret/maas-collector/interface_beta_commun_cred")

    with pytest.raises(PermissionError, match="interface_beta_commun_cred"):
        start_collector(credential_backend="vault", vault=vault_args)


def test_vault_without_any_requested_entry_logs_an_error(
    fake_vault, vault_args, caplog
):
    """a wrong mount or prefix reads nothing: it must not go unnoticed"""
    vault_args.path_prefix = "maas-collector/wrong"

    start_collector(credential_backend="vault", vault=vault_args)

    errors = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.ERROR
    ]
    assert len(errors) == 1
    assert "VAULT_PATH_PREFIX" in errors[0]


def test_monitor_probes_with_the_rotated_credentials(
    clock, credential_file, probed_dir
):
    monitor = start_monitor(credential_file, probed_dir)

    sent_headers = []

    def fake_get(_session, _url, headers=None, **_kwargs):
        sent_headers.append(headers)
        return ProbeResponse()

    try:
        probed = monitor.meta_dict["basic_test"]

        with patch("requests.Session.get", fake_get):
            probed.run_probe()

            set_password(credential_file, "basic_test", "rotated")
            clock.now += 300
            monitor.refresh_credentials()

            probed.run_probe()
    finally:
        monitor.exit_gracefully(9, "")

    assert sent_headers == [
        {"my_super_header": "Basic bXlfc3VwZXJfdXNlcjpteV9zdXBlcl9wYXNzd29yZA=="},
        {"my_super_header": "Basic bXlfc3VwZXJfdXNlcjpyb3RhdGVk"},
    ]


def test_monitor_reads_the_credentials_once_at_startup(
    credential_file, probed_dir, monkeypatch
):
    loads = []
    load = FileCredentialProvider.load

    def counting_load(self, names):
        loads.append(set(names))
        return load(self, names)

    monkeypatch.setattr(FileCredentialProvider, "load", counting_load)

    monitor = start_monitor(credential_file, probed_dir)
    monitor.exit_gracefully(9, "")

    assert len(loads) == 1
    assert "basic_test" in loads[0]


def test_monitor_probes_interfaces_without_credential_entry(
    credential_file, probed_dir
):
    """some interfaces need no credentials: the monitor still probes them"""
    edit_entries(credential_file, remove_entry("basic_test"))

    monitor = start_monitor(credential_file, probed_dir)
    monitor.exit_gracefully(9, "")

    assert "basic_test" in monitor.meta_dict

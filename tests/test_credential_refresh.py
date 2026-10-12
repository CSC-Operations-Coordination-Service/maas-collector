"""Tests for picking up rotated credentials without restarting the collector"""

import json
import logging
import os
import shutil
from unittest.mock import patch

import hvac
import pytest

from maas_collector.rawdata.collector import credentialmixin
from maas_collector.rawdata.collector.credentialprovider import VaultArgs
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.collector.odatacollector import (
    ODataCollector,
    ODataConfiguration,
)
from maas_collector.rawdata.configuration import load_json as load_configuration_json

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


@patch("maas_collector.rawdata.collector.monitorcollector.find_configurations")
def test_monitor_probes_with_the_rotated_credentials(
    find_conf_mock, clock, credential_file, monkeypatch
):
    # start from an empty class level probe authentication cache
    monkeypatch.setattr(ODataCollector, "_ODataCollector__auth_cache", {})
    find_conf_mock.return_value = [PROBED_CONF]

    monitor = InterfaceMonitor(
        CollectorArgs(
            rawdata_config=MONITOR_CONF, credential_file=str(credential_file)
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    monitor.setup()

    sent_headers = []

    def fake_get(_session, _url, headers=None, **_kwargs):
        sent_headers.append(headers)
        return ProbeResponse()

    try:
        probed_config = monitor.meta_dict["basic_test"].config

        with patch("requests.Session.get", fake_get):
            ODataCollector.probe_item(probed_config, "some-uuid")

            set_password(credential_file, "basic_test", "rotated")
            clock.now += 300
            monitor.refresh_credentials()

            ODataCollector.probe_item(probed_config, "some-uuid")
    finally:
        monitor.exit_gracefully(9, "")

    assert sent_headers == [
        {"my_super_header": "Basic bXlfc3VwZXJfdXNlcjpteV9zdXBlcl9wYXNzd29yZA=="},
        {"my_super_header": "Basic bXlfc3VwZXJfdXNlcjpyb3RhdGVk"},
    ]


class EvictedAfterLookup(dict):
    """a cache whose entries a concurrent refresh evicts right after each lookup"""

    def __contains__(self, key):
        found = super().__contains__(key)
        self.pop(key, None)
        return found


def test_probe_survives_a_concurrent_credential_refresh(monkeypatch):
    """monitor retry threads probe while the main loop refreshes credentials"""
    cache = EvictedAfterLookup()
    monkeypatch.setattr(ODataCollector, "_ODataCollector__auth_cache", cache)

    config = next(load_configuration_json(PROBED_CONF, ODataCollector.CONFIG_CLASS))
    for name, value in COMMON_CRED.items():
        setattr(config, name, value)

    with patch("requests.Session.get", return_value=ProbeResponse()):
        ODataCollector.probe_item(config, "some-uuid")
        status = ODataCollector.probe_item(config, "some-uuid")

    assert status["status"] == "Available"

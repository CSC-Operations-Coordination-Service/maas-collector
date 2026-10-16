"""An entry with enabled: false stays in the configuration but is not run"""

import copy
import json
import logging
import os
import shutil
from unittest.mock import Mock, patch

import pytest

from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.collector.httpcollector import HttpCollector
from maas_collector.rawdata.collector.jiraxcollector import JIRAExtendedCollector
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
from maas_collector.rawdata.collector.odatacollector import ODataCollector
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    ConfigValidationError,
    validate_config_sources,
)
from maas_collector.rawdata.configstore import config_classes_for
from maas_collector.rawdata.replay import ReplayArgs

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")
BASIC = os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")
MONITORING = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")
CREDENTIALS = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")

WINDOW = ("2026-10-01T00:00:00", "2026-10-02T00:00:00", "s", 0)


def basic_entry():
    with open(BASIC, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)["collectors"][0]


def entry(name, **extra):
    """an odata entry derived from the basic test configuration"""
    meta = copy.deepcopy(basic_entry())
    meta["interface_name"] = name
    meta["model"] = "TestProduct"
    meta.update(extra)
    return meta


def write_conf(directory, **entries):
    """one file holding the model and an entry per name: {name: extra keys}"""
    content = {
        "models": [basic_entry()["model"]],
        "collectors": [entry(name, **extra) for name, extra in entries.items()],
    }
    with open(os.path.join(directory, "conf.json"), "w", encoding="UTF-8") as out:
        json.dump(content, out)


def make_odata(tmp_path, **entries):
    write_conf(tmp_path, **entries)
    collector = ODataCollector.__new__(ODataCollector)
    collector.logger = logging.getLogger("test")
    collector.configs = []
    collector.disabled_interface_names = set()
    collector._messenger = Mock()
    collector.args = CollectorArgs(
        rawdata_config_dir=str(tmp_path), credential_file=CREDENTIALS
    )
    return collector


def test_disabled_entry_is_not_in_configs(tmp_path):
    collector = make_odata(tmp_path, A={}, B={"enabled": False})
    collector.load_config()
    assert [c.interface_name for c in collector.configs] == ["A"]
    assert collector.disabled_interface_names == {"B"}
    assert collector.get_configuration_by_interface_name("B") is None


def test_entry_without_enabled_is_enabled(tmp_path):
    collector = make_odata(tmp_path, A={}, B={"enabled": True})
    collector.load_config()
    assert [c.interface_name for c in collector.configs] == ["A", "B"]
    assert collector.disabled_interface_names == set()


@pytest.mark.parametrize("value", ["false", 0, None])
def test_non_boolean_enabled_is_rejected(value):
    content = {"collectors": [entry("A", enabled=value)]}
    with pytest.raises(ConfigValidationError, match="enabled"):
        validate_config_sources(
            [ConfigSource("a.json", content)], config_classes_for, False
        )


def test_boolean_enabled_is_accepted():
    content = {"collectors": [entry("A", enabled=False)]}
    validate_config_sources(
        [ConfigSource("a.json", content)], config_classes_for, False
    )


def test_http_replay_of_a_disabled_interface_says_it_is_disabled(tmp_path):
    collector = make_odata(tmp_path, A={}, B={"enabled": False})
    collector.load_config()
    with pytest.raises(ValueError, match="B is disabled"):
        HttpCollector.ingest_replay(collector, ReplayArgs(["B"], *WINDOW))


def test_jira_replay_of_a_disabled_interface_says_it_is_disabled():
    collector = JIRAExtendedCollector.__new__(JIRAExtendedCollector)
    collector.logger = logging.getLogger("test")
    collector.configs = [Mock(interface_name="A", end_point="x")]
    collector.disabled_interface_names = {"B"}
    collector.replay_failures = []
    collector.args = Mock()
    collector._healthcheck = Mock()
    with patch.object(JIRAExtendedCollector, "ingest_tickets") as ingest:
        with pytest.raises(ValueError, match="B is disabled"):
            collector.ingest_replay(ReplayArgs(["A", "B"], *WINDOW))
    ingest.assert_not_called()


def test_monitor_does_not_probe_a_disabled_interface(tmp_path):
    write_conf(tmp_path, A={}, B={"enabled": False})
    shutil.copy(MONITORING, tmp_path / "monitoring.json")
    monitor = InterfaceMonitor(
        CollectorArgs(
            rawdata_config=MONITORING,
            rawdata_config_dir=str(tmp_path),
            credential_file=CREDENTIALS,
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection",
        return_value=True,
    ), patch("maas_collector.rawdata.messenger.Messenger.setup", return_value=True):
        monitor.setup()
    monitor.exit_gracefully(9, "")
    assert "B" not in monitor.meta_dict
    assert "A" in monitor.meta_dict

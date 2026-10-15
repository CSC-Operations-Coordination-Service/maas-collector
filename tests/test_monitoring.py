""" This file test monitoring configuration """

import json
import os
import logging
import shutil
import sys
from unittest.mock import patch

from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitorConfiguration,
    InterfaceMonitor,
)
from maas_collector.rawdata.collector.filecollector import CollectorArgs
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore

TEST_CONF = os.path.join(
    os.path.dirname(__file__), "conf", "maas-collector-interface-monitoring.json"
)

CREDENTIAL_FILE = os.path.join(
    os.path.dirname(__file__), "conf", "test-maas-collector-credentials.json"
)

CONF_WITH_PROBE = os.path.join(
    os.path.dirname(__file__), "conf", "test-maas-collector-odata-basic.json"
)

CONF_WITHOUT_PROBE = os.path.join(
    os.path.dirname(__file__), "conf", "test-maas-collector-odata-no-auth.json"
)

CONFS = os.path.join(os.path.dirname(__file__), "conf")


@patch(
    "opensearchpy.connection.connections.Connections.create_connection",
    return_value=True,
)
@patch("maas_collector.rawdata.messenger.Messenger.setup", return_value=True)
def test_monitoring_disable(amqp_mock, es_mock, tmp_path):
    """this test load configuration for 2 interface monitoring with :
        basic_test interface is probed
        no_auth interface is not probed

    Args:
        amqp_mock (_type_): rabbitmq mock
        es_mock (_type_): database mock
        tmp_path (Path): directory of the probed configurations
    """
    for conf in (CONF_WITH_PROBE, CONF_WITHOUT_PROBE):
        shutil.copy(conf, tmp_path)

    args = CollectorArgs(
        rawdata_config=TEST_CONF,
        rawdata_config_dir=str(tmp_path),
        credential_file=CREDENTIAL_FILE,
    )
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

    monitoring = InterfaceMonitor(
        args,
        InterfaceMonitorConfiguration(**{"interface_name": "OMCS_Monitoring"}),
    )

    monitoring.setup()

    assert len(monitoring.meta_dict) == 1
    assert list(monitoring.meta_dict.values())[0].name == "basic_test"

    monitoring.exit_gracefully(9, "")


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def test_monitor_finds_the_probed_configurations_in_opensearch(fake_opensearch):
    store = ConfigStore(fake_opensearch, "maas-collector-config")
    store.publish(
        "monitor/main.json", read_json(TEST_CONF), ["monitor-main"], "a", "v1"
    )
    for path, conf in (
        ("odata/basic.json", CONF_WITH_PROBE),
        ("odata/no-auth.json", CONF_WITHOUT_PROBE),
    ):
        store.publish(path, read_json(conf), ["odata", "monitor-main"], "a", "v1")

    monitoring = InterfaceMonitor(
        CollectorArgs(
            es_url="http://opensearch.example:9200",
            config_backend="opensearch",
            config_store=ConfigStoreArgs(service="monitor-main"),
            credential_file=CREDENTIAL_FILE,
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    with patch("opensearchpy.connection.connections.Connections.create_connection"):
        monitoring.setup()
    monitoring.exit_gracefully(9, "")

    assert list(monitoring.meta_dict) == ["basic_test"]
    assert monitoring.meta_dict["basic_test"].configuration_path == "odata/basic.json"

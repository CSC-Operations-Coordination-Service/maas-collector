import argparse
import datetime
import socket
import sys

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata.cli.lib.args import common_parser, odata_parser
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator.contract import ReplayRequest
from maas_collector.rawdata.operator.replay import (
    ReplayNotPossible,
    build_command,
    child_environment,
    find_entry,
)

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)


def req(days=1, lane="short", name="ITF_A", operation_id="op1"):
    return ReplayRequest(
        operation_id,
        name,
        START,
        START + datetime.timedelta(days=days),
        "bob",
        lane,
    )


def entry(name="ITF_A", cls="ODataCollectorConfiguration", **extra):
    return {"class": cls, "interface_name": name, **extra}


@pytest.fixture
def store(fake_opensearch):
    return ConfigStore(fake_opensearch)


def add(client, path, collectors, services=("odata",), **kwargs):
    add_config_document(
        client, path, 1, list(services), {"collectors": collectors}, **kwargs
    )


def test_command_for_an_odata_entry():
    command = build_command("odata", "/c", req(), 18081, 3)
    assert command[:4] == [
        sys.executable,
        "-m",
        "maas_collector.rawdata.cli.odata",
        "-v",
    ]
    assert command[4:] == [
        "-d",
        "/c",
        "--healthcheck-port",
        "18081",
        "--replay-interface-name",
        "ITF_A",
        "--replay-start-date",
        "2026-10-01T00:00:00Z",
        "--replay-end-date",
        "2026-10-02T00:00:00Z",
        "--replay-retry",
        "3",
        "--replay-suffix=op1",
    ]


def test_command_keeps_an_operation_id_starting_with_a_dash():
    command = build_command("odata", "/c", req(operation_id="-x"), 18081, 3)
    parser = argparse.ArgumentParser(parents=[common_parser(), odata_parser()])
    namespace = parser.parse_args(command[3:])
    assert namespace.replay_suffix == "-x"
    assert namespace.replay_retry == 3


def test_jira_entry_uses_jirax(store, fake_opensearch):
    add(
        fake_opensearch,
        "j.json",
        [entry("JIRA", "JiraExtendedCollectorConfiguration")],
        ["jira"],
    )
    found = find_entry(store, "jira", "JIRA")
    assert found["class"] == "JiraExtendedCollectorConfiguration"
    assert build_command("jirax", "/c", req(), 1, 3)[2].endswith(".jirax")


def test_find_entry_returns_the_raw_entry(store, fake_opensearch):
    add(fake_opensearch, "a.json", [entry("OTHER"), entry(foo=1)])
    assert find_entry(store, "odata", "ITF_A") == entry(foo=1)


@pytest.mark.parametrize(
    "case, message",
    [
        ("missing", "not found"),
        ("deleted", "ITF_A is deleted"),
        ("other_deleted", "not found"),
        ("other_service", "only to sftp"),
        ("disabled", "is disabled"),
        ("s3_class", "cannot be replayed"),
    ],
)
def test_find_entry_refuses(store, fake_opensearch, case, message):
    if case in ("deleted", "other_deleted"):
        name = "ITF_A" if case == "deleted" else "OTHER"
        add(fake_opensearch, "a.json", [entry(name)])
        # a tombstone, as ConfigStore.delete publishes it: content {}
        store.delete("a.json", "bob", "gone")
    elif case == "other_service":
        add(fake_opensearch, "a.json", [entry()], ["sftp"])
    elif case == "disabled":
        add(fake_opensearch, "a.json", [entry(enabled=False)])
    elif case == "s3_class":
        add(fake_opensearch, "a.json", [entry(cls="S3CollectorConfiguration")])
    with pytest.raises(ReplayNotPossible, match=message):
        find_entry(store, "odata", "ITF_A")


def test_child_environment_drops_the_config_backend():
    env = child_environment(
        {
            "CONFIG_BACKEND": "opensearch",
            "CONFIG_SERVICE": "odata",
            "CONFIG_INDEX": "i",
            "REPLAY_FOO": "1",
            "RAWDATA_CONFIG": "/x.json",
            "RAWDATA_CONFIG_DIR": "/x",
            "ES_URL": "http://x",
            "PATH": "/bin",
        }
    )
    assert env == {"ES_URL": "http://x", "PATH": "/bin"}


def test_child_environment_gives_the_lane_its_own_instance_id():
    env = child_environment({"MAAS_INSTANCE_ID": "odata.1", "PATH": "/bin"}, "short")
    assert env["MAAS_INSTANCE_ID"] == "odata.1-replay-short"


def test_child_environment_without_instance_id_uses_the_host_name():
    env = child_environment({"PATH": "/bin"}, "short")
    assert env["MAAS_INSTANCE_ID"] == f"{socket.gethostname()}-replay-short"


def test_a_worker_without_custom_lanes_finds_no_such_interface(store, fake_opensearch):
    """what a worker that does not know custom requests does with the name"""
    add(fake_opensearch, "a.json", [entry()])
    with pytest.raises(ReplayNotPossible, match="not found"):
        find_entry(store, "odata", "ITF_A@custom")

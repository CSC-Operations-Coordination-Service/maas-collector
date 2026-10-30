import copy
import datetime
import json
import os

import kombu
import pytest
from maas_engine.replay import (
    DONE,
    FAILED,
    RUNNING,
    ChildRunner,
    Lane,
    LaneTimeout,
    ReplayWorker,
)
from maas_engine.replay.request import ReplayRequest

from fakeopensearch import add_config_document

from maas_collector.rawdata.collector.filecollector import BACKUP_FAILED_EXIT_CODE
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator import handler as handler_module
from maas_collector.rawdata.operator.handler import CollectorReplayHandler

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf")


def req(days=1, lane="short", name="ITF_A", operation_id="op1"):
    return ReplayRequest(
        operation_id, name, START, START + datetime.timedelta(days=days), "bob", lane
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


class FakeRecorder:
    def __init__(self):
        self.calls = []

    def update(self, operation_id, status, exit_code=None, details=None):
        call = (operation_id, status)
        if exit_code is not None:
            call += (exit_code,)
        self.calls.append(call)
        self.details = details
        return True


class FakeRunner:
    def __init__(self, code=0, tail="done", boom=None):
        self.code, self.tail, self.boom = code, tail, boom
        self.commands = []

    def terminate(self):
        pass

    def run(self, command, env, timeout, progress, progress_interval=30):
        self.commands.append((command, env, timeout))
        self.copy = command[command.index("-d") + 1].split(os.sep)[-2]
        self.conf_files = sorted(
            name
            for name in os.listdir(command[command.index("-d") + 1])
            if not name.startswith(".")
        )
        if self.boom:
            raise self.boom
        return self.code, self.tail


def lane(store, runner, tmp_path, recorder=None, name="short", port=18081):
    """the replay worker of maas-engine with the collector's handler, and its lane"""
    recorder = recorder or FakeRecorder()
    lanes = [
        Lane("short", "q-short", "k.short", LaneTimeout(base=100)),
        Lane("long", "q-long", "k.long", LaneTimeout(base=100, per_day=50)),
    ]
    worker = ReplayWorker(
        lambda: None,
        recorder,
        CollectorReplayHandler(store, "odata", ("short", "long"), port, 3),
        lanes,
        kombu.Exchange("test"),
        str(tmp_path),
    )
    worker.runners[name] = runner
    chosen = worker.lanes[name]
    return (
        type(
            "Lane",
            (),
            {"handle": lambda self, request: worker.handle(chosen, request)},
        )(),
        recorder,
    )


@pytest.fixture
def odata_store(store, fake_opensearch):
    add(fake_opensearch, "a.json", [entry()])
    return store


def test_handle_records_running_then_done(odata_store, tmp_path):
    runner = FakeRunner()
    replay, recorder = lane(odata_store, runner, tmp_path)
    replay.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    command, env, timeout = runner.commands[0]
    assert command[command.index("-d") + 1] == str(tmp_path / runner.copy / "odata")
    assert runner.conf_files == ["a.json"]
    assert timeout == 100


def test_handle_passes_the_replay_retry_of_the_handler(odata_store, tmp_path):
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path)
    replay_lane.handle(req())
    command = runner.commands[0][0]
    assert command[command.index("--replay-retry") + 1] == "3"


def test_each_lane_has_its_own_health_check_port(odata_store, tmp_path):
    ports = {}
    for name in ("short", "long"):
        runner = FakeRunner()
        replay_lane, _ = lane(odata_store, runner, tmp_path, name=name)
        replay_lane.handle(req())
        command = runner.commands[0][0]
        ports[name] = command[command.index("--healthcheck-port") + 1]
    assert ports == {"short": "18081", "long": "18082"}


def test_handle_takes_the_timeout_of_its_own_lane(odata_store, tmp_path):
    """the lane of the request is the backend's label: the queue decides"""
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path, name="long")
    replay_lane.handle(req(days=10, lane="short"))
    assert runner.commands[0][2] == 10 * 50


def test_the_child_gets_its_lane_in_its_instance_id(odata_store, tmp_path):
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path, name="long")
    replay_lane.handle(req())
    assert runner.commands[0][1]["MAAS_INSTANCE_ID"].endswith("-replay-long")


def test_handle_records_failed_when_the_sync_skipped_a_file(
    odata_store, fake_opensearch, tmp_path
):
    # written straight into the index: ConfigStore.publish refuses this path
    add(fake_opensearch, "../evil.json", [entry("OTHER")])
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path)
    replay_lane.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "odata/../evil.json" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []


def test_handle_records_failed_with_exit_code_and_tail(odata_store, tmp_path):
    replay, recorder = lane(odata_store, FakeRunner(75, "boom line"), tmp_path)
    replay.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED, 75)]
    assert "exit code 75" in recorder.details
    assert "boom line" in recorder.details


def test_handle_of_a_disabled_interface_records_failed_and_returns(
    store, fake_opensearch, tmp_path
):
    add(fake_opensearch, "a.json", [entry(enabled=False)])
    runner = FakeRunner()
    replay, recorder = lane(store, runner, tmp_path)
    replay.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "disabled" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []


def test_handle_removes_its_config_copy(odata_store, tmp_path):
    replay, _ = lane(odata_store, FakeRunner(), tmp_path)
    replay.handle(req())
    assert os.listdir(tmp_path) == []


def test_handle_never_raises_and_cleans_up(odata_store, tmp_path):
    replay, recorder = lane(odata_store, FakeRunner(boom=OSError("no exec")), tmp_path)
    replay.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "no exec" in recorder.details
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("bad_id", ["..", ".", "/"])
def test_handle_keeps_to_its_work_folder_whatever_the_operation_id(
    odata_store, tmp_path, bad_id
):
    workdir = tmp_path / "work"
    workdir.mkdir()
    (workdir / "other-lane").mkdir()
    (tmp_path / "sibling.txt").write_text("x")
    replay, recorder = lane(odata_store, FakeRunner(), workdir)
    replay.handle(req(operation_id=bad_id))
    assert recorder.calls == [(bad_id, RUNNING), (bad_id, DONE, 0)]
    assert (tmp_path / "sibling.txt").exists()
    assert (workdir / "other-lane").is_dir()
    assert os.listdir(workdir) == ["other-lane"]


def test_two_handles_get_distinct_copies(odata_store, tmp_path):
    runner = FakeRunner()
    replay, _ = lane(odata_store, runner, tmp_path)
    replay.handle(req())
    replay.handle(req())
    assert runner.commands[0][0] != runner.commands[1][0]


def odata_entry(**extra):
    """an odata entry the real collector loads, with its model"""
    with open(
        os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json"), encoding="UTF-8"
    ) as conf_fd:
        meta = copy.deepcopy(json.load(conf_fd)["collectors"][0])
    meta["interface_name"] = "ITF_A"
    meta.update(extra)
    return meta


def test_replay_the_child_refuses_records_failed_with_exit_code_1(
    store, tmp_path, monkeypatch
):
    """The real odata collector runs on a copy where the entry is disabled.

    find_entry refuses a disabled entry before any child runs, so the entry is
    disabled between find_entry and the copy, as a publish would do in that gap:
    only the child sees it disabled.
    """
    store.publish("a.json", {"collectors": [odata_entry()]}, ["odata"], "bob", "v1")
    real_sync = handler_module.sync_configs

    def sync_after_a_publish(*args):
        store.publish(
            "a.json",
            {"collectors": [odata_entry(enabled=False)]},
            ["odata"],
            "bob",
            "disabled",
        )
        return real_sync(*args)

    monkeypatch.setattr(handler_module, "sync_configs", sync_after_a_publish)
    # never reached: the connection of the child is lazy
    monkeypatch.setenv("ES_URL", "http://localhost:9200")
    monkeypatch.setenv(
        "CREDENTIAL_FILE",
        os.path.join(CONF_DIR, "test-maas-collector-credentials.json"),
    )
    replay_lane, recorder = lane(store, ChildRunner(), tmp_path, port=0)

    replay_lane.handle(req())

    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED, 1)]
    assert "an interface failed during the replay" in recorder.details
    assert "Interface ITF_A is disabled" in recorder.details


def test_describe_failure_names_the_count_and_the_backup():
    text = CollectorReplayHandler.describe_failure(
        BACKUP_FAILED_EXIT_CODE, "collected\nbackup failed: 3 files to archive-a"
    )

    assert text.startswith("3 files could not be backed up to archive-a\n")
    assert "collected" in text


def test_describe_failure_finds_the_line_among_later_output():
    # Review Focus 3
    tail = "backup failed: 3 files to archive-a\nINFO exiting\nWARNING closing connection"

    assert CollectorReplayHandler.describe_failure(
        BACKUP_FAILED_EXIT_CODE, tail
    ).startswith("3 files could not be backed up to archive-a")


def test_describe_failure_without_the_line_falls_back():
    assert "files could not be backed up" in CollectorReplayHandler.describe_failure(
        BACKUP_FAILED_EXIT_CODE, "boom"
    )


def test_describe_failure_keeps_the_other_codes():
    assert "an interface failed during the replay" in (
        CollectorReplayHandler.describe_failure(1, "tail")
    )
    assert "exit code 9" in CollectorReplayHandler.describe_failure(9, "tail")

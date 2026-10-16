import argparse
import copy
import datetime
import json
import os
import socket
import subprocess
import sys
import threading
import time

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata.cli.lib.args import common_parser, odata_parser
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator import replay
from maas_collector.rawdata.operator.contract import ReplayRequest
from maas_collector.rawdata.operator.records import DONE, FAILED, RUNNING
from maas_collector.rawdata.operator.replay import (
    ChildRunner,
    ReplayLane,
    ReplayNotPossible,
    build_command,
    child_environment,
    find_entry,
    replay_timeout,
)

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf")


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


def test_long_timeout_scales_with_the_window():
    assert replay_timeout("long", req(days=10), 6 * 3600, 3 * 3600) == 30 * 3600
    assert replay_timeout("long", req(days=1), 6 * 3600, 3 * 3600) == 6 * 3600
    assert replay_timeout("short", req(days=10), 6 * 3600, 3 * 3600) == 6 * 3600


def test_silent_child_is_killed_at_the_timeout():
    started = time.monotonic()
    code, tail = ChildRunner().run(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        os.environ.copy(),
        timeout=1,
        progress=lambda _: None,
    )
    assert code == -9
    assert "killed after 1s" in tail
    assert time.monotonic() - started < 10


def test_runner_returns_exit_code_and_last_lines():
    script = "import sys\nfor i in range(30): print('line', i)\nsys.exit(3)"
    code, tail = ChildRunner().run(
        [sys.executable, "-c", script], os.environ.copy(), 30, lambda _: None
    )
    lines = tail.splitlines()
    assert code == 3
    assert len(lines) == 20
    assert lines[-1] == "line 29"


def test_runner_reports_progress_while_the_child_is_silent():
    seen = []
    code, _ = ChildRunner().run(
        [sys.executable, "-c", "import time; print('hi', flush=True); time.sleep(1.5)"],
        os.environ.copy(),
        30,
        seen.append,
        progress_interval=0.3,
    )
    assert code == 0
    assert len(seen) >= 2
    assert "hi" in seen[-1]


def test_terminate_stops_the_running_child_and_is_a_noop_otherwise():
    runner = ChildRunner()
    result = []
    thread = threading.Thread(
        target=lambda: result.append(
            runner.run(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                os.environ.copy(),
                60,
                lambda _: None,
            )
        )
    )
    thread.start()
    deadline = time.monotonic() + 5
    while runner._child is None and time.monotonic() < deadline:
        time.sleep(0.05)
    runner.terminate()
    thread.join(10)
    assert not thread.is_alive()
    assert result[0][0] != 0
    assert "killed after" not in result[0][1]
    runner.terminate()


def test_terminate_before_run_starts_no_child(monkeypatch):
    def popen(*_args, **_kwargs):
        raise AssertionError("a child was started")

    monkeypatch.setattr(subprocess, "Popen", popen)
    runner = ChildRunner()
    runner.terminate()

    code, tail = runner.run([sys.executable, "-c", "pass"], {}, 60, lambda _: None)

    assert (code, tail) == (-15, "stopped before start")
    # still safe to call
    runner.terminate()


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
    recorder = recorder or FakeRecorder()
    return (
        ReplayLane(
            "odata",
            name,
            store,
            recorder,
            runner,
            str(tmp_path),
            port,
            100,
            50,
            3,
        ),
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


def test_handle_passes_the_replay_retry_of_the_lane(odata_store, tmp_path):
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path)
    replay_lane.handle(req())
    command = runner.commands[0][0]
    assert command[command.index("--replay-retry") + 1] == "3"


def test_handle_takes_the_timeout_of_its_own_lane(odata_store, tmp_path):
    """the lane of the request is the backend's label: the queue decides"""
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path, name="long")
    replay_lane.handle(req(days=10, lane="short"))
    assert runner.commands[0][2] == 10 * 50


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


def test_undecodable_output_does_not_stall_the_child():
    script = (
        "import sys\n"
        "sys.stdout.buffer.write(b'bad \\xff\\xfe\\n' * 100000)\n"
        "sys.stdout.buffer.flush()"
    )
    code, tail = ChildRunner().run(
        [sys.executable, "-c", script], os.environ.copy(), 30, lambda _: None
    )
    assert code == 0
    assert "\ufffd" in tail


def test_child_is_stopped_when_run_exits_by_exception(tmp_path):
    runner = ChildRunner()
    pid_file = tmp_path / "pid"
    script = (
        f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(30)"
    )

    def boom(_):
        raise RuntimeError("progress failed")

    with pytest.raises(RuntimeError):
        runner.run(
            [sys.executable, "-c", script],
            os.environ.copy(),
            60,
            boom,
            progress_interval=1,
        )
    assert runner._child is None
    with pytest.raises(ProcessLookupError):
        os.kill(int(pid_file.read_text()), 0)


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
    real_sync = replay.sync_configs

    def sync_after_a_publish(*args):
        store.publish(
            "a.json",
            {"collectors": [odata_entry(enabled=False)]},
            ["odata"],
            "bob",
            "disabled",
        )
        return real_sync(*args)

    monkeypatch.setattr(replay, "sync_configs", sync_after_a_publish)
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

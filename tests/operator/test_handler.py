import builtins
import copy
import dataclasses
import datetime
import json
import os
import threading

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

from maas_collector.rawdata.collector.filecollector import (
    BACKUP_FAILED_EXIT_CODE,
    TRANSFER_FAILED_EXIT_CODE,
)
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.operator import handler as handler_module
from maas_collector.rawdata.operator.handler import CollectorReplayHandler, Prepared

START = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC)
CONF_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "conf")


def req(days=1, lane="short", name="ITF_A", operation_id="op1"):
    return ReplayRequest(
        operation_id, name, START, START + datetime.timedelta(days=days), "bob", lane
    )


def custom_req(lane="short", **fields):
    """a request for ITF_A@custom, with the fields of a custom recollect"""
    return ReplayRequest(
        "op1",
        "ITF_A@custom",
        START,
        START + datetime.timedelta(days=1),
        "bob",
        lane,
        extra={"interface": "ITF_A", **fields},
    )


WINDOW = (
    "PublicationDate ge {publication_start_date} and "
    "PublicationDate le {publication_end_date}"
)
CUSTOM = {
    "odata_query_filter": WINDOW + " and contains(Name,'TYPE_A')",
    "download_file_pattern": ["*.EOF*"],
}


def entry(name="ITF_A", cls="ODataCollectorConfiguration", **extra):
    # the copy is validated with the class of its entries, which need these keys
    return {
        "class": cls,
        "interface_name": name,
        "routing_key": "k",
        "id_field": "id",
        **extra,
    }


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
        conf_dir = command[command.index("-d") + 1]
        self.conf_files = sorted(
            name for name in os.listdir(conf_dir) if not name.startswith(".")
        )
        self.conf = {}
        for folder, _dirs, names in os.walk(conf_dir):
            for name in names:
                if name.startswith("."):
                    continue
                path = os.path.join(folder, name)
                with open(path, encoding="UTF-8") as conf_fd:
                    self.conf[os.path.relpath(path, conf_dir)] = json.load(conf_fd)
        if self.boom:
            raise self.boom
        return self.code, self.tail


class FakeProvider:
    """a credential provider that knows some entries"""

    def __init__(self, *known):
        self.known = known
        self.loaded = []

    def load(self, names):
        self.loaded.append(list(names))
        return {name: {} for name in names if name in self.known}


def make_worker(
    store,
    tmp_path,
    recorder,
    port=18081,
    service="odata",
    credential_provider=None,
    single=False,
):
    """the replay worker of maas-engine with the collector's handler, four lanes"""
    lanes = [
        Lane("short", "q-short", "k.short", LaneTimeout(base=100)),
        Lane("long", "q-long", "k.long", LaneTimeout(base=100, per_day=50)),
        Lane("custom-short", "q-cshort", "k.cshort", LaneTimeout(base=100)),
        Lane("custom-long", "q-clong", "k.clong", LaneTimeout(base=100, per_day=50)),
    ]
    if single:
        lanes = [
            Lane("single", "q-single", "k.short", LaneTimeout(base=100, per_day=50))
        ]
    return ReplayWorker(
        lambda: None,
        recorder,
        CollectorReplayHandler(
            store,
            service,
            ("single",) if single else ("short", "long"),
            port,
            3,
            credential_provider=credential_provider,
        ),
        lanes,
        kombu.Exchange("test"),
        str(tmp_path),
    )


def lane(
    store,
    runner,
    tmp_path,
    recorder=None,
    name="short",
    port=18081,
    service="odata",
    credential_provider=None,
):
    """the replay worker of maas-engine with the collector's handler, and its lane"""
    recorder = recorder or FakeRecorder()
    worker = make_worker(store, tmp_path, recorder, port, service, credential_provider)
    worker.runners[name] = runner
    chosen = worker.lanes[name]
    return (
        type(
            "Lane",
            (),
            {
                "handle": lambda self, request: worker.handle(chosen, request),
                "worker": worker,
            },
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


@pytest.mark.parametrize(
    "name, port",
    [
        ("short", "18081"),
        ("long", "18082"),
        ("custom-short", "18081"),
        ("custom-long", "18082"),
    ],
)
def test_a_custom_lane_has_the_health_check_port_of_its_slot(
    odata_store, tmp_path, name, port
):
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path, name=name)
    replay_lane.handle(
        req() if not name.startswith("custom") else custom_req(configuration="x")
    )
    command = runner.commands[0][0]
    assert command[command.index("--healthcheck-port") + 1] == port


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


def test_the_child_of_a_custom_lane_gets_the_engine_lane_in_its_instance_id(
    odata_store, tmp_path
):
    runner = FakeRunner()
    replay_lane, _ = lane(odata_store, runner, tmp_path, name="custom-long")
    replay_lane.handle(custom_req(configuration="x"))
    assert runner.commands[0][1]["MAAS_INSTANCE_ID"].endswith("-replay-custom-long")


def test_a_custom_request_on_a_custom_lane_runs_the_real_interface(
    odata_store, tmp_path
):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(configuration="named@2"))
    command = runner.commands[0][0]
    assert command[command.index("--replay-interface-name") + 1] == "ITF_A"
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]


@pytest.mark.parametrize("fields", [{"overrides": {}}, {"configuration": "x"}])
def test_a_request_with_one_field_set_is_custom(odata_store, tmp_path, fields):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(**fields))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert len(runner.commands) == 1


def test_an_empty_archive_is_set_so_the_request_is_custom_but_its_choice_is_refused(
    odata_store, tmp_path
):
    """{} is set (Task 3's rule): the request is custom, then it has no backup"""
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(archive={}))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "archive.backup" in recorder.details
    assert runner.commands == []


@pytest.mark.parametrize("name", ["short", "long"])
def test_a_custom_request_on_a_plain_lane_is_refused(
    odata_store, tmp_path, name, caplog
):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name=name)
    replay_lane.handle(custom_req(configuration="x"))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "plain lane" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []
    # the engine's refusal: no traceback is logged
    assert not [r for r in caplog.records if r.levelname == "ERROR"]


@pytest.mark.parametrize("name", ["short", "custom-short"])
@pytest.mark.parametrize(
    "extra", [{"overrides": {"product_per_page": 5}}, {"archive": {"backup": "a"}}]
)
def test_custom_fields_without_the_custom_name_are_refused_on_any_lane(
    odata_store, tmp_path, name, extra
):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name=name)
    request = ReplayRequest(
        "op1", "ITF_A", START, START + datetime.timedelta(days=1), "bob", "short", extra
    )
    replay_lane.handle(request)
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "@custom" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize(
    "name, extra, word",
    [
        ("ITF_A@custom", {"interface": "ITF_A"}, "overrides"),
        ("ITF_A@custom", {"interface": "ITF_A", "configuration": ""}, "overrides"),
        ("ITF_A@custom", {"configuration": "x"}, "interface"),
        ("ITF_A@custom", {"interface": "OTHER", "configuration": "x"}, "interface"),
        ("A@b@custom", {"interface": "A@b", "configuration": "x"}, "@"),
    ],
)
def test_a_malformed_custom_request_is_refused(
    odata_store, tmp_path, name, extra, word
):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    request = ReplayRequest(
        "op1", name, START, START + datetime.timedelta(days=1), "bob", "short", extra
    )
    replay_lane.handle(request)
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert word in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []


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
    tail = (
        "backup failed: 3 files to archive-a\nINFO exiting\nWARNING closing connection"
    )

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


class BlockingRunner(FakeRunner):
    """a child that runs until it is released, counting the children of its slot"""

    def __init__(self, tracker, slot):
        super().__init__()
        self.tracker, self.slot = tracker, slot
        self.started, self.release = threading.Event(), threading.Event()

    def run(self, command, env, timeout, progress, progress_interval=30):
        with self.tracker["lock"]:
            running = self.tracker["running"]
            running[self.slot] = running.get(self.slot, 0) + 1
            self.tracker["max_slot"][self.slot] = max(
                self.tracker["max_slot"].get(self.slot, 0), running[self.slot]
            )
            self.tracker["max_all"] = max(
                self.tracker["max_all"], sum(running.values())
            )
        self.started.set()
        self.release.wait(5)
        with self.tracker["lock"]:
            self.tracker["running"][self.slot] -= 1
        return 0, "done"


def start(worker, name, request):
    thread = threading.Thread(target=worker.handle, args=(worker.lanes[name], request))
    thread.start()
    return thread


def test_a_custom_lane_shares_the_slot_of_its_plain_lane(odata_store, tmp_path):
    recorder = FakeRecorder()
    worker = make_worker(odata_store, tmp_path, recorder)
    tracker = {"lock": threading.Lock(), "running": {}, "max_slot": {}, "max_all": 0}
    runners = {
        "short": BlockingRunner(tracker, "short"),
        "custom-short": BlockingRunner(tracker, "short"),
        "long": BlockingRunner(tracker, "long"),
    }
    worker.runners.update(runners)
    short = start(worker, "short", req(operation_id="s"))
    assert runners["short"].started.wait(5)
    custom = start(
        worker,
        "custom-short",
        dataclasses.replace(custom_req(configuration="x"), operation_id="c"),
    )
    long = start(worker, "long", req(operation_id="l"))
    assert runners["long"].started.wait(5)
    # the other plain lane runs beside the short one, the custom lane waits
    assert not runners["custom-short"].started.wait(0.3)
    assert ("c", RUNNING) in recorder.calls
    runners["short"].release.set()
    assert runners["custom-short"].started.wait(5)
    runners["custom-short"].release.set()
    runners["long"].release.set()
    for thread in (short, custom, long):
        thread.join(5)
        assert not thread.is_alive()
    assert tracker["max_slot"] == {"short": 1, "long": 1}
    assert tracker["max_all"] == 2
    assert [call for call in recorder.calls if call[1] == DONE] == [
        ("s", DONE, 0),
        ("c", DONE, 0),
        ("l", DONE, 0),
    ] or {call[0] for call in recorder.calls if call[1] == DONE} == {"s", "c", "l"}
    assert os.listdir(tmp_path) == []


def test_a_failed_request_frees_the_slot(odata_store, tmp_path, monkeypatch):
    recorder = FakeRecorder()
    worker = make_worker(odata_store, tmp_path, recorder)
    worker.runners["custom-short"] = FakeRunner(boom=OSError("no exec"))
    real_sync = handler_module.sync_configs
    failing = {"armed": False}

    def sync_failing_once(*args):
        if failing["armed"]:
            failing["armed"] = False
            raise RuntimeError("index down")
        return real_sync(*args)

    monkeypatch.setattr(handler_module, "sync_configs", sync_failing_once)
    unknown = dataclasses.replace(
        custom_req(configuration="x"), interface_name="NOPE@custom"
    )
    unknown = dataclasses.replace(
        unknown, extra={"interface": "NOPE", "configuration": "x"}
    )
    refused = [
        ("short", custom_req(configuration="x")),  # refused before the slot
        ("custom-short", unknown),  # prepare fails inside the slot
        ("custom-short", custom_req(configuration="x")),  # sync raises
        ("custom-short", custom_req(configuration="x")),  # the child cannot start
    ]
    for number, (name, request) in enumerate(refused):
        failing["armed"] = number == 2
        thread = start(
            worker, name, dataclasses.replace(request, operation_id=f"f{number}")
        )
        thread.join(5)
        assert not thread.is_alive()
        runner = FakeRunner()
        worker.runners["short"] = runner
        plain = start(worker, "short", req(operation_id=f"p{number}"))
        plain.join(5)
        assert not plain.is_alive(), f"the slot stayed locked after {name} {number}"
        assert (f"p{number}", DONE, 0) in recorder.calls
        assert len(runner.commands) == 1
    assert [c for c in recorder.calls if c[1] == FAILED and c[0].startswith("f")] != []
    assert os.listdir(tmp_path) == []


def slot_is_free(replay_lane, name="short"):
    free = replay_lane.worker.handler._slots[name].acquire(blocking=False)
    if free:
        replay_lane.worker.handler._slots[name].release()
    return free


def test_the_overrides_are_applied_to_the_copy_only(store, fake_opensearch, tmp_path):
    add(
        fake_opensearch,
        "a.json",
        [
            entry(
                odata_query_filter=WINDOW,
                download_file_pattern=["*"],
                product_per_page=1000,
            ),
            entry("OTHER"),
        ],
    )
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM))
    [itf, other] = runner.conf["a.json"]["collectors"]
    assert (
        itf["odata_query_filter"],
        itf["download_file_pattern"],
        itf["product_per_page"],
    ) == (CUSTOM["odata_query_filter"], ["*.EOF*"], 1000)
    assert other == entry("OTHER")
    # the index is untouched
    stored = store.get("a.json")["content"]["collectors"][0]
    assert stored["download_file_pattern"] == ["*"]
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]


@pytest.mark.parametrize(
    "overrides, word",
    [
        ({"odata_product_url": "https://other.example"}, "odata_product_url"),
        ({"odata_query_filter": "contains(Name,'x')"}, "ge"),
        ({"odata_query_filter": WINDOW + " or Name eq 'x'"}, "or"),
        ({"product_per_page": 5000}, "product_per_page"),
        ({"interface_credentials": "missing"}, "credential"),
    ],
)
def test_a_refused_override_fails_the_record_and_leaves_nothing(
    odata_store, tmp_path, overrides, word
):
    runner = FakeRunner()
    replay_lane, recorder = lane(
        odata_store,
        runner,
        tmp_path,
        name="custom-short",
        credential_provider=FakeProvider("cred-b"),
    )
    replay_lane.handle(custom_req(overrides=overrides))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert word in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []
    assert slot_is_free(replay_lane)


def test_the_interface_credentials_override_must_exist_for_the_service(
    odata_store, tmp_path
):
    provider = FakeProvider("cred-b")
    runner = FakeRunner()
    replay_lane, recorder = lane(
        odata_store,
        runner,
        tmp_path,
        name="custom-short",
        credential_provider=provider,
    )
    replay_lane.handle(custom_req(overrides={"interface_credentials": "cred-b"}))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert runner.conf["a.json"]["collectors"][0]["interface_credentials"] == "cred-b"
    assert provider.loaded == [["cred-b"]]
    # without a provider the name is taken as it is
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides={"interface_credentials": "any"}))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]


def test_a_jira_entry_takes_a_jql_override(store, fake_opensearch, tmp_path):
    add(
        fake_opensearch,
        "j.json",
        [
            entry(
                "ITF_A",
                cls="JiraExtendedCollectorConfiguration",
                jql_query="updated > {date_criteria}",
            )
        ],
        services=("jirax",),
    )
    runner = FakeRunner()
    replay_lane, recorder = lane(
        store, runner, tmp_path, name="custom-short", service="jirax"
    )
    jql = "project = X AND updated > {date_criteria}"
    replay_lane.handle(custom_req(overrides={"jql_query": jql}))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert runner.conf["j.json"]["collectors"][0]["jql_query"] == jql
    assert runner.commands[0][0][2] == "maas_collector.rawdata.cli.jirax"


NO_LAMBDA_EXTRACTOR = {
    "class": "XMLExtractor",
    "args": {
        "attr_map": {"a": "b"},
        "converter_map": {"a": {"type": "python", "python": "lambda x: x"}},
    },
}


def test_prepare_never_evaluates_the_configuration(
    store, fake_opensearch, tmp_path, monkeypatch
):
    add(fake_opensearch, "a.json", [entry(extractor=NO_LAMBDA_EXTRACTOR)])

    def boom(*_args, **_kwargs):
        raise AssertionError("eval called")

    monkeypatch.setattr(builtins, "eval", boom)
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]


def test_a_lambda_that_is_not_a_plain_lambda_fails_the_replay_before_any_child(
    store, fake_opensearch, tmp_path
):
    extractor = copy.deepcopy(NO_LAMBDA_EXTRACTOR)
    extractor["args"]["converter_map"]["a"]["python"] = "lambda x=1: x"
    add(fake_opensearch, "a.json", [entry(extractor=extractor)])
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "lambda" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []
    assert slot_is_free(replay_lane)


def test_two_enabled_entries_of_one_name_are_refused(store, fake_opensearch, tmp_path):
    add(fake_opensearch, "a.json", [entry()])
    add(fake_opensearch, "b.json", [entry()])
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "2" in recorder.details and "ITF_A" in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []
    assert slot_is_free(replay_lane)


def test_a_disabled_twin_is_left_alone(store, fake_opensearch, tmp_path):
    add(fake_opensearch, "a.json", [entry()])
    add(fake_opensearch, "b.json", [entry(enabled=False)])
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert runner.conf["b.json"]["collectors"] == [entry(enabled=False)]
    assert runner.conf["a.json"]["collectors"][0]["download_file_pattern"] == ["*.EOF*"]


def test_a_plain_request_is_not_validated_or_changed(odata_store, tmp_path):
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path)
    replay_lane.handle(req())
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert runner.conf == {"a.json": {"collectors": [entry()]}}


def add_backup(
    client, name="archive-a", version=1, services=("odata",), deleted=False, **kwargs
):
    """a backup definition, as `backups/<name>.json`"""
    content = (
        {}
        if deleted
        else {"backups": [{"name": name, "type": "BackupLocal", "directory": "/data"}]}
    )
    add_config_document(
        client,
        kwargs.pop("path", f"backups/{name}.json"),
        version,
        list(services),
        content,
        deleted=deleted,
        **kwargs,
    )


CHOICE = {
    "backup": "archive-a",
    "backup_version": None,
    "duplicates": "skip",
    "force": False,
}


def archive_of(runner):
    return runner.conf["a.json"]["collectors"][0]["archive"]


def test_the_archive_choice_is_written_into_the_entry_with_the_backup_pinned(
    odata_store, fake_opensearch, tmp_path
):
    add_backup(fake_opensearch, version=3)
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    choice = {**CHOICE, "duplicates": "replace_if_changed", "force": True}
    replay_lane.handle(custom_req(archive=choice))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert archive_of(runner) == {
        "backup": "archive-a",
        "backup_version": 3,
        "duplicates": "replace_if_changed",
        "force": True,
    }


def test_an_existing_rule_keeps_its_fields_and_download(
    store, fake_opensearch, tmp_path
):
    rule = {
        "backup": "old",
        "fields": {"checksum": "origin_checksum_md5"},
        "download": {"url": "https://bytes.example/{id}", "quote_id": False},
    }
    add(fake_opensearch, "a.json", [entry(archive=rule)])
    add_backup(fake_opensearch)
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(archive=CHOICE))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert archive_of(runner) == {
        "backup": "archive-a",
        "backup_version": 1,
        "duplicates": "skip",
        "force": False,
        "fields": rule["fields"],
        "download": rule["download"],
    }


def test_a_named_version_is_kept_when_it_still_names_the_backup(
    odata_store, fake_opensearch, tmp_path
):
    add_backup(fake_opensearch, version=1, latest=False)
    add_backup(fake_opensearch, version=2)
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(archive={**CHOICE, "backup_version": 1}))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    assert archive_of(runner)["backup_version"] == 1


def build_backup_case(client, case):
    """the index for each way a backup cannot serve"""
    if case == "untagged":
        add_backup(client, services=("other",))
    elif case == "deleted":
        add_backup(client, version=1, latest=False)
        add_backup(client, version=2, deleted=True)
    elif case == "version_missing":
        add_backup(client)
    elif case == "version_tombstone":
        add_backup(client, version=1, latest=False, deleted=True)
        add_backup(client, version=2)
    elif case == "version_untagged":
        add_backup(client, version=1, latest=False, services=("other",))
        add_backup(client, version=2)
    elif case == "two_files":
        add_backup(client)
        add_backup(client, path="backups/copy.json")


@pytest.mark.parametrize(
    "case, version, word",
    [
        ("unknown", None, "archive-a"),
        ("untagged", None, "service"),
        ("deleted", None, "archive-a"),
        ("version_missing", 9, "version 9"),
        ("version_tombstone", 1, "version 1"),
        ("version_untagged", 1, "version 1"),
        ("two_files", None, "archive-a"),
    ],
)
def test_a_backup_that_cannot_serve_is_refused(
    odata_store, fake_opensearch, tmp_path, case, version, word
):
    build_backup_case(fake_opensearch, case)
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(archive={**CHOICE, "backup_version": version}))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert word in recorder.details
    assert runner.commands == []
    assert os.listdir(tmp_path) == []
    assert slot_is_free(replay_lane)


def test_an_archive_choice_on_a_jira_entry_is_refused(store, fake_opensearch, tmp_path):
    add(
        fake_opensearch,
        "j.json",
        [entry(cls="JiraExtendedCollectorConfiguration")],
        services=("jirax",),
    )
    add_backup(fake_opensearch, services=("jirax",))
    runner = FakeRunner()
    replay_lane, recorder = lane(
        store, runner, tmp_path, name="custom-short", service="jirax"
    )
    replay_lane.handle(custom_req(archive=CHOICE))
    assert recorder.calls == [("op1", RUNNING), ("op1", FAILED)]
    assert "archive" in recorder.details
    assert runner.commands == []


def test_an_archive_choice_without_overrides_runs_the_live_query(
    store, fake_opensearch, tmp_path
):
    live = entry(
        odata_query_filter=WINDOW,
        download_file_pattern=["*"],
        product_per_page=500,
    )
    add(fake_opensearch, "a.json", [live])
    add_backup(fake_opensearch)
    runner = FakeRunner()
    replay_lane, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(archive=CHOICE))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    written = runner.conf["a.json"]["collectors"][0]
    assert {k: v for k, v in written.items() if k != "archive"} == live
    assert written["archive"]["backup"] == "archive-a"


def test_overrides_and_archive_together(odata_store, fake_opensearch, tmp_path):
    add_backup(fake_opensearch, version=2)
    runner = FakeRunner()
    replay_lane, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay_lane.handle(custom_req(overrides=CUSTOM, archive=CHOICE))
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]
    written = runner.conf["a.json"]["collectors"][0]
    assert written["download_file_pattern"] == ["*.EOF*"]
    assert written["archive"]["backup_version"] == 2


def test_the_single_lane_runs_plain_and_custom_requests_one_at_a_time(
    odata_store, tmp_path
):
    recorder = FakeRecorder()
    worker = make_worker(odata_store, tmp_path, recorder, single=True)
    runner = FakeRunner()
    worker.runners["single"] = runner
    lane_single = worker.lanes["single"]
    worker.handle(lane_single, req())
    worker.handle(
        lane_single,
        dataclasses.replace(custom_req(configuration="x"), operation_id="op2"),
    )
    assert recorder.calls == [
        ("op1", RUNNING),
        ("op1", DONE, 0),
        ("op2", RUNNING),
        ("op2", DONE, 0),
    ]
    for command, env, _timeout in runner.commands:
        assert command[command.index("--healthcheck-port") + 1] == "18081"
        assert command[command.index("--replay-interface-name") + 1] == "ITF_A"
        assert env["MAAS_INSTANCE_ID"].endswith("-replay-single")


def test_the_child_is_given_the_service_of_the_handler(store):
    command, _env = CollectorReplayHandler(
        store, "svc", ("short", "long"), 18081, 3
    ).command(Prepared("/copy", "odata"), req(), "short")
    assert "--config-service=svc" in command


def test_describe_failure_of_unsent_transfer_requests():
    text = CollectorReplayHandler.describe_failure(
        TRANSFER_FAILED_EXIT_CODE, "line\ntransfer requests failed: 3 documents"
    )
    assert text.splitlines()[0] == "3 documents could not be sent to the transfer stage"
    assert "line" in text


def test_describe_failure_without_the_count_line():
    assert (
        CollectorReplayHandler.describe_failure(TRANSFER_FAILED_EXIT_CODE, "line").splitlines()[0]
        == "transfer requests could not all be sent"
    )


def test_describe_failure_of_transfer_and_backup_failures_names_both():
    tail = (
        "backup failed: 2 files to archive-a\n"
        "transfer requests failed: 3 documents\nINFO exiting"
    )
    assert CollectorReplayHandler.describe_failure(
        TRANSFER_FAILED_EXIT_CODE, tail
    ).splitlines()[:2] == [
        "3 documents could not be sent to the transfer stage",
        "2 files could not be backed up to archive-a",
    ]

"""
Replay lane of the operator worker: run the collector in replay mode as a child.
"""

import collections
import logging
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Mapping

from maas_collector.rawdata.collector.filecollector import REPLAY_FAILED_EXIT_CODE
from maas_collector.rawdata.configstore import ConfigStore
from maas_collector.rawdata.configsync import sync_configs
from maas_collector.rawdata.operator.contract import ReplayRequest
from maas_collector.rawdata.operator.records import (
    DONE,
    FAILED,
    RUNNING,
    OperationRecorder,
)

LOGGER = logging.getLogger("maas_collector.operator")

REPLAY_MODULES = {
    "ODataCollectorConfiguration": "odata",
    "JiraExtendedCollectorConfiguration": "jirax",
}
TAIL_LINES = 20
# seconds a child gets to exit after terminate() before it is killed
TERMINATE_GRACE = 10
KILLED_EXIT_CODE = -9
# exit code of a run refused because the runner was terminated before it started
STOPPED_EXIT_CODE = -15
SECONDS_PER_DAY = 86400

DROPPED_ENVIRONMENT = (
    "CONFIG_BACKEND",
    "CONFIG_SERVICE",
    "CONFIG_INDEX",
    "RAWDATA_CONFIG",
    "RAWDATA_CONFIG_DIR",
)
DROPPED_PREFIX = "REPLAY_"


class ReplayNotPossible(ValueError):
    """The requested interface cannot be replayed"""


def _declares(document: dict, interface_name: str) -> bool:
    return any(
        entry.get("interface_name") == interface_name
        for entry in (document.get("content") or {}).get("collectors", [])
    )


def _was_deleted(
    store: ConfigStore, tombstones: list[str], interface_name: str
) -> bool:
    """a tombstone has no content: its older versions tell what it declared"""
    return any(
        not version.get("deleted") and _declares(version, interface_name)
        for path in tombstones
        for version in store.history(path)
    )


def find_entry(store: ConfigStore, service: str, interface_name: str) -> dict:
    """Return the raw collector entry of an interface of the service

    Raises:
        ReplayNotPossible: absent, deleted, only under another service, disabled
            or of a class that cannot be replayed
    """
    live, elsewhere, tombstones = [], [], []
    for document in store.latest():
        services = document.get("services", [])
        if document.get("deleted"):
            if service in services:
                tombstones.append(document["path"])
            continue
        for entry in (document.get("content") or {}).get("collectors", []):
            if entry.get("interface_name") != interface_name:
                continue
            if service in services:
                live.append(entry)
            else:
                elsewhere.extend(services)

    if not live:
        if _was_deleted(store, tombstones, interface_name):
            raise ReplayNotPossible(f"{interface_name} is deleted")
        if elsewhere:
            raise ReplayNotPossible(
                f"{interface_name} does not belong to service {service}, "
                f"only to {', '.join(sorted(set(elsewhere)))}"
            )
        raise ReplayNotPossible(f"{interface_name} not found in service {service}")

    enabled = [entry for entry in live if entry.get("enabled", True) is not False]
    if not enabled:
        raise ReplayNotPossible(f"{interface_name} is disabled")
    entry = enabled[0]
    if entry.get("class") not in REPLAY_MODULES:
        raise ReplayNotPossible(
            f"{interface_name} of class {entry.get('class')} cannot be replayed"
        )
    return entry


def _format_date(value) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def build_command(
    module: str,
    conf_dir: str,
    request: ReplayRequest,
    healthcheck_port: int,
    replay_retry: int,
) -> list[str]:
    """Collector invocation, as an argument list"""
    return [
        sys.executable,
        "-m",
        f"maas_collector.rawdata.cli.{module}",
        "-v",
        "-d",
        conf_dir,
        "--healthcheck-port",
        str(healthcheck_port),
        "--replay-interface-name",
        request.interface_name,
        "--replay-start-date",
        _format_date(request.start_date),
        "--replay-end-date",
        _format_date(request.end_date),
        "--replay-retry",
        str(replay_retry),
        # one item: argparse would take an id starting with - for an option
        f"--replay-suffix={request.operation_id}",
    ]


def child_environment(environ: Mapping[str, str], lane: str = "") -> dict[str, str]:
    """Environment of the child: without what makes it read another configuration

    With a lane, the child gets an instance id of its own, so its telemetry does not
    merge with the collector it was started from.
    """
    env = {
        name: value
        for name, value in environ.items()
        if name not in DROPPED_ENVIRONMENT and not name.startswith(DROPPED_PREFIX)
    }
    if lane:
        base = environ.get("MAAS_INSTANCE_ID") or socket.gethostname()
        env["MAAS_INSTANCE_ID"] = f"{base}-replay-{lane}"
    return env


def replay_timeout(
    lane: str,
    request: ReplayRequest,
    short_timeout: float,
    long_timeout_per_day: float,
) -> float:
    """Seconds a replay may run in a lane: fixed for the short lane, scaled by window
    if long"""
    if lane == "short":
        return short_timeout
    days = (request.end_date - request.start_date).total_seconds() / SECONDS_PER_DAY
    return max(short_timeout, days * long_timeout_per_day)


class ChildRunner:
    """Run a child process with a deadline that does not depend on its output"""

    def __init__(self):
        self._lock = threading.Lock()
        self._child = None
        self._kill_at = None
        self._closed = False

    def terminate(self) -> None:
        """Terminate the running child, if any, and start no other one; safe to call
        from another thread"""
        with self._lock:
            # sticky: a run about to start would otherwise escape the stop
            self._closed = True
            if self._child is not None and self._child.poll() is None:
                self._kill_at = time.monotonic() + TERMINATE_GRACE
                self._child.terminate()

    @staticmethod
    def _kill(child) -> None:
        try:
            child.kill()
        except OSError:  # already gone
            pass

    # one loop with three clocks: exit, progress, deadline
    # pylint: disable-next=too-many-locals
    def run(
        self,
        command: list[str],
        env: dict,
        timeout: float,
        progress: Callable[[str], None],
        progress_interval: float = 30,
    ) -> tuple[int, str]:
        """Run command, echo its output, and keep its last lines

        progress receives those lines every progress_interval seconds. The child
        is killed after timeout seconds even when it prints nothing.

        Returns:
            tuple[int, str]: exit code (-9 when killed for the timeout, -15 without
                a child once terminate() was called), output tail
        """
        tail = collections.deque(maxlen=TAIL_LINES)
        with self._lock:
            if self._closed:
                return STOPPED_EXIT_CODE, "stopped before start"
            self._kill_at = None
            # pylint: disable-next=consider-using-with
            child = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
            )
            self._child = child

        def read():
            for line in child.stdout:
                sys.stdout.write(line)
                tail.append(line.rstrip())

        reader = threading.Thread(target=read, daemon=True)
        reader.start()

        started = time.monotonic()
        deadline = started + timeout
        next_progress = started + progress_interval
        timed_out = False
        try:
            while True:
                with self._lock:
                    kill_at = self._kill_at
                wake = min(deadline, next_progress)
                if kill_at is not None:
                    wake = min(wake, kill_at)
                try:
                    child.wait(timeout=max(0.0, wake - time.monotonic()))
                    break
                except subprocess.TimeoutExpired:
                    pass
                now = time.monotonic()
                if now >= deadline:
                    timed_out = True
                    self._kill(child)
                    child.wait()
                    break
                if kill_at is not None and now >= kill_at:
                    self._kill(child)
                    child.wait()
                    break
                if now >= next_progress:
                    progress("\n".join(tail))
                    next_progress = now + progress_interval
            reader.join(timeout=5)
        finally:
            with self._lock:
                self._child = None
            if child.poll() is None:
                self._kill(child)
                child.wait()

        if timed_out:
            tail.append(f"killed after {timeout}s")
            return KILLED_EXIT_CODE, "\n".join(tail)
        return child.returncode, "\n".join(tail)


class ReplayLane:
    """Handle replay requests of one service and one lane"""

    # the lane is configured by many independent settings
    # pylint: disable-next=too-many-arguments,too-many-positional-arguments
    def __init__(
        self,
        service: str,
        lane: str,
        store: ConfigStore,
        recorder: OperationRecorder,
        runner: ChildRunner,
        workdir: str,
        healthcheck_port: int,
        short_timeout: float,
        long_timeout_per_day: float,
        replay_retry: int,
    ):
        self.service = service
        self.lane = lane
        self.store = store
        self.recorder = recorder
        self.runner = runner
        self.workdir = workdir
        self.healthcheck_port = healthcheck_port
        self.short_timeout = short_timeout
        self.long_timeout_per_day = long_timeout_per_day
        self.replay_retry = replay_retry

    def handle(self, request: ReplayRequest) -> None:
        """Replay the request and record its outcome; never raises"""
        copy = None
        try:
            self.recorder.update(request.operation_id, RUNNING)
            os.makedirs(self.workdir, exist_ok=True)
            # never built from the request: operation_id is not a safe file name
            copy = tempfile.mkdtemp(dir=self.workdir, prefix="replay-")
            entry = find_entry(self.store, self.service, request.interface_name)
            synced = sync_configs(self.store, copy, [self.service])
            if synced.skipped:
                # the child would run without them, maybe without the interface
                self.recorder.update(
                    request.operation_id,
                    FAILED,
                    details="configuration files the copy skipped: "
                    + ", ".join(synced.skipped),
                )
                return
            command = build_command(
                REPLAY_MODULES[entry["class"]],
                os.path.join(copy, self.service),
                request,
                self.healthcheck_port,
                self.replay_retry,
            )
            code, tail = self.runner.run(
                command,
                child_environment(os.environ, self.lane),
                # the queue the request came from decides, not its label
                replay_timeout(
                    self.lane, request, self.short_timeout, self.long_timeout_per_day
                ),
                lambda lines: self.recorder.update(
                    request.operation_id, RUNNING, details=lines
                ),
            )
            if code == 0:
                self.recorder.update(
                    request.operation_id, DONE, exit_code=0, details=tail
                )
            else:
                self.recorder.update(
                    request.operation_id,
                    FAILED,
                    exit_code=code,
                    details=self._failure_details(code, tail),
                )
        except ReplayNotPossible as error:
            self.recorder.update(request.operation_id, FAILED, details=str(error))
        except Exception as error:  # pylint: disable=broad-except
            LOGGER.exception("Replay %s failed", request.operation_id)
            self.recorder.update(
                request.operation_id, FAILED, details=f"{type(error).__name__}: {error}"
            )
        finally:
            if copy is not None:
                shutil.rmtree(copy, ignore_errors=True)

    @staticmethod
    def _failure_details(code: int, tail: str) -> str:
        if code == REPLAY_FAILED_EXIT_CODE:
            reason = "an interface failed during the replay"
        else:
            reason = f"exit code {code}"
        return f"{reason}\n{tail}"

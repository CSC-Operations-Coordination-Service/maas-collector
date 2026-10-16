# MAAS operator — part 1: maas-collector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make maas-collector ready to be driven by the MAAS operator: exact
replay windows, jira replay, a failing replay that exits non-zero, an
`enabled` flag, a `config sync` command, and an `operator_worker` that runs
replays and product checks from RabbitMQ requests.

**Architecture:** Small, test-first changes in the existing collectors and
config CLI, then a new `maas_collector.rawdata.operator` package (contract,
records, replay lane, product check, worker) with its CLI
`maas_collector.rawdata.cli.operator_worker`. The worker generalises
`download-manager/maas-engine-download-manager/src/maas_download_manager/replay/worker.py`.

**Tech Stack:** Python 3.11, opensearch-dsl via maas-model, kombu (already a
dependency), pytest via tox.

**Spec:** `docs/superpowers/specs/2026-10-16-maas-operator-design.md`
(sections 1a–1f and "Message and record contract").

## Global Constraints

- Work on a git-flow feature branch `feature/maas-operator-collector` off
  `develop`, in a git worktree (another session may use the main checkout).
- No new runtime dependency.
- Normal (non-replay) collection behaviour is unchanged by every task.
- Exchange `maas-operator-exchange` (topic, durable); routing keys
  `maas.operator.<service>.replay.<lane>` and `maas.operator.<service>.check`;
  queues `maas-operator-<service>-replay-<lane>` and `maas-operator-<service>-check`;
  lanes `short` (window ≤ 24 h) and `long`.
- Operation records: index `maas-operation`; the worker only sends partial
  updates by id (`status` RUNNING|DONE|FAILED, `updated_at`, `exit_code`,
  `details` = last 20 output lines) and never creates a record.
- Dates on the wire: ISO 8601 UTC with `Z`.
- Every task adds its bullet(s) under a new `## Version 3.13.0` heading at the
  top of `CHANGELOG.md` (same prose style as 3.12.0) and updates the relevant
  `docs/*.md` (`replay.md`, `configuration.md`, `opensearch_config.md`, new
  `operator_worker.md`).
- Run the full suite with `tox` before each commit; expected: all pass.
- Edit `collector/filecollector.py` for `FileCollectorConfiguration`, never the
  stale `collector/filecollectorconfiguration.py`.

## Review Focus

- A product name containing `'` (`S1A_X'Y.SAFE`) → the OData filter escapes it
  as `''`; the check never builds a broken or injected query. Test in Task 8.
- A replay child that hangs **without printing** → still killed at its timeout
  (the DM worker only checked the deadline when a line arrived). Test in Task 7.
- `config sync` into a folder holding files it didn't write (credential files
  in `csc-ocs-ansible/configs`) → those files are never deleted. Test in Task 5.
- `"enabled": "false"` or `0` in a collector entry → rejected by validation,
  never read as "enabled". Test in Task 4.
- A replay request whose interface was disabled, deleted or moved to another
  service between publish and pickup → the record goes FAILED with the reason;
  the worker keeps running. Test in Task 7.

---

### Task 0: Branch and worktree

- [ ] **Step 1:** `git flow feature start maas-operator-collector` from
  `develop` in a new worktree (superpowers:using-git-worktrees), carrying the
  spec and this plan.
- [ ] **Step 2:** `tox` on the untouched branch. Expected: all pass (baseline).
- [ ] **Step 3:** Commit `docs/superpowers/specs/2026-10-16-maas-operator-design.md`
  and this plan: `docs: :memo: Design the maas operator and plan its collector part`.

### Task 1: Replays use exactly the requested window (spec 1a)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/http/abstract_query_strategy.py:52-56`
- Modify: `src/maas_collector/rawdata/collector/odata/query_strategy.py:54-57`
- Test: `tests/test_replay_window.py` (new)

**Interfaces:**
- Produces: strategies treat `journal.is_a_replay_journal` (already set by
  `CollectorReplayJournal`, `journal.py:284`) as "replay": no
  `end_date_time_offset`, no `odata_start_offset`.

- [ ] **Step 1: Write the failing tests** — build the v4 odata strategy
  (`ODataCollector.IMPL_DIR["v4"]`) with a Mock collector/session, a config
  with `end_date_time_offset=15`, `odata_start_offset=30`, patching
  `build_authentication`:

```python
def test_replay_window_is_exact():
    journal = replay_journal(last_date=None)   # is_a_replay_journal=True
    strategy = make_strategy(start=START, end=END, journal=journal)
    assert strategy.start_date == START
    assert strategy.end_date == END

def test_normal_run_still_applies_both_offsets():
    journal = live_journal(last_date=START)    # is_a_replay_journal=False
    strategy = make_strategy(start=None, end=END, journal=journal)
    assert strategy.end_date == END - timedelta(minutes=15)
    assert strategy.start_date == START - timedelta(minutes=30)

def test_resumed_replay_starts_at_journal_last_date_without_offset():
    journal = replay_journal(last_date=START + timedelta(hours=1))
    strategy = make_strategy(start=START, end=END, journal=journal)
    assert strategy.start_date == START + timedelta(hours=1)
```

- [ ] **Step 2:** `pytest tests/test_replay_window.py -v` → first and third FAIL.
- [ ] **Step 3:** In both `__init__`s, skip the offset when
  `journal.is_a_replay_journal`. Normal path untouched.
- [ ] **Step 4:** Tests PASS; `tox` passes.
- [ ] **Step 5:** CHANGELOG + `docs/replay.md` ("a replay collects exactly
  `--replay-start-date`..`--replay-end-date`; `odata_start_offset` and
  `end_date_time_offset` apply to live collection only"). Commit
  `fix: :bug: Collect exactly the requested window on replay`.

### Task 2: A failed replay exits with status 1 (spec 1f)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/httpcollector.py:151-256` (`ingest_replay`)
- Modify: `src/maas_collector/rawdata/cli/odata.py` (`odata_collector_main`)
- Modify: `src/maas_collector/rawdata/collector/filecollector.py` (`FileCollector.__init__`: new attribute)
- Test: `tests/test_interface_isolation.py` (extend, reuse `make_collector`)

**Interfaces:**
- Produces: `FileCollector.replay_failures: list[str]` (interface names whose
  replay exhausted its retries, `[]` by default);
  `REPLAY_FAILED_EXIT_CODE = 1` in `filecollector.py`; replay CLIs call
  `sys.exit(REPLAY_FAILED_EXIT_CODE)` after `run()` when `replay_failures` is
  non-empty. Task 3 (jirax) uses the same attribute and exit code.

- [ ] **Step 1: Failing tests**

```python
def test_exhausted_replay_is_listed_in_replay_failures():
    # ingest_http always raises, retry=1
    collector.ingest_replay(ReplayArgs(["A"], START_S, END_S, "s", 1))
    assert collector.replay_failures == ["A"]

def test_successful_replay_has_no_failures(): ...
    assert collector.replay_failures == []

def test_odata_cli_exits_1_when_a_replay_failed(monkeypatch):
    # patch ODataCollector.setup/run; run() sets replay_failures = ["A"]
    with pytest.raises(SystemExit) as exit_info:
        odata_collector_main(REPLAY_ARGV)
    assert exit_info.value.code == 1
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Append the interface name where `ingest_replay` logs
  "Failed replay"; exit in `odata_collector_main`. Exit code 75 (config
  changed) is unaffected.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** CHANGELOG + `docs/replay.md`. Commit
  `fix: :bug: Exit 1 when a replayed interface failed`.

### Task 3: Jira replay (spec 1b)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/jiraxcollector.py` (`ingest` l.87-127, `ingest_tickets` l.184-296)
- Modify: `src/maas_collector/rawdata/cli/jirax.py` (exit code as Task 2)
- Test: `tests/test_jirax_replay.py` (new)

**Interfaces:**
- Consumes: `CollectorReplayJournal(config, start_date, end_date, suffix, nb_of_retry=…)`,
  `FileCollector.get_configuration_by_interface_name`, `replay_failures`.
- Produces: `JIRAExtendedCollector.ingest_replay(replay_args: ReplayArgs) -> None`;
  `jql_for(config, journal) -> str` (module-level, pure).

- [ ] **Step 1: Failing tests** (`JQL = 'project = SOA AND updated > {date_criteria} ORDER BY updated ASC'`)

```python
def test_live_jql_is_unchanged():
    assert jql_for(cfg(JQL), live_journal(last_date=None)) == JQL.format(date_criteria="-30d")
    assert jql_for(cfg(JQL), live_journal(last_date=D1)) == JQL.format(date_criteria='"2026-10-01 10:00"')

def test_replay_jql_is_bounded_by_start_and_end():
    jql = jql_for(cfg(JQL), replay_journal(start=D1, end=D2, last_date=None))
    assert jql == 'project = SOA AND updated > "2026-10-01 10:00" AND updated <= "2026-10-02 10:00" ORDER BY updated ASC'

def test_resumed_replay_starts_at_last_date(): ...

def test_ingest_in_replay_mode_only_collects_the_replayed_interfaces():
    # two configs A, B; args.replay interface_name=["B"]; ingest_tickets patched
    assert [c.interface_name for c in called_configs] == ["B"]
    assert isinstance(called_journal, CollectorReplayJournal)

def test_unknown_replay_interface_raises_before_collecting(): ...

def test_failed_jira_replay_is_listed_in_replay_failures(): ...
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** `ingest` branches on `self.args.replay` to `ingest_replay`
  (no retry loop: one attempt per interface, failure → `replay_failures`;
  success → `journal.complete()`). `jql_for` builds `date_criteria` as
  `'"<start>" AND updated <= "<end>"'` in replay (start = `max(start, last_date)`),
  so every existing `updated > {date_criteria}` template works unchanged.
  Replace the `# TODO handle start/stop for replay`.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** CHANGELOG + `docs/replay.md` (jira section: needs
  `updated > {date_criteria}` in `jql_query`). Commit
  `feat: :sparkles: Replay jira interfaces over a window`.

### Task 4: `enabled` flag on collector entries (spec 1c)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/filecollector.py:120-257` (field), `load_config()` (~l.375)
- Modify: `src/maas_collector/rawdata/configuration.py:147-156` (type check)
- Modify: `src/maas_collector/rawdata/collector/httpcollector.py` (`ingest_replay` message)
- Modify: `src/maas_collector/rawdata/collector/monitorcollector.py:384-426` (`setup` skip)
- Test: `tests/test_enabled_flag.py` (new)

**Interfaces:**
- Produces: `FileCollectorConfiguration.enabled: bool = True`;
  `FileCollector.disabled_interface_names: set[str]` filled by `load_config()`,
  which keeps disabled entries out of `self.configs`.

- [ ] **Step 1: Failing tests**

```python
def test_disabled_entry_is_not_in_configs(tmp_path):   # file backend, entries A (enabled) and B (enabled: false)
    collector.load_config()
    assert [c.interface_name for c in collector.configs] == ["A"]
    assert collector.disabled_interface_names == {"B"}

def test_entry_without_enabled_is_enabled(): ...

@pytest.mark.parametrize("value", ["false", 0, None])
def test_non_boolean_enabled_is_rejected(value):
    with pytest.raises(ConfigValidationError, match="enabled"):
        validate_config_sources([ConfigSource("a.json", content_with(enabled=value))], config_classes_for)

def test_replay_of_a_disabled_interface_says_it_is_disabled():
    with pytest.raises(ValueError, match="B is disabled"):
        collector.ingest_replay(ReplayArgs(["B"], START_S, END_S, "s", 0))

def test_monitor_does_not_probe_a_disabled_interface(): ...
    assert "B" not in monitor.meta_dict
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Field + `load_config` filter (log `"interface %s is disabled"`
  at INFO once per load); `build_collector_configuration` raises `ValueError`
  naming `enabled` when present and not `bool` (so `validate_config_sources`
  and `ConfigStore.publish` reject it); monitor skips `not config.enabled`
  next to `no_probe`.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** CHANGELOG + `docs/configuration.md`. Commit
  `feat: :sparkles: Disable a collector entry without deleting it`.

### Task 5: `config sync` (spec 1e)

**Files:**
- Create: `src/maas_collector/rawdata/configsync.py`
- Modify: `src/maas_collector/rawdata/cli/config.py` (subparser + `COMMANDS["sync"]`)
- Test: `tests/test_configsync.py` (new), `tests/test_config_cli.py` (one CLI test)

**Interfaces:**
- Consumes: `ConfigStore.latest(service="")`, `check_path`, `check_service`
  (`configstore.py`), the realpath guard used by `export_command`
  (`cli/config.py:301`) — move it into `configsync.py` and have
  `export_command` import it.
- Produces:

```python
MANIFEST_NAME = ".maas-config-sync.json"   # per <out>/<service>/, lists the paths sync wrote

@dataclass
class SyncResult:
    written: list[str]; unchanged: list[str]; removed: list[str]; skipped: list[str]

def sync_configs(store: ConfigStore, out_dir: str, services: Sequence[str] = ()) -> SyncResult
```
  Layout `<out>/<service>/<path>` as `export`; empty `services` = every
  service in the index. Content serialised like `export` (indent 4,
  `ensure_ascii=False`, trailing newline); a file whose bytes already match is
  not rewritten. Removes only paths listed in the previous manifest that are
  no longer current (deleted/tombstoned or gone). Task 7 uses `sync_configs`.

- [ ] **Step 1: Failing tests** (`fake_opensearch` + `add_config_document`)

```python
def test_first_sync_writes_every_current_path(tmp_path): ...
def test_second_sync_leaves_unchanged_files_untouched(tmp_path):
    mtime = ...; result = sync_configs(store, tmp_path)
    assert result.written == [] and file.stat().st_mtime_ns == mtime
def test_deleted_path_is_removed(tmp_path):        # tombstone after first sync
    assert result.removed == ["odata/a.json"] and not (tmp_path / "odata/a.json").exists()
def test_files_sync_did_not_write_are_never_removed(tmp_path):
    (tmp_path / "odata" / "credentials.json").write_text("{}")
    sync_configs(store, tmp_path)
    assert (tmp_path / "odata" / "credentials.json").exists()
def test_sync_of_one_service_touches_only_its_folder(tmp_path): ...
def test_sync_never_writes_outside_its_directory(tmp_path): ...   # path "../x.json" → skipped
def test_cli_sync_prints_counts(...):   # run("sync", str(out)) → 0, "synced 2 written, 0 unchanged, 0 removed"
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement; CLI `sync out_dir [--service S ...]`, return 1
  when `skipped` is non-empty.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** CHANGELOG + `docs/opensearch_config.md`. Commit
  `feat: :sparkles: Sync the configuration index into a folder`.

### Task 6: Operator contract and operation records (spec "Message and record contract")

**Files:**
- Create: `src/maas_collector/rawdata/operator/__init__.py`, `operator/contract.py`, `operator/records.py`
- Test: `tests/operator/test_contract.py`, `tests/operator/test_records.py`

**Interfaces — Produces** (Tasks 7–9 and the backend rely on these names):

```python
# contract.py
EXCHANGE_NAME = "maas-operator-exchange"
LANES = ("short", "long")
SHORT_LANE_MAX = datetime.timedelta(hours=24)
def lane_for(start: datetime, end: datetime) -> str
def replay_routing_key(service: str, lane: str) -> str      # maas.operator.<service>.replay.<lane>
def replay_queue_name(service: str, lane: str) -> str       # maas-operator-<service>-replay-<lane>
def check_routing_key(service: str) -> str                  # maas.operator.<service>.check
def check_queue_name(service: str) -> str                   # maas-operator-<service>-check
def exchange() -> kombu.Exchange                            # topic, durable

@dataclass(frozen=True)
class ReplayRequest:
    operation_id: str; interface_name: str; start_date: datetime; end_date: datetime
    requested_by: str = ""; lane: str = "short"
    @classmethod
    def from_body(cls, body: bytes | str | dict) -> "ReplayRequest"   # ValueError on missing field, bad date, start >= end, unknown lane
    def to_body(self) -> dict                                        # dates as ...Z

@dataclass(frozen=True)
class CheckRequest:
    operation_id: str; interface_name: str; product_name: str = ""; product_id: str = ""
    @classmethod
    def from_body(cls, body) -> "CheckRequest"     # ValueError unless exactly one of name/id

@dataclass
class CheckReply:
    status: str                      # "ok" | "error"
    remote: dict | None = None; stored: dict | None = None
    diff: list[dict] = field(default_factory=list)   # {"field", "remote", "stored"}
    error: str | None = None
    def to_body(self) -> dict

# records.py
OPERATION_INDEX = "maas-operation"
RUNNING, DONE, FAILED = "RUNNING", "DONE", "FAILED"
class OperationRecorder:
    def __init__(self, client, index: str = OPERATION_INDEX,
                 clock: Callable[[], datetime] = lambda: datetime.now(datetime.UTC))
    def update(self, operation_id: str, status: str, exit_code: int | None = None,
               details: str | None = None) -> bool   # client.update(..., body={"doc": ...}, retry_on_conflict=3); False + error log on 404 or any error, never raises
```

- [ ] **Step 1: Failing tests** — round trips (`ReplayRequest.from_body(r.to_body()) == r`),
  `lane_for` at exactly 24 h → `"short"`, 24 h + 1 s → `"long"`, every
  `ValueError` case, the four name functions with service `odata`,
  `OperationRecorder.update` sends only the given fields plus `updated_at`,
  returns False on a 404 (`NotFoundError`) without creating anything.
- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** Commit `feat: :sparkles: Define the operator message contract`.

### Task 7: Replay lane (spec 1d, replay)

**Files:**
- Create: `src/maas_collector/rawdata/operator/replay.py`
- Test: `tests/operator/test_replay.py`

**Interfaces:**
- Consumes: Task 6 (`ReplayRequest`, `OperationRecorder`, statuses), Task 5
  (`sync_configs`), `ConfigStore.latest(service=…)`, `REPLAY_FAILED_EXIT_CODE`.
- Produces:

```python
REPLAY_MODULES = {"ODataCollectorConfiguration": "odata",
                  "JiraExtendedCollectorConfiguration": "jirax"}
TAIL_LINES = 20
class ReplayNotPossible(ValueError): ...
def find_entry(store: ConfigStore, service: str, interface_name: str) -> dict   # raw collector entry; ReplayNotPossible if absent / deleted / other service / enabled is False / class not in REPLAY_MODULES
def build_command(module: str, conf_dir: str, request: ReplayRequest, healthcheck_port: int) -> list[str]
def child_environment(environ: Mapping[str, str]) -> dict[str, str]   # drops CONFIG_BACKEND, CONFIG_SERVICE, CONFIG_INDEX, REPLAY_*
def replay_timeout(request: ReplayRequest, short_timeout: float, long_timeout_per_day: float) -> float
    # short lane: short_timeout; long: max(short_timeout, days(window) * long_timeout_per_day)
class ChildRunner:
    def run(self, command: list[str], env: dict, timeout: float,
            progress: Callable[[str], None], progress_interval: float = 30) -> tuple[int, str]
class ReplayLane:
    def __init__(self, service: str, lane: str, store: ConfigStore, recorder: OperationRecorder,
                 runner: ChildRunner, workdir: str, healthcheck_port: int,
                 short_timeout: float, long_timeout_per_day: float)
    def handle(self, request: ReplayRequest) -> None   # never raises
```
  `build_command` = `[sys.executable, "-m", f"maas_collector.rawdata.cli.{module}", "-v",
  "-d", conf_dir, "--healthcheck-port", str(port), "--replay-interface-name",
  name, "--replay-start-date", start_z, "--replay-end-date", end_z,
  "--replay-suffix", operation_id]` (list, never a shell string).
  `ChildRunner` enforces the deadline with a timer independent of output
  (kill, append `"killed after {timeout}s"`, exit code −9). `handle`: RUNNING →
  `sync_configs(store, <workdir>/<operation_id>, [service])` → run on
  `<…>/<service>` → DONE if exit 0 else FAILED (exit code + tail); temp dir
  removed in `finally`; `ReplayNotPossible` or any error → FAILED with the reason.

- [ ] **Step 1: Failing tests** (fake runner + `fake_opensearch`)

```python
def test_command_for_an_odata_entry(): assert build_command("odata", "/c", REQ, 18081)[:4] == [sys.executable, "-m", "maas_collector.rawdata.cli.odata", "-v"]
def test_jira_entry_uses_jirax(): ...
@pytest.mark.parametrize("case", ["missing", "deleted", "other_service", "disabled", "s3_class"])
def test_find_entry_refuses(case): ...
def test_child_environment_drops_the_config_backend(): ...
def test_long_timeout_scales_with_the_window(): assert replay_timeout(req(days=10, lane="long"), 6*3600, 3*3600) == 30*3600
def test_silent_child_is_killed_at_the_timeout():
    code, tail = ChildRunner().run([sys.executable, "-c", "import time; time.sleep(30)"], os.environ.copy(), timeout=1, progress=lambda _: None)
    assert code != 0 and "killed after 1" in tail
def test_handle_records_running_then_done(): assert recorder.calls == [(id, RUNNING), (id, DONE, 0)]
def test_handle_records_failed_with_exit_code_and_tail(): ...
def test_handle_of_a_disabled_interface_records_failed_and_returns(): ...
def test_handle_removes_its_config_copy(): ...
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** Commit `feat: :sparkles: Run operator replays in lanes`.

### Task 8: Product check (spec 1d, check)

**Files:**
- Create: `src/maas_collector/rawdata/operator/check.py`
- Test: `tests/operator/test_check.py`

**Interfaces:**
- Consumes: Task 6 (`CheckRequest`, `CheckReply`), `OpenSearchConfigProvider(index, service).load()`,
  `load_json_dict(content, ODataCollectorConfiguration, name)`, model
  resolution as in `FileCollector.load_config` (string model → `get_model`),
  `CredentialMixin.set_credential_attributes(config, provider.load([...]))`,
  `HttpMixin.create_http_session(config)`, `build_authentication(config.auth_method, config, session)`,
  `ActionIterator(path, config, force_update=True)` (`model.py:12`).
- Produces:

```python
IGNORED_FIELDS = ("reportName", "ingestionTime")
def odata_quote(value: str) -> str                    # "'" -> "''"
def product_query_url(config, request: CheckRequest) -> str
    # name: <product_url><odata_entity_location><odata_entities>?$filter=Name eq '<quoted>'
    # id:   ODataCollector.build_get_an_entity_query(config, <id>)
def diff_documents(remote: dict, stored: dict) -> list[dict]   # union of keys minus IGNORED_FIELDS, sorted by field
class ProductChecker:
    def __init__(self, service: str, config_index: str, credential_provider, workdir: str)
    def check(self, request: CheckRequest) -> CheckReply   # never raises; status "error" + message on any failure
```
  Flow: load the service's entries, pick the interface (error if missing,
  disabled, or not `ODataCollectorConfiguration` v4); GET the URL with auth;
  0 results → `remote=None`; single-entity response wrapped as `{"value": [..]}`;
  write the payload into a temp file and take the first action from
  `ActionIterator` as the document maas would store (`_source`, `_index`, `_id`);
  `stored` = that `_index`/`_id` from OpenSearch or `None`.

- [ ] **Step 1: Failing tests** (`responses`/Mock session, `fake_opensearch`, `fake_vault`)

```python
def test_quote_in_a_name_is_doubled():
    assert "Name eq 'S1A_X''Y.SAFE'" in unquote(product_query_url(CFG, CheckRequest("o", "IF", product_name="S1A_X'Y.SAFE")))
def test_id_uses_the_entity_url(): ...
def test_missing_on_remote_and_in_maas(): r = checker.check(REQ); assert r.status == "ok" and r.remote is None and r.stored is None
def test_present_in_both_and_identical(): assert r.diff == []
def test_present_in_both_with_a_different_publication_date():
    assert r.diff == [{"field": "publication_date", "remote": "2026-10-02T00:00:00.000Z", "stored": "2026-10-01T00:00:00.000Z"}]
def test_report_name_and_ingestion_time_are_not_differences(): ...
def test_remote_http_error_is_an_error_reply(): assert r.status == "error" and "503" in r.error
def test_non_odata_interface_is_an_error_reply(): ...
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** Commit `feat: :sparkles: Check a product against its interface`.

### Task 9: Worker and CLI (spec 1d)

**Files:**
- Create: `src/maas_collector/rawdata/operator/worker.py`, `src/maas_collector/rawdata/cli/operator_worker.py`, `docs/operator_worker.md`
- Modify: `docs/index.rst` (toctree, if docs use it), `CHANGELOG.md`
- Test: `tests/operator/test_worker.py`

**Interfaces:**
- Consumes: Tasks 6–8.
- Produces:

```python
@dataclass
class OperatorSettings:
    service: str                       # CONFIG_SERVICE, required
    config_index: str = "maas-collector-config"
    operation_index: str = "maas-operation"
    short_timeout: float = 6 * 3600    # OPERATOR_SHORT_TIMEOUT
    long_timeout_per_day: float = 3 * 3600   # OPERATOR_LONG_TIMEOUT_PER_DAY
    poll_interval: float = 5           # OPERATOR_POLL_INTERVAL
    healthcheck_port: int = 18081      # short lane child; long lane child uses +1
    workdir: str = tempfile.gettempdir()     # OPERATOR_WORKDIR
class OperatorWorker:
    def __init__(self, connection_factory, settings: OperatorSettings, lanes: dict[str, ReplayLane],
                 checker: ProductChecker, sleep=time.sleep)
    def run(self) -> None              # one thread per replay lane + one for checks; returns after stop()
    def stop(self, *_args) -> None     # sets should_stop, terminates running children
def operator_worker_main(argv: list[str]) -> int
```
  Replay lanes poll with `basic_get` and ack on receipt, like the DM worker
  (a replay is never redelivered after a worker crash; the record stays RUNNING —
  documented). A malformed body is acked, logged at ERROR, and skipped; when
  it carries an `operation_id` the record goes FAILED. The check thread
  consumes, runs `ProductChecker.check`, publishes `CheckReply.to_body()` on
  the default exchange with `routing_key=reply_to`, `correlation_id`, and
  `expiration` = 60 s; a check without `reply_to` is dropped with a warning.
  CLI parents `[common_parser()]` + `--operator-*` options, args via
  `get_collector_args(CollectorArgs, namespace)`, credentials via
  `build_credential_provider(args)`, OpenSearch connection as the other CLIs,
  kombu connection from the AMQP args; SIGTERM/SIGINT → `stop()`.

- [ ] **Step 1: Failing tests** (kombu `memory://` transport)

```python
def test_replay_message_reaches_its_lane(): publish(replay_routing_key("odata", "long"), REQ_LONG) ; run_until_idle(); assert long_lane.handled == [REQ_LONG] and short_lane.handled == []
def test_malformed_replay_body_is_skipped_and_worker_keeps_running(): ...
def test_malformed_body_with_operation_id_records_failed(): ...
def test_check_reply_goes_to_reply_to_with_correlation_id(): ...
def test_check_is_answered_while_a_long_replay_runs(): ...   # long lane blocked on an Event
def test_stop_terminates_a_running_child(): ...
def test_cli_requires_a_service():
    with pytest.raises(SystemExit) as exit_info: operator_worker_main([])
    assert exit_info.value.code == 2   # argparse: CONFIG_SERVICE / --config-service missing
```

- [ ] **Step 2:** Run → FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** PASS; `tox`.
- [ ] **Step 5:** `docs/operator_worker.md` (env/options, queues, record
  updates, "one worker per collector service", "credentials from OpenBao like
  the collector"), CHANGELOG. Commit
  `feat: :sparkles: Add the operator worker`.

### Task 10: Branch review

- [ ] **Step 1:** `tox` on the branch tip. Expected: all pass.
- [ ] **Step 2:** superpowers:requesting-code-review on the whole branch;
  fix confirmed findings.
- [ ] **Step 3:** superpowers:finishing-a-development-branch (no release in
  this plan; the backend plan pins this branch until 3.13.0 is released).

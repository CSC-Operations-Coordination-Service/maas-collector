# Continuous Archive (maas-collector) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Archive every product indexed through an odata entry with an `archive` rule into a named backup, once, in two stages. The collector publishes transfer requests. A new transfer stage then downloads, checks, de-duplicates, uploads and reports each product, records conflicts and gives the backend the gaps.

**Architecture:** A new package `maas_collector.rawdata.transfer` holds:
- the request contract and its confirmed publisher;
- the archive rule;
- the archive reports;
- the duplicate decision and the conflicts;
- the request's backup;
- the download;
- the gaps.

Stage 1 is the existing collector. After `extract_from_file`, it hands the documents of an archived entry to a `TransferRequester`: in live mode the created and updated ones, in replay every extracted one. Stage 2, `TransferStage`, is a `FileCollector` subclass. It loads its service's configuration and credentials, restarts on a new configuration and stops on SIGTERM, as the collectors do. Its `loop_body` takes requests with `basic_get`, acknowledges them on receipt and holds no connection during a transfer, as maas-engine's replay worker does. It transfers one product at a time through the backup sinks, which gain an explicit object key.

**Tech Stack:** Python 3.11, kombu 5 (py-amqp publisher confirms, `memory://` in tests), requests (streamed GET, redirects handled by hand), the existing backup sinks (boto3, local copy), opensearch-py through maas-model, pytest with `fake_opensearch`.

**Spec:** `docs/superpowers/specs/2026-10-29-maas-operator-backups-recollect-dm-design.md` (C1, C2, C3, C4 collector side, A3 archive reports) and **Contracts:** `docs/superpowers/plans/2026-10-29-archive-0-contracts.md` (§1 `archive`, §3 "No service-specific code", §4).

**Contract deviation:**
- **`ArchiveRule` gains two fields.** It keeps the contract's five fields, in order, and gains two trailing fields with defaults: `backup_version: int | None = None` and `force: bool = False`. Plan 2's `prepare` writes a recollect's archive choice (`backup`, `backup_version`, `duplicates`, `force`) into the copied entry's `archive` (spec B3c). The replay child reads these values back from the rule to fill its requests, and `from_entry` must accept them.
- **`find_gaps` gains two keyword-only parameters.** It keeps the contract's six parameters and adds `now` and `config_index`.
- **The requester's name on the replay child.** The contracts leave open how the replay child learns who asked (B3c). Plan 2 Task 6 defines it: `ReplayArgs.requested_by: str = ""`, filled by `--replay-requested-by` / `REPLAY_REQUESTED_BY`, and `build_command` passes it. This plan only consumes it: Task 0 Step 2 asserts that these names exist and stops otherwise. If plan 2 used another name, use that name everywhere this plan writes `requested_by` on `ReplayArgs`, and keep the contracts' `TransferRequest.requested_by` unchanged.

**Names this plan adds beyond the contracts (nothing renamed):**
- `contract.py`: `ORIGINS`, `transfer_exchange()`, `transfer_queue(service)`.
- `amqp.py`: `TransferPublisher`, `TransferPublishError`, `take_body`.
- `rule.py`: `DEFAULT_FIELDS`, `ArchiveRule.default`, `check_archive_rule`, `product_of`, `utc_datetime`.
- `odatacollector.py`: `ODataCollectorConfiguration.ARCHIVABLE`, `.archive` and `.archive_rule` (the contracts' schema reports `archivable` and `archive_rule.keys` from them, Task 2).
- `stage.py`: `RequestRefused`, `TransferStage.request_context` (Task 10a); `TransferStage.setup` declares the queue of the service (Task 11).
- `reports.py`: `REPORT_INDEX`, `ARCHIVE_ORIGIN`, `OUTCOMES`, `CONFLICT_ERROR`, `write_reports`, `archive_report`, `read_products`, `missing_product`.
- `conflicts.py`: `OPEN`, `RESOLVED`, `open_conflict`, `resolve_conflict`.
- `filecollector.py`: `TRANSFER_FAILED_EXIT_CODE`.

**Notes for the backend (plan 4):**
- **Download requests.** Plan 4 writes a download's `REQUESTED` reports with `requested_reports(request, [product_of(...)])` and `write_reports(...)`.
- **Counting conflicts.** An operation's conflict is a `SKIPPED` archive report whose `error_type` is `ArchiveConflict` (`CONFLICT_ERROR`). Plan 4 counts an operation's `conflicts` from those reports.

**Assumed from plan 2 (not in the contracts):**
- **The requester.** The replay child receives the requester (B3c) as `ReplayArgs.requested_by: str`, with `--replay-requested-by` / `REPLAY_REQUESTED_BY` and the `build_command` item `--replay-requested-by=<request.requested_by>`, all defined by plan 2 Task 6. This plan re-implements none of it (Task 0 Step 2 checks it). If plan 2 names the field otherwise, use plan 2's name in Task 5.
- **Plan 1's check this plan relies on:** an entry whose `archive.backup` equals its file `backup` is refused by plan 1 Task 3 (`validate_config_sources`). `check_archive_rule` does not repeat it.
- **Plan 2's code this plan touches:** `rawdata/configschema.py` (Task 2, conditional), `rawdata/operator/replay.py` (`build_command`: only the `service` parameter) and `rawdata/operator/handler.py` (`command`, `describe_failure`), in Tasks 6a and 6b.

## Global Constraints

- **Exchange and queue:** exchange `maas-transfer-exchange` (topic, durable), routing key `maas.transfer.<service>`, queue `maas-transfer-<service>`. Requests are published mandatory and confirmed: a failed publish fails a replay (FAILED) and is retried by the live collector at its next loop.
- **Request content:** "A request carries the documents' ids and index (several per request), the interface and service, `backup`, `backup_version`, `duplicates`, `force` (false unless set), `operation_id` (empty for the live flow), `requested_by`, `origin` (`live`, `recollect`, `download`)."
- **Which documents:** live, the documents created or updated. In replay, every document the run extracted, created, updated or unchanged. These requests go on the transfer path only: no `-f`, no `--force-message`.
- **Duplicate rule:** `duplicates` is `skip` (the default) or `replace_if_changed`. "A forced transfer always transfers and replaces." "A product without a checksum can only be compared by name: `replace_if_changed` behaves like `skip`, and no conflict can be detected."
- **Already archived:** "the last archive report of `(backup, interface, product name, destination)` has status `OK` or `REPLACED`".
- **Reports:** they go to `raw-data-backup` with `origin` `"archive"`. Statuses are `OK`, `KO`, `REPLACED`, `SKIPPED`, `REQUESTED`. "An operation's skips are reported (`SKIPPED`), live skips are not." The publisher of an operation writes one `REQUESTED` report per product.
- **Object key:** "`<directory>/<interface_name>/[YYYY/MM/DD/]<product file name>`, the date being the product's publication date".
- **Download URL:** the default is `<odata_product_url>/odata/v1/Products('<id>')/$value`. "The transfer stage never follows a redirect while sending a bearer token."
- **Download and retries:** the product is "streamed in chunks to a working directory made with `mkdtemp` (never built from the product's id or name)", and its size and checksum are checked. "Bounded retries everywhere: a permanent error (4xx, a checksum mismatch that persists) ends as a `KO` report and the request is acknowledged; nothing requeues forever."
- **Conflicts:** index `maas-archive-conflict`, one document per `(backup, interface, product name)`, status `OPEN` | `RESOLVED`. A later `OK` or `REPLACED` transfer sets it `RESOLVED`.
- **Gap:** "a product of an archived interface indexed more than a set delay ago (default 6 hours) with no archive report in its backup".
- **No service-specific code** (contracts §3): code, tests and docs never name a particular service, interface or download host. Tests use `svc`, `ITF_A`, `archive-a`, `catalogue.example`, `bytes.example`, `storage.example`.
- **No secret readable back:** no S3 key or credential value in a configuration, report, log or repr.
- **Unchanged behaviour:** the collector's own download (`odata_download_product`, `odatacollector.py:305`) and live collection without `archive` stay as they are.
- **Changelog and docs:** a task that changes behaviour an operator sees adds its bullets under `## Unreleased` → `### Continuous archive` in `CHANGELOG.md` and its section to `docs/archive.md`. Those tasks are 2 (rules, and the exchange, queue and request body, which `docs/archive.md` first needs), 3, 6, 11 (download rules, report statuses, conflicts, the note on triggering consumers) and 12. Task 14 renames the section for the release. The other tasks have no docs step.
- **Tests:** they run with `TZ=UTC`, as tox sets it. Before every commit, `TZ=UTC python -m pytest -q` must report all tests passing.
- **Commits:** a gitmoji subject (`feat: :sparkles: …`, `docs: :memo: …`), with the message ending in a blank line and the co-author line: `git commit -m "<subject>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.
- **Test file names:** files under `tests/transfer/` start with `test_transfer_` or `test_archive_`. pytest imports test modules by basename, and `tests/operator/test_contract.py` already exists.

## Review Focus

1. **Checksum case.** Input: a catalogue checksum in upper case or with blanks (`" D0C6BA… "`), compared with the lower-case MD5 the stage computes. Expected: it is the same checksum, so there is no checksum KO, no conflict and no replacement. Tests:
   - Task 7: `test_uppercase_catalogue_checksum_is_the_same_md5`;
   - Task 8: case `same-upper`;
   - Task 10a: `test_uppercase_catalogue_checksum_matches_the_archived_one`.
2. **Stop in the middle of a request** (a deploy, SIGTERM). Expected: the in-flight product gets no report, and every unfinished document is published again; nothing is lost. Tests: Task 10a `test_a_stop_during_the_download_propagates_and_reports_nothing`; Task 10b `test_stop_returns_the_documents_left`; Task 11 `test_documents_left_by_a_stop_are_republished`.
3. **Publication date on a backup with `calendar_tree`.** Inputs: a date with a non-UTC offset, or a missing date. Expected: the key uses the UTC day; a missing date is KO and never falls back to today's date. Tests: Task 3 `test_object_key_uses_the_utc_day_of_an_offset_date`; Task 10a `test_missing_publication_date_with_calendar_tree_is_ko`.
4. **Product name that is not a bare file name** (`../P1.zip`, `a/P1.zip`, empty). Expected: KO; nothing is written outside the temporary directory and no key is built from it. Tests: Task 7 `test_unsafe_product_name_is_permanent`; Task 10a `test_unsafe_product_name_is_ko_and_writes_nothing`.
5. **Raw document without a size or checksum** (a catalogue without `ContentLength` or `Checksum`, `0` included). Expected: no size or checksum check, comparison by name only, never a mismatch KO. Tests: Task 2 `test_product_of_leaves_unknown_values_empty`; Task 7 `test_unknown_size_and_checksum_are_not_checked`; Task 8 cases `no-checksum-*`.

---

## File structure

Paths are relative to `src/maas_collector/` unless they start with `tests/` or `docs/`.

| file | responsibility | task |
| --- | --- | --- |
| `rawdata/transfer/__init__.py` | package docstring only | 1 |
| `rawdata/transfer/contract.py` | names, `TransferRequest` and its body | 1 |
| `rawdata/transfer/amqp.py` | confirmed mandatory publisher, `take_body` | 1 |
| `rawdata/transfer/rule.py` | `ArchiveRule`, its validation, `product_of`, `utc_datetime` | 2 |
| `rawdata/collector/odatacollector.py` | `archive`, `archive_rule`, `ARCHIVABLE` on `ODataCollectorConfiguration` | 2 |
| `rawdata/configprovider.py` | `check_archive_rule` in `validate_config_sources` | 2 |
| `backup/backup.py`, `s3_backup.py`, `local_backup.py`, `sftp_backup.py` | `object_key`, `backup_file(..., key=)` | 3 |
| `rawdata/transfer/reports.py` | archive reports: model, write, state, `REQUESTED`, products, `missing_product` | 4 |
| `rawdata/model.py` | `ActionIterator.unmodified_documents` | 5 |
| `rawdata/transfer/requester.py` | indexed documents → requests (+ `REQUESTED`, missing documents included), pending, failures | 5 |
| `rawdata/collector/filecollector.py`, `rawdata/cli/odata.py` | wiring, `TRANSFER_FAILED_EXIT_CODE` | 6a |
| `rawdata/operator/replay.py`, `rawdata/operator/handler.py` | `--config-service` for the child, failure text | 6b |
| `rawdata/configschema.py` | `archive` key and `archivable`, only when plan 2's schema needs it | 2 |
| `rawdata/transfer/download.py` | URL, streamed download, checks, redirects, retries | 7 |
| `rawdata/transfer/duplicates.py` | `decide` (C1 duplicates, mirrors) | 8 |
| `rawdata/transfer/conflicts.py` | `maas-archive-conflict` documents | 8 |
| `rawdata/transfer/backups.py` | backup of a request (version, service tag), keys, sink | 9 |
| `rawdata/transfer/stage.py` | `TransferStage`: product flow (10a), request flow (10b), loop and queue declaration (11) | 10a, 10b, 11 |
| `rawdata/cli/transfer.py` | `python -m maas_collector.rawdata.cli.transfer` | 11 |
| `rawdata/transfer/gaps.py` | `find_gaps` | 12 |
| `tests/fakeopensearch.py` | `range` queries (plan 4 copies this file from the branch tip, after Task 12) | 12 |
| `docs/archive.md` (new), `docs/backup.md`, `docs/replay.md`, `docs/operator_worker.md`, `docs/index.md`, `CHANGELOG.md` | documentation | 2, 3, 6a, 6b, 11, 12; 14 (changelog section of the release) |

---

### Task 0: Branch and baseline

- [ ] **Step 1: Branch in plan 1's and plan 2's checkout.** Work in `/home/aagazzi/work/maas-collector-satu`, the checkout of plans 1 and 2 (no new worktree: plans 4 and 6 install and copy files from this path as "plan 3's tip"). Check that the working tree is clean and that `git branch --show-current` names plan 2's branch (`git log --oneline -3` shows plan 2's last commit). Then `git switch -c feature/archive-collector`. Work with the virtual environment plans 1 and 2 built there (`/home/aagazzi/work/maas-collector-satu/.venv`, `pip install -e ".[testing]"`; the testing extra brings maas-engine for `tests/operator`). Every path of this plan is relative to this checkout.
- [ ] **Step 2: Check that the names consumed from plans 1 and 2 exist.**

Run:

```bash
python -c "
from maas_collector.backup.named import BACKUP_NAME, MAX_NAME_LENGTH, SECRET_FIELDS, backup_definitions, resolve_backup
from maas_collector.backup.backup import OK, KO, REPLACED, SKIPPED, REQUESTED, BackupReport
BackupReport(origin='archive', backup_name='a', operation_id='o', product_id='i', product_name='p', written_md5='m', catalogue_checksum='c')
from maas_collector.rawdata.operator.overrides import DUPLICATES
from maas_collector.rawdata.collector.filecollector import BACKUP_FAILED_EXIT_CODE
from maas_collector.rawdata.replay import ReplayArgs
from maas_collector.rawdata.cli.lib.args import replay_parser
from maas_collector.rawdata.operator.replay import build_command
assert 'requested_by' in ReplayArgs.__dataclass_fields__, 'plan 2 Task 6 is missing'
assert any('--replay-requested-by' in a.option_strings for a in replay_parser()._actions), 'plan 2 Task 6 is missing'
print(BACKUP_FAILED_EXIT_CODE)"
```

Expected: plan 1's exit code, with no assertion error. Note it. An `AssertionError` means plan 2 did not define the replay requester: stop and finish plan 2 (this plan does not re-implement it). Task 6a takes the first code from 69 up that is not plan 1's `BACKUP_FAILED_EXIT_CODE` (nor `REPLAY_FAILED_EXIT_CODE` or `CONFIG_CHANGED_EXIT_CODE`) as `TRANSFER_FAILED_EXIT_CODE`.

- [ ] **Step 3: Run the baseline suite.** Run: `TZ=UTC python -m pytest -q`. Expected: all pass.
- [ ] **Step 4: Commit the plan.** Copy this plan into `docs/superpowers/plans/`, then:

```bash
git add docs/superpowers/plans/2026-10-29-archive-3-collector-archive.md
git commit -m "docs: :memo: Plan the continuous archive of maas-collector" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 1: Transfer request contract and confirmed publisher (C2)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/__init__.py`, with the docstring "The continuous archive: transfer requests and the transfer stage"
- Create: `src/maas_collector/rawdata/transfer/contract.py`, `src/maas_collector/rawdata/transfer/amqp.py`
- Test: `tests/transfer/test_transfer_contract.py`, `tests/transfer/test_transfer_amqp.py`

**Interfaces:**
- Consumes:
  - `DUPLICATES` (plan 2, `rawdata/operator/overrides.py`);
  - `_load(body) -> dict` (`rawdata/operator/contract.py:48`), which decodes bytes, str or dict and raises `ValueError` otherwise.
- Produces:

```python
# contract.py
TRANSFER_EXCHANGE = "maas-transfer-exchange"
ORIGINS = ("live", "recollect", "download")
def transfer_routing_key(service: str) -> str      # "maas.transfer.<service>"
def transfer_queue_name(service: str) -> str       # "maas-transfer-<service>"
def transfer_exchange() -> kombu.Exchange          # topic, durable
def transfer_queue(service: str) -> kombu.Queue    # durable, on transfer_exchange(), routing key transfer_routing_key(service)
@dataclass(frozen=True)
class TransferRequest:                             # fields exactly as the contracts, §4
    def to_body(self) -> dict
    @classmethod
    def from_body(cls, body: bytes | str | dict) -> "TransferRequest"   # ValueError naming the field
# amqp.py
class TransferPublishError(Exception): ...
def transfer_connection(amqp_url: str) -> kombu.Connection
    # transport_options: confirm_publish=True, client_properties.connection_name "transfer@<hostname>"
class TransferPublisher:
    def __init__(self, connection_factory: Callable[[], kombu.Connection], max_retries: int = 3)
    def publish(self, request: TransferRequest) -> None   # TransferPublishError: returned (no queue bound), nacked, broker unreachable
    def close(self) -> None
def take_body(connection_factory: Callable[[], kombu.Connection], service: str)
    # body of one message of transfer_queue(service), acknowledged on receipt, connection closed; None when empty
```

The JSON body of a request (unknown fields are ignored on reading):

```json
{"service": "svc", "interface_name": "ITF_A",
 "documents": [{"id": "d1", "index": "raw-data-a-2026"}],
 "backup": "archive-a", "backup_version": null, "duplicates": "skip", "force": false,
 "operation_id": "", "requested_by": "", "origin": "live"}
```

- [ ] **Step 1: Write the failing tests.**

```python
# tests/transfer/test_transfer_contract.py
BODY = {"service": "svc", "interface_name": "ITF_A",
        "documents": [{"id": "d1", "index": "raw-data-a-2026"}, {"id": "d2", "index": "raw-data-a-2025"}],
        "backup": "archive-a", "backup_version": 3, "duplicates": "replace_if_changed", "force": True,
        "operation_id": "op-1", "requested_by": "alice", "origin": "recollect"}

def test_names():
    assert (TRANSFER_EXCHANGE, transfer_routing_key("svc"), transfer_queue_name("svc")) == (
        "maas-transfer-exchange", "maas.transfer.svc", "maas-transfer-svc")
    assert transfer_exchange().type == "topic" and transfer_exchange().durable
    assert (transfer_queue("svc").name, transfer_queue("svc").routing_key) == ("maas-transfer-svc", "maas.transfer.svc")

def test_body_round_trip():
    request = TransferRequest.from_body(json.dumps(BODY).encode())
    assert request.documents == (("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2025"))
    assert request.to_body() == BODY

def test_live_defaults():
    keys = ("service", "interface_name", "documents", "backup", "backup_version", "duplicates")
    request = TransferRequest.from_body({key: BODY[key] for key in keys})
    assert (request.force, request.operation_id, request.requested_by, request.origin) == (False, "", "", "live")

@pytest.mark.parametrize("change, field", [
    ({"documents": []}, "documents"), ({"documents": [{"id": "d1"}]}, "documents"),
    ({"service": ""}, "service"), ({"interface_name": 3}, "interface_name"), ({"backup": None}, "backup"),
    ({"backup_version": 0}, "backup_version"), ({"backup_version": True}, "backup_version"),
    ({"duplicates": "keep"}, "duplicates"), ({"force": "yes"}, "force"), ({"origin": "manual"}, "origin"),
])
def test_bad_body_is_refused_naming_the_field(change, field):
    with pytest.raises(ValueError, match=field):
        TransferRequest.from_body(BODY | change)

def test_body_that_is_not_an_object_is_refused():
    with pytest.raises(ValueError):
        TransferRequest.from_body(b"[1]")
```

```python
# tests/transfer/test_transfer_amqp.py
REQUEST = TransferRequest("svc", "ITF_A", (("d1", "raw-data-a-2026"),), "archive-a", None, "skip")
memory = lambda: kombu.Connection("memory://")   # the memory transport shares one broker per process

def test_request_reaches_the_queue_of_its_service():
    with memory() as connection:
        queue = transfer_queue("svc")(connection.default_channel)
        queue.declare()
        TransferPublisher(memory).publish(REQUEST)
        message = queue.get()
    assert TransferRequest.from_body(message.body) == REQUEST
    assert message.delivery_info["routing_key"] == "maas.transfer.svc"

def test_take_body_acknowledges_on_receipt():        # queue declared, REQUEST published
    assert TransferRequest.from_body(take_body(memory, "svc")) == REQUEST
    assert take_body(memory, "svc") is None

def test_publish_is_mandatory_persistent_and_confirmed():   # RecordingConnection keeps Producer.publish kwargs
    assert (kwargs["mandatory"], kwargs["delivery_mode"], kwargs["routing_key"]) == (True, "persistent", "maas.transfer.svc")
    assert transfer_connection("amqp://u:p@broker//").transport_options["confirm_publish"] is True

def test_returned_request_is_an_error():             # the fake producer calls its on_return inside publish
    with pytest.raises(TransferPublishError, match="maas.transfer.svc"):
        publisher.publish(REQUEST)

def test_broker_error_is_an_error_and_the_next_publish_reconnects():   # first publish raises OSError
    with pytest.raises(TransferPublishError):
        publisher.publish(REQUEST)
    publisher.publish(REQUEST)
    assert factory_calls == 2
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_contract.py tests/transfer/test_transfer_amqp.py -v`. Expected: FAIL with `ModuleNotFoundError: No module named 'maas_collector.rawdata.transfer'`.

- [ ] **Step 3: Implement the contract and `TransferPublisher`.**

`from_body` checks the fields:
- `service`, `interface_name` and `backup`: non-empty str;
- `documents`: a non-empty list of `{"id", "index"}`, both non-empty str;
- `backup_version`: `None` or an int ≥ 1 that is not a bool;
- `duplicates`: in `DUPLICATES`;
- `force`: a bool, `False` when absent;
- `operation_id` and `requested_by`: str, `""` when absent;
- `origin`: in `ORIGINS`, `"live"` when absent.

`TransferPublisher` keeps one connection and one producer until an error closes them; the producer is built with `on_return`, which records the returned message. `publish` calls `producer.publish(request.to_body(), exchange=transfer_exchange(), routing_key=…, serializer="json", delivery_mode="persistent", mandatory=True, declare=[transfer_exchange()], retry=True, retry_policy={"max_retries": max_retries, "interval_start": 0, "interval_step": 2, "interval_max": 10})`. With publisher confirms, py-amqp has read the broker's answer when `publish` returns, so a recorded return is known at that point. In that case, close and raise `TransferPublishError(f"no queue bound to {routing_key} on {TRANSFER_EXCHANGE}: is the transfer stage of {service} running?")`. Any other exception also closes and raises `TransferPublishError` from it.

The publisher never declares the queue: the stage declares it in `TransferStage.setup()` (Task 11), before it awaits its first request, and an unroutable request is how a deployment without a stage shows up (the same reasoning as spec B3a). `take_body` is maas-engine's `ReplayWorker.next_body` without its wait loop and does not declare the queue either. The tests of this task declare it by hand.

- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS. Then `TZ=UTC python -m pytest -q`: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer tests/transfer
git commit -m "feat: :sparkles: Define the transfer request and its confirmed publisher" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 2: Archive rules (C1)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/rule.py`, `docs/archive.md`
- Modify: `src/maas_collector/rawdata/collector/odatacollector.py:38-118` (`ODataCollectorConfiguration`)
- Modify: `src/maas_collector/rawdata/configprovider.py`: the per-entry loop of `validate_config_sources`, at `:346-359` in 3.13.0
- Modify (conditional, Step 3b): plan 2's `src/maas_collector/rawdata/configschema.py`
- Modify: `docs/index.md` (add `- [Continuous archive](archive)` after Backup), `CHANGELOG.md`
- Test: `tests/transfer/test_archive_rule.py`, `tests/transfer/test_archive_schema.py`

**Interfaces:**
- Consumes:
  - `BACKUP_NAME`, `MAX_NAME_LENGTH` (plan 1, `backup/named.py`);
  - `DUPLICATES` (plan 2);
  - plan 1's `validate_config_sources(sources, config_classes_for, resolve_models=True, evaluate=True)`;
  - `config_classes_for` (`configstore.py:129`);
  - `build_collector_configuration` (`configuration.py:108`).
- Produces:

```python
DEFAULT_FIELDS = {"product_id": "product_id", "product_name": "product_name", "size": "content_length",
                  "checksum": "checksum", "checksum_algorithm": "MD5", "publication_date": "publication_date"}
@dataclass(frozen=True)
class ArchiveRule:
    backup: str
    duplicates: str
    fields: dict[str, str]
    download_url: str | None
    quote_id: bool
    backup_version: int | None = None     # contract deviation, see the top
    force: bool = False                   # contract deviation, see the top
    @classmethod
    def from_entry(cls, entry: dict) -> "ArchiveRule | None"   # None without "archive" or with null; ValueError naming the key
    @classmethod
    def default(cls, backup: str) -> "ArchiveRule"             # "skip", DEFAULT_FIELDS, the collector's URL form, quoted id
    def new_checksum(self)                                     # hashlib object of checksum_algorithm, name lookup order in Step 3
def check_archive_rule(entry: dict, configuration_classes: Iterable[type]) -> None
def product_of(source: dict, rule: ArchiveRule, document_id: str, document_index: str) -> dict
    # {"document_id", "document_index", "product_id", "product_name", "size", "checksum", "publication_date"}
def utc_datetime(value) -> datetime.datetime | None            # None when absent; naive read as UTC; ValueError when unparsable
# ODataCollectorConfiguration
ARCHIVABLE: typing.ClassVar[bool] = True
archive: dict = None
archive_rule: "ArchiveRule | None" = field(default=None, init=False)   # set by __post_init__ from archive
```

- [ ] **Step 1: Write the failing tests.**

```python
ENTRY = {"class": "ODataCollectorConfiguration", "interface_name": "ITF_A", "id_field": "product_id",
         "routing_key": "", "archive": {"backup": "archive-a"}}
def entry(**archive): return {**ENTRY, "archive": {"backup": "archive-a", **archive}}

def test_entry_without_archive_has_no_rule():
    assert ArchiveRule.from_entry({"interface_name": "ITF_A"}) is None
    assert ArchiveRule.from_entry({"interface_name": "ITF_A", "archive": None}) is None

def test_defaults():
    assert ArchiveRule.from_entry(ENTRY) == ArchiveRule("archive-a", "skip", DEFAULT_FIELDS, None, True, None, False)
    assert ArchiveRule.default("archive-a") == ArchiveRule.from_entry(ENTRY)

def test_fields_are_merged_over_the_defaults():
    rule = ArchiveRule.from_entry(entry(fields={"checksum": "origin_checksum_md5"}))
    assert rule.fields == DEFAULT_FIELDS | {"checksum": "origin_checksum_md5"}

def test_download_template_with_an_unquoted_id():
    rule = ArchiveRule.from_entry(entry(download={"url": "https://bytes.example/odata/v1/Products({id})/$value", "quote_id": False}))
    assert (rule.download_url, rule.quote_id) == ("https://bytes.example/odata/v1/Products({id})/$value", False)

def test_recollect_choice_is_read():
    rule = ArchiveRule.from_entry(entry(duplicates="replace_if_changed", backup_version=4, force=True))
    assert (rule.duplicates, rule.backup_version, rule.force) == ("replace_if_changed", 4, True)

@pytest.mark.parametrize("archive, word", [
    ("archive-a", "archive"), ({}, "backup"), ({"backup": "Archive_A"}, "backup"), ({"backup": "a" * 65}, "backup"),
    ({"backup": "archive-a", "duplicates": "keep"}, "duplicates"),
    ({"backup": "archive-a", "fields": {"name": "x"}}, "name"),
    ({"backup": "archive-a", "fields": {"checksum": ""}}, "checksum"),
    ({"backup": "archive-a", "fields": {"checksum_algorithm": "CRC99"}}, "checksum_algorithm"),
    ({"backup": "archive-a", "fields": {"checksum_algorithm": "BLAKE3"}}, "checksum_algorithm"),   # hashlib lacks it: refused at load
    ({"backup": "archive-a", "download": {"url": "https://bytes.example/Products/$value"}}, "{id}"),
    ({"backup": "archive-a", "download": {"url": "ftp://bytes.example/{id}"}}, "download.url"),
    ({"backup": "archive-a", "download": {"quote_id": "no"}}, "quote_id"),
    ({"backup": "archive-a", "backup_version": 0}, "backup_version"),
    ({"backup": "archive-a", "force": 1}, "force"),
    ({"backup": "archive-a", "buckets": ["b1"]}, "buckets"),
])
def test_bad_rule_is_refused_naming_the_key(archive, word):
    with pytest.raises(ValueError, match=re.escape(word)):
        ArchiveRule.from_entry({**ENTRY, "archive": archive})

@pytest.mark.parametrize("algorithm, name", [("MD5", "md5"), ("SHA-256", "sha256"), ("sha256", "sha256"), ("SHA3-256", "sha3_256")])
def test_new_checksum_follows_the_algorithm(algorithm, name):
    assert ArchiveRule.from_entry(entry(fields={"checksum_algorithm": algorithm})).new_checksum().name == name

def test_configuration_parses_its_rule_when_built():
    config = build_collector_configuration(ENTRY, ODataCollectorConfiguration, register=False)
    assert config.archive_rule == ArchiveRule.default("archive-a")
    assert build_collector_configuration({**ENTRY, "archive": None}, ODataCollectorConfiguration, register=False).archive_rule is None

@pytest.mark.parametrize("evaluate", [True, False])
def test_validation_refuses_a_bad_rule_in_both_modes(evaluate):
    with pytest.raises(ConfigValidationError, match="duplicates"):
        validate_config_sources([ConfigSource("a.json", {"collectors": [entry(duplicates="keep")]})],
                                config_classes_for, resolve_models=False, evaluate=evaluate)

def test_validation_refuses_a_rule_on_an_entry_that_cannot_archive():   # class "" builds as FileCollectorConfiguration
    with pytest.raises(ConfigValidationError, match="odata"):
        validate_config_sources([ConfigSource("a.json", {"collectors": [{**ENTRY, "class": ""}]})],
                                config_classes_for, resolve_models=False)

def test_validation_refuses_the_same_backup_for_files_and_archive():      # guards plan 1's check: it passes before this task's code
    content = {"collectors": [{**ENTRY, "backup": "archive-a"}],
               "backups": [{"name": "archive-a", "type": "BackupLocal", "directory": "/archive"}]}
    with pytest.raises(ConfigValidationError, match="archive-a"):
        validate_config_sources([ConfigSource("a.json", content)], config_classes_for, resolve_models=False)

def test_product_of_reads_the_rule_fields():
    rule = ArchiveRule.from_entry(entry(fields={"checksum": "md5"}))
    source = {"product_id": "id-1", "product_name": "P1.zip", "content_length": "3000", "md5": " ABC ",
              "publication_date": "2026-03-04T10:00:00.000Z"}
    assert product_of(source, rule, "d1", "raw-data-a-2026") == {
        "document_id": "d1", "document_index": "raw-data-a-2026", "product_id": "id-1", "product_name": "P1.zip",
        "size": 3000, "checksum": "ABC", "publication_date": "2026-03-04T10:00:00.000Z"}

def test_product_of_leaves_unknown_values_empty():                       # Review Focus 5
    product = product_of({"product_name": "P1.zip", "content_length": 0, "checksum": "  "},
                         ArchiveRule.default("archive-a"), "d1", "i")
    assert (product["size"], product["checksum"], product["product_id"], product["publication_date"]) == (None, None, None, None)

def test_utc_datetime():
    assert utc_datetime("2026-03-04T23:30:00-02:00") == datetime.datetime(2026, 3, 5, 1, 30, tzinfo=datetime.UTC)
    assert utc_datetime("2026-03-04T10:00:00") == datetime.datetime(2026, 3, 4, 10, tzinfo=datetime.UTC)
    assert utc_datetime(None) is None
    with pytest.raises(ValueError):
        utc_datetime("yesterday")
```

```python
# tests/transfer/test_archive_schema.py
def test_schema_describes_the_archive_key():
    schema = configuration_schema()
    odata = schema["collector_classes"]["ODataCollectorConfiguration"]
    assert odata["archivable"] is True
    assert odata["keys"]["archive"]["group"] == "archive" and odata["keys"]["archive"]["description"]
    assert "archive_rule" not in odata["keys"]
    assert set(schema["archive_rule"]["keys"]) == {"backup", "duplicates", "fields", "download"}   # not backup_version, not force
    assert [name for name, cls in schema["collector_classes"].items() if cls["archivable"]] == ["ODataCollectorConfiguration"]
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_archive_rule.py tests/transfer/test_archive_schema.py -v`. Expected: FAIL with `ModuleNotFoundError ... transfer.rule`, and the schema test with a `KeyError` or `AssertionError` on `archive` (`test_validation_refuses_the_same_backup_for_files_and_archive` cannot be collected before the import works; once it can, it passes with plan 1's code alone).

- [ ] **Step 3: Implement `ArchiveRule.from_entry`, `check_archive_rule` and the configuration fields.**

`from_entry` validates the rule as follows. Messages start with the interface name and name the key.
- `archive` must be an object whose keys are within `backup`, `duplicates`, `fields`, `download`, `backup_version`, `force`.
- `backup` matches `BACKUP_NAME` and is at most `MAX_NAME_LENGTH` characters.
- `duplicates` is in `DUPLICATES`, `skip` by default.
- The keys of `fields` are within `DEFAULT_FIELDS`, and its values are non-empty str. Its `checksum_algorithm` must be known to `hashlib`. The lookup tries, in order, `hashlib.new(name.lower())`, then `name.lower().replace("-", "_")`, then `name.lower().replace("-", "")`, and keeps the first that works (`SHA-256` works as `sha-256`, `SHA3-256` as `sha3-256`; `sha_256` alone is refused by hashlib). A name none of them accepts (for example `BLAKE3`) is refused at load. `new_checksum` uses the same lookup, and `from_entry` runs it so that the refusal happens at load.
- The keys of `download` are within `url` and `quote_id`. `url` is `None` or an `http(s)://` str containing `{id}`. `quote_id` is a bool, `True` by default.
- `backup_version` and `force` follow the same rules as in Task 1.

`product_of` produces:
- `size`: an `int > 0`, digit strings accepted, `None` otherwise;
- `checksum`: stripped, `None` when blank;
- the other values: as stored, `None` when absent or `""`.

`check_archive_rule` does nothing without `archive`. Otherwise it raises `ValueError` in two cases:
- a class among the configuration classes has no `ARCHIVABLE`, with the message `"<interface>: archive rules are for odata entries, <Class> cannot archive"`;
- `from_entry` raises.

It does not compare the entry's `backup` with `rule.backup`: plan 1 Task 3's `validate_config_sources` already refuses that (spec A3), and runs in the same `try`.

`ODataCollectorConfiguration.__post_init__` sets `self.archive_rule = ArchiveRule.from_entry({"interface_name": self.interface_name, "archive": self.archive})`. `init=False` keeps `archive_rule` out of the JSON keys, as for `backup_report_model`.

In `validate_config_sources`, call `check_archive_rule(meta, config_classes_for(meta))` for every entry, in both modes, inside the `try` that turns errors into `ConfigValidationError`. `rule.py` imports only `backup/named.py` and `rawdata/operator/overrides.py`, so `odatacollector.py` imports it at module level. If that import creates a cycle through plan 2's module, move the `DUPLICATES` import into `from_entry`. Run `python -c "import maas_collector.rawdata.collector.odatacollector"`; it must succeed.

- [ ] **Step 3b: Teach plan 2's schema about the archive.** Plan 2's `configschema.py` derives each class's JSON keys from the dataclass fields. Adapt it so that:
  - it skips `init=False` fields, so `archive_rule` is never a key (as `backup_report_model`); if it does not skip them yet, make it skip them;
  - `archive` has a catalog entry: group `archive`, a one-line description (the archive rule of the entry, see `archive.md`), type `object`;
  - `archivable` of a class is `getattr(cls, "ARCHIVABLE", False)`;
  - `archive_rule.keys` lists the four editable keys `backup`, `duplicates`, `fields`, `download`, each with type and description, and never `backup_version` or `force` (set by `prepare`, not editable);
  - plan 2's schema tests (every key described, secret-key naming) must still pass with the new fields.

  If plan 2's schema module already derives all of this, this step changes nothing and the test of Step 1 is the check.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS. Then run the full suite, including plan 2's schema tests.
- [ ] **Step 5: Write the docs.** Write `docs/archive.md`:
  - an introduction: the two stages, and a link to `backup.md` for named backups;
  - "Archive rules": the JSON of spec C1 and a key table with defaults;
  - the meaning of `duplicates`, the `fields`, the download URL with `{id}` and `quote_id`, and the `checksum_algorithm` names accepted (those of `hashlib`);
  - the rules: odata entries only, and never the entry's own `backup`;
  - "Transfer requests, wire format" (so that the page exists before Task 6a): exchange `maas-transfer-exchange` (topic, durable), routing key `maas.transfer.<service>`, queue `maas-transfer-<service>` declared by the transfer stage, and the JSON body of Task 1.

  Add the CHANGELOG bullet.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/rule.py src/maas_collector/rawdata/collector/odatacollector.py src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/configschema.py tests/transfer/test_archive_rule.py tests/transfer/test_archive_schema.py docs/archive.md docs/index.md CHANGELOG.md
git commit -m "feat: :sparkles: Declare archive rules on odata collector entries" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 3: The backup sinks write at an explicit key (C1 stable keys)

**Files:**
- Modify: `src/maas_collector/backup/backup.py:184-212` (`get_backup_path`), `:315-386` (`backup_file`, `backup_file_implementation`)
- Modify: `src/maas_collector/backup/s3_backup.py:81-117`, `src/maas_collector/backup/local_backup.py:27-56`, and `src/maas_collector/backup/sftp_backup.py` (the signature of `backup_file_implementation` only)
- Modify: `docs/backup.md` (the destination path paragraph), `CHANGELOG.md`
- Test: `tests/test_backup.py`

**Interfaces:**
- Produces:

```python
CollectorBackup.object_key(self, interface_name: str, file_name: str, date: datetime.datetime | None = None) -> str
    # "<interface_name>/<file_name>"; with calendar_tree "<interface_name>/YYYY/MM/DD/<file_name>",
    # the UTC day of date (today, UTC, when None)
CollectorBackup.backup_file(self, config, path: str, key: str | None = None) -> list[BackupReport]
    # key: written at "<directory>/<key>" on every destination, "<directory>/<key>.gz" when compressed;
    # None: the dated layout, unchanged
CollectorBackup.backup_file_implementation(self, config, path: str, key: str | None = None) -> list[BackupReport]
CollectorBackup.get_backup_path(self, config, path, dirdatetime=None, key: str | None = None) -> tuple
    # with key: (directory part, file name) of "<directory>/<key>"
```

- [ ] **Step 1: Write the failing tests.** Plan 1's `local_backup` fixture creates its directory. New tests pass `FakeConfig(interface_name="ITF_A", model_name="ProductA")` as `CONFIG`.

```python
DAY = datetime.datetime(2026, 3, 4, 23, 30, tzinfo=datetime.UTC)

def test_object_key_without_calendar_tree(local_backup):
    assert local_backup().object_key("ITF_A", "P1.zip", DAY) == "ITF_A/P1.zip"

def test_object_key_files_by_the_given_day(local_backup):
    assert local_backup(calendar_tree=True).object_key("ITF_A", "P1.zip", DAY) == "ITF_A/2026/03/04/P1.zip"

def test_object_key_uses_the_utc_day_of_an_offset_date(local_backup):     # Review Focus 3
    late = datetime.datetime(2026, 3, 4, 23, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=-2)))
    assert local_backup(calendar_tree=True).object_key("ITF_A", "P1.zip", late) == "ITF_A/2026/03/05/P1.zip"

def test_local_backup_writes_at_the_key(local_backup, source_file, tmp_path):
    [report] = local_backup().backup_file(CONFIG, source_file, key="ITF_A/2026/03/04/P1.zip")
    target = tmp_path / "backup" / "ITF_A/2026/03/04/P1.zip"
    assert report.destination_path == str(target) and target.read_bytes() == pathlib.Path(source_file).read_bytes()

def test_s3_backup_writes_every_bucket_at_the_key(source_file):
    backup = build_s3_backup(buckets=["b1", "b2"], directory="archive")
    backup.backup_file(CONFIG, source_file, key="ITF_A/P1.zip")
    assert [call.args[1:] for call in backup.s3_client.upload_file.call_args_list] == [
        ("b1", "archive/ITF_A/P1.zip"), ("b2", "archive/ITF_A/P1.zip")]

def test_compressed_object_gets_gz_after_the_key(local_backup, tmp_path):    # a ".dat" source file
    [report] = local_backup(enable_gzip=True).backup_file(CONFIG, str(dat_file), key="ITF_A/P1.dat")
    assert report.destination_path.endswith("ITF_A/P1.dat.gz") and report.compressed

def test_without_key_the_layout_is_unchanged(local_backup, source_file, tmp_path):
    [report] = local_backup().backup_file(CONFIG, source_file)
    assert report.destination_path == str(tmp_path / "backup" / "ITF_A" / os.path.basename(source_file))
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/test_backup.py -v`. Expected: the new tests FAIL with `AttributeError: ... 'object_key'` or `TypeError: ... unexpected keyword argument 'key'`.
- [ ] **Step 3: Implement the explicit key.** Thread `key` from `backup_file` through `backup_file_implementation` to `get_backup_path`. `backup_file` appends `.gz` to the key when it compressed the file. Joins use `"/"`, as `get_backup_path` does. `object_key` converts `date` with `astimezone(datetime.UTC)`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Write the docs.** In `docs/backup.md`: with an explicit key (used by the archive, see `archive.md`), the file goes to `<directory>/<key>`; `object_key` builds the archive layout `<interface_name>/[YYYY/MM/DD/]<file>` from a given date. Add the CHANGELOG bullet.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/backup tests/test_backup.py docs/backup.md CHANGELOG.md
git commit -m "feat: :sparkles: Let the backup sinks write at an explicit key" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 4: Archive reports (A3)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/reports.py`
- Test: `tests/transfer/test_archive_reports.py`

**Interfaces:**
- Consumes:
  - Task 1: `TransferRequest`; Task 2: `ArchiveRule`, `product_of`;
  - plan 1: `BackupReport` with the A3 fields, and `OK, KO, REPLACED, SKIPPED, REQUESTED`;
  - `BackupReportWriter(model).write(reports, report_name="", report_folder="", **bulk_kwargs) -> (stored, errors)` (`rawdata/backup_report.py:32`);
  - `build_model_class(meta, register=False)` (`configuration.py:20`).
- Produces:

```python
REPORT_INDEX = "raw-data-backup"
ARCHIVE_ORIGIN = "archive"
OUTCOMES = (OK, REPLACED, KO, SKIPPED)
CONFLICT_ERROR = "ArchiveConflict"            # error_type of a SKIPPED report that is a conflict
class ArchiveReportError(RuntimeError): ...
def report_model() -> type                    # built once: index REPORT_INDEX, partition ingestionTime "%Y",
                                              # one field per BackupReport field (str Keyword, details Text, int Long,
                                              # float Float, bool Boolean, datetime Date)
def write_reports(reports: list[BackupReport]) -> None      # one bulk "create", refresh="wait_for"; ArchiveReportError if one is not stored
def archive_fields(request: TransferRequest, product: dict) -> dict
    # origin, backup_name, operation_id, interface_name, product_id, product_name, catalogue_checksum ("" when None)
def archive_report(request: TransferRequest, product: dict, status: str, **fields) -> BackupReport
    # archive_fields + filename = source_filename = product name + start_time = end_time = now (UTC) + fields
def requested_reports(request: TransferRequest, products: list[dict]) -> list[BackupReport]
    # one REQUESTED report per product; its details are product.get("details", "")
def missing_product(document: tuple[str, str]) -> dict
    # a document that is not in the index: product_of's keys, document_id/document_index as given, product_id "" and product_name "",
    # size/checksum/publication_date None, and "details": f"document {id} not found in {index}"
def archived_state(client, backup: str, interface: str, product_name: str) -> dict[str, dict]
def read_products(client, documents: Sequence[tuple[str, str]], rule: ArchiveRule) -> tuple[list[dict], list[tuple[str, str]]]
    # (products found, in order, built with product_of; (id, index) of the documents not found)
```

- [ ] **Step 1: Write the failing tests.** In the fake, archive reports are written straight into the index `raw-data-backup` (the alias in production). `report(**fields)` builds a report source with the defaults `origin "archive"`, `backup_name "archive-a"`, `interface_name "ITF_A"`, `product_name "P1.zip"`.

```python
REQUEST = TransferRequest("svc", "ITF_A", (("d1", "i1"), ("d2", "i1")), "archive-a", 2, "skip", False, "op-1", "alice", "recollect")
P1 = {"document_id": "d1", "document_index": "i1", "product_id": "id-1", "product_name": "P1.zip",
      "size": 3, "checksum": "abc", "publication_date": None}

def test_requested_reports_one_per_product():
    [first, second] = requested_reports(REQUEST, [P1, P1 | {"product_name": "P2.zip", "product_id": "id-2"}])
    assert (first.status, first.origin, first.backup_name, first.operation_id, first.interface_name,
            first.product_id, first.product_name, first.catalogue_checksum, first.destination) == (
        "REQUESTED", "archive", "archive-a", "op-1", "ITF_A", "id-1", "P1.zip", "abc", "")
    assert second.product_name == "P2.zip"

def test_missing_product_has_an_empty_name_and_names_the_document():
    product = missing_product(("d9", "i1"))
    assert (product["product_name"], product["product_id"], product["details"]) == ("", "", "document d9 not found in i1")
    [report] = requested_reports(REQUEST, [product])
    assert (report.status, report.product_name, report.details) == ("REQUESTED", "", "document d9 not found in i1")

def test_archived_state_keeps_the_last_report_per_destination(fake_opensearch):
    add(report(destination="b1", status="OK", start_time="2026-10-01T00:00:00.000Z"))
    add(report(destination="b1", status="KO", start_time="2026-10-02T00:00:00.000Z"))
    add(report(destination="b2", status="REPLACED", start_time="2026-10-01T00:00:00.000Z"))
    add(report(destination="b3", status="OK", origin="collector"))
    add(report(destination="b4", status="OK", product_name="P2.zip"))
    add(report(destination="b5", status="OK", backup_name="archive-b"))
    add(report(destination="b6", status="REQUESTED"))
    state = archived_state(fake_opensearch, "archive-a", "ITF_A", "P1.zip")
    assert {name: value["status"] for name, value in state.items()} == {"b1": "KO", "b2": "REPLACED"}

def test_archived_state_without_index_is_empty(fake_opensearch):
    assert archived_state(fake_opensearch, "archive-a", "ITF_A", "P1.zip") == {}

def test_write_reports_creates_documents_in_the_yearly_index(fake_opensearch):
    with patch("maas_collector.rawdata.backup_report.bulk", return_value=(1, [])) as bulk:
        write_reports(requested_reports(REQUEST, [P1]))
    [action] = bulk.call_args.args[1]
    assert action["_op_type"] == "create" and action["_index"] == f"raw-data-backup-{datetime.datetime.now(datetime.UTC).year}"
    assert (action["_source"]["origin"], action["_source"]["status"]) == ("archive", "REQUESTED")
    assert bulk.call_args.kwargs["refresh"] == "wait_for"

def test_write_reports_raises_when_a_report_is_not_stored(fake_opensearch):
    with patch("maas_collector.rawdata.backup_report.bulk", return_value=(0, [{"create": {"status": 500}}])):
        with pytest.raises(ArchiveReportError):
            write_reports(requested_reports(REQUEST, [P1]))

def test_report_model_declares_every_report_field():
    assert set(BackupReport().to_dict()) <= set(report_model()._INITIAL_FIELDS)
    assert report_model().Index.name == "raw-data-backup"

def test_read_products_maps_the_rule_fields_and_lists_the_missing(fake_opensearch):
    fake_opensearch.index(index="i1", id="d1", body={"product_id": "id-1", "product_name": "P1.zip", "content_length": 3, "checksum": "abc"})
    products, missing = read_products(fake_opensearch, (("d1", "i1"), ("d9", "i1")), ArchiveRule.default("archive-a"))
    assert products == [P1] and missing == [("d9", "i1")]
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_archive_reports.py -v`. Expected: FAIL with `ModuleNotFoundError ... transfer.reports`.
- [ ] **Step 3: Implement the reports module.**
  - `archived_state` searches `REPORT_INDEX` with a `bool.filter` on: term `origin` = `archive`; terms `backup_name`, `interface_name` and `product_name`; terms `status` in `[OK, REPLACED, KO]`. It sorts on `{"start_time": {"order": "desc"}}`, takes `size` 100, keeps the first hit per `destination`, and answers `{}` on `NotFoundError`.
  - `read_products` calls `client.get(index=…, id=…)` per document. `NotFoundError` means missing.
  - `missing_product` is the one place that builds the stand-in of a missing document, so that its `REQUESTED` report (Task 5) and its `KO` report (Task 10b) carry the same product name (empty) and the same details.
  - `write_reports` calls `BackupReportWriter(report_model()).write(reports, refresh="wait_for")` and raises when errors come back.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/reports.py tests/transfer/test_archive_reports.py
git commit -m "feat: :sparkles: Read and write the archive reports" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 5: Transfer requests from indexed documents (C2)

**Files:**
- Modify: `src/maas_collector/rawdata/model.py`, `ActionIterator`: `:60` (attribute) and `:157-166` (the unchanged branch)
- Create: `src/maas_collector/rawdata/transfer/requester.py`
- Test: `tests/test_action_iterator_extra_fields.py` (one test), `tests/transfer/test_transfer_requester.py`

**Interfaces:**
- Consumes:
  - Task 1: `TransferRequest`, `TransferPublisher`, `TransferPublishError`;
  - Task 2: `config.archive_rule`;
  - Task 4: `read_products`, `requested_reports`, `missing_product`, `write_reports`, `ArchiveReportError`;
  - `ReplayArgs.suffix`, the operation id passed as `--replay-suffix`;
  - `ReplayArgs.requested_by` (plan 2 Task 6, checked by Task 0 Step 2).
- Produces:

```python
ActionIterator.unmodified_documents: list[tuple[str, str]]   # (id, stored index), next to unmodified_ids
REQUEST_BATCH_SIZE = 10
MAX_PENDING_REQUESTS = 10_000
class TransferRequester:
    failures: int                      # replay: documents whose request was not sent
    pending: list[TransferRequest]     # live: requests whose publish failed, for the next loop
    def __init__(self, publisher, service: str, replay: ReplayArgs | None,
                 client_factory=es_connections.get_connection, report_writer=write_reports)
    def documents_indexed(self, config, indexed: list[tuple[str, str]], unchanged: list[tuple[str, str]]) -> None   # never raises
    def retry_pending(self) -> None    # never raises
    def close(self) -> None            # closes the publisher
```

- [ ] **Step 1: Write the failing tests.**
  - `FakePublisher` records requests and raises the errors queued in its `fail` list. `FakeWriter` records reports and can raise. Both append `"publish"` and `"reports"` to a shared `events` list.
  - `config = SimpleNamespace(interface_name="ITF_A", model_name="ProductA", archive_rule=ArchiveRule.default("archive-a"))`.
  - `REPLAY = ReplayArgs(["ITF_A"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", "op-1", 3, requested_by="alice")`.

```python
def test_live_requests_name_the_created_and_updated_documents():
    live.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [("d3", "i0")])
    assert publisher.requests == [TransferRequest("svc", "ITF_A", (("d1", "i1"), ("d2", "i1")), "archive-a", None, "skip",
                                                  False, "", "", "live")]

def test_replay_requests_name_every_extracted_document():
    replay.documents_indexed(config, [("d1", "i1")], [("d3", "i0")])
    [request] = publisher.requests
    assert (request.documents, request.origin, request.operation_id, request.requested_by) == (
        (("d1", "i1"), ("d3", "i0")), "recollect", "op-1", "alice")

def test_requests_carry_at_most_ten_documents():
    live.documents_indexed(config, [(f"d{n}", "i1") for n in range(25)], [])
    assert [len(request.documents) for request in publisher.requests] == [10, 10, 5]

def test_recollect_choice_is_carried():     # archive_rule = replace(default, duplicates="replace_if_changed", backup_version=4, force=True)
    assert (request.duplicates, request.backup_version, request.force) == ("replace_if_changed", 4, True)

def test_operation_writes_one_requested_report_per_product_before_publishing(fake_opensearch):   # raw d1, d3 in the fake
    replay.documents_indexed(config, [("d1", "i1")], [("d3", "i0")])
    assert events == ["reports", "publish"]
    assert [(r.status, r.product_name, r.operation_id) for r in writer.reports] == [
        ("REQUESTED", "P1.zip", "op-1"), ("REQUESTED", "P3.zip", "op-1")]

def test_operation_reports_a_missing_document_as_requested_too(fake_opensearch):    # raw d1 only; d9 is not indexed
    replay.documents_indexed(config, [("d1", "i1"), ("d9", "i1")], [])
    assert events == ["reports", "publish"] and len(publisher.requests) == 1
    assert [(r.status, r.product_name, r.details) for r in writer.reports] == [
        ("REQUESTED", "P1.zip", ""), ("REQUESTED", "", "document d9 not found in i1")]

def test_replay_without_operation_id_writes_no_report():    # ReplayArgs suffix ""
    assert writer.reports == [] and len(publisher.requests) == 1

def test_failed_publish_in_replay_is_counted():
    publisher.fail = [TransferPublishError("no queue")]
    replay.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [])
    assert (replay.failures, replay.pending) == (2, [])

def test_failed_report_write_counts_and_publishes_nothing():
    writer.fail = ArchiveReportError("down")
    replay.documents_indexed(config, [("d1", "i1"), ("d2", "i1")], [])
    assert (replay.failures, publisher.requests) == (2, [])

def test_failed_live_publish_is_kept_for_the_next_loop():
    publisher.fail = [TransferPublishError("down")]
    live.documents_indexed(config, [("d1", "i1")], [])
    assert len(live.pending) == 1 and publisher.requests == []
    live.retry_pending()
    assert live.pending == [] and len(publisher.requests) == 1

def test_pending_requests_are_bounded(monkeypatch, caplog):
    monkeypatch.setattr(requester_module, "MAX_PENDING_REQUESTS", 2)
    publisher.fail = [TransferPublishError("down")] * 3
    for name in ("d1", "d2", "d3"):
        live.documents_indexed(config, [(name, "i1")], [])
    assert [request.documents for request in live.pending] == [(("d2", "i1"),), (("d3", "i1"),)]
    assert "dropped" in caplog.text
```

```python
# tests/test_action_iterator_extra_fields.py, reusing build_iterator, DeletionRow and existing_row
def test_unchanged_document_keeps_its_stored_index():
    iterator = build_iterator([])
    iterator.config.model = DeletionRow
    extract = {"product_name": "PRODUCT_A", "attachment_id": "10400", "reportName": "OMCS-1234_LTA_Werum_DelList.csv"}
    iterator.process_document("PRODUCT_A", extract, existing_row("PRODUCT_A", "10400"))
    assert iterator.unmodified_documents == [("PRODUCT_A", "raw-data-product-deletion-static")]
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_requester.py tests/test_action_iterator_extra_fields.py -v`. Expected: FAIL (module missing, `AttributeError: unmodified_documents`).
- [ ] **Step 3: Implement `TransferRequester` and `unmodified_documents`.**
  - `unmodified_documents` appends `(document_id, document.meta.index)` next to `unmodified_ids.append`.
  - Documents are de-duplicated in order, then cut into batches of `REQUEST_BATCH_SIZE`.
  - The rule's `backup`, `backup_version`, `duplicates` and `force` fill each request.
  - Live: `origin "live"`, empty operation and requester. A failed publish appends to `pending`; above `MAX_PENDING_REQUESTS`, the oldest request is dropped with a CRITICAL log: `dropped <n> transfer requests of <interface>: their products will show as gaps`.
  - Replay: `origin "recollect"`, `operation_id = replay.suffix or ""`, `requested_by` from plan 2's field.
  - With an operation id, each batch is handled in order: `read_products`, then `requested_reports` over the products found followed by `missing_product(document)` for each document not found (so every document of the request has its `REQUESTED` report and plan 4's totals add up when the stage reports the missing ones KO), then `report_writer`, then `publish`. Without an operation id nothing is written, missing documents included.
  - A failed report write or publish adds the batch size to `failures`, is logged at ERROR, and is never retried in replay.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/model.py src/maas_collector/rawdata/transfer/requester.py tests/test_action_iterator_extra_fields.py tests/transfer/test_transfer_requester.py
git commit -m "feat: :sparkles: Turn indexed documents into transfer requests" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6a: The collectors publish their transfer requests (C2 wiring)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/filecollector.py`:
  - the exit codes (`:52-56`);
  - `__init__` (`:302-368`) and `load_config` (`:387-444`);
  - `extract_from_file` (`:616-732`);
  - `run` (`:901-977`) and `post_loop` (`:1139-1163`).
- Modify: `src/maas_collector/rawdata/cli/odata.py:47-48` (as plan 1 Task 9 left it)
- Modify: `docs/archive.md` ("Transfer requests"), `docs/replay.md` (exit code), `CHANGELOG.md`
- Test: `tests/transfer/test_transfer_collector.py`; `tests/test_interface_isolation.py` (`:215-230`) and every other test that patches `ODataCollector` and calls `odata_collector_main` (`grep -rn odata_collector_main tests/`): set `collector.transfer_failures = 0` on their `Mock` collectors, as plan 1 did for `backup_failures`, otherwise the `Mock` attribute is truthy and the CLI exits

**Interfaces:**
- Consumes:
  - Task 5: `TransferRequester`;
  - Task 1: `TransferPublisher`, `transfer_connection`;
  - Task 2: `config.archive_rule`;
  - plan 1: `BACKUP_FAILED_EXIT_CODE`.
- Produces:

```python
TRANSFER_FAILED_EXIT_CODE: int     # the first code from 69 up that is none of CONFIG_CHANGED_EXIT_CODE, REPLAY_FAILED_EXIT_CODE, BACKUP_FAILED_EXIT_CODE
FileCollector.transfer_service -> str                    # property: args.config_store.service, "" without it
FileCollector.transfer_requester -> TransferRequester    # property, built on first use into self._transfer_requester (None before)
FileCollector.transfer_failures -> int                   # property: the requester's failures, 0 when never built
# the child's last line on that exit: "transfer requests failed: <count> documents"
```

- [ ] **Step 1: Write the failing tests.**
  - The collector under test is `FileCollector(CollectorArgs())` with `_healthcheck`, `_messenger` and `_transfer_requester` replaced by `Mock()`.
  - `config = SimpleNamespace(model=SimpleNamespace(__name__="ProductA"), archive_rule=ArchiveRule.default("archive-a"), backup=None, routing_key="k", interface_name="ITF_A")`.
  - `parallel_bulk` is patched to yield `created` for `("d1", "i1")` and `updated` for `("d2", "i1")`.
  - `ActionIterator` is patched with a stub whose `unmodified_ids == ["d3"]`, `unmodified_documents == [("d3", "i0")]`, `document_count == 2`, `action_iterator_errors == []`.

```python
def test_created_updated_and_unchanged_documents_go_to_the_requester():
    collector.extract_from_file("/tmp/page.json", config)
    collector._transfer_requester.documents_indexed.assert_called_once_with(config, [("d1", "i1"), ("d2", "i1")], [("d3", "i0")])

def test_entry_without_rule_builds_no_requester():
    collector._transfer_requester = None
    collector.extract_from_file("/tmp/page.json", SimpleNamespace(**vars(config) | {"archive_rule": None}))
    assert collector._transfer_requester is None

def test_unchanged_documents_get_no_product_message():
    collector.extract_from_file("/tmp/page.json", config)
    assert [call.args[1] for call in collector._messenger.handle_message.call_args_list] == ["d1", "d2"]

def test_archive_rule_without_service_fails_at_load(tmp_path):      # an odata entry with "archive" in tmp_path/conf
    with pytest.raises(ValueError, match="CONFIG_SERVICE"):
        ODataCollector(CollectorArgs(rawdata_config_dir=str(tmp_path / "conf")), ODataConfiguration()).load_config()

def test_each_loop_retries_the_pending_requests_then_closes():      # watch_period 0, loop_body patched
    collector.run(["x"])
    collector._transfer_requester.retry_pending.assert_called_once()
    collector._transfer_requester.close.assert_called()

def test_odata_cli_exits_on_transfer_failures(capsys):    # setup/run patched; run sets collector._transfer_requester = Mock(failures=3)
    with pytest.raises(SystemExit) as exit_info:
        odata_collector_main(REPLAY_ARGV)
    assert exit_info.value.code == TRANSFER_FAILED_EXIT_CODE
    assert capsys.readouterr().out.strip().splitlines()[-1] == "transfer requests failed: 3 documents"

def test_a_failed_replay_keeps_exit_code_1():    # replay_failures ["ITF_A"], 3 transfer failures and backup_failures {"archive-a": 2}
    assert exit_info.value.code == REPLAY_FAILED_EXIT_CODE
    assert capsys.readouterr().out.strip().splitlines()[-2:] == ["backup failed: 2 files to archive-a", "transfer requests failed: 3 documents"]

def test_transfer_and_backup_failures_print_both_lines_and_the_transfer_code_wins(capsys):   # replay_failures [], 3 transfer failures, backup_failures {"archive-a": 2}
    assert exit_info.value.code == TRANSFER_FAILED_EXIT_CODE
    assert capsys.readouterr().out.strip().splitlines()[-2:] == ["backup failed: 2 files to archive-a", "transfer requests failed: 3 documents"]

def test_backup_failure_alone_keeps_plan_1_code(capsys):     # no transfer failure, backup_failures {"archive-a": 2}
    assert exit_info.value.code == BACKUP_FAILED_EXIT_CODE
    assert capsys.readouterr().out.strip().splitlines()[-1] == "backup failed: 2 files to archive-a"

def test_exit_codes_are_distinct():
    assert len({CONFIG_CHANGED_EXIT_CODE, REPLAY_FAILED_EXIT_CODE, BACKUP_FAILED_EXIT_CODE, TRANSFER_FAILED_EXIT_CODE}) == 4
    assert TRANSFER_FAILED_EXIT_CODE >= 69
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_collector.py -v`. Expected: FAIL (`ImportError: TRANSFER_FAILED_EXIT_CODE`).
- [ ] **Step 3: Wire the requester into the collectors.**
  - **`extract_from_file`:**
    - Take `rule = getattr(config, "archive_rule", None)`.
    - When the rule is set, collect `(document_id, index)` in the created and updated branches.
    - In `finally`, after the `force_message` block and before the backup, call `self.transfer_requester.documents_indexed(config, indexed, self._action_iterator.unmodified_documents)`, only when the rule is set.
  - **`transfer_requester`:** imports `amqp` and `requester` inside the property, so the collector modules load no archive code and no cycle can go through plan 2's modules. It builds `TransferRequester(TransferPublisher(lambda: transfer_connection(self.args.amqp_url)), self.transfer_service, self.args.replay)`.
  - **`load_config`:** after the configurations are built, if `transfer_service` is empty and an enabled configuration has an `archive_rule`, raise `ValueError("archive rules of <interfaces> need the collector's service: set CONFIG_SERVICE or --config-service")`.
  - **`run` and `post_loop`:** when the requester is built, `run` calls `retry_pending()` after `refresh_credentials()`, and `post_loop` adds its `close` to the callables.
  - **odata CLI:** end plan 1's block (the one after `collector.run`) so that both failure lines are printed before any exit, backup line first, transfer line last (`flush=True`):
    1. when `collector.backup_failures`, print `collector.backup_failure_line()` (plan 1 already prints it when `replay_failures` wins);
    2. when `collector.transfer_failures`, print `transfer requests failed: <n> documents`;
    3. then exit with the first that applies: `REPLAY_FAILED_EXIT_CODE` (`replay_failures`), `TRANSFER_FAILED_EXIT_CODE`, `BACKUP_FAILED_EXIT_CODE`.

    The transfer code wins over the backup code: a product whose request was never sent is missing from the archive, and the operator's `describe_failure` (Task 6b) names the backup count too, so neither is lost. The jirax CLI is unchanged (it never archives).
  - **`TRANSFER_FAILED_EXIT_CODE`:** the first code from 69 (`EX_UNAVAILABLE`) up that differs from the three other codes, the value Task 0 Step 2 printed for plan 1.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Write the docs.**
  - `docs/archive.md`, "Transfer requests": live vs replay; batches of 10; mandatory and confirmed publishing; `REQUESTED` reports for operations; the collector needs its service (`CONFIG_SERVICE`); a replay exits with `TRANSFER_FAILED_EXIT_CODE` when requests could not be sent (write the constant's value, not a number copied from this plan).
  - `docs/replay.md`: that exit code and its last line, and the rule when several failures happen (both lines printed, backup then transfer; the code is 1 with a failed interface, else the transfer code, else the backup code).
  - The CHANGELOG bullets.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/rawdata/collector/filecollector.py src/maas_collector/rawdata/cli/odata.py tests/transfer/test_transfer_collector.py tests/test_interface_isolation.py tests/test_enabled_flag.py docs CHANGELOG.md
git commit -m "feat: :sparkles: Publish transfer requests from the collectors" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 6b: The operator's replay child knows its service and the transfer failure (C2 wiring)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/replay.py:211-238` (`build_command`: the `service` parameter only), `src/maas_collector/rawdata/operator/handler.py:89-113` (`command`, `describe_failure`)
- Modify: `docs/operator_worker.md` (the child's `--config-service`), `CHANGELOG.md`
- Test: `tests/operator/test_replay.py`, `tests/operator/test_handler.py`

**Interfaces:**
- Consumes: Task 6a's `TRANSFER_FAILED_EXIT_CODE` and its last line; plan 1's `BACKUP_FAILURE_LINE`; plan 2's `build_command` item `--replay-requested-by=<request.requested_by>` (not touched here).
- Produces:

```python
build_command(module, conf_dir, request, healthcheck_port, replay_retry, service: str = "") -> list[str]
    # appends "--config-service=<service>" as its last item when set; "--replay-requested-by=..." is plan 2's and stays
CollectorReplayHandler.describe_failure(code, tail)   # gains the transfer exit code
```

- [ ] **Step 1: Write the failing tests.**

```python
# tests/operator/test_replay.py
def test_command_names_the_service():
    assert build_command("odata", "/c", req(), 18081, 3, service="svc")[-1] == "--config-service=svc"

def test_command_names_the_requester():      # plan 2's behaviour: it passes before this task; if it fails, plan 2 is incomplete, do not re-implement it here
    assert "--replay-requested-by=alice" in build_command("odata", "/c", req(requested_by="alice"), 18081, 3)

# tests/operator/test_handler.py
def test_the_child_is_given_the_service_of_the_handler(store):
    command, _env = CollectorReplayHandler(store, "svc", ("short", "long"), 18081, 3).command(Prepared("/copy", "odata"), req(), "short")
    assert "--config-service=svc" in command

def test_describe_failure_of_unsent_transfer_requests():
    text = CollectorReplayHandler.describe_failure(TRANSFER_FAILED_EXIT_CODE, "line\ntransfer requests failed: 3 documents")
    assert text.splitlines()[0] == "3 documents could not be sent to the transfer stage"

def test_describe_failure_without_the_count_line():
    assert CollectorReplayHandler.describe_failure(TRANSFER_FAILED_EXIT_CODE, "line").splitlines()[0] == "transfer requests could not all be sent"

def test_describe_failure_of_transfer_and_backup_failures_names_both():
    tail = "backup failed: 2 files to archive-a\ntransfer requests failed: 3 documents\nINFO exiting"
    assert CollectorReplayHandler.describe_failure(TRANSFER_FAILED_EXIT_CODE, tail).splitlines()[:2] == [
        "3 documents could not be sent to the transfer stage", "2 files could not be backed up to archive-a"]
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/operator/test_replay.py tests/operator/test_handler.py -v`. Expected: the new tests FAIL (`TypeError: ... 'service'`, `ImportError: TRANSFER_FAILED_EXIT_CODE` or the old `describe_failure` text); `test_command_names_the_requester` already passes.
- [ ] **Step 3: Implement.**
  - **`build_command`:** appends `f"--config-service={service}"` as a single item, like `--replay-suffix`, after plan 2's items. `CollectorReplayHandler.command` passes `service=self.service`. The child still drops `CONFIG_BACKEND`, so `--config-service` only names its transfer service.
  - **`describe_failure`:** reads the count from the tail's last `transfer requests failed: <n> documents` line and returns `"<n> documents could not be sent to the transfer stage"`, then the tail. Without such a line, the first line is `"transfer requests could not all be sent"`. When the tail also holds a `BACKUP_FAILURE_LINE` match (searched from the end), the second line is plan 1's `"<n> files could not be backed up to <backup>"`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Write the docs.** `docs/operator_worker.md`: the child gets `--config-service=<service>`, and the failure text when transfers and backups both failed. CHANGELOG bullet.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/rawdata/operator tests/operator docs/operator_worker.md CHANGELOG.md
git commit -m "feat: :sparkles: Give the replay child its service and report transfer failures" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 7: Product download (C1 download, C3 step 3)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/download.py`
- Test: `tests/transfer/test_transfer_download.py`

**Interfaces:**
- Consumes:
  - Task 2: `ArchiveRule` (`download_url`, `quote_id`, `new_checksum`);
  - `build_authentication(config.auth_method, config, session)` (`collector/http/authentication.py:340`);
  - `HttpMixin.create_http_session(config)` (`collector/httpmixin.py:24`);
  - the config's `get_config_product_url()`, `odata_entity_location`, `odata_entities`, `interface_name`.
- Produces:

```python
class TransferError(Exception): retryable = False
class TransientTransferError(TransferError): retryable = True
class PermanentTransferError(TransferError): ...
class TransferStopped(TransferError): ...          # the stage is stopping: not a failure, nothing is reported
@dataclass(frozen=True)
class Downloaded:
    path: str
    size: int
    md5: str           # lower-case hex
    checksum: str      # lower-case hex of the rule's checksum_algorithm
PERMANENT_STATUS = frozenset({400, 401, 403, 404, 405, 410, 422})   # 401/403 after one token refresh
MAX_REDIRECTS = 3
MAX_RETRY_DELAY = 60.0
def product_url(config, rule: ArchiveRule, product_id: str) -> str
def safe_file_name(name) -> str                    # PermanentTransferError unless a bare file name
def with_retries(action: Callable[[], T], max_attempts: int, delay: float, sleep: Callable[[float], None] = time.sleep) -> T
class ProductDownloader:
    def __init__(self, config, rule: ArchiveRule, session=None, authentication=None, *, chunk_size: int = 1 << 20,
                 connect_timeout: float = 10.0, read_timeout: float = 60.0, max_seconds: float = 900.0,
                 should_stop: Callable[[], bool] = lambda: False, tick: Callable[[], None] = lambda: None)
    def download(self, product: dict, directory: str) -> Downloaded   # one attempt, into <directory>/<safe product name>
```

- [ ] **Step 1: Write the failing tests.**
  - `FakeSession.get(url, **kwargs)` records `(url, headers, kwargs)` and returns the next scripted `FakeResponse(status, headers, chunks)`.
  - `FakeAuthentication.get_headers()` returns `{"Authorization": f"Bearer t{n}"}`, where `n` counts the expirations of `access_token_expiration_date`.
  - `CONFIG = SimpleNamespace(interface_name="ITF_A", auth_method="OAuth", odata_entity_location="/odata/v1/", odata_entities="Products", get_config_product_url=lambda: "https://catalogue.example")`.
  - `CONTENT = b"x" * 3000`, `MD5 = hashlib.md5(CONTENT).hexdigest()`.
  - `PRODUCT = {"product_id": "id-1", "product_name": "P1.zip", "size": 3000, "checksum": MD5, "publication_date": None, "document_id": "d1", "document_index": "i1"}`.

```python
def test_default_url_quotes_the_id():
    assert product_url(CONFIG, ArchiveRule.default("archive-a"), "id-1") == "https://catalogue.example/odata/v1/Products('id-1')/$value"

def test_template_url_on_another_host_with_an_unquoted_id():
    rule = replace(ArchiveRule.default("archive-a"), download_url="https://bytes.example/odata/v1/Products({id})/$value", quote_id=False)
    assert product_url(CONFIG, rule, "id-1") == "https://bytes.example/odata/v1/Products(id-1)/$value"

def test_a_quote_in_an_id_is_escaped():
    assert product_url(CONFIG, ArchiveRule.default("archive-a"), "a'b").endswith("Products('a%27%27b')/$value")

def test_streams_the_product_and_sends_the_token_to_the_configured_host(tmp_path):   # 200, CONTENT in 3 chunks
    assert downloader.download(PRODUCT, str(tmp_path)) == Downloaded(str(tmp_path / "P1.zip"), 3000, MD5, MD5)
    url, headers, kwargs = session.calls[0]
    assert (url, headers["Authorization"], headers["Accept-Encoding"]) == (
        "https://catalogue.example/odata/v1/Products('id-1')/$value", "Bearer t0", "identity")
    assert (kwargs["stream"], kwargs["allow_redirects"]) == (True, False)

def test_uppercase_catalogue_checksum_is_the_same_md5(tmp_path):           # Review Focus 1
    downloader.download(PRODUCT | {"checksum": f" {MD5.upper()} "}, str(tmp_path))

def test_unknown_size_and_checksum_are_not_checked(tmp_path):               # Review Focus 5
    downloader.download(PRODUCT | {"size": None, "checksum": None}, str(tmp_path))

def test_size_mismatch_is_transient(tmp_path):
    with pytest.raises(TransientTransferError, match="3000 bytes"):
        downloader.download(PRODUCT | {"size": 4000}, str(tmp_path))

def test_checksum_mismatch_is_transient(tmp_path):
    with pytest.raises(TransientTransferError, match="checksum"):
        downloader.download(PRODUCT | {"checksum": "0" * 32}, str(tmp_path))

@pytest.mark.parametrize("status", [400, 404, 410, 422])
def test_client_errors_are_permanent(status, tmp_path):           # the session answers `status` once
    with pytest.raises(PermanentTransferError, match=str(status)):
        downloader.download(PRODUCT, str(tmp_path))
    assert len(session.calls) == 1

@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_server_errors_are_transient(status, tmp_path):          # the session answers `status` once
    with pytest.raises(TransientTransferError, match=str(status)):
        downloader.download(PRODUCT, str(tmp_path))

def test_connection_error_is_transient(tmp_path):                 # session raises requests.ConnectionError("reset")
    with pytest.raises(TransientTransferError, match="reset"):
        downloader.download(PRODUCT, str(tmp_path))

def test_401_refreshes_the_token_once(tmp_path):                  # 401, then 200
    assert [headers["Authorization"] for _url, headers, _kw in session.calls] == ["Bearer t0", "Bearer t1"]

def test_a_redirect_is_followed_without_the_token(tmp_path):      # 302 Location https://storage.example/o?sig=1, then 200
    assert session.calls[1][0] == "https://storage.example/o?sig=1" and "Authorization" not in session.calls[1][1]

def test_401_after_a_redirect_names_the_host_to_configure(tmp_path):   # 302 to https://bytes.example/x, then 401
    with pytest.raises(PermanentTransferError, match=r"bytes\.example.*download\.url"):
        downloader.download(PRODUCT, str(tmp_path))

def test_redirects_are_bounded(tmp_path):                          # 302 four times
    with pytest.raises(PermanentTransferError, match="redirects"):
        downloader.download(PRODUCT, str(tmp_path))

@pytest.mark.parametrize("name", [None, "", ".", "..", "../P1.zip", "a/P1.zip", "a\\P1.zip", "P1\x00.zip"])
def test_unsafe_product_name_is_permanent(name, tmp_path):         # Review Focus 4
    with pytest.raises(PermanentTransferError):
        downloader.download(PRODUCT | {"product_name": name}, str(tmp_path))
    assert session.calls == [] and os.listdir(tmp_path) == []

@pytest.mark.parametrize("product_id", [None, ""])
def test_missing_product_id_is_permanent(product_id, tmp_path):
    with pytest.raises(PermanentTransferError, match="product_id"):
        downloader.download(PRODUCT | {"product_id": product_id}, str(tmp_path))
    assert session.calls == []

def test_stop_between_chunks(tmp_path):              # should_stop becomes true after the first chunk
    with pytest.raises(TransferStopped):
        downloader.download(PRODUCT, str(tmp_path))
    assert ticks == 1

def test_download_longer_than_max_seconds_is_transient(tmp_path):   # max_seconds=10; time.monotonic returns 0, then 11 after the first chunk
    with pytest.raises(TransientTransferError, match="10"):
        downloader.download(PRODUCT, str(tmp_path))

def test_with_retries_backs_off_on_transient_errors():
    assert with_retries(scripted(TransientTransferError("a"), TransientTransferError("b"), "ok"), 3, 5.0, sleeps.append) == "ok"
    assert sleeps == [5.0, 10.0]

def test_with_retries_raises_the_last_error_after_max_attempts():
    with pytest.raises(TransientTransferError, match="c"):       # scripted(Transient("a"), Transient("b"), Transient("c"))
        with_retries(scripted(TransientTransferError("a"), TransientTransferError("b"), TransientTransferError("c")), 3, 5.0, sleeps.append)
    assert sleeps == [5.0, 10.0]

@pytest.mark.parametrize("error", [PermanentTransferError("gone"), TransferStopped("stop")])
def test_with_retries_does_not_retry_permanent_errors_nor_a_stop(error):
    action = scripted(error, "never")
    with pytest.raises(type(error)):
        with_retries(action, 3, 5.0, sleeps.append)
    assert action.calls == 1 and sleeps == []
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_download.py -v`. Expected: FAIL with `ModuleNotFoundError ... transfer.download`.
- [ ] **Step 3: Implement `ProductDownloader`, `product_url` and `with_retries`.**
  - **Reference (do not move it as it is):** `/home/aagazzi/work/download-manager/maas-engine-download-manager/src/maas_download_manager/`. Take the error classification of `transfer/errors.py` (here `TransferError`, `TransientTransferError`, `PermanentTransferError`) and the streamed write with its size and checksum checks of `transfer/source.py`. Leave the maas-engine base, the `dm-download` documents and the source-specific OAuth: authentication here is the collector's own (`build_authentication`).
  - **`product_url`:** the template is `rule.download_url`, or `f"{config.get_config_product_url()}{config.odata_entity_location}{config.odata_entities}({{id}})/$value"`. Replace `{id}` with `str.replace`, not `format`. The id is `quote(product_id.replace("'", "''"), safe="")`, wrapped in quotes when `quote_id`.
  - **Request headers:** authentication headers plus `Accept-Encoding: identity`, with `stream=True`, `allow_redirects=False` and `timeout=(connect_timeout, read_timeout)`.
  - **Token refresh:** on 401/403 from the configured URL, expire the token and try once more. To expire it, set `access_token_expiration_date` to now when the authentication object has it.
  - **Redirects:** on a 3xx, follow `urljoin(url, Location)` with `Accept-Encoding` only, never the authentication headers, at most `MAX_REDIRECTS` hops. A 401/403 after a redirect is `PermanentTransferError(f"... {host} answered {status} without the token: set the archive rule's download.url to that host")`.
  - **Status classes:** statuses in `PERMANENT_STATUS` are permanent. Other 4xx except 408 and 429 are permanent; 408, 429, 5xx and `requests.RequestException` are transient.
  - **Writing:** stream to `<directory>/<safe name>`, updating the MD5 and `rule.new_checksum()` per chunk. Call `tick()` and check `should_stop()` (→ `TransferStopped`) and `max_seconds` (→ transient) per chunk.
  - **Checks:** compare the size when `size` is set, and the checksum after `.strip().lower()`.
  - **`with_retries`:** retries only `TransientTransferError`, sleeping `min(delay * 2 ** (attempt - 1), MAX_RETRY_DELAY)`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/download.py tests/transfer/test_transfer_download.py
git commit -m "feat: :sparkles: Download archived products as a stream" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Duplicates and conflicts (C1, C4)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/duplicates.py`, `src/maas_collector/rawdata/transfer/conflicts.py`
- Test: `tests/transfer/test_archive_duplicates.py`, `tests/transfer/test_archive_conflicts.py`

**Interfaces:**
- Consumes: Task 4's `archived_state` output (report sources with `status`, `catalogue_checksum`, `written_md5`, `start_time`, `end_time`); `datetime_to_zulu` (maas_model).
- Produces:

```python
@dataclass(frozen=True)
class Decision:
    transfer: tuple[str, ...] = ()       # destinations to write
    skip: tuple[str, ...] = ()           # destinations left as they are
    conflict: bool = False               # a conflict: nothing is written
    same_bytes_as: str = ""              # MD5 the downloaded bytes must have before a missing mirror is filled
    archived: frozenset = frozenset()    # destinations whose last report is OK or REPLACED: writing there is REPLACED
    reference: dict | None = None        # the archived report compared with
def same_checksum(a: str | None, b: str | None) -> bool | None   # None when either is blank; case and blanks ignored
def decide(destinations: Sequence[str], state: dict[str, dict], duplicates: str, force: bool, checksum: str | None) -> Decision
CONFLICT_INDEX = "maas-archive-conflict"
OPEN, RESOLVED = "OPEN", "RESOLVED"
def conflict_id(backup: str, interface: str, product_name: str) -> str   # md5 hex of json.dumps([backup, interface, product_name])
def open_conflict(client, backup: str, interface: str, product: dict, reference: dict, new_checksum: str, now: datetime.datetime) -> None
def resolve_conflict(client, backup: str, interface: str, product_name: str) -> bool   # True when an OPEN conflict became RESOLVED
```

`decide` uses the following terms:
- **archived** (A): the destinations whose last report is `OK`/`REPLACED`;
- **missing**: the others;
- **reference**: A's report with the latest `start_time`;
- **changed**: `same_checksum(checksum, reference["catalogue_checksum"]) is False`.

State destinations that are not in `destinations` are ignored. The decision table:

| case | transfer | skip | conflict | same_bytes_as |
| --- | --- | --- | --- | --- |
| `force` | all | — | no | "" |
| A empty | all | — | no | "" |
| `replace_if_changed` and changed | all | — | no | "" |
| `skip` and changed | — | — | yes | "" |
| otherwise (same, or a checksum unknown) | missing | A | no | the reference's `written_md5` |

- [ ] **Step 1: Write the failing tests.**

```python
OK1 = {"status": "OK", "catalogue_checksum": "aa", "written_md5": "m1", "start_time": "2026-10-01T00:00:00.000Z", "end_time": "2026-10-01T00:01:00.000Z"}
KO1 = OK1 | {"status": "KO"}

@pytest.mark.parametrize("destinations, state, duplicates, force, checksum, expected", [
    pytest.param(["b1", "b2"], {}, "skip", False, "aa", dict(transfer=("b1", "b2")), id="new"),
    pytest.param(["b1"], {"b1": OK1}, "skip", False, "aa", dict(skip=("b1",), same_bytes_as="m1"), id="same-skip"),
    pytest.param(["b1"], {"b1": OK1}, "skip", False, " AA ", dict(skip=("b1",), same_bytes_as="m1"), id="same-upper"),
    pytest.param(["b1"], {"b1": OK1}, "skip", False, "bb", dict(conflict=True), id="changed-skip"),
    pytest.param(["b1"], {"b1": OK1}, "replace_if_changed", False, "bb", dict(transfer=("b1",)), id="changed-replace"),
    pytest.param(["b1"], {"b1": OK1}, "replace_if_changed", False, None, dict(skip=("b1",), same_bytes_as="m1"), id="no-checksum-replace"),
    pytest.param(["b1"], {"b1": OK1 | {"catalogue_checksum": ""}}, "skip", False, "bb", dict(skip=("b1",), same_bytes_as="m1"), id="no-checksum-archived"),
    pytest.param(["b1"], {"b1": OK1}, "skip", True, "bb", dict(transfer=("b1",)), id="forced"),
    pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, "aa", dict(transfer=("b2",), skip=("b1",), same_bytes_as="m1"), id="mirror-same"),
    pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, None, dict(transfer=("b2",), skip=("b1",), same_bytes_as="m1"), id="no-checksum-mirror"),
    pytest.param(["b1", "b2"], {"b1": OK1}, "skip", False, "bb", dict(conflict=True), id="mirror-changed"),
    pytest.param(["b1"], {"b1": KO1}, "skip", False, "aa", dict(transfer=("b1",)), id="last-ko"),
    pytest.param(["b1", "b2"], {"b1": OK1}, "replace_if_changed", False, "bb", dict(transfer=("b1", "b2")), id="changed-replace-mirror"),
])
def test_decide(destinations, state, duplicates, force, checksum, expected):
    decision = decide(destinations, state, duplicates, force, checksum)
    defaults = dict(transfer=(), skip=(), conflict=False, same_bytes_as="")
    assert {key: getattr(decision, key) for key in defaults} == defaults | expected

def test_writing_an_archived_destination_is_a_replacement():
    assert decide(["b1", "b2"], {"b1": OK1}, "skip", True, "aa").archived == {"b1"}

def test_the_latest_archived_report_is_the_reference():
    late = OK1 | {"catalogue_checksum": "cc", "start_time": "2026-10-03T00:00:00.000Z"}
    assert decide(["b1", "b2"], {"b1": OK1, "b2": late}, "skip", False, "cc").reference == late
```

```python
# tests/transfer/test_archive_conflicts.py
PRODUCT = {"product_id": "id-1", "product_name": "P1.zip", "document_index": "i1", "publication_date": "2026-03-04T10:00:00.000Z"}
T1 = datetime.datetime(2026, 10, 1, tzinfo=datetime.UTC); T2 = T1 + datetime.timedelta(days=1)

def test_conflict_id_is_stable_and_distinct():
    assert conflict_id("archive-a", "ITF_A", "P1.zip") == conflict_id("archive-a", "ITF_A", "P1.zip") != conflict_id("archive-a", "ITF_A", "P2.zip")

def test_open_conflict_writes_the_document(fake_opensearch):
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    assert stored() == {"backup_name": "archive-a", "interface_name": "ITF_A", "product_name": "P1.zip", "product_id": "id-1",
                        "document_index": "i1", "status": "OPEN", "archived_md5": "m1", "archived_at": "2026-10-01T00:01:00.000Z",
                        "catalogue_checksum": "bb", "publication_date": "2026-03-04T10:00:00.000Z",
                        "first_seen": "2026-10-01T00:00:00.000Z", "last_seen": "2026-10-01T00:00:00.000Z"}

def test_a_conflict_seen_again_keeps_its_first_seen(fake_opensearch):
    open_conflict(..., "bb", T1); open_conflict(..., "dd", T2)
    assert (stored()["first_seen"], stored()["last_seen"], stored()["catalogue_checksum"]) == (
        "2026-10-01T00:00:00.000Z", "2026-10-02T00:00:00.000Z", "dd")

def test_a_resolved_conflict_seen_again_reopens_as_new(fake_opensearch):
    # open at T1, resolve, open at T2
    assert (stored()["status"], stored()["first_seen"]) == ("OPEN", "2026-10-02T00:00:00.000Z")

def test_resolve_answers_whether_it_resolved(fake_opensearch):
    assert resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip") is False
    open_conflict(fake_opensearch, "archive-a", "ITF_A", PRODUCT, OK1, "bb", T1)
    assert resolve_conflict(fake_opensearch, "archive-a", "ITF_A", "P1.zip") is True and stored()["status"] == "RESOLVED"
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_archive_duplicates.py tests/transfer/test_archive_conflicts.py -v`. Expected: FAIL with `ModuleNotFoundError`.
- [ ] **Step 3: Implement `decide`, `open_conflict` and `resolve_conflict`.**
  - `decide` follows the table above.
  - `open_conflict` reads the current document by `conflict_id`. It keeps `first_seen` when that document is `OPEN`, and writes the whole document with `client.index(..., refresh="wait_for")`. Dates use `datetime_to_zulu`.
  - `resolve_conflict` updates only `status`, with `refresh="wait_for"`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/duplicates.py src/maas_collector/rawdata/transfer/conflicts.py tests/transfer/test_archive_duplicates.py tests/transfer/test_archive_conflicts.py
git commit -m "feat: :sparkles: Decide archive duplicates and record conflicts" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 9: The backup a request names (C3)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/backups.py`
- Test: `tests/transfer/test_archive_backups.py`

**Interfaces:**
- Consumes:
  - plan 1: `backup_definitions`, `resolve_backup`, `SECRET_FIELDS`;
  - `ConfigStore.latest()` and `ConfigStore.get(path, version)` (`configstore.py:212`, `:195`) and `ConfigSource`; plan 2's `resolve_backup_version` is the model of the lookup (it returns a version, not the document, so this task has its own `find_backup_document`);
  - `instanciate_collector_backup` (`backup/__init__.py:13`);
  - `CredentialProvider.load(names)` and `.unreadable` (`credentialprovider.py:30`).
- Produces:

```python
class BackupUnavailable(ValueError): ...           # permanent: every product of the request ends KO
@dataclass(frozen=True)
class ResolvedBackup:
    name: str
    version: int | None
    block: dict                  # for instanciate_collector_backup, without any secret field
    credentials: str | None      # name of the credential entry holding the S3 keys
def find_backup_document(store: ConfigStore, service: str, name: str) -> dict
    # the one current document (store.latest(), not deleted, `service` in its services) whose content["backups"] defines `name`,
    # whatever its path, as plan 2's resolve_backup_version does; BackupUnavailable when none or several
def resolve_request_backup(sources: list[ConfigSource], store: ConfigStore | None, service: str,
                           name: str, version: int | None) -> ResolvedBackup
def backup_keys(provider, entry: str | None) -> dict    # {} without entry; else exactly the entry's SECRET_FIELDS
def build_sink(backup: ResolvedBackup, keys: dict, destinations: Sequence[str] | None = None,
               factory=instanciate_collector_backup)      # S3 buckets narrowed to destinations; keys merged into a copy
```

- [ ] **Step 1: Write the failing tests.**

```python
S3 = {"name": "archive-a", "type": "BackupS3", "directory": "archive", "buckets": ["b1", "b2"], "credentials": "backup-archive-a"}

def test_latest_definition_comes_from_the_loaded_sources():
    backup = resolve_request_backup([ConfigSource("backups/archive-a.json", {"backups": [S3]})], None, "svc", "archive-a", None)
    assert (backup.block["buckets"], backup.credentials, "credentials" in backup.block) == (["b1", "b2"], "backup-archive-a", False)

def test_pinned_version_is_read_from_the_store(fake_opensearch):
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": [S3 | {"directory": "v1"}]}, latest=False)
    add_config_document(fake_opensearch, "backups/archive-a.json", 2, ["svc"], {"backups": [S3]})
    assert resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1).block["directory"] == "v1"

def test_pinned_version_is_found_by_name_whatever_the_path(fake_opensearch):      # a backup put with cli.config under another path
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"], {"backups": [S3 | {"directory": "v1"}]}, latest=False)
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 2, ["svc"], {"backups": [S3]})
    assert resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1).block["directory"] == "v1"

def test_backup_defined_in_two_current_documents_is_refused(fake_opensearch):
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": [S3]})
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"], {"backups": [S3]})
    with pytest.raises(BackupUnavailable, match="several"):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)

def test_definition_of_another_service_is_not_a_candidate(fake_opensearch):
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["other"], {"backups": [S3]})
    add_config_document(fake_opensearch, "elsewhere/archive-a.json", 1, ["svc"], {"backups": [S3 | {"directory": "mine"}]})
    assert resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1).block["directory"] == "mine"

@pytest.mark.parametrize("latest_services, latest_deleted, version, word", [
    (["other"], False, 1, "tagged svc"), (["svc"], True, 1, "tagged svc"), (["svc"], False, 9, "version 9")])
def test_pinned_version_that_cannot_serve_is_refused(fake_opensearch, latest_services, latest_deleted, version, word):
    # version 1 is healthy: non-deleted and tagged ["svc"]; version 2 (the current document) is the parametrized one:
    # a tombstone, or tagged ["other"]; version 9 does not exist (the newest document is then healthy)
    add_config_document(fake_opensearch, "backups/archive-a.json", 1, ["svc"], {"backups": [S3]}, latest=False)
    add_config_document(fake_opensearch, "backups/archive-a.json", 2, latest_services, {"backups": [S3]}, deleted=latest_deleted)
    with pytest.raises(BackupUnavailable, match=word):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", version)

def test_backup_without_any_document_is_refused_when_pinned(fake_opensearch):
    with pytest.raises(BackupUnavailable, match="archive-a"):
        resolve_request_backup([], ConfigStore(fake_opensearch), "svc", "archive-a", 1)

def test_pinned_version_without_the_store_is_refused():
    with pytest.raises(BackupUnavailable, match="opensearch"):
        resolve_request_backup([], None, "svc", "archive-a", 1)

def test_unknown_backup_is_refused():
    with pytest.raises(BackupUnavailable, match="archive-b"):
        resolve_request_backup([], None, "svc", "archive-b", None)

def test_keys_are_the_two_s3_keys(): assert backup_keys(provider({"backup-archive-a": KEYS}), "backup-archive-a") == KEYS
def test_entry_with_another_field_is_refused_naming_it():
    with pytest.raises(BackupUnavailable, match="token"):
        backup_keys(provider({"backup-archive-a": KEYS | {"token": "x"}}), "backup-archive-a")

@pytest.mark.parametrize("make_provider, word", [
    (lambda: provider({}, unreadable={"backup-archive-a"}), "vault policy"),     # provider.unreadable names the entry
    (lambda: provider({}), "backup-archive-a"),                                  # the entry is absent
    (lambda: None, "no credential source")])                                     # FileCollector.credential_provider is None
def test_unreadable_or_absent_entry_is_refused(make_provider, word):
    with pytest.raises(BackupUnavailable, match=word):
        backup_keys(make_provider(), "backup-archive-a")

def test_sink_gets_the_keys_and_the_narrowed_buckets_on_a_copy():
    backup = resolve_request_backup([ConfigSource("b.json", {"backups": [S3]})], None, "svc", "archive-a", None)
    build_sink(backup, KEYS, ["b2"], factory=blocks.append)
    assert blocks[0]["buckets"] == ["b2"] and blocks[0]["s3_secret_key"] == "SECRET-1"
    assert "s3_secret_key" not in backup.block

def test_keys_are_never_logged(caplog):
    caplog.set_level(logging.DEBUG)
    backup = resolve_request_backup([ConfigSource("b.json", {"backups": [S3]})], None, "svc", "archive-a", None)
    build_sink(backup, KEYS, factory=lambda block: None)
    assert "SECRET-1" not in caplog.text and "SECRET-1" not in repr(backup)
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_archive_backups.py -v`. Expected: FAIL with `ModuleNotFoundError`.
- [ ] **Step 3: Implement backup resolution, keys and the sink.**
  - **Without a version:** `resolve_backup(name, backup_definitions(s.content for s in sources))`.
  - **With a version:** a tombstone or an untag is a newer version, because the store keeps one document per version (`configstore.py:195`). So find the current document by name, not by path (a backup published with `cli.config` can sit under any path, and only the backend checks that the name matches `backups/<name>.json`): `find_backup_document(store, service, name)` takes `store.latest()` and keeps the documents that are not deleted, carry `service` in their `services` and define `name` in `content["backups"]`. It raises `BackupUnavailable(f"backup {name} has no current definition tagged {service} (absent, deleted or untagged)")` for none and `BackupUnavailable(f"backup {name} is defined by several documents: {paths}")` for more than one. Then take the definition from `store.get(document["path"], version)["content"]` and refuse when that version is absent (`"version <n> of <name> does not exist"`) or does not define `name` (the `KeyError` of `resolve_backup`). Also refuse when `store` is `None` (the file backend has no versions, message naming `opensearch`).
  - `KeyError` and `ValueError` from plan 1 become `BackupUnavailable`.
  - `backup_keys` refuses any field outside `SECRET_FIELDS`, naming the field. It also refuses an entry that is missing or that the vault policy does not allow reading. With `provider is None` (`FileCollector.credential_provider` is `None` without a credential source, `credentialmixin.py:102`) and a non-`None` entry, it raises `BackupUnavailable("no credential source")`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/backups.py tests/transfer/test_archive_backups.py
git commit -m "feat: :sparkles: Resolve the backup a transfer request names" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10a: The transfer of one product (C3, C4 conflicts)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/stage.py` (`TransferSettings`, `RequestContext`, `RequestRefused`, `TransferStage` with `request_context` and `transfer_product`)
- Test: `tests/transfer/test_transfer_stage.py` (the fixture and the tests of this task; Task 10b adds its own to the same file)

**Reference (do not move it as it is):** the download manager's engine, under `/home/aagazzi/work/download-manager/maas-engine-download-manager/src/maas_download_manager/`.
- Take from it: the error classification of `transfer/errors.py` (transient: checksum and size mismatch, connection, 5xx; permanent: 4xx), the one-report-per-destination logic of `engines/reports/transfer.py`, and the report fields of `transfer/report.py`.
- Leave there: the maas-engine base (`DataEngine`), the `dm-download` documents and the source-specific authentication.

**Interfaces:**
- Consumes: Tasks 1-4, 7, 8 and 9, and these `FileCollector` members:
  - `load_config`, `get_configuration_by_interface_name` (`filecollector.py:580`, which raises `ValueError` on duplicates);
  - `disabled_interface_names`, `config_sources`, `credential_provider` (`None` without a credential source);
  - `should_stop_loop`, `_healthcheck`.
- Produces:

```python
@dataclass
class TransferSettings:
    max_attempts: int = 3            # download, upload per destination, report write
    retry_delay: float = 5.0         # doubled per attempt, at most 60 s
    max_download_seconds: float = 900.0
class RequestRefused(ValueError): ...               # the request cannot be served: every product ends KO
@dataclass
class RequestContext:
    request: TransferRequest
    config: ODataCollectorConfiguration
    rule: ArchiveRule                # the live rule's fields and download (ArchiveRule.default without one),
                                     # with the request's backup, backup_version, duplicates and force
    backup: ResolvedBackup
    keys: dict
    destinations: tuple[str, ...]    # build_sink(backup, keys).destinations
class TransferStage(FileCollector):
    CONFIG_CLASS = ODataCollectorConfiguration
    def __init__(self, args: CollectorArgs, settings: TransferSettings, publisher=None, *, sleep=time.sleep,
                 sink_factory=instanciate_collector_backup, downloader_factory=ProductDownloader,
                 report_writer=write_reports, take=None)
    service -> str                                       # property: args.config_store.service
    def get_configs(self, source) -> list                # its entries, built without "extractor"
    def request_context(self, request: TransferRequest) -> RequestContext   # RequestRefused
    def transfer_product(self, context: RequestContext, product: dict) -> list[BackupReport]   # TransferStopped propagates
```

**`request_context`**:
1. Resolve the entry with `get_configuration_by_interface_name`. Absent, disabled or ambiguous: `RequestRefused` naming the interface.
2. Build the rule with `dataclasses.replace(config.archive_rule or ArchiveRule.default(request.backup), backup=…, backup_version=…, duplicates=…, force=…)`.
3. Resolve the backup (`resolve_request_backup`, with the `ConfigStore` described below), its keys (`backup_keys`) and its destinations. `BackupUnavailable` becomes `RequestRefused` with the same message.

**`transfer_product`**:
1. `safe_file_name`; an unsafe name → KO per destination.
2. `decide(destinations, archived_state(...), rule.duplicates, rule.force, product["checksum"])`.
3. A conflict → `open_conflict`, plus `SKIPPED` reports with `error_type` `CONFLICT_ERROR` (operations only).
4. Nothing to transfer → `SKIPPED` reports for `decision.skip` (operations only).
5. Build the key with `object_key(interface, name, utc_datetime(publication_date))`. When `calendar_tree` is on and the date is missing or unparsable, the product is KO on every destination it was to go to.
6. Create a temporary directory with `mkdtemp(dir=args.working_directory, prefix="transfer-")`. Download with `with_retries`; a `TransferError` → KO per destination to write.
7. When `same_bytes_as` is set and differs from the downloaded MD5 → `open_conflict` with the downloaded MD5, and no upload. As in step 3, when the request has an operation id, write `SKIPPED` reports with `error_type` `CONFLICT_ERROR` for the destinations that were to be written (the ones to upload) and, as in step 10, `SKIPPED` reports for `decision.skip`; so the operation has a report for every destination and never stays pending (plan 4 closes a download when `pending == 0` and counts a conflict from these reports). Live requests write no report. Skip steps 8 and 9, then run step 12.
8. Upload with `build_sink(backup, keys, pending)` and `backup_file(config, path, key=key)`, retrying only the failed destinations.
9. Each final report is stamped with `archive_fields`, `written_md5` and `source_path=""`. Its status is `REPLACED` when the destination is in `decision.archived` and the upload succeeded.
10. `SKIPPED` reports for `decision.skip` (operations only).
11. When the transfer wrote every destination `OK`/`REPLACED`, call `resolve_conflict`.
12. Remove the temporary directory in `finally`.

- [ ] **Step 1: Write the failing tests.**

The fixture `env(tmp_path, fake_opensearch)` sets up the stage:
- `conf/itf.json` holds `{"collectors": [ENTRY], "models": [MODEL]}`. `ENTRY` is an `ODataCollectorConfiguration` for `ITF_A`, with `odata_product_url` `https://catalogue.example`, model `ProductA` (index `raw-data-a`), `archive` `{"backup": "archive-a"}`, and a JSONExtractor attr_map.
- `conf/backups/archive-a.json` holds a `BackupLocal` backup on `tmp_path/archive`, created with `mkdir`.
- The stage is `TransferStage(CollectorArgs(rawdata_config_dir=…, working_directory=str(tmp_path / "work"), config_store=ConfigStoreArgs(service="svc")), TransferSettings(retry_delay=1.0), FakePublisher(), sleep=sleeps.append, downloader_factory=downloads, report_writer=index_reports)`, followed by `load_config()` and `_healthcheck = Mock()`.
- `index_reports(reports)` writes each `report.to_dict()` into the fake's `raw-data-backup`. `reports()` lists them.
- `downloads` records `(config, rule, kwargs)`. Its downloader writes `CONTENT` (or raises the next scripted error) to `<directory>/<product name>` and records the directory.
- `raw("d1", **fields)` writes `{"product_id": "id-1", "product_name": "P1.zip", "content_length": 3000, "checksum": MD5, "publication_date": "2026-03-04T10:00:00.000Z", "interface_name": "ITF_A"} | fields` to `raw-data-a-2026`.
- `request(**changes)` is `replace(TransferRequest("svc", "ITF_A", (("d1", "raw-data-a-2026"),), "archive-a", None, "skip"), **changes)`.
- `env.transfer(request)` is the product flow without the request loop: `context = stage.request_context(request)`, then for each product of `read_products(...)`, `stage.transfer_product(context, product)` and `index_reports(...)`. Task 10b's `handle_request` replaces it.

```python
def test_new_product_is_transferred_and_reported_ok(env):
    env.transfer(request())
    assert (env.tmp / "archive/ITF_A/P1.zip").read_bytes() == CONTENT
    [report] = env.reports()
    assert {key: report[key] for key in ("status", "origin", "backup_name", "interface_name", "product_id", "product_name",
                                         "written_md5", "catalogue_checksum", "operation_id", "source_path")} == {
        "status": "OK", "origin": "archive", "backup_name": "archive-a", "interface_name": "ITF_A", "product_id": "id-1",
        "product_name": "P1.zip", "written_md5": MD5, "catalogue_checksum": MD5, "operation_id": "", "source_path": ""}

def test_stable_key_by_publication_date(env):      # the backup has calendar_tree: true
    env.transfer(request())
    assert (env.tmp / "archive/ITF_A/2026/03/04/P1.zip").exists()

def test_missing_publication_date_with_calendar_tree_is_ko(env):     # Review Focus 3
    env.raw("d1", publication_date=None)
    env.transfer(request())
    assert [(r["status"], "publication_date" in r["details"]) for r in env.reports()] == [("KO", True)]
    assert env.downloads.calls == []

def test_already_archived_product_is_skipped_live_without_report(env):
    env.transfer(request()); env.transfer(request())
    assert [r["status"] for r in env.reports()] == ["OK"] and len(env.downloads.calls) == 1

def test_already_archived_product_of_an_operation_is_reported_skipped(env):
    env.transfer(request()); env.transfer(request(operation_id="op-1", origin="download"))
    assert [(r["status"], r["operation_id"]) for r in env.reports()] == [("OK", ""), ("SKIPPED", "op-1")]

def test_uppercase_catalogue_checksum_matches_the_archived_one(env):  # Review Focus 1: archived with MD5, seen again with MD5.upper()
    assert [r["status"] for r in env.reports()] == ["OK"] and env.conflicts() == []

def test_changed_checksum_under_skip_opens_a_conflict(env):     # second time with checksum "0" * 32, operation op-1
    assert env.conflicts()[0]["status"] == "OPEN" and len(env.downloads.calls) == 1
    assert (env.reports()[-1]["status"], env.reports()[-1]["error_type"]) == ("SKIPPED", "ArchiveConflict")

def test_changed_checksum_under_replace_if_changed_replaces(env):   # second request duplicates="replace_if_changed", new CONTENT
    assert [r["status"] for r in env.reports()] == ["OK", "REPLACED"]
    assert (env.tmp / "archive/ITF_A/P1.zip").read_bytes() == NEW_CONTENT

def test_forced_transfer_replaces_and_resolves_the_conflict(env):   # open conflict, then force=True
    assert env.reports()[-1]["status"] == "REPLACED" and env.conflicts()[0]["status"] == "RESOLVED"

def test_mirror_is_filled_when_the_bytes_match(env):       # S3 backup b1, b2 through a fake sink; b1 holds an OK with written_md5 MD5
    assert env.sinks.uploads == [("b2", "archive/ITF_A/P1.zip")] and env.reports()[-1]["destination"] == "b2"

def test_mirror_is_not_filled_with_other_bytes(env):       # b1's written_md5 is "1" * 32; request(operation_id="op-1", origin="download")
    assert env.sinks.uploads == [] and env.conflicts()[0]["catalogue_checksum"] == MD5
    assert [(r["destination"], r["status"], r["error_type"], r["operation_id"]) for r in env.reports()[-2:]] == [
        ("b1", "SKIPPED", "", "op-1"), ("b2", "SKIPPED", "ArchiveConflict", "op-1")]       # no destination is left without a report

def test_live_mirror_conflict_writes_no_report(env):       # the same, request(): live
    assert env.sinks.uploads == [] and env.conflicts()[0]["status"] == "OPEN" and [r["destination"] for r in env.reports()] == ["b1"]   # only the seeded OK

def test_failed_bucket_is_retried_alone(env):              # the fake sink fails b2 once
    assert env.sinks.calls == [["b1", "b2"], ["b2"]] and [r["status"] for r in env.reports()] == ["OK", "OK"]

def test_download_failure_ends_ko_after_bounded_retries(env):   # three TransientTransferError
    assert [r["status"] for r in env.reports()] == ["KO"] and env.sleeps == [1.0, 2.0]

def test_permanent_download_error_is_ko_at_once(env):
    assert [r["status"] for r in env.reports()] == ["KO"] and env.sleeps == [] and len(env.downloads.calls) == 1

def test_unsafe_product_name_is_ko_and_writes_nothing(env):     # Review Focus 4: product_name "../P1.zip"
    assert [r["status"] for r in env.reports()] == ["KO"]
    assert list((env.tmp / "archive").rglob("*")) == [] and env.downloads.calls == []

def test_working_directory_is_never_named_after_the_product_and_is_removed(env):
    [directory] = env.downloads.directories
    assert os.path.basename(directory).startswith("transfer-") and "P1" not in directory and "id-1" not in directory
    assert os.listdir(env.tmp / "work") == []

def test_a_stop_during_the_download_propagates_and_reports_nothing(env):     # the download raises TransferStopped
    with pytest.raises(TransferStopped):
        env.transfer(request())
    assert env.reports() == [] and os.listdir(env.tmp / "work") == []

def test_unknown_interface_is_refused(env):
    with pytest.raises(RequestRefused, match="ITF_B"):
        env.stage.request_context(request(interface_name="ITF_B"))

def test_unknown_backup_is_refused(env):
    with pytest.raises(RequestRefused, match="archive-b"):
        env.stage.request_context(request(backup="archive-b"))

def test_request_context_takes_the_choice_of_the_request(env):
    context = env.stage.request_context(request(backup_version=None, duplicates="replace_if_changed", force=True))
    assert (context.rule.duplicates, context.rule.force, context.destinations) == ("replace_if_changed", True, (str(env.tmp / "archive"),))
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_stage.py -v`. Expected: FAIL with `ModuleNotFoundError ... transfer.stage`.
- [ ] **Step 3: Implement `request_context` and `transfer_product`.** Follow the two numbered flows above, in that order.
  - The downloader is built with `downloader_factory(config, rule, should_stop=lambda: self.should_stop_loop, tick=self._healthcheck.tick, max_seconds=settings.max_download_seconds)`, cached per interface (Task 10b clears it).
  - Reports come from `archive_report` or from the sink's reports stamped with `archive_fields`. They never carry the merged backup block.
  - The `ConfigStore` used for pinned versions is `ConfigStore(es_connections.get_connection(), args.config_store.index)` with the opensearch backend, and `None` otherwise.
  - Error classification and one report per destination follow the download manager's reference above.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/stage.py tests/transfer/test_transfer_stage.py
git commit -m "feat: :sparkles: Transfer an archived product into its backup" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 10b: The transfer of a request (C3)

**Files:**
- Modify: `src/maas_collector/rawdata/transfer/stage.py` (`handle_request`, `on_credentials_changed`)
- Test: `tests/transfer/test_transfer_stage.py` (the same fixture as Task 10a)

**Interfaces:**
- Consumes: Task 10a's `TransferStage.request_context`, `transfer_product`, `RequestRefused`; Task 4's `read_products`, `missing_product`, `archive_report`; `on_credentials_changed(interface_names)` (`credentialmixin.py:275`).
- Produces:

```python
TransferStage.handle_request(self, request: TransferRequest) -> tuple[tuple[str, str], ...]
    # never raises; the (id, index) documents a stop left, in order
TransferStage.on_credentials_changed(self, interface_names) -> None   # also drops the cached downloaders of those interfaces
```

**`handle_request`**:
1. A request for another service is logged at ERROR and dropped (no report).
2. `request_context`. On `RequestRefused`, the rule for reading the products is `ArchiveRule.default(request.backup)`, and every product ends KO with destination `""` and the message in `details`.
3. `read_products`: each missing document gets a KO report built from `missing_product(document)` (empty product name, `details` naming `<id>` and `<index>`), the stand-in of its `REQUESTED` report.
4. For each product: if `should_stop_loop`, return this document and the ones after it. Otherwise call `transfer_product`; `TransferStopped` also returns them. Write the reports with bounded retries (CRITICAL when they cannot be stored), then call `_healthcheck.tick()`.

- [ ] **Step 1: Write the failing tests.** `env.stage.handle_request` replaces `env.transfer`.

```python
def test_request_of_one_product_returns_nothing_left(env):
    assert env.stage.handle_request(request()) == () and [r["status"] for r in env.reports()] == ["OK"]

def test_unknown_interface_ends_every_product_ko(env):
    env.stage.handle_request(request(interface_name="ITF_B"))
    assert [(r["status"], "ITF_B" in r["details"]) for r in env.reports()] == [("KO", True)] and env.downloads.calls == []

def test_unknown_backup_ends_every_product_ko(env):
    env.stage.handle_request(request(backup="archive-b"))
    assert [(r["status"], r["destination"], "archive-b" in r["details"]) for r in env.reports()] == [("KO", "", True)]

def test_missing_document_is_ko_naming_it(env):
    env.stage.handle_request(request(documents=(("d9", "raw-data-a-2026"),)))
    [report] = env.reports()
    assert (report["status"], report["product_name"]) == ("KO", "")
    assert "d9" in report["details"] and "raw-data-a-2026" in report["details"]

def test_request_of_another_service_is_dropped(env, caplog):
    assert env.stage.handle_request(request(service="other")) == ()
    assert env.reports() == [] and env.downloads.calls == [] and "other" in caplog.text

def test_stop_returns_the_documents_left(env):        # Review Focus 2: d1's download raises TransferStopped
    assert env.stage.handle_request(request(documents=(("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2026")))) == (
        ("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2026"))
    assert env.reports() == []

def test_stop_between_products_keeps_the_first_report(env):      # should_stop_loop becomes true after d1
    assert env.stage.handle_request(request(documents=(("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2026")))) == (("d2", "raw-data-a-2026"),)
    assert [r["product_name"] for r in env.reports()] == ["P1.zip"]

def test_same_product_twice_in_a_request_is_transferred_once(env):    # d1 and d2 name P1.zip
    env.stage.handle_request(request(documents=(("d1", "raw-data-a-2026"), ("d2", "raw-data-a-2026"))))
    assert len(env.downloads.calls) == 1 and [r["status"] for r in env.reports()] == ["OK"]

def test_report_write_is_retried_then_logged_critical(env, caplog):     # the writer fails three times, then the loop goes on
    env.stage.handle_request(request())
    assert env.sleeps == [1.0, 2.0] and any(record.levelname == "CRITICAL" for record in caplog.records)
    assert env.stage._healthcheck.tick.called

def test_changed_credentials_rebuild_the_downloader(env):
    env.stage.handle_request(request()); env.stage.on_credentials_changed(["ITF_A"]); env.stage.handle_request(request(force=True))
    assert len(env.downloads.factory_calls) == 2
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_stage.py -v`. Expected: the new tests FAIL with `AttributeError: ... 'handle_request'`.
- [ ] **Step 3: Implement `handle_request` and `on_credentials_changed`.** Follow the numbered flow above. The report write goes through `with_retries` (Task 7) with `settings.max_attempts` and `settings.retry_delay`, on a small callable that turns an `ArchiveReportError` into a `TransientTransferError`; after the last attempt the reports are logged at CRITICAL with their product names and the loop goes on.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/stage.py tests/transfer/test_transfer_stage.py
git commit -m "feat: :sparkles: Transfer the products of a request" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 11: The transfer stage service (C3)

**Files:**
- Create: `src/maas_collector/rawdata/cli/transfer.py`
- Modify: `src/maas_collector/rawdata/transfer/stage.py` (`setup`, `loop_body`, `MAX_LOOP_SECONDS`)
- Modify: `docs/archive.md` ("The transfer stage"), `CHANGELOG.md`
- Test: `tests/transfer/test_transfer_cli.py`

**Interfaces:**
- Consumes:
  - Tasks 10a and 10b: `TransferStage`, `TransferSettings`, `handle_request`;
  - Task 1: `take_body`, `transfer_connection`, `TransferPublisher`, `TransferRequest.from_body`;
  - `common_parser()`, `get_collector_args(CollectorArgs, namespace)` and `EnvDefault` (`cli/lib/args.py:1246, 1419, 17`).
- Produces:

```python
MAX_LOOP_SECONDS = 60     # stage.py: loop_body returns to run() at least this often, for the configuration and credential checks
TransferStage.setup(self) -> None
    # super().setup(), then declares transfer_queue(self.service) on a connection of the stage, before any request is awaited
TransferStage.loop_body(self, path_list) -> None
    # takes requests until the queue is empty, should_stop_loop, or MAX_LOOP_SECONDS; a malformed body: ERROR and dropped;
    # the documents a stop left are published again as one request (CRITICAL naming them when that fails)
def transfer_parser() -> argparse.ArgumentParser
    # --transfer-max-attempts / TRANSFER_MAX_ATTEMPTS (3), --transfer-retry-delay / TRANSFER_RETRY_DELAY (5.0),
    # --transfer-max-download-seconds / TRANSFER_MAX_DOWNLOAD_SECONDS (900.0)
def transfer_main(argv: list[str]) -> int
```

- [ ] **Step 1: Write the failing tests.** In these tests `take` is an iterator's `__next__` and the stage's `handle_request` is replaced by a recorder.

```python
def test_stage_setup_declares_its_queue(stage):      # a fresh memory:// broker; the queue does not exist before setup; FileCollector.setup patched
    stage.setup()
    TransferPublisher(memory).publish(REQUEST)        # no TransferPublishError: the request is routed
    assert TransferRequest.from_body(take_body(memory, "svc")) == REQUEST

def test_cli_requires_a_service():
    with pytest.raises(SystemExit) as exit_info:
        transfer_main([])
    assert exit_info.value.code == 2

def test_cli_requires_opensearch_and_amqp(monkeypatch, capsys):   # no ES_URL, no AMQP_URL in the environment
    with pytest.raises(SystemExit) as exit_info:
        transfer_main(["--config-service", "svc"])
    assert exit_info.value.code == 2 and "ES_URL" in capsys.readouterr().err

def test_cli_builds_the_stage_from_the_collector_options(monkeypatch):    # TransferStage.setup and .run patched
    transfer_main(["--config-service", "svc", "--transfer-max-attempts", "5", "--es-url", "http://os:9200", "--amqp-url", "amqp://broker//"])
    assert (built.settings.max_attempts, built.args.config_store.service) == (5, "svc")
    assert run_calls == [("transfer",)]

def test_loop_body_drains_the_queue(stage):          # take yields BODY_1, BODY_2, None
    stage.loop_body(["transfer"])
    assert [request.documents for request in handled] == [(("d1", "i1"),), (("d2", "i1"),)]

def test_malformed_request_is_dropped_and_the_loop_goes_on(stage, caplog):    # take yields b"{", BODY_1, None
    assert len(handled) == 1 and "malformed" in caplog.text

def test_documents_left_by_a_stop_are_republished(stage):     # Review Focus 2: handle_request returns (("d2", "i1"),)
    assert stage.publisher.requests == [replace(REQUEST, documents=(("d2", "i1"),))]

def test_loop_body_returns_after_max_loop_seconds(stage, monkeypatch):   # time.monotonic returns 0, then 61 after the first request; take yields BODY_1 forever
    stage.loop_body(["transfer"])
    assert len(handled) == 1
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_transfer_cli.py -v`. Expected: FAIL with `ModuleNotFoundError ... cli.transfer`.
- [ ] **Step 3: Implement the CLI, `setup` and `loop_body`.**
  - `setup` calls `super().setup()`, then declares `transfer_queue(self.service)` (and its exchange) with `queue.declare()` on a connection from `transfer_connection(self.args.amqp_url)`, closed afterwards. Requests published while the stage was down are then already routed when it starts; a deployment without a stage still shows up as an unroutable publish.
  - `transfer_main` follows `operator_worker_main` (`cli/operator_worker.py:171`).
  - It calls `parser.error` without `--config-service` ("one transfer stage per collector service"), and without the OpenSearch or the AMQP connection.
  - It calls `setup_logging`, then builds `TransferStage(args, TransferSettings(...), TransferPublisher(lambda: transfer_connection(args.amqp_url)))`.
  - It then runs `stage.setup()` and `stage.run(("transfer",))`, returns 0, and ends with `if __name__ == "__main__": sys.exit(transfer_main(sys.argv[1:]))`.
  - The default `take` is `lambda: take_body(lambda: transfer_connection(args.amqp_url), self.service)`.
  - The idle poll period is the collectors' `WATCH_PERIOD` (`-p`).
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Write the docs.** Write `docs/archive.md`, "The transfer stage":
  - the command; one per collector service, started before the archive is used (it declares `maas-transfer-<service>`);
  - one product at a time per replica;
  - the variables: the collectors' `CONFIG_*`, `CREDENTIAL_*`, `VAULT_*`, `ES_*`, `AMQP_*`, `WORKING_DIR`, `WATCH_PERIOD`, plus `TRANSFER_*`;
  - acknowledgement on receipt, and the republish on stop (a crash leaves gaps or pending products);
  - the download rules: no redirect with the token, and `download.url` for a host other than the catalogue's;
  - the per-destination retries; the report statuses, with `ArchiveConflict` on conflict skips (a mirror that cannot be filled with the other bytes included); conflicts;
  - "Triggering consumers (not implemented)": spec C8's note, kept here because no code does it yet:
    - an event per archived product: after an `OK` or `REPLACED` report, the stage could publish on a topic exchange (for example `maas-archive-exchange`, routing key `archived.<backup>.<interface>`) the backup, the interface, the product id and name, the object keys, size, MD5, `replaced`, `operation_id` and the time; each consumer binds its own queue; the report is written when the object is known complete, which makes it the natural hook;
    - the report index: a consumer can poll `raw-data-backup` for a backup's new `OK` and `REPLACED` archive documents (by `ingestionTime`), with no change to the stage;
    - storage notifications: bucket notifications, if the storage offers them (to be checked for the actual endpoints);
    - whichever is chosen: emit only once the object is complete, and make a replacement distinguishable from a first arrival.

  Add the CHANGELOG bullet.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/rawdata/cli/transfer.py src/maas_collector/rawdata/transfer/stage.py tests/transfer/test_transfer_cli.py docs/archive.md CHANGELOG.md
git commit -m "feat: :sparkles: Run the transfer stage of a collector service" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 12: Gaps (C4)

**Files:**
- Create: `src/maas_collector/rawdata/transfer/gaps.py`
- Modify: `tests/fakeopensearch.py` (`matches`: `range` with `gte`, `gt`, `lte`, `lt`, comparing ISO dates as datetimes, `Z` accepted)
- Modify: `docs/archive.md` ("Gaps"), `CHANGELOG.md`
- Test: `tests/transfer/test_archive_gaps.py`

**Interfaces:**
- Consumes:
  - Task 2: `ArchiveRule.from_entry`, `ArchiveRule.default`, `product_of`;
  - Task 4: `REPORT_INDEX`, `ARCHIVE_ORIGIN`, `OUTCOMES`;
  - `current_documents(client, index)` (`configprovider.py:140`), `DEFAULT_CONFIG_INDEX`;
  - `opensearchpy.helpers.scan`, `datetime_to_zulu`.
- Produces:

```python
def find_gaps(client, entry: dict, backup: str, start: datetime.datetime, end: datetime.datetime,
              delay: datetime.timedelta, *, now: datetime.datetime | None = None,
              config_index: str = DEFAULT_CONFIG_INDEX) -> list[dict]
    # [{"product_id", "product_name", "document_index", "indexed_at"}], sorted by indexed_at
```

Rules:
- **Raw index:** `entry["model"]["index"]` for an inline model. For a model name, the `index` of the model with that name among the current, non-deleted documents of `config_index`; `ValueError` when it is unknown or defined with two indexes.
- **`interface_name` field:** the model (inline, or the named one found through `config_index`) must declare a field named `interface_name` in its `fields`. Without it, the products of that interface could not be told apart and `find_gaps` would answer `[]` for a product the archive should hold: raise `ValueError` naming the interface and the model instead.
- **Candidate products:** the documents with `interface_name` equal to the entry's, and `start <= ingestionTime < min(end, now - delay)`.
- **Fields:** read through the entry's rule, or `DEFAULT_FIELDS` without one.
- **Gap:** a product with no archive report whose status is in `OUTCOMES` for `(backup, interface, product name)`. A `REQUESTED` report alone leaves a gap.

- [ ] **Step 1: Write the failing tests.** In the fake, raw documents go to `raw-data-a` with `ingestionTime` and `interface_name`; reports go to `raw-data-backup`. `NOW = 2026-10-29T12:00Z`, `DELAY = 6 h`, and the window is 2026-10-28T00:00Z → 2026-10-29T12:00Z.

```python
def test_product_without_outcome_is_a_gap(fake_opensearch):   # ENTRY's inline model declares interface_name
    raw("d1", "P1.zip", indexed="2026-10-28T10:00:00.000Z")
    assert find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW) == [
        {"product_id": "id-1", "product_name": "P1.zip", "document_index": "raw-data-a", "indexed_at": "2026-10-28T10:00:00.000Z"}]

@pytest.mark.parametrize("status", ["OK", "REPLACED", "KO", "SKIPPED"])
def test_product_with_an_outcome_is_not_a_gap(fake_opensearch, status):          # raw d1 and an archive report `status`
    assert find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW) == []

def test_requested_only_is_still_a_gap(fake_opensearch):                         # raw d1 and a REQUESTED report only
    assert [gap["product_name"] for gap in find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW)] == ["P1.zip"]

def test_product_indexed_within_the_delay_is_not_yet_a_gap(fake_opensearch):     # indexed 2026-10-29T08:00Z
    assert find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW) == []

def test_window_bounds_the_products(fake_opensearch):                            # one indexed 2026-10-27T23:59Z, one 2026-10-28T00:00Z
    assert [gap["product_name"] for gap in find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW)] == ["P2.zip"]

def test_documents_of_another_interface_are_ignored(fake_opensearch):            # raw d1 with interface_name ITF_B
    assert find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW) == []

def test_report_of_another_backup_does_not_count(fake_opensearch):               # raw d1, OK report in archive-b
    assert [gap["product_name"] for gap in find_gaps(fake_opensearch, ENTRY, "archive-a", START, END, DELAY, now=NOW)] == ["P1.zip"]

def test_model_without_interface_name_field_is_refused(fake_opensearch):
    entry = ENTRY | {"model": {"name": "ProductA", "index": "raw-data-a", "fields": [{"name": "product_name", "type": "Keyword"}]}}
    with pytest.raises(ValueError, match="ITF_A.*interface_name"):
        find_gaps(fake_opensearch, entry, "archive-a", START, END, DELAY, now=NOW)
def test_model_name_is_resolved_through_the_configuration_index(fake_opensearch):
    add_config_document(fake_opensearch, "models.json", 1, ["svc"], {"models": [{"name": "ProductA", "index": "raw-data-a", "fields": [{"name": "interface_name", "type": "Keyword"}]}]})
    assert len(find_gaps(fake_opensearch, ENTRY | {"model": "ProductA"}, "archive-a", START, END, DELAY, now=NOW)) == 1

def test_named_model_without_interface_name_field_is_refused(fake_opensearch):
    add_config_document(fake_opensearch, "models.json", 1, ["svc"], {"models": [{"name": "ProductA", "index": "raw-data-a", "fields": []}]})
    with pytest.raises(ValueError, match="interface_name"):
        find_gaps(fake_opensearch, ENTRY | {"model": "ProductA"}, "archive-a", START, END, DELAY, now=NOW)
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC python -m pytest tests/transfer/test_archive_gaps.py -v`. Expected: FAIL with `ModuleNotFoundError ... transfer.gaps`.
- [ ] **Step 3: Implement `find_gaps` and the fake's `range`.** Resolve the raw index and check the model's `interface_name` field first (the rules above). Scan the raw documents with `scan`. Query their outcomes in chunks of 1024 names with a `terms` filter on `product_name`, plus `origin`, `backup_name`, `interface_name` and `status` in `OUTCOMES`. Add `range` to the fake's `matches()`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the full suite. Expected: all pass.
- [ ] **Step 5: Write the docs.** In `docs/archive.md`, "Gaps": what counts as a gap; the model of the entry must declare an `interface_name` field and the extractor must fill it, otherwise `find_gaps` raises `ValueError` naming the interface; the backend calls `find_gaps` with its 6-hour delay. Add the CHANGELOG bullet.
- [ ] **Step 6: Commit.**

```bash
git add src/maas_collector/rawdata/transfer/gaps.py tests/fakeopensearch.py tests/transfer/test_archive_gaps.py docs/archive.md CHANGELOG.md
git commit -m "feat: :sparkles: Find the products the archive is missing" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 13: Branch review

- [ ] **Step 1: Run the full suite.** Run: `TZ=UTC python -m pytest -q` on the branch tip. Expected: all pass.
- [ ] **Step 2: Check for service-specific names.** Run: `grep -rniE "cdse|dataspace|copernicus|auxip|exprivia" src/maas_collector/rawdata/transfer src/maas_collector/rawdata/cli/transfer.py tests/transfer docs/archive.md`. Expected: no output (contracts §3).
- [ ] **Step 3: Request a code review.** Use superpowers:requesting-code-review on the whole branch against plan 2's branch, then fix the confirmed findings, one commit each.
- [ ] **Step 4: Merge the branch.** Use superpowers:finishing-a-development-branch, option "merge locally": merge `feature/archive-collector` (which carries plans 1 and 2 beneath it) into `develop` in `/home/aagazzi/work/maas-collector-satu`, run the full suite on `develop`, and do not push yet. The release is Task 14 (Rollout step 1).

### Task 14: Release 3.14.0 (Rollout step 1)

Plans 4 and 6 consume the images `maas-collector:3.14.0` and `maas-collector-operator:3.14.0` (plan 6 R4.0 only checks that they exist); this task makes them exist. It follows the repository's release flow: a `release/x.y.z` branch merged into `main`, the tag `x.y.z` (version from the tag through setuptools_scm), the merge of the tag back into `develop`. A push of a tag matching `[0-9]+.[0-9]+.[0-9]+` runs `.github/workflows/maas-collector.yml`: tests, the package to Nexus, the `maas-collector` image (`docker-registry` job) and the operator image.

**Files:**
- Modify: `CHANGELOG.md` (`## Unreleased` becomes `## Version 3.14.0`)
- No code and no test file: the checks are commands.

**Interfaces:**
- Consumes: Task 13's merged `develop`.
- Produces: the tag `3.14.0` on `main`, the package and the two images `maas-collector:3.14.0` and `maas-collector-operator:3.14.0`, which plan 4 Task 0 (`pip install`) and plan 6 R4.0 rely on.

- [ ] **Step 1: Create the release branch.** `git switch -c release/3.14.0 develop`.
- [ ] **Step 2: Version the changelog.** Rename `## Unreleased` to `## Version 3.14.0` (the heading style of `## Version 3.13.0`), then check that the section holds the entries of all three plans and nothing else: `grep -n "^### " CHANGELOG.md | head -20` lists, under it, the satellite, named backups, backup reports, custom recollects and continuous archive sections; every "Before upgrading" note of plans 1 to 3 is still in it. Commit: `git commit -am "docs: :memo: Put the archive entries in the 3.14.0 changelog section" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"`.
- [ ] **Step 3: Gate on the release branch.** Run `TZ=UTC python -m pytest -q` (all pass) and `grep -rniE "cdse|dataspace|copernicus|auxip|exprivia" src docs/archive.md` (no output).
- [ ] **Step 4: Merge and tag (publishes: run it only on the maintainer's go-ahead).**

```bash
git switch main && git merge --no-ff release/3.14.0 -m "Merge branch 'release/3.14.0'"
git tag 3.14.0
git describe --tags --exact-match        # must print 3.14.0
git push origin main 3.14.0              # the tag starts the release workflow
git switch develop && git merge --no-ff 3.14.0 -m "Merge tag '3.14.0' into develop" && git push origin develop
```

- [ ] **Step 5: Check what the next plans rely on.** Expected for each:
  - `gh run list --workflow maas-collector.yml --limit 3` shows the run of the tag `3.14.0` as `completed success`, with the `ci`, `docker-registry` and operator-image jobs green;
  - on a node (plan 6 R4.0's command): `docker pull 127.0.0.1:8085/maas-collector:3.14.0 && docker pull 127.0.0.1:8085/maas-collector-operator:3.14.0` both succeed;
  - the collector image holds the archive: `docker run --rm --entrypoint python 127.0.0.1:8085/maas-collector:3.14.0 -c "import maas_collector.rawdata.transfer.stage, maas_collector.rawdata.cli.transfer"` exits 0;
  - the operator image holds the operator worker and the archive: `docker run --rm --entrypoint python 127.0.0.1:8085/maas-collector-operator:3.14.0 -c "import maas_engine.replay, maas_collector.rawdata.transfer.stage"` exits 0;
  - the package: `pip download --no-deps maas-collector==3.14.0 -d <empty directory>` from the Nexus index succeeds (plan 4 can then install the release instead of the checkout).
  
  If an image or the package is missing, fix the workflow run (re-run it) before plans 4 and 6 go on; do not move the tag.

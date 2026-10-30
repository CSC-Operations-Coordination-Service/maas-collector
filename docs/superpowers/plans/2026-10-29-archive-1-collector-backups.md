# Named Backups, Safe Validation and Backup Reports (maas-collector) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let collector entries use named, shared backups whose S3 keys live in credential entries, refuse every unsafe or dangling configuration write inside `ConfigStore`, validate configurations without evaluating operator code, and make a replay fail when its files could not be backed up.

**Architecture:** A pure module `backup/named.py` defines what a backup definition is and resolves names; it imports no extractor. `configprovider.py` validates and checks references across files, `configstore.py` adds the destination guard and the integrity check to every write, and `rawdata/extractor/static.py` parses lambdas without evaluating them. The file collector resolves names at load, `CredentialMixin` carries the S3 keys of the backups in a sensitive field refreshed on rotation, and a replay counts its failed backups and exits with a code of its own.

**Tech Stack:** Python 3.11, dataclasses, `ast` and `inspect` (static validation), boto3 (mocked), opensearch-py through the in-memory `FakeOpenSearch`, pytest.

**Spec:** `docs/superpowers/specs/2026-10-29-maas-operator-backups-recollect-dm-design.md` (A1 collector side, A2, A3, A4, B1 static validation) and **Contracts:** `docs/superpowers/plans/2026-10-29-archive-0-contracts.md` (§1, §2).

**Names this plan adds beyond the contracts (nothing renamed, no contract deviation):**
- `backup/named.py`: `CREDENTIAL_NAME`, `DEFINITION_KEYS`, `check_definition`, `apply_named_backup`, `destination_changes`.
- `configprovider.py`: `integrity_problems(sources)`.
- `filecollector.py`: `BACKUP_FAILURE_FORMAT`, `BACKUP_FAILURE_LINE`, `FileCollector.backup_failures`, `FileCollector.build_backup_block`, `FileCollector.count_backup_failure`, `FileCollector.backup_failure_line`; fields `backup_name` and `backup_credentials_entry` on `FileCollectorConfiguration`.
- `credentialmixin.py`: `CredentialMixin.set_backup_credentials`.
- `backup.py`: `CollectorBackupConfiguration.backup_name`.
- `BACKUP_FAILED_EXIT_CODE = 74` (distinct from 1 and 75; plan 3's `TRANSFER_FAILED_EXIT_CODE` is 69).

## Global Constraints

- **Repository and branch:** `/home/aagazzi/work/maas-collector-satu`, branch `feature/satu-xml-extractor` (from `origin/develop`, 3.13.0). All paths below are relative to it, and `src/maas_collector/` is written `…/`.
- **Python and timezone:** Python 3.11 or later (`datetime.UTC`), and every test run sets `TZ=UTC`, as `tox.ini` does.
- **Virtual environment, to run the whole suite.** The shared venv `/home/aagazzi/work/maas-collector/.venv` imports `maas_collector` from this worktree when `PYTHONPATH=src` is set (checked: `PYTHONPATH=src …/python -c "import maas_collector; print(maas_collector.__file__)"` prints `/home/aagazzi/work/maas-collector-satu/src/maas_collector/__init__.py`), but it has no `maas_engine`, so `tests/operator` cannot run there. `tox.ini` runs `pytest` with `extras = testing`, and `setup.cfg` makes `testing` bring `maas-engine`. Build a venv in the worktree (`.venv*/` is git-ignored):
  - With the Nexus index configured (`pip config list` shows it): `python3.11 -m venv .venv && .venv/bin/pip install -e '.[testing]'`.
  - Without it (public PyPI has neither `maas-model` nor `maas-engine`; not run when this plan was written), install the sibling checkouts first:
    ```bash
    python3.11 -m venv .venv
    .venv/bin/pip install -e '/home/aagazzi/work/maas-model[vault]'
    mkdir -p /tmp/maas-engine-src && git -C /home/aagazzi/work/maas-engine archive --prefix=maas-engine-3.2.0/ 3.2.0 | tar -x -C /tmp/maas-engine-src
    SETUPTOOLS_SCM_PRETEND_VERSION=3.2.0 .venv/bin/pip install /tmp/maas-engine-src/maas-engine-3.2.0
    .venv/bin/pip install -e '.[testing]'
    ```
  - Check: `.venv/bin/python -c "import maas_engine.replay, maas_collector; print(maas_collector.__file__)"` prints a path under `/home/aagazzi/work/maas-collector-satu/src`.
  - If neither works, use the shared venv for everything but `tests/operator`, and say so in the final report instead of skipping silently.
- **Test commands.** `RUN` below stands for `cd /home/aagazzi/work/maas-collector-satu && TZ=UTC .venv/bin/python -m pytest -q -o addopts=""`. With the shared venv, replace `.venv/bin/python` by `PYTHONPATH=src /home/aagazzi/work/maas-collector/.venv/bin/python` and add `-p no:cacheprovider --ignore=tests/operator`.
  - Whole suite: `RUN`. The baseline of this branch is **342 passed** without `tests/operator` (shared venv, checked).
  - Test file names are unique by basename (pytest imports test modules by basename): every new file of this plan is `tests/test_backup_*.py`, `tests/test_config_backups.py`, `tests/test_configstore_backups.py` or `tests/test_static_validation.py`.
- **Configuration content:** `CONFIG_KEYS` becomes `{"collectors", "models", "amqp", "backups"}`; one file per backup, `backups/<name>.json`, content `{"backups": [definition]}`.
- **Definition:** keys `name` (`[a-z0-9][a-z0-9-]*`, at most 64 characters, unique among the files a service loads), `description`, `type` (`BackupS3` or `BackupLocal`), `directory`, `calendar_tree`, `enable_gzip`, `report_model` (collector file backups only), and for S3 `buckets`, `s3_endpoint_url`, `s3_region`, `s3_signature_version`, `credentials` (the name of a credential entry, which must start with `backup-`). Several buckets are mirrors.
- **Secret fields:** `s3_access_key` and `s3_secret_key` are refused in any backup block, named or inline, by `validate_config_sources` and by the loader itself. An inline S3 block names a `credentials` entry too.
- **Destination keys:** `type`, `s3_endpoint_url`, `directory`, `calendar_tree`, `enable_gzip` and a removed bucket cannot change once the backup has reports; the guard lives in `ConfigStore` (publish, rollback, delete then re-create). A backup keeps its file path: a name that has reports cannot be defined by another path (reports are matched on the name alone, so a same-named backup of another service with reports also blocks).
- **Reference integrity:** in `ConfigStore` for every write (publish, rollback, delete): refused when, with the write applied, an entry or archive rule of an affected service (enabled or not) names a backup the service does not load, or a backup names a `report_model` the service does not define.
- **Filesystem backup:** `BackupLocal` "creates folders below `directory` but no longer `directory` itself; when it does not exist, the backup fails (KO)".
- **Credentials at run time:** `CredentialMixin` adds the backups' credential entries to its credential names and copies only `s3_access_key` and `s3_secret_key` into `backup_credentials` (field metadata `sensitive`), refreshed on rotation; "an entry holding any other field fails the backup at load, naming the field". `CollectorBackupS3Configuration` marks both keys sensitive and masks them in its repr.
- **Reports:** `OK, KO, REPLACED, SKIPPED, REQUESTED = "OK", "KO", "REPLACED", "SKIPPED", "REQUESTED"`; `BackupReport` gains `origin: str = "collector"`, `backup_name`, `operation_id`, `product_id`, `product_name`, `written_md5`, `catalogue_checksum` (all `""`). An entry may not use the same backup as its file `backup` and in its `archive` rule.
- **Replay (A4):** in replay mode, every file whose collector backup did not end with `OK` reports for all its destinations, the "Cannot backup" path included, is counted; the collector collects everything it can, then exits with `BACKUP_FAILED_EXIT_CODE` and the last line `backup failed: <count> files to <backup name>`; `CollectorReplayHandler.describe_failure` turns it into `N files could not be backed up to <backup>`. Live mode is unchanged.
- **Safe validation (B1):** `validate_config_sources(sources, config_classes_for, resolve_models=True, evaluate=True)`; with `evaluate=False` no lambda or converter is evaluated: each `python` string is parsed with `ast.parse(..., mode="eval")` and must be a `Lambda` without default arguments, an extractor's class and argument names are checked against its signature without building it. An `interface_name` containing `@` is refused.
- **No service-specific code (user rule):** code, tests, docs and defaults never name a particular service or interface. Tests use `ITF_A`, `archive-a`, `archive-b`, `bucket-a`, `https://storage.example`; the files this plan edits (`tests/test_backup.py`, `tests/test_backup_report.py`, `docs/backup.md`) lose their existing service-specific names in the task that edits them.
- **Secrets in tests:** use `test-access-key` and `test-secret-key` (the repository runs gitleaks as a pre-commit hook; never an `AKIA…` shaped value).
- **Commits:** a gitmoji subject as in this repository (`feat: :sparkles: …`, `fix: :bug: …`, `test: :white_check_mark: …`, `docs: :memo: …`, `refactor: :recycle: …`), committed with `git commit -m "<subject>" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"` (a blank line, then the co-author line). Add explicit paths, never `git add -A`.
- **CHANGELOG:** prose bullets under `## Unreleased`, in `### Named backups`, `### Backup reports` and `### Safe configuration validation` (create the section in the task that first needs it), after the existing `### Satellite unavailability reports`.
- **Before every commit:** `RUN` reports no failure.

## Review Focus

1. **An S3 credential entry holding only one of the two keys** (a half-rotated or hand-edited entry). Expected: the backup of the entry fails and is counted, never an upload with the ambient AWS credentials of the container. Test: Task 7 `test_entry_with_one_key_only_fails_the_backup`.
2. **Reports only in an older yearly partition** (`raw-data-backup-2025` holds the reports, the year is now 2026), then a destination change. Expected: refused. Test: Task 8 `test_reports_in_an_older_yearly_partition_block_a_destination_change`.
3. **The failure line followed by later output** (logging at exit) and a replay that fails both ways. Expected: `describe_failure` still finds the count, and the line is printed even when exit code 1 wins. Tests: Task 9 `test_describe_failure_finds_the_line_among_later_output`, `test_replay_failure_wins_but_the_backup_line_is_printed`.
4. **`config import` of a deploy tree whose entry file sorts before `backups/`** (`airbus/x.json`). Expected: the backups, and the models they name, are published first, so the import does not refuse the entries. Test: Task 10 `test_import_publishes_backups_before_the_entries_using_them`.
5. **A filesystem backup whose `directory` is relative or climbs with `..`** (it would resolve against the working directory of the process, the "unmounted path fills the container disk" case again). Expected: refused when the definition is validated. Test: Task 2 `test_local_directory_must_be_absolute_without_dot_dot_segments`.

---

## File structure

| file | responsibility | task |
| --- | --- | --- |
| `…/backup/named.py` (new) | definition checks, name resolution, destination changes, `apply_named_backup` | 2, 6 |
| `tests/backup_samples.py` (new) | shared samples: `LOCAL`, `S3`, `KEYS`, `entry`, `backups_file`, `models_file` | 2 |
| `…/rawdata/configprovider.py` | `CONFIG_KEYS`, `ConfigIntegrityError`, `BackupDestinationError`, `integrity_problems`, backup and `@` checks, `evaluate` | 3, 4 |
| `…/rawdata/extractor/static.py` (new) | `check_lambda_source`, `check_extractor_static` | 4 |
| `…/rawdata/configuration.py` | `build_collector_configuration(..., evaluate=True)` | 4 |
| `…/backup/backup.py`, `s3_backup.py`, `local_backup.py` | report fields, `backup_name`, masked S3 repr, local directory rule | 5 |
| `…/rawdata/collector/filecollector.py` | fields on the configuration, load-time resolution, backup block, failure counting, exit codes | 6, 7, 9 |
| `…/rawdata/collector/credentialmixin.py` | backup credential entries, rotation | 7 |
| `…/rawdata/configstore.py` | `evaluate`, `report_index`, integrity check, destination guard | 4, 8 |
| `…/rawdata/cli/odata.py`, `jirax.py` | exit on backup failures | 9 |
| `…/rawdata/operator/handler.py` | `describe_failure` | 9 |
| `…/rawdata/cli/config.py` | import order | 10 |
| `tests/fakeopensearch.py` | wildcard index names in `search`, `add_report_document` | 8 |
| `docs/backup.md`, `docs/opensearch_config.md`, `docs/replay.md`, `docs/operator_worker.md`, `CHANGELOG.md` | documentation | each task |

---

### Task 1: Review and commit the removal of `SatUnavailabilityExtractor`

**Files:**
- Commit as they are: `CHANGELOG.md`, `src/maas_collector/rawdata/extractor/plugins/__init__.py`, `src/maas_collector/rawdata/extractor/plugins/satunavailability.py` (deleted, already staged), `tests/test_extractor_satu.py`
- Copy in: `docs/superpowers/specs/2026-10-29-maas-operator-backups-recollect-dm-design.md`, `docs/superpowers/plans/2026-10-29-archive-0-contracts.md`, `docs/superpowers/plans/2026-10-29-archive-1-collector-backups.md` (from `/home/aagazzi/work/maas-collector/docs/superpowers/`)

**Interfaces:** Consumes nothing. Produces the clean base the other tasks build on, and the virtual environment of Global Constraints.

- [ ] **Step 1: Set up the environment.** Follow Global Constraints (virtual environment) and run its check command. Expected: the path under the worktree's `src`.
- [ ] **Step 2: Run the tests of the work in progress.** Run: `RUN tests/test_extractor_satu.py`. Expected: `4 passed`.
- [ ] **Step 3: Review the diff, change nothing.** `git status --short` shows the four paths (the deletion staged). Check each point and note any finding in the final report instead of editing:
  - `grep -rn "SatUnavailabilityExtractor\|satunavailability" src tests docs CHANGELOG.md` prints exactly two lines, both in `CHANGELOG.md` (the two sentences of the entry; `docs/changelog.md` only includes the file and matches nothing);
  - `tests/test_extractor_satu.py` covers the old format (`…-OLD.EOF.xml`), the new one (`…-NEW.EOF.xml`) and a ground segment without orbit, with `SATU_EXTRACTOR_ARGS` using `root_path`;
  - the CHANGELOG entry sits under `## Unreleased`, above `## Version 3.13.0`, and tells to switch the configuration before upgrading.
- [ ] **Step 4: Run the whole suite.** Run: `RUN`. Expected: all pass (342 without `tests/operator`).
- [ ] **Step 5: Commit the work in progress.**

```bash
git add CHANGELOG.md src/maas_collector/rawdata/extractor/plugins/__init__.py src/maas_collector/rawdata/extractor/plugins/satunavailability.py tests/test_extractor_satu.py
git commit -m "refactor: :recycle: Extract the satellite unavailability reports with the XMLExtractor" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Commit the feature documents.** Copy the spec, the contracts and this plan into the same paths of the worktree, then:

```bash
git add docs/superpowers/specs/2026-10-29-maas-operator-backups-recollect-dm-design.md docs/superpowers/plans/2026-10-29-archive-0-contracts.md docs/superpowers/plans/2026-10-29-archive-1-collector-backups.md
git commit -m "docs: :memo: Plan the named backups of maas-collector" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: What a named backup is (`backup/named.py`)

**Files:**
- Create: `src/maas_collector/backup/named.py`, `tests/backup_samples.py`, `tests/test_backup_named.py`
- Modify: `docs/backup.md` (a "Named backups" section), `CHANGELOG.md`

**Interfaces:**
- Consumes: nothing.
- Produces (contract §2, plus the names this plan adds):

```python
BACKUP_NAME = re.compile(r"[a-z0-9][a-z0-9-]*")        # always used with fullmatch
MAX_NAME_LENGTH = 64
CREDENTIAL_PREFIX = "backup-"
CREDENTIAL_NAME = re.compile(r"backup-[A-Za-z0-9._-]+")
SECRET_FIELDS = ("s3_access_key", "s3_secret_key")
DESTINATION_KEYS = ("type", "s3_endpoint_url", "directory", "calendar_tree", "enable_gzip", "buckets")
DEFINITION_KEYS = frozenset({"name", "description", "type", "directory", "calendar_tree", "enable_gzip",
                             "report_model", "buckets", "s3_endpoint_url", "s3_region",
                             "s3_signature_version", "credentials"})
def check_backup_block(block: dict, where: str) -> None
def check_definition(definition: dict, where: str) -> None
def backup_definitions(contents: Iterable[dict]) -> dict[str, dict]
def resolve_backup(reference: str | dict, definitions: dict[str, dict]) -> tuple[dict, str | None]
def destination_changes(old: dict, new: dict) -> list[str]
```

`tests/backup_samples.py` (shared by Tasks 2 to 10):

```python
LOCAL = {"name": "archive-a", "type": "BackupLocal", "directory": "/data/archive-a"}
S3 = {"name": "archive-b", "type": "BackupS3", "directory": "archive", "buckets": ["bucket-a", "bucket-b"],
      "s3_endpoint_url": "https://storage.example", "s3_region": "region-1", "s3_signature_version": "s3v4",
      "credentials": "backup-archive-b"}
KEYS = {"s3_access_key": "test-access-key", "s3_secret_key": "test-secret-key"}
ODATA = "ODataCollectorConfiguration"   # class of every test entry that carries an `archive` rule
def entry(interface_name="ITF_A", **extra) -> dict
# {"id_field": "", "routing_key": "", "interface_name": interface_name,
#  "model": {"name": "ModelA", "index": "", "fields": []},
#  "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}}, **extra}
# (LogExtractor.__init__ requires `pattern`, FileCollectorConfiguration requires `id_field` and `routing_key`)
# An entry carrying an `archive` rule always passes class="ODataCollectorConfiguration" (ODATA = "ODataCollectorConfiguration"
# in this module): without `class` the entry builds as FileCollectorConfiguration, which plan 3 Task 2 (check_archive_rule)
# refuses an `archive` on. Unknown keys are ignored before plan 3, so these tests pass on both branches.
def backups_file(*definitions) -> dict               # {"backups": list(definitions)}
def models_file(*names) -> dict                      # {"models": [{"name": n, "index": "", "fields": []}, ...]}
```

- [ ] **Step 1: Write the failing tests** in `tests/test_backup_named.py`:

```python
def test_definitions_are_indexed_by_name_across_contents():
    found = backup_definitions([backups_file(LOCAL), {"collectors": []}, backups_file(S3)])
    assert set(found) == {"archive-a", "archive-b"} and found["archive-a"] == LOCAL

def test_a_name_defined_twice_is_refused():
    with pytest.raises(ValueError, match="archive-a"):
        backup_definitions([backups_file(LOCAL), backups_file(LOCAL)])

@pytest.mark.parametrize("name", ["", "A", "-a", "a_b", "a b", "a" * 65, "ab\n", "é", 7, None])
def test_bad_names_are_refused(name):
    with pytest.raises(ValueError, match="name"):
        backup_definitions([backups_file(LOCAL | {"name": name})])

def test_a_name_of_64_characters_is_accepted():
    assert "a" * 64 in backup_definitions([backups_file(LOCAL | {"name": "a" * 64})])

@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_a_secret_field_is_refused_in_a_definition_and_in_an_inline_block(field):
    with pytest.raises(ValueError, match=field) as error:
        backup_definitions([backups_file(S3 | {field: "test-secret-key"})])
    assert "test-secret-key" not in str(error.value)
    with pytest.raises(ValueError, match=field):
        check_backup_block({"type": "BackupLocal", "directory": "/d", field: "x"}, "ITF_A backup")

def test_the_message_names_where():
    with pytest.raises(ValueError, match="entry ITF_A"):
        check_backup_block({"type": "BackupLocal", "directory": "/d", "s3_secret_key": "x"}, "entry ITF_A")

@pytest.mark.parametrize("credentials", ["archive-b", "backup-", "backup-a/b", "Backup-a", 7])
def test_a_bad_credentials_name_is_refused(credentials):
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block(S3 | {"credentials": credentials}, "x")

def test_an_s3_block_must_name_credentials_and_a_local_one_must_not():
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block({k: v for k, v in S3.items() if k != "credentials"}, "x")
    with pytest.raises(ValueError, match="credentials"):
        check_backup_block(LOCAL | {"credentials": "backup-x"}, "x")

@pytest.mark.parametrize("base, change, word", [
    (LOCAL, {"type": "BackupSFTP"}, "type"), (LOCAL, {"directory": ""}, "directory"), (LOCAL, {"buckets": ["b"]}, "buckets"),
    (S3, {"buckets": []}, "buckets"), (S3, {"buckets": "bucket-a"}, "buckets"), (LOCAL, {"bucket": "x"}, "bucket"),
    (LOCAL, {"calendar_tree": "yes"}, "calendar_tree"), (LOCAL, {"enable_gzip": 1}, "enable_gzip"),
    (LOCAL, {"report_model": 3}, "report_model"), (LOCAL, {"description": 3}, "description")])
def test_a_definition_that_breaks_a_rule_is_refused(base, change, word):
    with pytest.raises(ValueError, match=word): check_definition(base | change, "x")

def test_an_s3_definition_without_buckets_is_refused():
    with pytest.raises(ValueError, match="buckets"): check_definition({k: v for k, v in S3.items() if k != "buckets"}, "x")

def test_local_directory_must_be_absolute_without_dot_dot_segments():     # Review Focus 5
    for directory in ("data", "./data", "/data/../etc", "/data/.."):
        with pytest.raises(ValueError, match="directory"):
            check_definition(LOCAL | {"directory": directory}, "x")
    check_definition(LOCAL | {"directory": "/data/archive.v2"}, "x")

def test_resolving_a_name_drops_name_description_and_credentials():
    block, credentials = resolve_backup("archive-b", {"archive-b": S3 | {"description": "d"}})
    assert credentials == "backup-archive-b"
    assert set(block) == {"type", "directory", "buckets", "s3_endpoint_url", "s3_region", "s3_signature_version"}

def test_the_resolved_block_does_not_alias_the_definition():
    definitions = {"archive-b": dict(S3)}
    block, _ = resolve_backup("archive-b", definitions)
    block["buckets"].append("other")
    assert definitions["archive-b"]["buckets"] == ["bucket-a", "bucket-b"]

def test_an_unknown_name_raises_key_error():
    with pytest.raises(KeyError, match="nope"):
        resolve_backup("nope", {})

def test_an_inline_block_is_returned_without_its_credentials_entry():
    block, credentials = resolve_backup({"type": "BackupS3", "directory": "d", "buckets": ["b"], "credentials": "backup-x"}, {})
    assert (credentials, "credentials" in block) == ("backup-x", False)
    assert resolve_backup({"type": "BackupLocal", "directory": "d"}, {}) == ({"type": "BackupLocal", "directory": "d"}, None)

def test_an_inline_block_with_a_secret_is_refused_when_resolved():
    with pytest.raises(ValueError, match="s3_access_key"):
        resolve_backup({"type": "BackupS3", "directory": "d", "s3_access_key": "x", "credentials": "backup-x"}, {})

@pytest.mark.parametrize("reference", [[], 3, None])
def test_a_reference_that_is_neither_a_name_nor_a_block_is_refused(reference): ...   # ValueError

@pytest.mark.parametrize("change, expected", [
    ({"directory": "/other"}, ["directory"]), ({"calendar_tree": True}, ["calendar_tree"]),
    ({"enable_gzip": True}, ["enable_gzip"]), ({"type": "BackupLocal"}, ["type"]),
    ({"s3_endpoint_url": "https://other.example"}, ["s3_endpoint_url"]),
    ({"buckets": ["bucket-a"]}, ["buckets"]),                      # a removed bucket
    ({"buckets": ["bucket-b", "bucket-a", "bucket-c"]}, []),       # an added bucket, another order
    ({"description": "x"}, []), ({"report_model": "M"}, []), ({"credentials": "backup-other"}, []),
    ({"s3_region": "region-2"}, [])])
def test_destination_changes(change, expected):
    assert destination_changes(S3, S3 | change) == expected

def test_destination_defaults_are_normalized():
    assert destination_changes(LOCAL, LOCAL | {"calendar_tree": False, "enable_gzip": False}) == []

def test_destination_keys_are_the_contract_ones():
    assert DESTINATION_KEYS == ("type", "s3_endpoint_url", "directory", "calendar_tree", "enable_gzip", "buckets")
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_backup_named.py`. Expected: FAIL with `ModuleNotFoundError: No module named 'maas_collector.backup.named'`.
- [ ] **Step 3: Implement `named.py`.** Pure functions, importing only `re` and `typing`.
  - `check_backup_block` (any block, named or inline): raises `ValueError` starting with `where`, naming the field and never echoing a value, for a `SECRET_FIELDS` key; for a `name` present that fails `BACKUP_NAME.fullmatch` or exceeds `MAX_NAME_LENGTH`; for a `credentials` that is not a string `CREDENTIAL_NAME.fullmatch`es; for `type == "BackupS3"` without `credentials`; for `credentials` on any other type.
  - `check_definition` (a definition): `check_backup_block`, plus: a dict whose keys are within `DEFINITION_KEYS` (unknown keys listed); `name` present; `type` in `("BackupS3", "BackupLocal")`; `directory` a non-empty string, and for `BackupLocal` absolute (starts with `/`) with no `..` segment; `calendar_tree` and `enable_gzip` booleans when present; `description` a string and `report_model` a string or `None` when present; `BackupS3` needs `buckets`, a non-empty list of strings; `BackupLocal` holds none of `buckets`, `s3_endpoint_url`, `s3_region`, `s3_signature_version`.
  - `backup_definitions`: for each content with a `backups` list, `check_definition(definition, f"backup {definition.get('name')!r}")`, then index by name; a duplicate raises `ValueError` naming it.
  - `resolve_backup`: for a `str`, `definitions[name]` (`KeyError` when absent); for a `dict`, `check_backup_block(reference, "inline backup block")`; any other type is a `ValueError`. Returns a deep copy of the block without `name`, `description` and `credentials`, and the `credentials` value or `None`.
  - `destination_changes`: the keys of `DESTINATION_KEYS` other than `buckets` whose value differs after applying the defaults `calendar_tree=False`, `enable_gzip=False`, others `None`, in `DESTINATION_KEYS` order; then `"buckets"` when a bucket of `old` is missing from `new`.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_backup_named.py`. Expected: all pass.
- [ ] **Step 5: Document and commit.**
  - `docs/backup.md`: a section "Named backups" with the definition keys table (the Global Constraints list), the file convention `backups/<name>.json`, "several buckets are mirrors", and "a filesystem `directory` is an absolute path in the container".
  - `CHANGELOG.md`: a `### Named backups` section with one bullet: a configuration file may hold `backups`, one definition per file, with the rules above, validated by `maas_collector.backup.named`.

```bash
git add src/maas_collector/backup/named.py tests/backup_samples.py tests/test_backup_named.py docs/backup.md CHANGELOG.md
git commit -m "feat: :sparkles: Define named backups" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Backups in configuration files, references and `@` (validation)

**Files:**
- Modify: `src/maas_collector/rawdata/configprovider.py` (`CONFIG_KEYS` at :32, `ConfigValidationError` at :35, `validate_config_sources` at :305), `docs/opensearch_config.md`, `CHANGELOG.md`
- Test: `tests/test_config_backups.py`

**Interfaces:**
- Consumes: Task 2's `backup_definitions`, `check_backup_block`, `BACKUP_NAME`, `MAX_NAME_LENGTH`; `config_classes_for` (`configstore.py:129`).
- Produces (contract §2):

```python
CONFIG_KEYS = frozenset({"collectors", "models", "amqp", "backups"})
class ConfigIntegrityError(ConfigValidationError):
    problems: list[str]                          # one line per dangling reference
    def __init__(self, label: str, problems: list[str])      # reason = "; ".join(problems)
class BackupDestinationError(ConfigValidationError): ...     # (label, reason), raised by Task 8
def integrity_problems(sources: list[ConfigSource]) -> list[str]
    # the sources are the files ONE service loads. Lines (each starts with the file path):
    #   "<path>: interface <name> uses backup <backup>, which is not defined"
    #   "<path>: interface <name> archives to backup <backup>, which is not defined"
    #   "<path>: backup <backup> names report model <model>, which is not defined"
    #   "backup <backup> is defined in both <path1> and <path2>"
    # entries enabled or not; a model is defined by a "models" item or an inline "model" dict of an entry
def validate_config_sources(sources, config_classes_for, resolve_models: bool = True) -> None
    # gains (resolve_models=True only, since a single file cannot resolve names of another):
    # raises ConfigIntegrityError(label="backups", problems) when integrity_problems(sources) is not empty
```

- [ ] **Step 1: Write the failing tests** in `tests/test_config_backups.py`. Helper: `def validate(*contents, **kwargs): validate_config_sources([ConfigSource(f"f{i}.json", c) for i, c in enumerate(contents)], config_classes_for, **kwargs)`.

```python
def test_backups_is_a_configuration_key():
    assert CONFIG_KEYS == {"collectors", "models", "amqp", "backups"} and is_config_content({"backups": []})

def test_a_backups_only_file_validates(): validate(backups_file(LOCAL, S3))

def test_an_entry_naming_a_backup_of_another_file_validates():
    validate(backups_file(LOCAL), {"collectors": [entry(backup="archive-a")]})

@pytest.mark.parametrize("enabled", [True, False])
def test_an_unknown_name_is_refused_even_on_a_disabled_entry(enabled):
    with pytest.raises(ConfigIntegrityError) as error:
        validate({"collectors": [entry(backup="nope", enabled=enabled)]})
    assert "ITF_A" in error.value.problems[0] and "nope" in error.value.problems[0]

def test_a_single_file_check_does_not_resolve_names():
    validate({"collectors": [entry(backup="nope")]}, resolve_models=False)

def test_every_dangling_reference_is_listed():
    with pytest.raises(ConfigIntegrityError) as error:
        validate({"collectors": [entry("ITF_A", backup="nope"), entry("ITF_B", archive={"backup": "gone"}, **{"class": ODATA})]})
    assert len(error.value.problems) == 2

@pytest.mark.parametrize("resolve", [True, False])
def test_a_secret_in_an_inline_block_or_a_definition_is_refused(resolve):
    inline = {"type": "BackupS3", "directory": "d", "buckets": ["b"], "credentials": "backup-x", "s3_secret_key": "x"}
    with pytest.raises(ConfigValidationError, match="s3_secret_key"):
        validate({"collectors": [entry(backup=inline)]}, resolve_models=resolve)
    with pytest.raises(ConfigValidationError, match="s3_access_key"):
        validate(backups_file(S3 | {"s3_access_key": "x"}), resolve_models=resolve)

def test_an_inline_s3_block_naming_credentials_validates(): ...      # no error, mode resolve_models True and False
def test_a_local_inline_block_still_validates(): ...                # {"type": "BackupLocal", "directory": "/d"}, no error
@pytest.mark.parametrize("backup", [["a"], 3, True])
def test_a_backup_that_is_neither_a_name_nor_a_block_is_refused(backup): ...    # ConfigValidationError, match "backup"
def test_a_backup_name_that_breaks_the_name_rule_is_refused(): ...  # "Bad_Name" and a 65 character name -> ConfigValidationError

def test_the_same_backup_for_the_file_backup_and_the_archive_is_refused_in_both_modes():
    content = {"collectors": [entry(backup="archive-a", archive={"backup": "archive-a"}, **{"class": ODATA})]}
    for resolve in (True, False):
        with pytest.raises(ConfigValidationError, match="archive-a"): validate(backups_file(LOCAL), content, resolve_models=resolve)
def test_different_backups_for_the_file_backup_and_the_archive_validate(): ...   # backups_file(LOCAL, S3), entry(backup="archive-a", archive={"backup": "archive-b"}, **{"class": ODATA}): no error

@pytest.mark.parametrize("enabled", [True, False])
def test_an_interface_name_with_at_is_refused(enabled):
    with pytest.raises(ConfigValidationError, match="@"):
        validate({"collectors": [entry("ITF_A@custom", enabled=enabled)]}, resolve_models=False)

def test_a_report_model_of_a_definition_must_be_defined():
    with pytest.raises(ConfigIntegrityError, match="ReportM"): validate(backups_file(LOCAL | {"report_model": "ReportM"}))
    validate(backups_file(LOCAL | {"report_model": "ReportM"}), models_file("ReportM"))      # in another file
    validate(backups_file(LOCAL | {"report_model": "ModelA"}), {"collectors": [entry()]})      # an inline model
def test_a_name_defined_in_two_files_is_refused_and_in_one_file_even_alone(): ...   # ConfigIntegrityError match "both" for two files; ConfigValidationError for one file with resolve_models=False
def test_integrity_problems_is_empty_for_a_consistent_set(): ...   # integrity_problems(sources) == []
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_config_backups.py`. Expected: FAIL (`ImportError: cannot import name 'ConfigIntegrityError'`).
- [ ] **Step 3: Implement in `configprovider.py`.**
  - Add `"backups"` to `CONFIG_KEYS`; define `ConfigIntegrityError`, `BackupDestinationError` after `ConfigValidationError`.
  - In `validate_config_sources`, inside the existing per-source `try` (so errors carry the source label): `content["backups"]` must be a list (add it to the list-type check), then `backup_definitions([content])` (definition rules and duplicates within the file); per entry: an `interface_name` containing `@` is refused (message mentions `@`, any mode); `backup` must be `None`, a name (`BACKUP_NAME.fullmatch` and at most `MAX_NAME_LENGTH`) or a dict passed to `check_backup_block(backup, f"interface {name}: backup")`, anything else is refused; an `archive` dict whose `backup` equals the entry's `backup` name is refused naming it (the two would write each other's objects).
  - After the loop, when `resolve_models`, after the existing model resolution: `problems = integrity_problems(sources)`; raise `ConfigIntegrityError("backups", problems)` when not empty. `integrity_problems` tolerates malformed content (`.get`), as stored documents may predate a rule.
  - Update the `validate_config_sources` docstring: `resolve_models` also resolves backup names and report models of definitions.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_config_backups.py tests/test_config_provider.py tests/test_config_refresh.py tests/test_configstore.py`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/opensearch_config.md`: under "Documents and versions", a paragraph on `backups/<name>.json` files, their `services` tags and the checks of `validate_config_sources`. `CHANGELOG.md` `### Named backups`: a bullet on the `backups` key, the refused secret fields, `@` in interface names, the refused same backup for file backup and archive.

```bash
git add src/maas_collector/rawdata/configprovider.py tests/test_config_backups.py docs/opensearch_config.md CHANGELOG.md
git commit -m "feat: :sparkles: Validate backups in configuration files" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Validation that never evaluates (B1, static)

**Files:**
- Create: `src/maas_collector/rawdata/extractor/static.py`, `tests/test_static_validation.py`
- Modify: `src/maas_collector/rawdata/configuration.py` (`build_collector_configuration` at :108), `src/maas_collector/rawdata/configprovider.py` (`validate_config_sources`), `src/maas_collector/rawdata/configstore.py` (constructor at :157, `publish` at :274), `docs/opensearch_config.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `build_extractor` (`configuration.py:85`) for its name resolution (`getattr(extractor, meta["class"])`), `BaseExtractor`.
- Produces (contract §2):

```python
def check_lambda_source(code: str) -> None
    # ValueError unless code is a str that ast.parse(code, mode="eval") reads as a Lambda with no default
    # argument (positional or keyword-only); the lambda is never evaluated
def check_extractor_static(extractor_meta: dict) -> None
    # ValueError: the class is not a concrete BaseExtractor of the extractor package; its "args" (plus an
    # extractor-level "converter_map") do not bind to the constructor signature; or a value under the
    # keys "python" or "rpython", found at any depth of args and converter_map, is not check_lambda_source-valid
    # (the message names the field)
build_collector_configuration(meta, configuration_class, register=True, evaluate=True)
    # evaluate=False: check_extractor_static(meta["extractor"]) instead of build_extractor; the configuration gets extractor=None
validate_config_sources(sources, config_classes_for, resolve_models=True, evaluate=True)
ConfigStore(client, index=DEFAULT_CONFIG_INDEX, evaluate: bool = True)   # report_index is added by Task 8
```

- [ ] **Step 1: Write the failing tests** in `tests/test_static_validation.py`:

```python
LAMBDA_ENTRY = lambda python: {"id_field": "", "routing_key": "", "interface_name": "ITF_A", "model": {"name": "ModelA", "index": "", "fields": []},
    "extractor": {"class": "CSVExtractor", "args": {"attr_map": ["a", {"field": "b", "python": python}]}}}

def test_a_lambda_is_accepted(): check_lambda_source("lambda row: row[0].upper()"); check_lambda_source("lambda x, *, y: x")

@pytest.mark.parametrize("code", ["x", "", "lambda x, y=1: x", "lambda x, *, y=1: x", "lambda x: x)(", "(lambda x: x)(1)",
                                  "__import__('os').system('true')", "[lambda x: x]", 5, None])
def test_check_lambda_source_refuses(code):
    with pytest.raises(ValueError): check_lambda_source(code)

def test_nothing_is_evaluated(monkeypatch, tmp_path):
    marker = tmp_path / "marker"
    def boom(*_a, **_k): raise AssertionError("evaluated")
    monkeypatch.setattr(builtins, "eval", boom); monkeypatch.setattr(builtins, "exec", boom)
    check_lambda_source(f"lambda x: open({str(marker)!r}, 'w')")                      # parsed, not run
    with pytest.raises(ValueError):                                                    # a default argument would run at definition
        check_lambda_source(f"lambda x, y=open({str(marker)!r}, 'w'): x")
    assert not marker.exists()

def test_the_extractor_class_must_be_a_concrete_extractor():   # "Nope", "get_hash_func", "BaseExtractor", "os" -> ValueError
def test_arguments_are_checked_against_the_signature():       # unknown arg -> its name; XMLExtractor without attr_map -> "attr_map"
def test_lambdas_are_found_in_lists_dicts_and_both_python_keys():
    check_extractor_static(LAMBDA_ENTRY("lambda r: r[0]")["extractor"])
    check_extractor_static({"class": "XMLExtractor", "args": {"attr_map": {"x": {"rpython": "lambda root: 1"}}}})
    with pytest.raises(ValueError, match="rpython|x"): check_extractor_static({"class": "XMLExtractor", "args": {"attr_map": {"x": {"rpython": "1 + 1"}}}})
def test_python_converters_are_checked_at_both_levels():
    ok = {"f": {"type": "python", "python": "lambda v: v"}}
    check_extractor_static({"class": "LogExtractor", "args": {"pattern": ".*"}, "converter_map": ok})
    check_extractor_static({"class": "LogExtractor", "args": {"pattern": ".*", "converter_map": ok}})
    with pytest.raises(ValueError, match="f"):
        check_extractor_static({"class": "LogExtractor", "args": {"pattern": ".*"}, "converter_map": {"f": {"type": "python", "python": "oops"}}})

def test_static_validation_never_evaluates(monkeypatch):
    monkeypatch.setattr(builtins, "eval", boom)
    validate_config_sources([ConfigSource("a.json", {"collectors": [LAMBDA_ENTRY("lambda r: r[0]")]})], config_classes_for, resolve_models=False, evaluate=False)
def test_the_default_mode_still_evaluates(monkeypatch):      # control: same call, evaluate left True, eval patched to raise -> ConfigValidationError
def test_static_validation_refuses_a_bad_lambda_with_the_source_label():   # match "a.json" and "python"
def test_static_validation_still_checks_the_rest():          # unknown "class": ConfigValidationError; bad model type; backups; '@'
def test_every_python_string_of_tests_conf_passes_statically():   # walk tests/conf/**/*.json for "python"/"rpython" strings -> check_lambda_source
def test_a_configstore_without_evaluation_publishes_and_rolls_back_statically(fake_opensearch, monkeypatch):
    store = ConfigStore(fake_opensearch, INDEX, evaluate=False); monkeypatch.setattr(builtins, "eval", boom)
    store.publish(PATH, {"collectors": [LAMBDA_ENTRY("lambda r: r[0]")]}, ["odata"], "alice", "v1")
    with pytest.raises(ConfigValidationError): store.publish(PATH, {"collectors": [LAMBDA_ENTRY("nope")]}, ["odata"], "alice", "v2")
    store.rollback(PATH, 1, "alice", "back")
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_static_validation.py`. Expected: FAIL (`ModuleNotFoundError: …extractor.static`).
- [ ] **Step 3: Implement.**
  - `static.py`: `check_lambda_source` with `ast.parse(code, mode="eval")` (a `SyntaxError` becomes `ValueError`; a non-string is a `ValueError`); `check_extractor_static` resolving the class with `getattr(extractor, meta["class"], None)` from `maas_collector.rawdata.extractor`, requiring `inspect.isclass`, `issubclass(…, BaseExtractor)` and `not inspect.isabstract`, binding `inspect.signature(cls).bind(**args)` with the extractor-level `converter_map` merged as `build_extractor` does (`TypeError` becomes a `ValueError` naming the argument), then a recursive walk of `args` and `converter_map` over dicts and lists applying `check_lambda_source` to every value under `python` or `rpython`, naming the field path in the message.
  - `configuration.py`: add the `evaluate` parameter; an entry without `extractor` is handled as today.
  - `configprovider.py`: pass `evaluate` to `build_collector_configuration`.
  - `configstore.py`: store `self.evaluate`; `publish` passes `evaluate=self.evaluate` (rollback goes through `publish`).
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_static_validation.py tests/test_configuration.py tests/test_configstore.py tests/test_lambda_namespace.py`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/opensearch_config.md`: add a `## Validation` section after `## Applying a change: the collector restarts` (the file has none): the two modes, and that the backend uses `ConfigStore(..., evaluate=False)`. `CHANGELOG.md`: a `### Safe configuration validation` section with a bullet on `evaluate=False`, `rawdata/extractor/static.py` and `ConfigStore(evaluate=…)`.

```bash
git add src/maas_collector/rawdata/extractor/static.py src/maas_collector/rawdata/configuration.py src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/configstore.py tests/test_static_validation.py docs/opensearch_config.md CHANGELOG.md
git commit -m "feat: :sparkles: Validate configurations without evaluating their lambdas" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Backup sinks and report fields (A1 local, A3 fields)

**Files:**
- Modify: `src/maas_collector/backup/backup.py` (`BackupReport` :16, `CollectorBackupConfiguration` :93, `new_report` :192), `src/maas_collector/backup/s3_backup.py` (:16), `src/maas_collector/backup/local_backup.py` (:27-47), `tests/test_backup.py`, `tests/test_backup_report.py`, `tests/test_backup_report_integration.py`, `tests/conf/backup_report/models.json`, `docs/backup.md`, `CHANGELOG.md`

**Interfaces:**
- Consumes: nothing.
- Produces:

```python
OK, KO, REPLACED, SKIPPED, REQUESTED = "OK", "KO", "REPLACED", "SKIPPED", "REQUESTED"   # in backup/backup.py
# BackupReport gains: origin: str = "collector", backup_name: str = "", operation_id: str = "",
#   product_id: str = "", product_name: str = "", written_md5: str = "", catalogue_checksum: str = ""
# CollectorBackupConfiguration gains: backup_name: str = ""    (the sinks put it in every report)
# CollectorBackupS3Configuration: s3_access_key and s3_secret_key get field metadata {"sensitive": True}, and a __repr__ masking them
# CollectorBackupLocal: no longer creates `directory`; KO (FileNotFoundError) when it is not an existing directory
```

- [ ] **Step 1: Write the failing tests** in `tests/test_backup.py`, and adapt the existing ones. First make the existing tests fit the new rule: the `local_backup` fixture (`tests/test_backup.py:43`) creates `tmp_path / "backup"` before building the sink; in `tests/test_backup_report_integration.py` the tests that set `directory` to `str(tmp_path / "backup")` create that folder first, and `test_extract_from_file_without_report_model_writes_nothing` asserts `os.path.isfile(tmp_path / "backup" / "BACKED_UP" / "some-report.log")` instead of `isdir`. In `tests/test_backup.py`, `tests/test_backup_report.py` and `docs/backup.md` replace the service-specific names, applying the longest first so that a prefix does not eat a longer name: `CDSE_S1_OCN_FILES` by `ITF_A_FILES`, then `CDSE_S1_OCN` by `ITF_A`; `cdse/sentinel-1/ocn` by `archive/ocn`; `cdse-backup-cold` by `bucket-b`, then `cdse-backup` by `bucket-a`. The destination path of `tests/test_backup_report.py:65` becomes `archive/ocn/2026/08/01/ITF_A/S1A_IW_OCN.SAFE`, and its `report_name="CDSE_S1_OCN.json"` (:129, :138) becomes `ITF_A.json`. `grep -in cdse` on these three files must print nothing, and `grep -n "bucket-a-cold" tests docs` must print nothing (it would show a wrong order). Then add the seven new report fields (`origin`, `backup_name`, `operation_id`, `product_id`, `product_name`, `written_md5`, `catalogue_checksum`, all `Keyword`) to `MODEL_META` of `tests/test_backup_report.py` and to `tests/conf/backup_report/models.json`.

```python
def test_a_local_backup_fails_when_its_directory_does_not_exist(tmp_path, source_file):
    backup = instanciate_collector_backup({"type": "BackupLocal", "directory": str(tmp_path / "missing")})
    [report] = backup.backup_file(FakeConfig(), source_file)
    assert (report.status, report.error_type) == ("KO", "FileNotFoundError") and "missing" in report.details
    assert not (tmp_path / "missing").exists()

def test_a_local_backup_whose_directory_is_a_file_fails(tmp_path, source_file): ...     # directory = an existing file: status KO

def test_a_local_backup_still_creates_the_folders_below_its_directory(local_backup, source_file, tmp_path):
    [report] = local_backup(calendar_tree=True).backup_file(FakeConfig(), source_file)
    assert report.status == "OK" and os.path.isfile(report.destination_path)
    assert report.destination_path.startswith(str(tmp_path / "backup") + "/20")

def test_the_report_carries_the_backup_name_and_the_collector_origin(local_backup, source_file):
    [report] = local_backup(backup_name="archive-a").backup_file(FakeConfig(), source_file)
    assert (report.backup_name, report.origin) == ("archive-a", "collector")
    assert local_backup().backup_file(FakeConfig(), source_file)[0].backup_name == ""

def test_every_s3_report_carries_the_backup_name(): ...        # build_s3_backup(backup_name="archive-b"), two buckets -> both reports
def test_new_report_fields_default_to_empty():
    report = BackupReport()
    assert (report.origin, report.backup_name, report.operation_id, report.product_id, report.product_name,
            report.written_md5, report.catalogue_checksum) == ("collector", "", "", "", "", "", "")
    assert (OK, KO, REPLACED, SKIPPED, REQUESTED) == ("OK", "KO", "REPLACED", "SKIPPED", "REQUESTED")
def test_report_to_dict_has_the_new_fields(): ...      # the seven keys are in BackupReport(origin="archive", ...).to_dict()
def test_backup_name_is_an_accepted_backup_key(tmp_path): ...   # instanciate_collector_backup with "backup_name"
def test_the_s3_configuration_masks_both_keys_in_its_repr():
    config = CollectorBackupS3Configuration(directory="d", s3_access_key="test-access-key", s3_secret_key="test-secret-key")
    assert "test-access-key" not in repr(config) and "test-secret-key" not in repr(config)
    fields = {f.name: f for f in dataclasses.fields(config)}
    assert fields["s3_access_key"].metadata["sensitive"] and fields["s3_secret_key"].metadata["sensitive"]
```

and in `tests/test_backup_report.py`:

```python
def test_build_action_source_carries_the_new_fields(model):
    report = BackupReport(origin="archive", backup_name="archive-a", operation_id="op1", product_id="p1", product_name="P1.zip",
                          written_md5="m", catalogue_checksum="c", status="OK", start_time=NOW)
    source = BackupReportWriter(model).build_action(report)["_source"]
    assert source["backup_name"] == "archive-a" and source["origin"] == "archive" and source["written_md5"] == "m"
def test_a_model_without_the_new_fields_still_warns(caplog): ...      # existing behaviour, with the old MODEL_META fields
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_backup.py tests/test_backup_report.py tests/test_backup_report_integration.py`. Expected: FAIL (`AttributeError`/`TypeError` on the new fields; the missing directory is still created).
- [ ] **Step 3: Implement.**
  - `backup.py`: the five status constants; the seven `BackupReport` fields in its "context" group, with their docstrings; `CollectorBackupConfiguration.backup_name: str = ""`; `new_report` passes `backup_name=self.args.backup_name or ""`.
  - `s3_backup.py`: `field(default=None, metadata={"sensitive": True})` on both keys, and a `__repr__` that prints `"*******"` for fields whose metadata is sensitive, as `FileCollectorConfiguration.__repr__` (`filecollector.py:196`) does.
  - `local_backup.py`: inside the `report_transfer` block, before `os.makedirs(parent_folder, …)`, raise `FileNotFoundError(f"backup directory {directory} does not exist")` when `not os.path.isdir(self.args.directory)`; `report_transfer` already turns an `OSError` into a KO report.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_backup.py tests/test_backup_report.py tests/test_backup_report_integration.py`, then `RUN`. Expected: all pass.
- [ ] **Step 5: Document and commit.**
  - `docs/backup.md`: the `FileSystem` section says `directory` must exist (a missing one is a KO report, "so an unmounted path does not silently fill the container's own disk"); the report fields table gains the seven fields with a line on `origin` (`collector` here, `archive` for plan 3's transfer stage) and the statuses.
  - `CHANGELOG.md`: a `### Backup reports` section: the new fields and statuses, and the filesystem backup no longer creating its `directory`.

```bash
git add src/maas_collector/backup tests/test_backup.py tests/test_backup_report.py tests/test_backup_report_integration.py tests/conf/backup_report/models.json docs/backup.md CHANGELOG.md
git commit -m "feat: :sparkles: Name the backup in its reports and keep the local directory out of its reach" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Resolve backup names at load (A2)

**Files:**
- Modify: `src/maas_collector/backup/named.py`, `src/maas_collector/rawdata/collector/filecollector.py` (`FileCollectorConfiguration` :124-178, `load_config` :387-444), `docs/backup.md`, `CHANGELOG.md`
- Test: `tests/test_backup_load.py`

**Interfaces:**
- Consumes: Task 2's `backup_definitions`, `resolve_backup`; Task 5's `CollectorBackupConfiguration.backup_name`.
- Produces:

```python
# backup/named.py
def apply_named_backup(config, definitions: dict[str, dict]) -> None
    # config.backup falsy: nothing. Otherwise (block, entry) = resolve_backup(config.backup, definitions):
    # config.backup = block; config.backup_name = the name when it was a str, else ""; config.backup_credentials_entry = entry or "".
    # An unknown name raises ValueError("interface <name>: backup <ref> is not defined by any file the service loads");
    # an inline block with a secret field raises ValueError (resolve_backup's)
# FileCollectorConfiguration (all init=False, so a configuration file naming one fails loudly):
#   backup_name: str = ""    backup_credentials_entry: str = ""
#   backup_credentials: dict = field(default_factory=dict, metadata={"sensitive": True})
```

- [ ] **Step 1: Write the failing tests** in `tests/test_backup_load.py` (helpers: `write(tmp_path, files: dict[str, dict]) -> Path` writes the JSON files under `tmp_path` and returns it, `load(path) -> FileCollector` builds `FileCollector(CollectorArgs(rawdata_config_dir=str(tmp_path)))` and calls `load_config()`):

```python
FILES = {"backups/archive-a.json": backups_file(LOCAL), "entries.json": {"collectors": [entry(backup="archive-a")]}}

def test_a_name_is_resolved_for_the_entry(tmp_path):
    config = load(write(tmp_path, FILES)).configs[0]
    assert config.backup == {"type": "BackupLocal", "directory": "/data/archive-a"}
    assert (config.backup_name, config.backup_credentials_entry) == ("archive-a", "")

def test_the_report_model_of_the_definition_is_resolved(tmp_path):    # report_model "ReportBackup" defined by a models file
    config = load(write(tmp_path, FILES | {"backups/archive-a.json": backups_file(LOCAL | {"report_model": "ReportBackup"}), "models.json": models_file("ReportBackup")})).configs[0]
    assert config.backup_report_model.__name__ == "ReportBackup"

def test_an_s3_name_gives_the_credentials_entry_and_no_secret(tmp_path):
    config = load(write(tmp_path, {"b.json": backups_file(S3), "e.json": {"collectors": [entry(backup="archive-b")]}})).configs[0]
    assert config.backup_credentials_entry == "backup-archive-b" and "credentials" not in config.backup and config.backup["buckets"] == ["bucket-a", "bucket-b"]

@pytest.mark.parametrize("enabled", [True, False])
def test_an_unknown_name_is_a_load_error_for_enabled_and_disabled_entries(tmp_path, enabled):
    with pytest.raises(ValueError, match="nope"): load(write(tmp_path, {"e.json": {"collectors": [entry(backup="nope", enabled=enabled)]}}))

def test_a_name_defined_twice_fails_the_load(tmp_path): ...
def test_an_inline_block_is_kept_and_has_no_name(tmp_path): ...       # backup_name == "", backup == the block
@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_an_inline_block_with_a_secret_fails_at_load_without_validation(tmp_path, field): ...   # ValueError match field
def test_a_definition_with_a_secret_fails_at_load(tmp_path): ...
@pytest.mark.parametrize("field", ["backup_name", "backup_credentials", "backup_credentials_entry"])
def test_the_resolved_fields_are_not_settable_from_json(tmp_path, field):
    assert not FileCollectorConfiguration.__dataclass_fields__[field].init
    with pytest.raises(TypeError): load(write(tmp_path, {"e.json": {"collectors": [entry() | {field: "x"}]}}))
def test_a_configuration_synced_from_the_index_resolves_like_the_files(tmp_path, fake_opensearch):
    store = ConfigStore(fake_opensearch); store.publish("backups/archive-a.json", backups_file(LOCAL), ["svc"], "a", "c")
    store.publish("entries.json", {"collectors": [entry(backup="archive-a")]}, ["svc"], "a", "c"); sync_configs(store, str(tmp_path / "copy"), ["svc"])
    assert load(tmp_path / "copy" / "svc").configs[0].backup_name == "archive-a"       # the operator worker's copy
def test_a_file_backed_up_through_a_name_writes_the_name_in_its_report(default_connection, tmp_path):
    # models: shutil.copy("tests/conf/backup_report/models.json", tmp_path / "models.json") (its `Backup` model has index "raw-data-backup")
    (tmp_path / "backup").mkdir()
    definition = LOCAL | {"directory": str(tmp_path / "backup"), "report_model": "Backup"}
    collector = load(write(tmp_path, {"backups/archive-a.json": backups_file(definition), "entries.json": {"collectors": [entry(backup="archive-a")]}}))
    # then as test_extract_from_file_stores_the_reports (test_backup_report_integration.py:92): patched parallel_bulk and backup_report.bulk
    source = bulk_mock.call_args.args[1][0]["_source"]
    assert (source["backup_name"], source["origin"], source["status"]) == ("archive-a", "collector", "OK")
def test_a_name_without_report_model_backs_up_and_reports_nothing(default_connection, tmp_path): ...    # same setup with no report_model: bulk_mock.call_count == 0 and the file exists under tmp_path/"backup"
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_backup_load.py`. Expected: FAIL (`AttributeError: … backup_name`).
- [ ] **Step 3: Implement.** The three fields on `FileCollectorConfiguration` right after `backup_report_model`, with docstrings; `apply_named_backup` in `named.py`; in `FileCollector.load_config`, keep every configuration built (`get_configs`) in a local list, enabled or not, then right after the source loop call `definitions = backup_definitions(source.content for source in sources)` and `apply_named_backup(config, definitions)` for each of them, before the model resolution (so `config.backup.get("report_model")` sees the resolved block). `backup_credentials` is masked by the existing `__repr__` through its metadata.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_backup_load.py tests/test_backup_report_integration.py tests/test_filecollector.py tests/test_config_refresh.py`, then `RUN`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/backup.md`: "Using a named backup": `"backup": "<name>"`, resolved at load for every entry (enabled or not), an unknown name is a load error, inline blocks stay accepted without secret fields. `CHANGELOG.md` `### Named backups`: the resolution at load and the refusal of inline keys at load ("a local file with `s3_access_key` or `s3_secret_key` in a backup block no longer loads: move them to a credential entry").

```bash
git add src/maas_collector/backup/named.py src/maas_collector/rawdata/collector/filecollector.py tests/test_backup_load.py docs/backup.md CHANGELOG.md
git commit -m "feat: :sparkles: Resolve named backups when a collector loads" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: S3 keys in credential entries, with rotation (A2)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/credentialmixin.py` (`credential_names` :114, `load_credentials` :127, `_apply_changed_credentials` :239), `src/maas_collector/rawdata/collector/filecollector.py` (`backup_and_report` :734), `docs/backup.md`, `CHANGELOG.md`
- Test: `tests/test_backup_credentials.py`

**Interfaces:**
- Consumes: Task 6's `backup_credentials_entry`, `backup_credentials`; `SECRET_FIELDS`; `CredentialProvider.load(names)` (`credentialprovider.py:37`).
- Produces:

```python
CredentialMixin.set_backup_credentials(config, credential_dict: dict) -> None   # staticmethod
    # no entry named: nothing. Entry absent from credential_dict: config.backup_credentials = {} and a warning naming the entry.
    # Entry with a field outside SECRET_FIELDS: ValueError naming the field(s), never a value.
    # Otherwise config.backup_credentials = {key: entry[key] for key in SECRET_FIELDS if key in entry}
# credential_names(): also getattr(config, "backup_credentials_entry", "") of each target
FileCollector.build_backup_block(self, config) -> dict
    # copy of config.backup + {"backup_name": config.backup_name} + config.backup_credentials;
    # ValueError("credential entry <entry> of backup <name> has no <field>") when an entry is named and either key is empty
```

`backup_and_report` builds its sink with `instanciate_collector_backup(self.build_backup_block(config))`, inside the existing `try`.

- [ ] **Step 1: Write the failing tests** in `tests/test_backup_credentials.py`. Fixtures: the autouse `no_database_nor_bus` of `tests/test_credential_refresh.py:88` and its `FakeClock`/`clock` (copy them); `rewrite_entry(tmp_path, entry: dict) -> None` rewrites the `backup-archive-b` entry of `tmp_path / "credentials.json"` as `{"name": "backup-archive-b", **entry}`; `vault_args` is a copy of the fixture of `tests/test_credential_refresh.py:319` (only its `VaultArgs` matters; `fake_vault` comes from `tests/conftest.py:42`); `start(tmp_path, entry=KEYS, **args)` passes `credential_file` only when `args` names no `credential_backend`, and writes `{"interfaces": [{"name": "backup-archive-b", **entry}]}` to `credentials.json` (an empty list when `entry` is `None`), the files of Task 6 with `S3` and an entry using `archive-b`, and returns a started `FileCollector` (`credential_file=…`, `load_config()`).

```python
def test_the_keys_go_to_backup_credentials_not_to_the_block(tmp_path):
    config = start(tmp_path).configs[0]
    assert config.backup_credentials == KEYS and "s3_secret_key" not in config.backup

def test_the_credential_names_include_the_backup_entry(tmp_path): assert "backup-archive-b" in start(tmp_path).credential_names()

def test_an_entry_with_another_field_fails_the_load_naming_it_but_never_its_value(tmp_path):
    with pytest.raises(ValueError, match="token") as error: start(tmp_path, entry=KEYS | {"token": "test-token-value"})
    assert "test-token-value" not in str(error.value) and "test-secret-key" not in str(error.value)

def test_the_block_is_built_on_a_copy_with_the_keys(tmp_path):
    collector = start(tmp_path); config = collector.configs[0]
    block = collector.build_backup_block(config)
    assert block["s3_secret_key"] == "test-secret-key" and block["backup_name"] == "archive-b" and "s3_secret_key" not in config.backup

def test_an_absent_entry_fails_the_backup_not_the_load(tmp_path, caplog):
    collector = start(tmp_path, entry=None)           # the entry is missing from the file
    with pytest.raises(ValueError, match="backup-archive-b"): collector.build_backup_block(collector.configs[0])
    assert "backup-archive-b" in caplog.text

@pytest.mark.parametrize("missing", SECRET_FIELDS)
def test_entry_with_one_key_only_fails_the_backup(tmp_path, missing):          # Review Focus 1
    collector = start(tmp_path, entry={k: v for k, v in KEYS.items() if k != missing})
    path = tmp_path / "f.log"; path.write_text("x")
    with patch("boto3.client") as client:
        collector.backup_and_report(collector.configs[0], str(path))
    client.assert_not_called()       # no client built, so no fallback on the ambient credentials; "Cannot backup" in caplog.text

def test_the_s3_client_gets_the_keys_of_the_entry_at_each_file(tmp_path):
    collector = start(tmp_path); path = tmp_path / "f.log"; path.write_text("x")
    with patch("boto3.client") as client: collector.backup_and_report(collector.configs[0], str(path))
    assert client.call_args.kwargs["aws_access_key_id"] == "test-access-key"

def test_rotation_reaches_the_keys(tmp_path, clock):
    collector = start(tmp_path); rewrite_entry(tmp_path, {"s3_access_key": "new-access", "s3_secret_key": "new-secret"})
    clock.now += 300; collector.refresh_credentials()
    assert collector.build_backup_block(collector.configs[0])["s3_secret_key"] == "new-secret"

def test_rotation_reaches_an_entry_without_any_interface_credentials(tmp_path, clock, caplog):   # nothing named interface_credentials
    ...   # same rotation; assert "No credential configuration found" not in caplog.text

def test_a_change_of_another_entry_leaves_the_keys_alone(tmp_path, clock): ...   # rotate an unrelated entry: backup_credentials == KEYS, no "Credentials changed for backup-archive-b" in caplog
def test_rotation_logs_no_key_value(tmp_path, clock, caplog): ...                 # "new-secret" and "test-secret-key" not in caplog.text
def test_an_inline_s3_block_naming_credentials_gets_its_keys(tmp_path): ...       # entry with an inline S3 block naming "backup-inline": its entry is read, backup_name == ""
def test_vault_entries_are_read_and_rotated(clock, fake_vault, vault_args, tmp_path):
    fake_vault.put("secret", "maas-collector/backup-archive-b", KEYS)
    collector = start(tmp_path, entry=None, credential_backend="vault", vault=vault_args)
    assert collector.configs[0].backup_credentials == KEYS
    fake_vault.put("secret", "maas-collector/backup-archive-b", {"s3_access_key": "new-access", "s3_secret_key": "new-secret"})
    clock.now += 300; collector.refresh_credentials()
    assert collector.build_backup_block(collector.configs[0])["s3_secret_key"] == "new-secret"
def test_a_vault_policy_denying_the_entry_stops_the_load(fake_vault, vault_args, tmp_path):     # as test_credential_refresh.py:407
    fake_vault.put("secret", "maas-collector/backup-archive-b", KEYS)
    fake_vault.denied.add("secret/maas-collector/backup-archive-b")
    with pytest.raises(PermissionError, match="backup-archive-b"):
        start(tmp_path, entry=None, credential_backend="vault", vault=vault_args)
def test_the_configuration_repr_masks_the_keys(tmp_path): assert "test-secret-key" not in repr(start(tmp_path).configs[0])
def test_a_failing_backup_logs_no_key(tmp_path, caplog):
    with patch("boto3.client", side_effect=RuntimeError("boom")): collector.backup_and_report(config, path)
    assert "Cannot backup" in caplog.text and "test-secret-key" not in caplog.text and "test-access-key" not in caplog.text
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_backup_credentials.py`. Expected: FAIL (`AttributeError: … set_backup_credentials` or `build_backup_block`).
- [ ] **Step 3: Implement.**
  - `credentialmixin.py`: `credential_names` adds the backup entry; `load_credentials` calls `set_backup_credentials` for every target after `set_credential_attributes`; `_apply_changed_credentials` restructured so that a target is updated when `changed` meets its interface names (existing path, including the "removed" warnings and `set_credential_attributes`) or its backup entry (`set_backup_credentials` only, no "No credential configuration" warning), and listed in `interface_names` in either case. A `ValueError` from a refresh is already caught by `refresh_credentials`' broad `except`, which keeps the current values.
  - `filecollector.py`: `build_backup_block` and its use in `backup_and_report`; `FileCollector.__init__` is untouched here.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_backup_credentials.py tests/test_credential_refresh.py tests/test_commun_credentials.py tests/test_credential_provider.py`, then `RUN`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/backup.md`: replace the inline `s3_access_key` / `s3_secret_key` of the S3 example by `"credentials": "backup-<name>"`, add the "Credentials" section (an entry of the credential file or of the vault named `backup-…`, holding exactly `s3_access_key` and `s3_secret_key`, refreshed with the others, any other field refused at load naming it) and an example credential file entry with placeholder values. `CHANGELOG.md` `### Named backups`: the entries, the rotation, the masked repr.

```bash
git add src/maas_collector/rawdata/collector/credentialmixin.py src/maas_collector/rawdata/collector/filecollector.py tests/test_backup_credentials.py docs/backup.md CHANGELOG.md
git commit -m "feat: :sparkles: Read the S3 keys of a backup from a credential entry" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Destination guard and reference integrity inside `ConfigStore` (A1, A2)

**Files:**
- Modify: `src/maas_collector/rawdata/configstore.py` (constructor :157, `publish` :226-320), `tests/fakeopensearch.py` (`search` :115), `docs/opensearch_config.md` (Roles, new "Backups" section), `CHANGELOG.md`
- Test: `tests/test_configstore_backups.py`

**Interfaces:**
- Consumes: Task 2's `destination_changes`, `backup_definitions`; Task 3's `integrity_problems`, `ConfigIntegrityError`, `BackupDestinationError`; `ConfigStore.latest` (:212), `history` (:189).
- Produces:

```python
ConfigStore(client, index=DEFAULT_CONFIG_INDEX, evaluate: bool = True, report_index: str = "raw-data-backup")
ConfigStore._check_destinations(self, path, content, history) -> None      # BackupDestinationError
ConfigStore._has_reports(self, backup_name: str) -> bool
ConfigStore._check_integrity(self, path, content, services, deleted, history) -> None   # ConfigIntegrityError
# publish, rollback (through publish) and delete (through publish) call both, after the existing validation and
# service checks and before the document is indexed; nothing is written when one raises.
# tests/fakeopensearch.py: add_report_document(client, index, backup_name, **fields); search() expands a "*" index name
# over the existing indices and a wildcard matching none answers no hit (real OpenSearch does) instead of NotFoundError
```

Semantics:
- **Destination guard** (content not a tombstone): the previous definitions are those of the last non-deleted version of the path (a tombstone is skipped, so delete then re-create is compared with what the tombstone replaced). For each new definition whose name was defined there with `destination_changes(old, new)` not empty, and `_has_reports(name)`: raise `BackupDestinationError(path, f"backup {name}: {', '.join(changes)} cannot change once the backup has reports; derive a new backup instead")`. A name that the previous definitions of this path do not hold (a new name, or a name moved from another path, for example `backups/archive-a2.json` re-creating a deleted `archive-a` with another directory) is not compared with anything: when `_has_reports(name)` is true it raises `BackupDestinationError(path, f"backup {name}: the name already has reports; a backup keeps its file path, derive a new backup under a new name")`. Reports are matched on the name alone, so a same-named backup of another service that has reports also blocks (accepted, conservative; the backend's "name matches path" check does not protect `cli.config`). `_has_reports` runs `client.search(index=f"{report_index}*", body={"size": 1, "_source": False, "query": {"term": {"backup_name": name}}})` and is true when a hit comes back; `NotFoundError` is "no reports"; any other error propagates (the change is refused when the report index cannot be read). It runs only when a destination key changed or a definition's name is new to its path.
- **Integrity**: the affected services are the services of the new version (unless a tombstone) and those of the current version (unless it is a tombstone), so untagging, deleting and renaming are covered. For each, the sources are the current non-deleted documents of that service (`self.latest(service=…)`) except `path`, plus `ConfigSource(path, content)` when the write keeps the service. Any `integrity_problems` lines raise `ConfigIntegrityError(path, [f"service {service}: {line}", …])`.

- [ ] **Step 1: Write the failing tests** in `tests/test_configstore_backups.py`. Helpers: `INDEX`, `BACKUPS = "backups/archive-a.json"`, `ENTRIES = "odata/entries.json"`, `put(store, path, content, services=("odata",), **kwargs)` (author `alice`, comment `test`), a `store` fixture `ConfigStore(fake_opensearch, INDEX)`, `history_length(store, path)`.

```python
def test_a_backup_and_an_entry_using_it_publish_in_that_order(store):
    put(store, BACKUPS, backups_file(LOCAL)); put(store, ENTRIES, {"collectors": [entry(backup="archive-a")]})

# --- integrity
def test_an_entry_naming_an_unknown_backup_is_refused_and_nothing_is_written(store):
    with pytest.raises(ConfigIntegrityError) as error: put(store, ENTRIES, {"collectors": [entry(backup="nope")]})
    assert "ITF_A" in error.value.problems[0] and "nope" in error.value.problems[0] and store.history(ENTRIES) == []
def test_a_backup_tagged_for_another_service_only_does_not_serve(store): ...             # refused
def test_deleting_a_backup_still_used_is_refused_even_by_a_disabled_entry(store):   # enabled False entry; store.delete(BACKUPS, ...)
def test_untagging_a_service_that_uses_the_backup_is_refused_and_one_that_does_not_is_accepted(store):
    # backup tagged ["odata", "jirax"], entries only under odata: put(...services=["odata"]) accepted; with an entry under jirax: refused
def test_renaming_a_backup_used_by_an_entry_is_refused(store): ...
def test_rolling_back_to_a_version_without_the_backup_is_refused(store): ...    # v1 defines archive-a, v2 archive-b used by an entry, rollback to 1
def test_rolling_back_to_a_tombstone_is_refused_while_the_backup_is_used(store): # v1 define, v2 delete, v3 define again + entry, rollback(BACKUPS, 2)
def test_an_archive_rule_naming_an_unknown_backup_is_refused(store): ...   # the entry is entry(archive={"backup": "gone"}, **{"class": ODATA}), expected ConfigIntegrityError
def test_the_backup_may_live_in_another_file_of_the_service(store): ...
def test_a_report_model_must_be_defined_by_the_service(store):
    with pytest.raises(ConfigIntegrityError, match="ReportM"): put(store, BACKUPS, backups_file(LOCAL | {"report_model": "ReportM"}))
    put(store, "odata/models.json", models_file("ReportM")); put(store, BACKUPS, backups_file(LOCAL | {"report_model": "ReportM"}))
    with pytest.raises(ConfigIntegrityError, match="ReportM"): store.delete("odata/models.json", "alice", "c")
def test_an_inline_model_of_an_entry_of_another_file_defines_the_report_model(store): ...
def test_a_name_defined_in_two_files_of_one_service_is_refused_but_not_across_services(store): ...
def test_every_write_runs_the_check_on_the_services_it_affects_only(store, monkeypatch):
    with monkeypatch.context() as bypass:                 # a dangling reference already stored for service jirax
        bypass.setattr(store, "_check_integrity", lambda *args, **kwargs: None)
        put(store, "jirax/entries.json", {"collectors": [entry("ITF_J", backup="archive-a")]}, services=("jirax",))
    put(store, "odata/entries.json", {"collectors": [entry("ITF_A")]})     # accepted: service jirax is not scanned
    with pytest.raises(ConfigIntegrityError, match="service jirax"):      # a write that does affect jirax is refused
        put(store, "jirax/other.json", {"collectors": [entry("ITF_K")]}, services=("jirax",))

# --- destination guard
@pytest.mark.parametrize("change", [{"directory": "/data/other"}, {"calendar_tree": True}, {"enable_gzip": True},
                                    {"s3_endpoint_url": "https://other.example"}, {"buckets": ["bucket-a"]}])
def test_a_destination_change_is_refused_once_the_backup_has_reports(store, fake_opensearch, change):
    put(store, BACKUPS, backups_file(S3)); add_report_document(fake_opensearch, "raw-data-backup-2026", "archive-b")
    with pytest.raises(BackupDestinationError, match=next(iter(change))): put(store, BACKUPS, backups_file(S3 | change))
def test_the_same_changes_are_accepted_while_there_is_no_report(store): ...
def test_extending_a_backup_with_reports_is_accepted(store, fake_opensearch):
    # description, report_model (defined), an added bucket, credentials: all accepted
def test_reports_of_another_backup_do_not_block(store, fake_opensearch): ...
def test_reports_in_an_older_yearly_partition_block_a_destination_change(store, fake_opensearch):   # Review Focus 2
    add_report_document(fake_opensearch, "raw-data-backup-2025", "archive-b")      # nothing in raw-data-backup-2026
def test_the_guard_applies_to_a_rollback(store, fake_opensearch): ...        # v1 dir X, v2 dir Y, reports appear, rollback to 1 refused
def test_delete_then_recreate_with_another_destination_is_refused_and_with_the_same_is_accepted(store, fake_opensearch): ...
def test_a_name_with_reports_cannot_move_to_another_path(store, fake_opensearch):
    put(store, BACKUPS, backups_file(LOCAL)); add_report_document(fake_opensearch, "raw-data-backup-2026", "archive-a")
    store.delete(BACKUPS, "alice", "c")
    with pytest.raises(BackupDestinationError, match="already has reports") as error:
        put(store, "backups/archive-a2.json", backups_file(LOCAL | {"directory": "/data/other"}))
    assert "archive-a" in str(error.value) and store.history("backups/archive-a2.json") == []
def test_a_new_name_is_accepted_without_reports_and_blocked_by_reports_of_another_service(store, fake_opensearch):
    put(store, "backups/archive-c.json", backups_file(LOCAL | {"name": "archive-c"}))          # no reports: accepted
    add_report_document(fake_opensearch, "raw-data-backup-2026", "archive-d")                   # written by a backup of service jirax
    with pytest.raises(BackupDestinationError, match="already has reports"):
        put(store, "backups/archive-d.json", backups_file(LOCAL | {"name": "archive-d"}))
def test_a_missing_report_index_means_no_reports(store): ...                    # the fake has no raw-data-backup* index at all
def test_an_unreadable_report_index_refuses_the_change(store, fake_opensearch, monkeypatch):
    # patch fake_opensearch.search to raise AuthorizationException(403, "forbidden") when the index starts with "raw-data-backup"
    with pytest.raises(AuthorizationException): put(store, BACKUPS, backups_file(S3 | {"directory": "other"}))
def test_the_report_index_is_configurable(fake_opensearch): ...                 # ConfigStore(client, INDEX, report_index="custom-reports")
def test_ordinary_writes_do_not_query_the_report_index(store, fake_opensearch, monkeypatch):
    # spy on fake_opensearch.search: publishing an entries file and a description-only edit never search an index starting with "raw-data-backup"
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_configstore_backups.py`. Expected: FAIL (`ImportError: cannot import name 'add_report_document'`, then unguarded writes succeeding).
- [ ] **Step 3: Implement** as specified above; `publish` calls `_check_destinations` (when not a tombstone) and `_check_integrity` after `check_service` of each service and before computing the version document. The wildcard in `FakeOpenSearch.search` uses `fnmatch` over `self.documents`; `add_report_document` indexes a document `{"backup_name": …, **fields}` under a generated id.
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_configstore_backups.py tests/test_configstore.py tests/test_configsync.py tests/test_config_cli.py tests/test_config_provider.py`, then `RUN`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/opensearch_config.md`: a "Backups" section (one file per backup, extend versus derive, the destination keys, the guard querying `raw-data-backup*` so the account of `config` and of the backend needs read access to that index, reports matched on the name alone (a same-named backup of another service with reports also blocks: accepted, conservative; a name with reports cannot move to another path), the integrity refusals and their message), and a line in "Roles". `CHANGELOG.md` `### Named backups`: the guard and the integrity check, "in `ConfigStore`, so `cli.config` and the backend share them".

```bash
git add src/maas_collector/rawdata/configstore.py tests/fakeopensearch.py tests/test_configstore_backups.py docs/opensearch_config.md CHANGELOG.md
git commit -m "feat: :sparkles: Guard backup destinations and references in the configuration store" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: A replay fails when its files could not be backed up (A4)

**Files:**
- Modify: `src/maas_collector/rawdata/collector/filecollector.py` (constants :53-56, `__init__` :302, `backup_and_report` :734), `src/maas_collector/rawdata/cli/odata.py` (:47-48), `src/maas_collector/rawdata/cli/jirax.py` (:38-39), `src/maas_collector/rawdata/operator/handler.py` (`describe_failure` :107), `tests/test_interface_isolation.py` (:217-230), `tests/test_jirax_replay.py` (:202-215), `tests/operator/test_handler.py`, `docs/replay.md` (:53), `docs/operator_worker.md` (:108), `CHANGELOG.md`
- Test: `tests/test_backup_replay_failure.py`

**Interfaces:**
- Consumes: Task 5's `OK`; Task 7's `build_backup_block`; `ReplayArgs` (`rawdata/replay.py`).
- Produces (contract §2):

```python
BACKUP_FAILED_EXIT_CODE = 74            # filecollector.py, next to REPLAY_FAILED_EXIT_CODE = 1 and CONFIG_CHANGED_EXIT_CODE = 75
BACKUP_FAILURE_FORMAT = "backup failed: {count} files to {backup}"
BACKUP_FAILURE_LINE = re.compile(r"backup failed: (?P<count>\d+) files to (?P<backup>.+)")
FileCollector.backup_failures: dict[str, int]                  # backup label -> files, filled in replay mode only
FileCollector.count_backup_failure(self, config) -> None       # label = config.backup_name or "(inline)"
FileCollector.backup_failure_line(self) -> str                 # BACKUP_FAILURE_FORMAT with the total and the sorted labels joined by ", "
CollectorReplayHandler.describe_failure(code, tail) -> str
    # BACKUP_FAILED_EXIT_CODE: "<N> files could not be backed up to <backup>\n<tail>", the line searched from the end of the tail;
    # without the line: "files could not be backed up\n<tail>"; other codes unchanged
```

Counting rule, in `backup_and_report`: when `self.args.replay` is set, a file counts once when its backup raised ("Cannot backup" path, no report needed) or when, for the file or its sidecar, `len(batch) != len(self._backup.destinations)` or a report is not `OK`. Failing to store the reports is not a backup failure. Both CLIs, after the existing `replay_failures` exit: `if collector.backup_failures: print(collector.backup_failure_line(), flush=True); sys.exit(BACKUP_FAILED_EXIT_CODE)`; when `replay_failures` is set the line is printed first and the code stays 1.

- [ ] **Step 1: Write the failing tests** in `tests/test_backup_replay_failure.py` (reuse the `default_connection` fixture and the `parallel_bulk` patch of `tests/test_backup_report_integration.py:35,92`; `replay_collector(tmp_path, backup: dict, report_model: bool = False, replay: bool = True) -> FileCollector` writes `backup` as the definition `archive-a` and an entry using it like Task 6 (with the `Backup` model of `tests/conf/backup_report/models.json` copied beside them when `report_model`), loads it, and sets `collector.args.replay = ReplayArgs(["ITF_A"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")` when `replay`; `ingest(collector, tmp_path) -> None` writes `tmp_path / "some-report.log"` and calls `collector.extract_from_file(path, collector.configs[0])` with `parallel_bulk` patched to return `[]` and `backup_report.bulk` patched to return `(1, [])`):

```python

def test_a_failed_transfer_is_counted_in_replay(tmp_path):           # BackupLocal on a missing directory: a KO report
    assert replay_collector(...).backup_failures == {"archive-a": 1}   # after ingest
def test_one_ko_bucket_out_of_two_counts_the_file_once(tmp_path): ...    # patch boto3.client, upload_file failing for bucket-b
def test_a_failure_without_any_report_is_counted(tmp_path):              # instanciate_collector_backup raises; no report_model
def test_a_sidecar_that_fails_counts_its_file_once(tmp_path): ...       # the file and its sidecar both KO: backup_failures == {"archive-a": 1}
def test_a_successful_backup_is_not_counted(tmp_path): ...             # backup_failures == {}
def test_live_mode_counts_nothing(tmp_path): ...                           # replay=False, same failing backup: backup_failures == {}
def test_failing_to_store_the_reports_is_not_a_backup_failure(tmp_path): ...   # backup_report.bulk raises, transfers OK: backup_failures == {}
def test_the_failure_line(tmp_path):
    collector.backup_failures = {"archive-b": 1, "archive-a": 2}
    assert collector.backup_failure_line() == "backup failed: 3 files to archive-a, archive-b"
def test_the_exit_codes_are_distinct(): assert len({1, 75, BACKUP_FAILED_EXIT_CODE}) == 3 and BACKUP_FAILED_EXIT_CODE == 74

REPLAY_ARGV = ["--replay-interface-name", "A", "--replay-start-date", "2026-01-01T00:00:00Z", "--replay-end-date", "2026-01-02T00:00:00Z"]

@pytest.mark.parametrize("cli, module, collector_class", [(odata_collector_main, "odata", "ODataCollector"),
                                                          (jirax_collector_main, "jirax", "JIRAExtendedCollector")])
def test_the_cli_exits_with_the_backup_code_and_prints_the_line_last(cli, module, collector_class, capsys):
    with patch(f"maas_collector.rawdata.cli.{module}.{collector_class}") as klass:
        collector = klass.return_value; collector.replay_failures = []; collector.backup_failures = {"archive-a": 3}
        collector.backup_failure_line.return_value = "backup failed: 3 files to archive-a"
        with pytest.raises(SystemExit) as exit_info: cli(REPLAY_ARGV)
    assert exit_info.value.code == BACKUP_FAILED_EXIT_CODE and capsys.readouterr().out.splitlines()[-1] == "backup failed: 3 files to archive-a"
def test_replay_failure_wins_but_the_backup_line_is_printed(capsys): ...      # Review Focus 3: replay_failures ["A"] and backup_failures {...}: code 1, the line is the last stdout line
def test_without_failures_the_cli_returns(): ...      # both empty: cli(REPLAY_ARGV) returns without SystemExit
```

and in `tests/operator/test_handler.py`:

```python
def test_describe_failure_names_the_count_and_the_backup():
    text = CollectorReplayHandler.describe_failure(BACKUP_FAILED_EXIT_CODE, "collected\nbackup failed: 3 files to archive-a")
    assert text.startswith("3 files could not be backed up to archive-a\n") and "collected" in text
def test_describe_failure_finds_the_line_among_later_output():               # Review Focus 3
    tail = "backup failed: 3 files to archive-a\nINFO exiting\nWARNING closing connection"
    assert CollectorReplayHandler.describe_failure(BACKUP_FAILED_EXIT_CODE, tail).startswith("3 files could not be backed up to archive-a")
def test_describe_failure_without_the_line_falls_back():
    assert "files could not be backed up" in CollectorReplayHandler.describe_failure(BACKUP_FAILED_EXIT_CODE, "boom")
def test_describe_failure_keeps_the_other_codes():  # 1 -> "an interface failed during the replay", 9 -> "exit code 9"
```

Also set `collector.backup_failures = {}` on the `Mock` collectors of the existing CLI tests (`tests/test_interface_isolation.py:217-230`, `tests/test_jirax_replay.py:202-215`), otherwise the `Mock` attribute is truthy and the CLI exits.

- [ ] **Step 2: Run the tests to verify they fail.** Run: `RUN tests/test_backup_replay_failure.py`. Expected: FAIL (`ImportError: cannot import name 'BACKUP_FAILED_EXIT_CODE'`).
- [ ] **Step 3: Implement** the constants, the counting in `backup_and_report` (the "Cannot backup" `except` marks the file failed), `count_backup_failure`, `backup_failure_line`, `self.backup_failures = {}` in `__init__`, the two CLI blocks, and `describe_failure` (scan `tail.splitlines()` from the end for `BACKUP_FAILURE_LINE.fullmatch`).
- [ ] **Step 4: Run the tests to verify they pass.** Run: `RUN tests/test_backup_replay_failure.py tests/test_interface_isolation.py tests/test_jirax_replay.py tests/test_enabled_flag.py tests/operator/test_handler.py`, then `RUN`. Expected: all pass.
- [ ] **Step 5: Document and commit.** `docs/replay.md` "Exit status": status 74 and the last line. `docs/operator_worker.md` (Replays, the exit code bullet): `N files could not be backed up to <backup>`. `CHANGELOG.md` `### Backup reports`: a replay exits with 74 when files could not be backed up, after collecting everything it could.

```bash
git add src/maas_collector/rawdata/collector/filecollector.py src/maas_collector/rawdata/cli/odata.py src/maas_collector/rawdata/cli/jirax.py src/maas_collector/rawdata/operator/handler.py tests/test_backup_replay_failure.py tests/test_interface_isolation.py tests/test_jirax_replay.py tests/operator/test_handler.py docs/replay.md docs/operator_worker.md CHANGELOG.md
git commit -m "feat: :sparkles: End a replay in failure when its files could not be backed up" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: `config import` order, and the closing checks

**Files:**
- Modify: `src/maas_collector/rawdata/cli/config.py` (`import_command`, the loop at :267), `docs/opensearch_config.md` ("Moving to the index"), `CHANGELOG.md`
- Test: `tests/test_config_cli.py`

**Interfaces:**
- Consumes: Task 8's integrity check (a write is refused while its references are missing).
- Produces: `import_command` publishes, in this order: files with neither `collectors` nor `backups` (models, amqp), then files with `backups`, then the others, each group by path.

- [ ] **Step 1: Write the failing test** in `tests/test_config_cli.py`:

```python
def test_import_publishes_backups_before_the_entries_using_them(env, store, tmp_path, capsys):   # Review Focus 4
    root = tmp_path / "collector"
    write_json(root / "odata" / "airbus" / "entries.json", {"collectors": [entry(backup="archive-a")]})    # sorts before "backups/"
    write_json(root / "odata" / "backups" / "archive-a.json", backups_file(LOCAL | {"report_model": "ReportM"}))
    write_json(root / "odata" / "zmodels.json", models_file("ReportM"))                                    # sorts last
    assert config_main(["import", str(root)]) == 0
    assert [store.get(p)["version"] for p in ("zmodels.json", "backups/archive-a.json", "airbus/entries.json")] == [1, 1, 1]
def test_a_dry_run_does_not_need_the_order(env, store, tmp_path): ...
```

(use the module's `config_main` invocation style of `test_import_merges_the_services_of_a_shared_path`, `tests/test_config_cli.py:103`).

- [ ] **Step 2: Run the test to verify it fails.** Run: `RUN tests/test_config_cli.py`. Expected: FAIL (`refused airbus/entries.json: …backup archive-a, which is not defined`).
- [ ] **Step 3: Implement** the sort key of the `for path in sorted(files)` loop of `import_command`.
- [ ] **Step 4: Run the closing checks.**
  - `RUN tests/test_config_cli.py`. Expected: all pass.
  - `RUN` (whole suite, `tests/operator` included in the worktree venv). Expected: no failure.
  - `grep -rIin "cdse" src/maas_collector/backup src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/configstore.py src/maas_collector/rawdata/extractor/static.py tests/backup_samples.py tests/test_backup*.py tests/test_config_backups.py tests/test_configstore_backups.py tests/test_static_validation.py docs/backup.md`. Expected: no output.
  - `git status --short`. Expected: nothing left uncommitted.
  - `git log --oneline -12`. Expected: one commit per task, subjects as in this plan.
- [ ] **Step 5: Document and commit.** `docs/opensearch_config.md` "Moving to the index": `import` publishes models, then backups, then entries; a report model defined inline by an entry that uses the backup naming it cannot be imported, put it in a `models` file. `CHANGELOG.md`: one sentence in `### Named backups`.

```bash
git add src/maas_collector/rawdata/cli/config.py tests/test_config_cli.py docs/opensearch_config.md CHANGELOG.md
git commit -m "fix: :bug: Import the backups before the entries that use them" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

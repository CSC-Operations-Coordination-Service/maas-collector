# Custom Recollects and the Configuration Schema (maas-collector) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the operator worker replay an interface with a bounded set of overridden keys and an archive choice, on custom lanes that a worker without them can never serve as a live replay, and let maas-collector describe its own configuration for the builder.

**Architecture:** A pure module (`rawdata/operator/overrides.py`) holds the allowlist, the structural window checks and `merge_overrides`. `CollectorReplayHandler` (on maas-engine 3.2.0's generic `ReplayWorker`) recognises a custom request by its `@custom` interface name, accepts it only on a custom-capable lane, applies it to the interface's entry in the configuration copy, validates the copy statically and starts the child under the real name. Custom lanes are extra maas-engine `Lane`s with their own queues; the handler shares one lock per plain lane ("slot") with its custom lane. A second pure module (`rawdata/configschema.py`) reflects the configuration classes, the extractors and the shapes of the other file parts into one JSON-able schema.

**Tech Stack:** Python 3.11, maas-engine 3.2.0 (`maas_engine.replay`), kombu 5 (`memory://` in tests), dataclasses and `inspect` reflection, pytest with `fake_opensearch`.

**Spec:** `docs/superpowers/specs/2026-10-29-maas-operator-backups-recollect-dm-design.md` (B1, B3a to B3d, the generic single-lane mode of C6) and **Contracts:** `docs/superpowers/plans/2026-10-29-archive-0-contracts.md` (§3)

**Contract deviation:** none for the names, bodies and routes of §3, which are used as written. One addition to a contract shape: the entry's `archive` gains `backup_version` and `force`, written by `prepare` (contracts §1 lists neither; plan 3's `ArchiveRule` reads them; they are never editable, so `archive_rule()` does not describe them). The plan **adds** these names (nothing renamed):
- `overrides.py`: `DEFAULT_ODATA_DATE_FIELD`, `ARCHIVE_CHOICE_KEYS`, `normalize_archive_choice`.
- `operator/lanes.py` (new; `contract.py` stays free of engine lane names): `SINGLE_LANE`, `CUSTOM_LANE_PREFIX`, `custom_lane_name`, `slot_of`, `accepts_custom`, `OperatorLane`, `OperatorReplayWorker`, `build_lanes`.
- `handler.py`: `custom_part`, `CustomPart`, `Prepared` gains `interface_name` and `slot`; `CollectorReplayHandler` gains `credential_provider`.
- `rawdata/replay.py`: `ReplayArgs.requested_by`; CLI `--replay-requested-by` / `REPLAY_REQUESTED_BY` (plan 3 assumes exactly this name).
- `configschema.py`: `UNMARKED_SECRET_KEYS`, `NOT_SECRET_KEYS`, `TYPE_OVERRIDES`, `GROUPS`, `config_classes`, `looks_like_credential`, `unclassified_credential_fields`, `entry_class_name`, `service_class_of`. The schema gains the top-level key `shapes` (exact structure in Task 13: plan 5's `ConfigSchema` type must read it), the descriptor keys `choices` (on enumerated keys) and `extra_args` (on an extractor class whose constructor takes `**kwargs`), and the pseudo-key `class` in every collector class's `keys`.
- `OperatorSettings.single_lane`, CLI `--operator-single-lane` / `OPERATOR_SINGLE_LANE`.

**Base and line numbers:** this plan starts from plan 1's branch. The line numbers below are those of 3.13.0 (`origin/develop`, before plan 1; checked against `handler.py`, `operator/replay.py`, `contract.py`, `docs/operator_worker.md`); find a symbol by name when a line moved. Plan 1 and plan 3 also edit `handler.py` (`describe_failure`), `replay.py` (`build_command`) and `configprovider.py`: never rewrite those functions, edit only what a step names.

## Global Constraints

- **No service-specific code (user decision):** maas-collector code, tests, docs and defaults never name a particular service or interface, nor a service-specific host, default or lane choice. Which service runs single-lane is deployment configuration (plan 6). Tests use `svc`, `odata`, `jirax`, `ITF_A`, `archive-a`.
- **Allowlist (B1):** odata `odata_query_filter`, `download_file_pattern`, `product_per_page` (1 to the live value or 1000, whichever is higher), `interface_credentials` (an entry that exists for the service); jira `jql_query`, `interface_credentials`. "Every other key stays live: identity, model, routing, exchange, extractor, URLs and hosts, `custom_query_suffix`, offsets, backups."
- **The window stays (B1):** `odata_query_filter`: the top-level `and` terms must include `<date field> ge {publication_start_date}` and `<date field> le {publication_end_date}`, the date field being the entry's (`filter_date_v4`, `PublicationDate` by default); no `or` at parenthesis depth 0 outside quoted strings. `jql_query`: the clause `updated > {date_criteria}` in a top-level `AND` term, and no `OR` at depth 0 outside quotes before any `ORDER BY`.
- **Names (B3a, B3b, contracts §3):** lanes `short`, `long`; custom lanes `custom-short`, `custom-long`; single-lane mode (`OPERATOR_SINGLE_LANE=true`) one lane `single`. Custom queue `maas-operator-<service>-replay-custom-<lane>`, bound only to `maas.operator.<service>.replay-custom.<lane>`. A custom request's `interface_name` is `<interface>@custom`, the real name in the body's `interface`; `CUSTOM_SUFFIX = "@custom"`.
- **Anti-goals that bind this plan:** a custom recollect run by a worker that does not understand it, as a live replay; an unbounded replay (a custom query that escapes the replay window); a recollect that writes what the live interface would not, or sends credentials to another host; operator-supplied code evaluated by the worker's parent process; a secret field readable back.
- **Static validation:** `prepare` validates the copied files with plan 1's `validate_config_sources(sources, config_classes_for, resolve_models=True, evaluate=False)`; nothing is evaluated in the worker's process.
- **Consumed from plan 1:** `maas_collector.backup.named` (`BACKUP_NAME`, `MAX_NAME_LENGTH`, `CREDENTIAL_PREFIX`, `SECRET_FIELDS`), `validate_config_sources(..., evaluate=False)`, `ConfigValidationError`.
- **Environment (tests need maas-engine 3.2.0, the `operator` extra):** `tests/operator` imports `maas_engine`. `setup.cfg` already pins `maas-engine>=3.1.0.post1.dev0,<4` in the `operator` and `testing` extras, so any install of `.[testing]` that resolves 3.2.0 works. Build the venv once:

  ```bash
  cd /home/aagazzi/work/maas-collector-satu
  python3.11 -m venv .venv                # .venv*/ is git-ignored
  # Option A, the Nexus PyPI index (as the CI does; PIP_INDEX_URL carries the credentials from ~/.netrc):
  .venv/bin/pip install -e ".[testing]"
  # Option B, no Nexus: the sibling checkouts, without touching their worktrees. maas-model FIRST: maas-engine 3.2.0 requires
  # maas-model[vault]>=3.1.0.dev0, which public PyPI does not carry, so installing the engine before it fails
  .venv/bin/pip install -e "/home/aagazzi/work/maas-model[vault]"
  mkdir -p ~/.cache/maas-engine-3.2.0 && git -C /home/aagazzi/work/maas-engine archive 3.2.0 | tar -x -C ~/.cache/maas-engine-3.2.0
  SETUPTOOLS_SCM_PRETEND_VERSION=3.2.0 .venv/bin/pip install ~/.cache/maas-engine-3.2.0
  .venv/bin/pip install -e ".[testing]"
  .venv/bin/python -c "import maas_engine.replay as r; print(r.ReplayWorker)"   # must print the class
  ```

  Baseline before any task: `TZ=UTC .venv/bin/python -m pytest tests/operator -q --no-cov` passes; note the count it prints (it changes with plan 1; no number is asserted here). Every test command below is run from the repository root with `TZ=UTC` (several tests compare naive timestamps).
- **Commit style:** `feat: :sparkles: ...`, `test: :white_check_mark: ...`, `docs: :memo: ...`; every commit message ends with a blank line then `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (`-m "subject" -m "Co-Authored-By: ..."`). `CHANGELOG.md` entries go under `## Unreleased`.

## Review Focus

The five inputs and failure modes most likely to bite a person using this, most likely first. Each is pinned by a test of the task that owns the code.

1. **A filter pasted from a text area** (newlines, double spaces, `AND`/`Or` in capitals) is neither refused when it is fine nor accepted when it escapes. Task 1: `test_window_checks_ignore_whitespace_and_keyword_case`.
2. **`&` or `#` in a query** (`contains(Name,'A&B')`, a pasted URL) ends the `$filter` in the raw URL the collector builds (`odata/query_strategy.py:294-306`): a second `$filter` or a `$top` rides in and the window is gone. Task 1: `test_odata_filter_cannot_carry_query_string_delimiters`.
3. **A stray brace** (`{x}`, `{}`, `{publication_start_date.__class__}`, one `{`) passes the window check but breaks `str.format` in the child after the lane is taken, or reads an attribute into the query. Task 1: `test_replacement_fields_are_the_window_placeholders_only`.
4. **A slot that stays locked**: a refused request, a failed `prepare` or a child that cannot start leaves the shared slot held, and the worker looks alive while its lane never replays again. Task 5: `test_a_failed_request_frees_the_slot`.
5. **Two entries with one interface name** (an enabled twin in another file): the overrides would land on one and the child load the other, or fail late. Task 7: `test_two_enabled_entries_of_one_name_are_refused`.

## File structure

| file | responsibility | task |
| --- | --- | --- |
| `src/maas_collector/rawdata/operator/overrides.py` (new) | `OVERRIDABLE_KEYS`, `DUPLICATES`, `OverrideRefused`, window checks, archive choice, `merge_overrides`. Pure: imports only the standard library and `backup/named.py` (plan 3's `rule.py` imports it from `odatacollector.py`, so it must create no cycle) | 1, 2 |
| `src/maas_collector/rawdata/operator/contract.py` | custom names, `ReplayRequest` fields, `is_custom`, `routing_key`, body round trip | 3 |
| `src/maas_collector/rawdata/operator/lanes.py` (new) | lane names and slots, `OperatorLane`, `OperatorReplayWorker`, `build_lanes` | 4, 9, 10 |
| `src/maas_collector/rawdata/operator/handler.py` | `custom_part`, `command` with the real name, slot lock, `prepare` on a custom lane | 4, 5, 7, 8 |
| `src/maas_collector/rawdata/operator/replay.py` | `build_command` passes the requester | 6 |
| `src/maas_collector/rawdata/replay.py`, `rawdata/cli/lib/args.py` | `ReplayArgs.requested_by`, `--replay-requested-by` | 6 |
| `src/maas_collector/rawdata/operator/worker.py`, `rawdata/cli/operator_worker.py` | `OperatorSettings.single_lane`, `--operator-single-lane`, lanes and handler wiring | 9, 10 |
| `src/maas_collector/rawdata/configschema.py` (new) | `SECRET_KEYS`, `configuration_schema()`, the catalog | 11, 12, 13 |
| `docs/custom_recollect.md` (new), `docs/operator_worker.md`, `docs/configuration_schema.md` (new), `docs/index.md`, `CHANGELOG.md` | documentation, folded into the task that makes it true | 2, 6, 9, 10, 13 |
| `tests/operator/test_overrides.py`, `test_lanes.py` (new), `test_contract.py`, `test_handler.py`, `test_replay.py`, `test_worker.py`; `tests/test_configschema.py` (new) | the tests of each task | all |

---

### Task 1: The window checks (B1)

**Files:**
- Create: `src/maas_collector/rawdata/operator/overrides.py`
- Test: `tests/operator/test_overrides.py`

**Interfaces:**
- Consumes: nothing.
- Produces:

```python
class OverrideRefused(ValueError): ...
DEFAULT_ODATA_DATE_FIELD = "PublicationDate"          # the default of ODataCollectorConfiguration.filter_date_v4
ODATA_PLACEHOLDERS = ("publication_start_date", "publication_end_date")
JIRA_CLAUSE = "updated > {date_criteria}"
def check_odata_window(odata_filter: str, date_field: str) -> None: ...   # OverrideRefused
def check_jql_window(jql: str) -> None: ...                               # OverrideRefused
```

- [ ] **Step 1: Write the failing tests** in `tests/operator/test_overrides.py`.

```python
WINDOW = "PublicationDate ge {publication_start_date} and PublicationDate le {publication_end_date}"

@pytest.mark.parametrize("flt", [
    WINDOW,
    WINDOW + " and contains(Name,'TYPE_A')",
    "contains(Name,'TYPE_A') and " + WINDOW,
    "(Collection/Name eq 'S1' or Collection/Name eq 'S2') and " + WINDOW,   # or at depth 1
    "contains(Name,'a or b (') and " + WINDOW,                              # or and a parenthesis in quotes
    "contains(Name,'it''s') and " + WINDOW,                                 # doubled quote
])
def test_odata_windows_that_stay(flt): check_odata_window(flt, "PublicationDate")

@pytest.mark.parametrize("flt, word", [
    ("PublicationDate le {publication_end_date}", "ge"),
    ("PublicationDate ge {publication_start_date}", "le"),
    ("PublicationDate gt {publication_start_date} and PublicationDate le {publication_end_date}", "ge"),
    ("CreationDate ge {publication_start_date} and CreationDate le {publication_end_date}", "PublicationDate"),
    ("(" + WINDOW + ")", "top-level"),                       # the terms are inside a group
    (WINDOW + " or Name eq 'x'", r"\bor\b"),
    ("Name eq 'x' or " + WINDOW, r"\bor\b"),
    (WINDOW + " and (Name eq 'a' or Name eq 'b') or Name eq 'c'", r"\bor\b"),
    (WINDOW + " and Name eq 'x", "quote"),
    (WINDOW + " and (Name eq 'x'", "parenthes"),
    (WINDOW + ")", "parenthes"),
    ("", "ge"),
])
def test_odata_windows_that_escape(flt, word):
    with pytest.raises(OverrideRefused, match=word): check_odata_window(flt, "PublicationDate")

def test_the_date_field_is_the_entrys():
    flt = "CreationDate ge {publication_start_date} and CreationDate le {publication_end_date}"
    check_odata_window(flt, "CreationDate")

def test_window_checks_ignore_whitespace_and_keyword_case():      # Review Focus 1
    check_odata_window("PublicationDate  ge\n{publication_start_date}\nAND\tPublicationDate le {publication_end_date}\n", "PublicationDate")
    with pytest.raises(OverrideRefused, match=r"(?i)\bor\b"):
        check_odata_window(WINDOW + " Or Name eq 'x'", "PublicationDate")
    check_jql_window("UPDATED  >\n{date_criteria} and project = X order  by updated")

@pytest.mark.parametrize("tail", ["&$top=1000000", "#", "?x=1", "\n&$filter=true"])
def test_odata_filter_cannot_carry_query_string_delimiters(tail):   # Review Focus 2, quoted or not
    for flt in (WINDOW + " and Name eq 'a" + tail + "'", WINDOW + tail):
        with pytest.raises(OverrideRefused, match="query string"): check_odata_window(flt, "PublicationDate")

@pytest.mark.parametrize("field", ["{x}", "{}", "{0}", "{publication_start_date.__class__}", "{publication_start_date[0]}",
                                   "{publication_start_date!r}", "{publication_start_date:>20}", "{", "}"])
def test_replacement_fields_are_the_window_placeholders_only(field):  # Review Focus 3, both languages
    with pytest.raises(OverrideRefused, match="replacement field|brace"):
        check_odata_window(WINDOW + f" and Name eq '{field}'", "PublicationDate")
    with pytest.raises(OverrideRefused, match="replacement field|brace"):
        check_jql_window(f"updated > {{date_criteria}} AND summary ~ \"{field}\"")

def test_doubled_braces_are_literal(): check_odata_window(WINDOW + " and Name eq '{{'", "PublicationDate")
```

The jira cases, same file:

```python
@pytest.mark.parametrize("jql", [
    "project = X AND updated > {date_criteria} ORDER BY updated ASC",
    "updated > {date_criteria}",
    "(project = A OR project = B) AND updated > {date_criteria} ORDER BY created",
    'summary ~ "a OR b" AND updated > {date_criteria}',
    "summary ~ 'a \\' OR b' AND updated > {date_criteria}",                          # backslash-escaped quote
    'updated > {date_criteria} AND status in (Closed, "TO BE VERIFIED") ORDER BY updated ASC',
])
def test_jql_windows_that_stay(jql): check_jql_window(jql)

@pytest.mark.parametrize("jql, word", [
    ("project = X", "updated"),
    ("updated >= {date_criteria}", "updated"),
    ("(updated > {date_criteria})", "top-level"),
    ("project = X OR updated > {date_criteria}", r"(?i)\bor\b"),
    ("updated > {date_criteria} or project = X ORDER BY updated", r"(?i)\bor\b"),
    ('updated > {date_criteria} AND summary ~ "x', "quote"),
    ("updated > {date_criteria} AND (a = 1", "parenthes"),
])
def test_jql_windows_that_escape(jql, word):
    with pytest.raises(OverrideRefused, match=word): check_jql_window(jql)
```
- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_overrides.py -q --no-cov`. Expected: FAIL at collection with `ModuleNotFoundError: No module named 'maas_collector.rawdata.operator.overrides'`.
- [ ] **Step 3: Implement the two checks in `overrides.py`.** One scanner serves both languages: `_scan(text, quotes, escape, keywords)` walks the string once, tracking the quote state (odata: `'` with `''` as the escape; jira: `"` and `'` with a backslash escape), the parenthesis depth, and yields the top-level words and the top-level cut points; an unbalanced quote or parenthesis raises `OverrideRefused` naming "quote" or "parenthesis". Then:
  - **Both:** `string.Formatter().parse(text)` must succeed (a lone brace raises: refuse "brace"), and every replacement field must be exactly one of the allowed placeholders, with no attribute, index, conversion or format spec (refuse "replacement field"). Collapse every run of whitespace to one space before comparing terms. Keywords (`and`, `or`, `ge`, `le`, `order by`) match case-insensitively on whole words at depth 0 outside quotes; the date field, compared with the entry's, is case-sensitive; the jira field name `updated` is not.
  - **Odata:** refuse `&`, `#` and `?` anywhere, quoted or not ("query string delimiter"); refuse any depth-0 `or`; split at depth-0 `and`; the terms must include `<date_field> ge {publication_start_date}` and `<date_field> le {publication_end_date}` (the message names the missing term and says "top-level" when the term only occurs inside a group).
  - **Jira:** cut the text at the first depth-0 `order by`; refuse any depth-0 `or` before it; split the rest at depth-0 `and`; one term must equal `updated > {date_criteria}`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS. Then `.venv/bin/python -c "import maas_collector.rawdata.operator.overrides"` prints nothing.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/overrides.py tests/operator/test_overrides.py
git commit -m "feat: :sparkles: Check that a custom query keeps the replay window" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: The allowlist, the archive choice and `merge_overrides` (B1)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/overrides.py`
- Create: `docs/custom_recollect.md`; Modify: `docs/index.md` (link under "Operator worker"), `CHANGELOG.md`
- Test: `tests/operator/test_overrides.py`

**Interfaces:**
- Consumes: Task 1; plan 1 `BACKUP_NAME`, `MAX_NAME_LENGTH`, `CREDENTIAL_PREFIX` (`backup/named.py`).
- Produces (contracts §3, exact):

```python
OVERRIDABLE_KEYS: dict[str, frozenset[str]] = {
    "ODataCollectorConfiguration": frozenset({"odata_query_filter", "download_file_pattern", "product_per_page", "interface_credentials"}),
    "JiraExtendedCollectorConfiguration": frozenset({"jql_query", "interface_credentials"}),
}
DUPLICATES = ("skip", "replace_if_changed")
ARCHIVE_CHOICE_KEYS = ("backup", "backup_version", "duplicates", "force")
MAX_PRODUCT_PER_PAGE = 1000
def merge_overrides(collector_class: str, live_entry: dict, overrides: dict) -> dict: ...
def check_archive_choice(collector_class: str, archive: dict | None) -> None: ...
def normalize_archive_choice(archive: dict) -> dict: ...
    # {"backup": str, "backup_version": int | None, "duplicates": "skip" | "replace_if_changed", "force": bool}; defaults None, "skip", False
```

- [ ] **Step 1: Write the failing tests.**

```python
ODATA = "ODataCollectorConfiguration"; JIRA = "JiraExtendedCollectorConfiguration"
LIVE = {"class": ODATA, "interface_name": "ITF_A", "odata_product_url": "https://remote.example", "product_per_page": 500,
        "odata_query_filter": WINDOW, "download_file_pattern": ["*"], "interface_credentials": "cred-a"}

def test_merge_replaces_the_overridden_keys_and_leaves_the_live_entry_alone():
    before = copy.deepcopy(LIVE)
    merged = merge_overrides(ODATA, LIVE, {"download_file_pattern": ["*.EOF*"], "product_per_page": 100})
    assert merged == {**LIVE, "download_file_pattern": ["*.EOF*"], "product_per_page": 100} and LIVE == before

@pytest.mark.parametrize("key", ["odata_product_url", "exchange_name", "routing_key", "interface_name", "model", "extractor",
                                 "custom_query_suffix", "odata_start_offset", "backup", "archive", "token", "client_password", "class", "enabled"])
def test_every_other_odata_key_is_refused_naming_it(key):
    with pytest.raises(OverrideRefused, match=key): merge_overrides(ODATA, LIVE, {key: "x"})

def test_jira_allows_two_keys_only():
    jira = {"class": JIRA, "interface_name": "J", "end_point": "https://j.example", "jql_query": "updated > {date_criteria}"}
    assert merge_overrides(JIRA, jira, {"jql_query": "project = X AND updated > {date_criteria}"})["jql_query"].startswith("project")
    with pytest.raises(OverrideRefused, match="end_point"): merge_overrides(JIRA, jira, {"end_point": "https://other.example"})
    with pytest.raises(OverrideRefused, match="jql_query"): merge_overrides(JIRA, jira, {"jql_query": "project = X"})

def test_a_class_without_overridable_keys_is_refused():
    with pytest.raises(OverrideRefused, match="FileCollectorConfiguration"): merge_overrides("FileCollectorConfiguration", {}, {"x": 1})

@pytest.mark.parametrize("value, live, ok", [(1, 500, True), (1000, 500, True), (1001, 500, False), (2000, 2000, True), (2001, 2000, False),
                                             (0, 500, False), (-1, 500, False), (True, 500, False), ("5", 500, False), (1.5, 500, False)])
def test_product_per_page_is_between_one_and_the_larger_of_live_and_1000(value, live, ok): ...   # live entry without the key counts as 1000

@pytest.mark.parametrize("value", ["*.EOF", [1], [""], None])
def test_download_file_pattern_is_a_list_of_non_empty_strings(value): ...     # refused; [] accepted (metadata only)

@pytest.mark.parametrize("name", ["", "backup-archive-a", "../other", "a/b", "a b", "x" * 129])
def test_interface_credentials_is_a_plain_entry_name(name): ...               # refused; "cred-b", "Cred_B.1" accepted

def test_the_odata_window_uses_the_live_entrys_date_field():
    live = {**LIVE, "filter_date_v4": "CreationDate"}
    with pytest.raises(OverrideRefused, match="CreationDate"): merge_overrides(ODATA, live, {"odata_query_filter": WINDOW})

def test_the_default_date_field_is_the_collectors():
    assert DEFAULT_ODATA_DATE_FIELD == ODataCollectorConfiguration.__dataclass_fields__["filter_date_v4"].default

def test_archive_choice():
    check_archive_choice(ODATA, None); check_archive_choice(JIRA, None)
    check_archive_choice(ODATA, {"backup": "archive-a", "backup_version": 3, "duplicates": "replace_if_changed", "force": True})
    assert normalize_archive_choice({"backup": "archive-a"}) == {"backup": "archive-a", "backup_version": None, "duplicates": "skip", "force": False}

@pytest.mark.parametrize("choice, word", [
    ({"backup": "archive-a"}, "archive"),                                  # with collector class JIRA
    ({}, "backup"), ({"backup": "Archive"}, "backup"), ({"backup": "a" * 65}, "backup"),
    ({"backup": "archive-a", "duplicates": "keep"}, "duplicates"),
    ({"backup": "archive-a", "force": "yes"}, "force"),
    ({"backup": "archive-a", "backup_version": 0}, "backup_version"), ({"backup": "archive-a", "backup_version": True}, "backup_version"),
    ({"backup": "archive-a", "fields": {}}, "fields"),                     # a choice carries no rule fields
])
def test_archive_choice_refusals(choice, word): ...                         # first case on JIRA, the others on ODATA; OverrideRefused match=word
```

- [ ] **Step 2: Run the tests to verify they fail.** Run the Task 1 command. Expected: the new tests FAIL with `ImportError: cannot import name 'merge_overrides'`.
- [ ] **Step 3: Implement.** `merge_overrides` deep-copies the live entry, refuses a class missing from `OVERRIDABLE_KEYS` and any key outside its set (message names the key and lists the allowed ones), checks each value as the tests fix it, runs `check_odata_window(filter, live.get("filter_date_v4") or DEFAULT_ODATA_DATE_FIELD)` or `check_jql_window`, and returns the merged copy. `interface_credentials` matches `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$` and does not start with `CREDENTIAL_PREFIX` (a backup's key entry is not an interface credential). `check_archive_choice` calls `normalize_archive_choice` for a non-`None` choice and refuses a class that is not `ODataCollectorConfiguration`. Import `BACKUP_NAME` etc. at module level; if `python -c "import maas_collector.rawdata.collector.odatacollector"` then fails with a cycle once plan 3 lands, move the import inside `normalize_archive_choice`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Task 1 command. Expected: PASS.
- [ ] **Step 5: Write the docs and commit.** `docs/custom_recollect.md`: the allowlist table, the window rules with the two accepted forms, the archive choice (`backup`, `backup_version`, `duplicates`, `force`), and the sentence that the live entry is resolved at run time. `CHANGELOG.md`, under `## Unreleased`: "### Custom recollects" with a bullet for the overrides module.

```bash
git add src/maas_collector/rawdata/operator/overrides.py tests/operator/test_overrides.py docs/custom_recollect.md docs/index.md CHANGELOG.md
git commit -m "feat: :sparkles: Merge the overridable keys of a recollect into an entry" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Custom requests in the contract (B3b)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/contract.py` (`ReplayRequest` at line 90, names at lines 23-30)
- Test: `tests/operator/test_contract.py`

**Interfaces:**
- Consumes: Task 2's `check_archive_choice` is **not** used here (the backend validates; the contract only carries).
- Produces (contracts §3, exact):

```python
CUSTOM_SUFFIX = "@custom"
def replay_custom_routing_key(service: str, lane: str) -> str: ...   # maas.operator.<service>.replay-custom.<lane>
def replay_custom_queue_name(service: str, lane: str) -> str: ...    # maas-operator-<service>-replay-custom-<lane>
# ReplayRequest gains: overrides: dict | None = None, configuration: str = "", archive: dict | None = None  (after `lane`)
#   property is_custom -> bool; def routing_key(self, service: str) -> str
```

**What "set" means** (the contract and the handler of Task 4 use the same rule): `overrides` and `archive` are set when they are not `None` (so `{}` is set), `configuration` is set when it is non-empty. `is_custom` is true when at least one of the three is set. A request is custom only with the `@custom` name **and** at least one field set; the fields without the name, or the name without any field, are refused.

- [ ] **Step 1: Write the failing tests** (append to `test_contract.py`).

```python
def test_custom_names():
    assert c.CUSTOM_SUFFIX == "@custom"
    assert c.replay_custom_routing_key("odata", "long") == "maas.operator.odata.replay-custom.long"
    assert c.replay_custom_queue_name("odata", "short") == "maas-operator-odata-replay-custom-short"

def test_a_plain_request_is_unchanged():
    r = c.ReplayRequest.from_body(replay())
    assert (r.is_custom, r.overrides, r.configuration, r.archive) == (False, None, "", None)
    assert r.to_body() == c.ReplayRequest.from_body(r.to_body()).to_body() and set(r.to_body()) == {
        "operation_id", "interface_name", "start_date", "end_date", "requested_by", "lane"}
    assert r.routing_key("odata") == "maas.operator.odata.replay.short"

@pytest.mark.parametrize("fields", [{"overrides": {"jql_query": "x"}}, {"overrides": {}}, {"configuration": "named@2"},
                                    {"archive": {"backup": "archive-a", "backup_version": 3, "duplicates": "skip", "force": False}}])
def test_a_custom_request_round_trips_with_the_suffixed_name(fields):
    r = c.ReplayRequest(**{**BASE, "lane": "long"}, **fields)
    body = r.to_body()
    assert (r.is_custom, body["interface_name"], body["interface"]) == (True, "ITF_A@custom", "ITF_A")
    assert c.ReplayRequest.from_body(body) == r
    assert r.routing_key("odata") == "maas.operator.odata.replay-custom.long"

@pytest.mark.parametrize("change", [
    {"interface_name": "ITF_A@custom"},                                      # suffix without the other fields: a live replay by an old worker
    {"overrides": {"a": 1}},                                                 # fields without the suffix
    {"overrides": {}},                                                       # {} is set: still fields without the suffix
    {"interface_name": "ITF_A@custom", "interface": "ITF_A", "configuration": ""},   # an empty configuration is not set: nothing set
    {"interface_name": "ITF_A@custom", "interface": "OTHER", "overrides": {"a": 1}},
    {"interface_name": "A@custom@custom", "interface": "A@custom", "overrides": {"a": 1}},
    {"interface_name": "ITF_A@custom", "interface": "ITF_A", "overrides": [1]},
    {"interface_name": "ITF_A@custom", "interface": "ITF_A", "archive": "archive-a"},
])
def test_inconsistent_custom_bodies_are_refused(change):
    with pytest.raises(ValueError): c.ReplayRequest.from_body(replay(**change))
```
`BASE` is the keyword set of the existing `replay()` converted to a `ReplayRequest` (`operation_id`, `interface_name="ITF_A"`, dates, `requested_by`).

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_contract.py -q --no-cov`. Expected: FAIL (`AttributeError: module ... has no attribute 'CUSTOM_SUFFIX'`).
- [ ] **Step 3: Implement** in `contract.py`. `ReplayRequest.interface_name` holds the **real** name; `to_body` writes `<real>@custom` plus `interface`, `overrides`, `configuration`, `archive` only when `is_custom`. `from_body`: a name ending with the suffix needs `interface` equal to the name without it, with no `@` left, and at least one of the three fields set; the three fields without the suffix are refused; `overrides` and `archive` must be objects or null, `configuration` a string. `contract.py` stays free of `maas_engine`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/contract.py tests/operator/test_contract.py
git commit -m "feat: :sparkles: Carry overrides, configuration and archive choice in a replay request" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: The handler on custom lanes: recognition, refusals and the real name (B3a, B3b)

**Files:**
- Create: `src/maas_collector/rawdata/operator/lanes.py` (pure part)
- Modify: `src/maas_collector/rawdata/operator/handler.py` (`Prepared` line 35, `__init__` 46, `prepare` 63, `command` 89, `cleanup` 102)
- Test: `tests/operator/test_lanes.py`, `tests/operator/test_handler.py`, `tests/operator/test_replay.py`

**Interfaces:**
- Consumes: Task 3's `CUSTOM_SUFFIX` and its rule of what is "set"; maas-engine's `ReplayRequest(extra=...)` (`maas_engine/replay/request.py`: unknown body fields land in `extra`); `find_entry` (`operator/replay.py:51`).
- Produces:

```python
# lanes.py
SINGLE_LANE = "single"; CUSTOM_LANE_PREFIX = "custom-"
def custom_lane_name(lane: str) -> str: ...      # "custom-short"
def slot_of(lane: str) -> str: ...               # "custom-short" -> "short"; "short" and "single" -> themselves
def accepts_custom(lane: str) -> bool: ...       # custom-* and single
# handler.py
@dataclass(frozen=True)
class CustomPart: interface_name: str; overrides: dict | None; configuration: str; archive: dict | None
def custom_part(request, lane: str) -> CustomPart | None: ...   # None for a plain request; the ENGINE's ReplayNotPossible when refused
Prepared(copy, module, interface_name: str = "")                # Task 5 adds slot
```

`lanes` of `CollectorReplayHandler` stays the tuple of **plain** lane names (the slots); `healthcheck_port + lanes.index(slot_of(lane))` is the child's port.

- [ ] **Step 1: Write the failing tests.** `test_lanes.py`: `slot_of("custom-long") == "long"`, `slot_of("single") == "single"`, `accepts_custom` is true for `custom-short`, `custom-long`, `single`, false for `short`, `long`. `test_handler.py`: extend the `lane()` helper to build the engine lanes `short`, `long`, `custom-short`, `custom-long` and add `custom_req(lane="short", **fields)` (name `ITF_A@custom`, `extra={"interface": "ITF_A", **fields}`):

```python
def test_a_custom_request_on_a_custom_lane_runs_the_real_interface(odata_store, tmp_path):
    runner = FakeRunner(); replay, recorder = lane(odata_store, runner, tmp_path, name="custom-short")
    replay.handle(custom_req(configuration="named@2"))
    command = runner.commands[0][0]
    assert command[command.index("--replay-interface-name") + 1] == "ITF_A"
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]

@pytest.mark.parametrize("fields", [{"overrides": {}}, {"configuration": "x"}, {"archive": {}}])
def test_a_request_with_one_field_set_is_custom(odata_store, tmp_path, fields): ...   # {} is set: on custom-short it runs (DONE)

@pytest.mark.parametrize("name", ["short", "long"])
def test_a_custom_request_on_a_plain_lane_is_refused(odata_store, tmp_path, name):
    ...handle(custom_req(configuration="x"))  ->  [("op1", RUNNING), ("op1", FAILED)], "plain lane" in recorder.details, no child, os.listdir(tmp_path) == []
    # recorded through the engine's handling of its ReplayNotPossible: no traceback logged (caplog has no ERROR record)

@pytest.mark.parametrize("name", ["short", "custom-short"])
def test_custom_fields_without_the_custom_name_are_refused_on_any_lane(...):
    # req() with extra={"overrides": {...}} or {"archive": {...}}  -> FAILED, "@custom" in details, no child

@pytest.mark.parametrize("name, extra, word", [("ITF_A@custom", {"interface": "ITF_A"}, "overrides"),          # suffix, nothing set
                                               ("ITF_A@custom", {"interface": "ITF_A", "configuration": ""}, "overrides"),   # empty configuration is not set
                                               ("ITF_A@custom", {"configuration": "x"}, "interface"),           # no interface
                                               ("ITF_A@custom", {"interface": "OTHER", "configuration": "x"}, "interface"),
                                               ("A@b@custom", {"interface": "A@b", "configuration": "x"}, "@")])
def test_a_malformed_custom_request_is_refused(...): ...   # FAILED, word in details, no child

def test_a_worker_without_custom_lanes_finds_no_such_interface(store, fake_opensearch):
    add(fake_opensearch, "a.json", [entry()])
    with pytest.raises(ReplayNotPossible, match="not found"): find_entry(store, "odata", "ITF_A@custom")   # tests/operator/test_replay.py: what 3.13.0 does with the name

@pytest.mark.parametrize("name, port", [("short", "18081"), ("long", "18082"), ("custom-short", "18081"), ("custom-long", "18082")])
def test_a_custom_lane_has_the_health_check_port_of_its_slot(...): ...

def test_the_child_of_a_custom_lane_gets_the_engine_lane_in_its_instance_id(odata_store, tmp_path):
    # beside test_the_child_gets_its_lane_in_its_instance_id: on "custom-long" MAAS_INSTANCE_ID ends with "-replay-custom-long"
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_lanes.py tests/operator/test_handler.py tests/operator/test_replay.py -q --no-cov`. Expected: FAIL (`ModuleNotFoundError: ...operator.lanes`, then the handler tests).
- [ ] **Step 3: Implement.**
  - `lanes.py`: the pure helpers above.
  - `custom_part(request, lane)`: a request whose `interface_name` ends with `CUSTOM_SUFFIX` is custom; its real name is `request.extra["interface"]`, which must equal the name without the suffix and contain no `@`; at least one field must be set (Task 3's rule: `overrides`/`archive` objects not `None`, `configuration` non-empty str). A custom request on a lane where `accepts_custom` is false is refused ("custom recollect on a plain lane"); custom fields without the suffix are refused on every lane ("overrides without the @custom interface name"). **It raises the engine's `ReplayNotPossible` (imported in `handler.py` as `EngineReplayNotPossible`)**, not the local `operator.replay.ReplayNotPossible(ValueError)`: it runs before the copy exists, outside the `try` of `prepare` that converts the local class, and `ReplayWorker.handle` records only the engine's class as a plain refusal (the local one would be recorded through the generic handler with a logged traceback). The helpers inside `prepare`'s `try` (Tasks 7 and 8) keep raising the local class.
  - Handler: `prepare` takes `custom_part` first, then looks the interface up (`find_entry`) under the real name and stores it in `Prepared.interface_name`; `command` builds the child with `dataclasses.replace(request, interface_name=prepared.interface_name or request.interface_name)`, so `--replay-interface-name` is the real name; the port is `self.healthcheck_port + self.lanes.index(slot_of(lane))`; `child_environment(environ, lane)` keeps the **engine lane name unchanged** (`...-replay-custom-short`, `...-replay-single`), which keeps each lane's telemetry apart. `describe_failure` is not touched.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the whole `tests/operator`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/lanes.py src/maas_collector/rawdata/operator/handler.py tests/operator
git commit -m "feat: :sparkles: Serve custom requests on custom lanes only" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: The custom lane shares the slot of its plain lane (B3a)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/handler.py`
- Test: `tests/operator/test_handler.py`

**Interfaces:**
- Consumes: Task 4's `slot_of`, `custom_part`, `Prepared`, `lane()` helper with four lanes.
- Produces: `Prepared(copy, module, interface_name: str = "", slot: threading.Lock | None = None)`; `CollectorReplayHandler._slots: dict[str, threading.Lock]`, one per name of `lanes`.

- [ ] **Step 1: Write the failing tests** (`test_handler.py`; `BlockingRunner` is a `FakeRunner` whose `run` waits on an event and counts the children running at once).

```python
def test_a_custom_lane_shares_the_slot_of_its_plain_lane(odata_store, tmp_path):
    # ONE handler, one ReplayWorker with the four lanes, runners replaced by a BlockingRunner that records the number of children running.
    # Threads: short request first (runner blocks), then custom-short, then long.
    # Assertions: while short runs, custom-short has not started (its record has no child yet) but long has; after the short child is released the
    # custom-short child runs; the maximum number of children ever running in the "short" slot is 1 and across slots 2.

def test_a_failed_request_frees_the_slot(odata_store, tmp_path):                          # Review Focus 4
    # one handler, one worker: (1) a custom request on lane "short" is refused, (2) a custom request whose configuration copy skips a file,
    # (3) a runner that raises OSError. After each, a plain request on "short" runs to DONE within the test's 5 s join.
```
Run the threads of both tests with `join(5)` and assert `not thread.is_alive()` so a leaked lock fails instead of hanging.

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_handler.py -q --no-cov -k slot`. Expected: FAIL (the custom-short child starts while the short child runs: the maximum running in the slot is 2).
- [ ] **Step 3: Implement.** `self._slots = {name: threading.Lock() for name in lanes}`. After `custom_part` (a refusal needs no slot), `prepare` **acquires the slot of `slot_of(lane)`** (blocking: the record stays `RUNNING` while it waits) and releases it on any exception; `Prepared.slot` carries it and `cleanup` releases it exactly once (set `prepared.slot = None` after). A plain request takes the slot of its own lane too, so the rule is the same for both.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the whole `tests/operator`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/handler.py tests/operator/test_handler.py
git commit -m "feat: :sparkles: Make a custom lane share the slot of its plain lane" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: The requester reaches the replay child (B3c)

**Files:**
- Modify: `src/maas_collector/rawdata/replay.py` (`ReplayArgs`), `src/maas_collector/rawdata/cli/lib/args.py` (`replay_parser` line 949, `ReplayArgs(...)` line 1458), `src/maas_collector/rawdata/operator/replay.py` (`build_command` line 98), `docs/operator_worker.md` ("Replays", line 91, the sentence listing the child's arguments), `CHANGELOG.md`
- Test: `tests/operator/test_replay.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `ReplayArgs.requested_by: str = ""` (a new **last** field, after `retry`); `--replay-requested-by` with env `REPLAY_REQUESTED_BY` and `dest="replay_requested_by"`; `build_command` appends `f"--replay-requested-by={request.requested_by}"` after `--replay-suffix=...` (one item, like the suffix). The operation id already travels as `--replay-suffix=<operation_id>`.

- [ ] **Step 1: Write the failing tests** (`test_replay.py`): update the expected list of `test_command_for_an_odata_entry` with `"--replay-requested-by=bob"` last, and add

```python
def test_the_requester_reaches_the_child_arguments():
    command = build_command("odata", "/c", req(), 18081, 3)
    parser = argparse.ArgumentParser(parents=[common_parser(), odata_parser()])
    namespace = parser.parse_args(command[3:])
    assert (namespace.replay_suffix, namespace.replay_requested_by) == ("op1", "bob")

def test_a_requester_starting_with_a_dash_stays_one_argument():
    ... req(requested_by="-x") parsed back as "-x"                       # req() gains a requested_by parameter

def test_replay_args_default_requester_is_empty():
    assert ReplayArgs("ITF_A", "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z").requested_by == ""
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_replay.py -q --no-cov`. Expected: FAIL (`AttributeError: 'Namespace' object has no attribute 'replay_requested_by'`).
- [ ] **Step 3: Implement** the field, the option (same `EnvDefault` pattern as `--replay-suffix`, default `""`, `type=str`), pass `requested_by=namespace.replay_requested_by` in `get_collector_args`, and the `build_command` item. `child_environment` already drops every `REPLAY_*` variable of the worker. In `docs/operator_worker.md` add `--replay-requested-by=<requester>` to the child's arguments (beside `--replay-suffix=<operation_id>`), and add a bullet under `## Unreleased`, "### Custom recollects" of `CHANGELOG.md`: the new option and its variable `REPLAY_REQUESTED_BY`.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then `TZ=UTC .venv/bin/python -m pytest tests -q --no-cov -x -k "replay or operator"`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/replay.py src/maas_collector/rawdata/cli/lib/args.py src/maas_collector/rawdata/operator/replay.py tests/operator/test_replay.py docs/operator_worker.md CHANGELOG.md
git commit -m "feat: :sparkles: Pass the requester of a replay to the collector" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: `prepare` applies the overrides on a custom lane (B3c)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/handler.py`
- Test: `tests/operator/test_handler.py`

**Interfaces:**
- Consumes: Task 2 `merge_overrides`, `OverrideRefused`; Tasks 4 and 5 `custom_part`, `Prepared`, the slot; `sync_configs` and `serialize` (`configsync.py`), `FileConfigProvider(config_dir=...).load()` (`configprovider.py:103`), `find_entry` (`replay.py:51`); plan 1 `validate_config_sources(..., evaluate=False)`, `ConfigValidationError`; `credential_provider.load(names)` (`collector/credentialprovider.py:37`).
- Produces: on a custom lane, `prepare` leaves in the copy the interface's entry with the overrides merged, the whole copy validated, and returns `Prepared(copy, module, interface_name=<real>)`. Any `OverrideRefused` or `ConfigValidationError` becomes the engine's `ReplayNotPossible` with the copy removed. `CollectorReplayHandler.__init__` gains `credential_provider=None` as its **last** parameter; the handler gets **its own** provider instance, never the checker's: `ProductChecker` reads `provider.unreadable` after each `load` and `VaultCredentialProvider.load` rewrites it on every call, so a `prepare` in a lane thread would change what a concurrent check reports (Task 9 wires `build_credential_provider(args)` a second time for it). The handler only calls `load([name])` and never reads `.unreadable`.

- [ ] **Step 1: Write the failing tests.** First fix the fixtures of `tests/operator/test_handler.py`: `validate_config_sources` builds **every** entry of the copy with its configuration class (`build_collector_configuration`), and `ODataCollectorConfiguration` requires `id_field` and `routing_key`, so the `entry()` helper (today `{"class": cls, "interface_name": name, **extra}`) gains `"routing_key": "k", "id_field": "id"` before `**extra` (harmless for the plain-lane tests, which never build it). Every fixture of the custom-lane tests (`odata_store`, `entry("OTHER")`, the jira entry, the twins) must build with its class; for the jira entry confirm with `build_collector_configuration("JiraExtendedCollectorConfiguration", {...})` that `interface_name` suffices. Define `WINDOW` again in `test_handler.py` (same text as `test_overrides.py`; do not import one test module from another). `FakeRunner` also keeps `self.conf = {name: json}` read from the copy's files before returning; the `lane()` helper gains `service="odata"` and `credential_provider=None` parameters for the handler (a fresh `FakeProvider` per test, as the handler owns its instance). `CUSTOM = {"odata_query_filter": WINDOW + " and contains(Name,'TYPE_A')", "download_file_pattern": ["*.EOF*"]}`.

```python
def test_the_overrides_are_applied_to_the_copy_only(store, fake_opensearch, tmp_path):
    add(fake_opensearch, "a.json", [entry(odata_query_filter=WINDOW, download_file_pattern=["*"], product_per_page=1000), entry("OTHER")])
    runner = FakeRunner(); replay, recorder = lane(store, runner, tmp_path, name="custom-short")
    replay.handle(custom_req(overrides=CUSTOM))
    [itf, other] = runner.conf["a.json"]["collectors"]
    assert (itf["odata_query_filter"], itf["download_file_pattern"], itf["product_per_page"]) == (CUSTOM["odata_query_filter"], ["*.EOF*"], 1000)
    assert other == entry("OTHER")
    assert store.get("a.json")["content"]["collectors"][0]["download_file_pattern"] == ["*"]     # the index is untouched
    assert recorder.calls == [("op1", RUNNING), ("op1", DONE, 0)]

@pytest.mark.parametrize("overrides, word", [
    ({"odata_product_url": "https://other.example"}, "odata_product_url"),
    ({"odata_query_filter": "contains(Name,'x')"}, "ge"),
    ({"odata_query_filter": WINDOW + " or Name eq 'x'"}, "or"),
    ({"product_per_page": 5000}, "product_per_page"),
    ({"interface_credentials": "missing"}, "credential"),                       # provider.load returns {} for it
])
def test_a_refused_override_fails_the_record_and_leaves_nothing(...):          # FAILED, word in details, no child, os.listdir(tmp_path) == [], slot free

def test_the_interface_credentials_override_must_exist_for_the_service(...): ... # FakeProvider.load(names) -> {"cred-b": {}} accepted; without provider: accepted

def test_a_jira_entry_takes_a_jql_override(...): ...        # service "jirax", class Jira, jql override with the window clause; the child module is "jirax"

def test_prepare_never_evaluates_the_configuration(odata_store, tmp_path, monkeypatch):
    monkeypatch.setattr(builtins, "eval", boom)                                  # raises AssertionError
    # a file with an extractor whose converter has {"type": "python", "python": "lambda x: x"} is in the service
    ...handle(custom_req(overrides=CUSTOM)) -> DONE, eval never called

def test_a_lambda_that_is_not_a_plain_lambda_fails_the_replay_before_any_child(...):
    # converter {"type": "python", "python": "lambda x=1: x"}: FAILED, "lambda" in details, no child  (plan 1's static check)

def test_two_enabled_entries_of_one_name_are_refused(store, fake_opensearch, tmp_path):   # Review Focus 5
    add(fake_opensearch, "a.json", [entry()]); add(fake_opensearch, "b.json", [entry()])
    ... custom_req(overrides=CUSTOM) -> FAILED, "2" and "ITF_A" in details; a disabled twin (entry(enabled=False) in b.json) is fine and keeps its text

def test_a_plain_request_is_not_validated_or_changed(...): ...   # on lane "short": the copy equals the index, as before
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_handler.py -q --no-cov`. Expected: the new tests FAIL (the overrides are not applied; `KeyError`/assertion on `runner.conf`).
- [ ] **Step 3: Implement in `prepare`, after `sync_configs` and before returning**, only when `custom_part` is not `None`. Helper functions go in `handler.py`:

```python
def entry_in_copy(service_dir: str, interface_name: str) -> tuple[str, dict, int]: ...   # (file path, content, index)
```
  Read the copy with `FileConfigProvider(config_dir=service_dir).load()`; the enabled entries named `interface_name` across all files must be exactly one (`ReplayNotPossible` naming the count and the name otherwise); disabled twins are left alone. Then:
  1. `merged = merge_overrides(entry["class"], entry_in_copy, part.overrides or {})`, where the class is the entry's own (`find_entry` has refused any class outside `REPLAY_MODULES`).
  2. When `interface_credentials` is overridden and `self.credential_provider` is set, refuse unless `provider.load([name])` returns the name.
  3. Write the file back with `configsync.serialize(content)`.
  4. Validate every file of the copy: `validate_config_sources(sources, lambda meta: [CONFIG_CLASS], resolve_models=True, evaluate=False)`, where `CONFIG_CLASS` is that of the collector of the interface's module (`get_collector_class_by_config_classname(entry["class"]).CONFIG_CLASS`), as the child builds every entry of its service with its own class (`FileCollector.config_classes_for`, `filecollector.py:457`).
  Keep `except ReplayNotPossible` / `BaseException` cleanup of the copy as today, and add `except (OverrideRefused, ConfigValidationError)` to the first branch.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then `TZ=UTC .venv/bin/python -m pytest tests/operator -q --no-cov`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/handler.py tests/operator/test_handler.py
git commit -m "feat: :sparkles: Apply a recollect's overrides to the configuration copy" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `prepare` writes the archive choice and pins the backup (B3c)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/handler.py`
- Test: `tests/operator/test_handler.py`

**Interfaces:**
- Consumes: Task 2 `check_archive_choice`, `normalize_archive_choice`; Task 7's copy handling; `ConfigStore.latest()` and `ConfigStore.get(path, version)` (`configstore.py:195`).
- Produces: `resolve_backup_version(store, service: str, name: str, version: int | None) -> int` (module function of `handler.py`) and, in the copy, `entry["archive"] = {**existing_archive, "backup": name, "backup_version": <int>, "duplicates": d, "force": f}` (an existing rule's `fields` and `download` are kept; plan 3 reads these keys back).

- [ ] **Step 1: Write the failing tests** (helper `add_backup(client, name="archive-a", version=1, services=("odata",), deleted=False)` writes `backups/<name>.json` with `{"backups": [{"name": name, "type": "BackupLocal", "directory": "/data"}]}` through `add_config_document`).

```python
def test_the_archive_choice_is_written_into_the_entry_with_the_backup_pinned(...):
    add_backup(fake_opensearch, version=3)       # latest
    replay.handle(custom_req(archive={"backup": "archive-a", "backup_version": None, "duplicates": "replace_if_changed", "force": True}))
    assert runner.conf["a.json"]["collectors"][0]["archive"] == {"backup": "archive-a", "backup_version": 3, "duplicates": "replace_if_changed", "force": True}

def test_an_existing_rule_keeps_its_fields_and_download(...):
    entry(archive={"backup": "old", "fields": {"checksum": "origin_checksum_md5"}, "download": {"url": "https://bytes.example/{id}", "quote_id": False}})
    # the written archive has the new backup, version, duplicates, force AND the same fields and download

def test_a_named_version_is_kept_when_it_still_names_the_backup(...): ...    # versions 1 (latest False) and 2; choice version 1 -> backup_version 1

@pytest.mark.parametrize("case, word", [
    ("unknown", "archive-a"), ("untagged", "service"), ("deleted", "archive-a"),
    ("version_missing", "version 9"), ("version_tombstone", "version 1"), ("version_untagged", "version 1"), ("two_files", "archive-a"),
])
def test_a_backup_that_cannot_serve_is_refused(...): ...                      # FAILED, word in details, no child, copy removed

def test_an_archive_choice_on_a_jira_entry_is_refused(...): ...               # "archive" in details
def test_an_archive_choice_without_overrides_runs_the_live_query(...): ...    # the entry's query keys are unchanged, only archive added
def test_overrides_and_archive_together(...): ...
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_handler.py -q --no-cov`. Expected: the new tests FAIL (`KeyError: 'archive'`).
- [ ] **Step 3: Implement.** `resolve_backup_version`: among `store.latest()` documents that are not deleted, tag `service` and define a backup called `name` (in `content["backups"]`, whatever the file path), there must be exactly one; its `version` is the answer when `version` is `None`. Otherwise `store.get(document["path"], version)` must exist, not be deleted, define `name` and tag `service`, else `ReplayNotPossible` naming the backup and `version N`. Call it after `check_archive_choice(entry["class"], part.archive)` and before writing the entry; the file path convention is `backups/<name>.json` (spec A1), which plan 3's `backup_path` also uses, but the lookup does not depend on it. Static validation of Task 7 then runs on the entry with its `archive` (plan 3 adds the key checks).
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then `TZ=UTC .venv/bin/python -m pytest tests/operator -q --no-cov`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/handler.py tests/operator/test_handler.py
git commit -m "feat: :sparkles: Write a recollect's archive choice into the copied entry" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Custom lanes in the worker (B3a)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/lanes.py`, `src/maas_collector/rawdata/cli/operator_worker.py` (lanes at lines 154-173, `LANES` import 30), `docs/operator_worker.md` ("Queues" table at line 73 and the request example under it, "Replays" at line 91), `CHANGELOG.md`
- Test: `tests/operator/test_lanes.py`, `tests/operator/test_worker.py` (`test_cli_wires_the_collector_handler_and_the_lanes` at line 405)

**Interfaces:**
- Consumes: Task 3's `replay_custom_queue_name`, `replay_custom_routing_key`; Task 4's `custom_lane_name`.
- Produces: `build_lanes(settings: OperatorSettings) -> list[Lane]` in `lanes.py`: `short`, `long`, `custom-short`, `custom-long` in this order, each custom lane with the queue `replay_custom_queue_name(service, lane)`, the key `replay_custom_routing_key(service, lane)` and the timeout of its plain lane (`LaneTimeout(base=settings.short_timeout, per_day=settings.long_timeout_per_day if lane == "long" else 0)`); `operator_worker_main` builds the handler with `LANES` as before and gives it **its own** credential provider, `credential_provider=build_credential_provider(args)` called a second time (the checker keeps the first: they run on different threads and `VaultCredentialProvider.load` rewrites `.unreadable`, which the checker reads, on every call).

- [ ] **Step 1: Write the failing tests.**

```python
def test_build_lanes_declares_a_custom_lane_beside_each_plain_one():
    lanes = build_lanes(OperatorSettings(service="svc", short_timeout=60, long_timeout_per_day=20))
    assert [(l.name, l.queue, l.routing_key, l.timeout.base, l.timeout.per_day) for l in lanes] == [
        ("short", "maas-operator-svc-replay-short", "maas.operator.svc.replay.short", 60, 0),
        ("long", "maas-operator-svc-replay-long", "maas.operator.svc.replay.long", 60, 20),
        ("custom-short", "maas-operator-svc-replay-custom-short", "maas.operator.svc.replay-custom.short", 60, 0),
        ("custom-long", "maas-operator-svc-replay-custom-long", "maas.operator.svc.replay-custom.long", 60, 20)]

def test_custom_queues_receive_only_the_custom_keys():
    # memory:// broker: declare queue_of(lane) for the four lanes of a ReplayWorker, publish a plain-short body and a custom-short body
    # with the two routing keys; the plain queue holds only the first, the custom queue only the second, long queues nothing
```
Update `test_cli_wires_...`: the `lanes` list has the four entries above, `handler.lanes == ("short", "long")`, and the handler's `credential_provider` is a different object from the checker's (`build_credential_provider` patched to return a new object per call).

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator/test_lanes.py tests/operator/test_worker.py -q --no-cov`. Expected: FAIL (`ImportError: cannot import name 'build_lanes'`).
- [ ] **Step 3: Implement** `build_lanes` and replace the list comprehension of `operator_worker_main` by it. Document in `docs/operator_worker.md`: the two new queue rows, the custom fields of the request body (`interface_name` with `@custom`, `interface`, `overrides`, `configuration`, `archive`), that a custom lane shares the slot of its plain lane (a custom request waits, its record staying `RUNNING`, while the other child of the slot runs), that a worker serves custom requests only on custom lanes, and that the publish of a custom request fails (mandatory) when no worker declared the custom queue. Mention nothing service-specific.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then `TZ=UTC .venv/bin/python -m pytest tests/operator -q --no-cov`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator/lanes.py src/maas_collector/rawdata/cli/operator_worker.py tests/operator docs/operator_worker.md CHANGELOG.md
git commit -m "feat: :sparkles: Declare a custom lane beside each lane of the operator worker" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: The generic single-lane mode (C6, B3a)

**Files:**
- Modify: `src/maas_collector/rawdata/operator/lanes.py`, `src/maas_collector/rawdata/operator/worker.py` (`OperatorSettings` at line 42), `src/maas_collector/rawdata/cli/operator_worker.py` (`operator_parser` line 46, `build_settings` 103, the handler and `ReplayWorker` construction), `docs/operator_worker.md`, `CHANGELOG.md`
- Test: `tests/operator/test_lanes.py`, `tests/operator/test_worker.py`, `tests/operator/test_handler.py`

**Interfaces:**
- Consumes: Task 9's `build_lanes`; maas-engine's `Lane` and `ReplayWorker.queue_of`.
- Produces:

```python
@dataclass(frozen=True)
class OperatorLane(Lane): extra_routing_keys: tuple[str, ...] = ()
class OperatorReplayWorker(ReplayWorker):
    def queue_of(self, lane) -> kombu.Queue: ...   # a queue bound to routing_key and every extra_routing_keys of an OperatorLane
OperatorSettings.single_lane: bool = False
```
In single-lane mode `build_lanes` returns exactly one `OperatorLane`: name `single`, queue `replay_queue_name(service, "single")`, `routing_key` the plain short key, `extra_routing_keys` = the plain long key, the custom short key, the custom long key (in this order), `LaneTimeout(base=short_timeout, per_day=long_timeout_per_day)`; the handler is built with `lanes=("single",)`. `operator_worker_main` builds `OperatorReplayWorker` in every mode (it behaves as `ReplayWorker` for a lane without extra keys); from this task the tests that patch `{module}.ReplayWorker` (`test_cli_wires_...`) patch `{module}.OperatorReplayWorker`.

- [ ] **Step 1: Write the failing tests.**

```python
def test_single_lane_mode_has_one_lane_with_every_key_on_its_queue():
    [lane] = build_lanes(OperatorSettings(service="svc", short_timeout=60, long_timeout_per_day=20, single_lane=True))
    assert (lane.name, lane.queue, lane.routing_key, lane.timeout.base, lane.timeout.per_day) == (
        "single", "maas-operator-svc-replay-single", "maas.operator.svc.replay.short", 60, 20)
    assert lane.extra_routing_keys == ("maas.operator.svc.replay.long", "maas.operator.svc.replay-custom.short", "maas.operator.svc.replay-custom.long")

def test_every_key_of_the_service_reaches_the_single_queue_and_no_other():    # memory:// broker
    # OperatorReplayWorker(..., [single lane]) declares queue_of(lane); publish on the four keys, on the check key and on another service's key;
    # exactly the four bodies are received from the queue; worker.lanes has one lane, so run() starts one thread (len(worker.threads) == 1)

def test_a_lane_without_extra_keys_binds_its_one_key():  ...                   # OperatorReplayWorker on build_lanes(default): same bindings as ReplayWorker

def test_the_single_lane_runs_plain_and_custom_requests_one_at_a_time(odata_store, tmp_path):
    # handler lanes=("single",), engine lane "single": a plain request and a custom request (configuration="x") both reach DONE; port 18081

@pytest.mark.parametrize("value, expected", [("true", True), ("True", True), ("1", True), ("false", False), ("0", False), ("", False)])
def test_the_single_lane_setting_is_read_from_the_environment(monkeypatch, value, expected):
    monkeypatch.setenv("OPERATOR_SINGLE_LANE", value); ...build_settings(build_parser().parse_args(["--config-service", "svc"])).single_lane == expected

def test_a_mistyped_single_lane_value_stops_the_worker(monkeypatch):           # Review: "ture" must not mean false
    monkeypatch.setenv("OPERATOR_SINGLE_LANE", "ture"); with pytest.raises(SystemExit): build_parser().parse_args(["--config-service", "svc"])

def test_cli_single_lane_wiring(...):   # as test_cli_wires_..., with --operator-single-lane true: one lane, handler.lanes == ("single",)
```
Add `OPERATOR_SINGLE_LANE` to `ENVIRONMENT` in `test_worker.py` and `single_lane=...` to the expected `OperatorSettings` of `test_cli_settings_from_environment`.

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/operator -q --no-cov`. Expected: FAIL (`TypeError: ... unexpected keyword argument 'single_lane'`).
- [ ] **Step 3: Implement.** `queue_of`: for a lane with extra keys, `kombu.Queue(lane.queue, bindings=[kombu.binding(self.exchange, key) for key in (lane.routing_key, *lane.extra_routing_keys)], durable=True)` (kombu then declares each binding and ignores `exchange=`); otherwise `super().queue_of(lane)`. The CLI option takes its value through a converter `flag(value)` that accepts `1/true/yes/on` and `0/false/no/off` (any case, `""` is false) and raises `argparse.ArgumentTypeError` otherwise (`EnvDefault` strings are converted by argparse, and `type=bool` would make `"false"` true). Document the setting in `docs/operator_worker.md`: one thread and one child at a time, plain and custom requests on one queue, and the deployment note that **the plain `-short`/`-long` queues of an earlier two-lane worker stay bound and keep receiving copies of the requests: delete them when switching a service**.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/operator src/maas_collector/rawdata/cli/operator_worker.py tests/operator docs/operator_worker.md CHANGELOG.md
git commit -m "feat: :sparkles: Add a single-lane mode to the operator worker" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: The schema's secret keys and collector classes (B3d)

**Files:**
- Create: `src/maas_collector/rawdata/configschema.py`, `tests/test_configschema.py`

**Interfaces:**
- Consumes: Task 2 `OVERRIDABLE_KEYS`; `maas_collector.rawdata.implementation._CONFIG_TO_COLLECTOR_DICT` (registry: configuration class name to collector class, `implementation/__init__.py:38`) plus `InterfaceMonitorCollectorConfiguration` (`collector/monitorcollector.py:45`, not in the registry); `REPLAY_MODULES` (`operator/replay.py:14`); plan 1 `SECRET_FIELDS`.
- Produces:

```python
SECRET_KEYS: frozenset[str]                  # every dataclass field marked sensitive, UNMARKED_SECRET_KEYS and plan 1's SECRET_FIELDS
UNMARKED_SECRET_KEYS = frozenset({"oauth_basic_credential"})
TYPE_OVERRIDES = {"download_file_pattern": "string_list"}   # by key name, applied after the annotation mapping
def config_classes() -> dict[str, type]: ...   # {class name: configuration class}: `{name: collector.CONFIG_CLASS}` of the registry, plus InterfaceMonitorCollectorConfiguration
NOT_SECRET_KEYS: frozenset[str]              # reviewed names that look like credentials and hold none
GROUPS = ("identity", "remote_query", "download", "extraction", "backup", "archive", "monitoring", "other")
def looks_like_credential(name: str) -> bool: ...                  # name contains password, secret, token, credential or key (case-insensitive)
def unclassified_credential_fields(classes: Iterable[type] | None = None) -> list[str]: ...   # None: config_classes().values(); "<Class>.<field>" looking like a credential, in neither set
def entry_class_name(entry: dict, service_class: str | None) -> str: ...   # service_class, else entry["class"], else "FileCollectorConfiguration"
def service_class_of(entries: Iterable[dict]) -> str | None: ...           # the one class the entries name; None if none; ValueError if several
def collector_classes() -> dict[str, dict]: ...                    # the "collector_classes" part of the schema
```
`SECRET_KEYS`, `unclassified_credential_fields` and `collector_classes` all **ignore `init=False` fields** (plan 1's runtime fields `backup_name`, `backup_credentials_entry` and `backup_credentials`, and `backup_report_model`: a configuration file cannot set them, so they are not JSON keys and `backup_credentials` is not a secret key). `NOT_SECRET_KEYS` is exactly `{"interface_credentials", "no_credential", "token_url", "token_field_header", "client_keyfile", "routing_key", "file_routing_key", "s3_max_keys", "credentials"}`; `credentials` is the name of a backup definition's credential entry.

- [ ] **Step 1: Write the failing tests.**

```python
def test_every_sensitive_field_is_a_secret_key():
    for cls in config_classes().values():
        assert {f.name for f in dataclasses.fields(cls) if f.metadata.get("sensitive")} <= SECRET_KEYS

def test_the_unmarked_credential_fields_are_secret_keys():
    assert "oauth_basic_credential" in SECRET_KEYS and {"s3_access_key", "s3_secret_key"} <= SECRET_KEYS

def test_no_field_looks_like_a_credential_without_being_classified():
    assert unclassified_credential_fields() == []
    @dataclasses.dataclass
    class Fresh: api_token_value: str = ""
    assert unclassified_credential_fields([Fresh]) == ["Fresh.api_token_value"]

def test_runtime_fields_are_not_keys_and_not_classified():           # plan 1's init=False fields on FileCollectorConfiguration
    keys = collector_classes()["ODataCollectorConfiguration"]["keys"]
    assert {"backup_credentials", "backup_credentials_entry", "backup_name", "backup_report_model"}.isdisjoint(keys)
    assert {"backup_credentials", "backup_credentials_entry"}.isdisjoint(SECRET_KEYS | NOT_SECRET_KEYS)

def test_a_name_is_secret_or_not_secret_never_both():
    assert not SECRET_KEYS & NOT_SECRET_KEYS

def test_a_secret_name_is_sensitive_in_every_class_that_has_it():
    keys = collector_classes()["MpipCollectorConfiguration"]["keys"]       # marks neither field in the dataclass
    assert keys["client_password"]["sensitive"] and keys["client_secret"]["sensitive"]

def test_key_descriptors():
    odata = collector_classes()["ODataCollectorConfiguration"]
    assert odata["replayable"] is True and collector_classes()["FTPCollectorConfiguration"]["replayable"] is False
    keys = odata["keys"]
    assert keys["odata_query_filter"]["overridable"] and keys["odata_query_filter"]["type"] == "string"
    assert (keys["product_per_page"]["type"], keys["product_per_page"]["default"]) == ("integer", 1000)
    assert (keys["download_file_pattern"]["type"], keys["download_file_pattern"]["default"]) == ("string_list", [])   # annotated with a bare `list`: TYPE_OVERRIDES
    assert (keys["class"]["type"], keys["class"]["required"], keys["class"]["default"]) == ("string", False, None)   # the pseudo-key
    assert keys["token"]["sensitive"] and not keys["interface_credentials"]["sensitive"]
    assert {k for k, v in keys.items() if v["required"]} == {"id_field", "routing_key"}
    assert {"backup_report_model", "model_meta", "backup_credentials"}.isdisjoint(keys)   # runtime fields are not JSON keys
    assert {k for k, v in keys.items() if v["overridable"]} == OVERRIDABLE_KEYS["ODataCollectorConfiguration"]

def test_archivable_follows_the_class_attribute(monkeypatch):
    monkeypatch.setattr(ODataCollectorConfiguration, "ARCHIVABLE", True, raising=False)
    assert collector_classes()["ODataCollectorConfiguration"]["archivable"] is True
    assert collector_classes()["JiraExtendedCollectorConfiguration"]["archivable"] is False

def test_an_entry_builds_with_the_class_of_its_service():
    assert entry_class_name({"interface_name": "SUB"}, "ODataCollectorConfiguration") == "ODataCollectorConfiguration"
    assert entry_class_name({"class": "JiraExtendedCollectorConfiguration"}, "ODataCollectorConfiguration") == "ODataCollectorConfiguration"
    assert entry_class_name({"class": "JiraExtendedCollectorConfiguration"}, None) == "JiraExtendedCollectorConfiguration"
    assert entry_class_name({}, None) == "FileCollectorConfiguration"
    assert service_class_of([{}, {"class": "ODataCollectorConfiguration"}, {"class": "ODataCollectorConfiguration"}]) == "ODataCollectorConfiguration"
    assert service_class_of([{}]) is None
    with pytest.raises(ValueError, match="several"): service_class_of([{"class": "A"}, {"class": "B"}])
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/test_configschema.py -q --no-cov`. Expected: FAIL with `ModuleNotFoundError: ...configschema`.
- [ ] **Step 3: Implement.** Types come from the dataclass annotations: `str`, `int`, `float`, `bool`, `list[str]`/`typing.List[str]` → `string_list`, other list → `list`, then `TYPE_OVERRIDES` by key name (`download_file_pattern` is annotated with a bare `list` at `odatacollector.py:107` and the collector also accepts a str there, yet the builder edits it as a list of strings), `dict`/`Dict[...]` → `object`, everything else (`typing.Any`, classes, callables) → `any`; `model` and `backup` are `any`, `extractor` is `object`. `default` is the field default, or the result of its `default_factory`, or `None` when it is not JSON-serialisable; `required` means no default and not `model`/`extractor`. Skip `init=False` fields and `model_meta`. Every collector class gets one extra pseudo-key `class` after its dataclass fields (`class` is not a dataclass field, yet every entry of `tests/conf` has it): type `string`, `required` false, `default` `None`, `sensitive` false, `overridable` false, `group` `identity` and description `configuration class of the entry` (Task 12's catalog carries them). `sensitive` is `name in SECRET_KEYS` (the name decides, whatever the class marks); `replayable` is the class name in `REPLAY_MODULES`; `archivable` is `getattr(cls, "ARCHIVABLE", False)` (plan 3 sets it). `group` and `description` are `None` here: Task 12 fills them from the catalog.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/configschema.py tests/test_configschema.py
git commit -m "feat: :sparkles: Describe the collector entry classes and their secret keys" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: The catalog, the backup types and the archive rule (B3d)

**Files:**
- Modify: `src/maas_collector/rawdata/configschema.py`
- Test: `tests/test_configschema.py`

**Interfaces:**
- Consumes: Task 11; `backup/__init__.py` `factory_type_dict` and the sink configuration dataclasses (`CollectorBackupConfiguration`, `CollectorBackupS3Configuration`); plan 1's definition keys (contracts §1 table) and `SECRET_FIELDS`.
- Produces: `CATALOG: dict[str, dict]` (key name → `{"group": <GROUPS member>, "description": str}`; a per-class override `CLASS_CATALOG[class][key]` wins over it), and
  `backup_types() -> dict` and `archive_rule() -> dict` (the `"backup_types"` and `"archive_rule"` parts of the schema), `collector_classes()` now carrying `group` and `description`.

- [ ] **Step 1: Write the failing tests.**

```python
@pytest.mark.parametrize("cls", ["ODataCollectorConfiguration", "JiraExtendedCollectorConfiguration", "FileCollectorConfiguration"])
def test_replayable_and_file_entry_keys_are_described_and_grouped(cls):
    for key, d in collector_classes()[cls]["keys"].items():
        assert d["description"] and d["group"] in GROUPS, key

def test_groups_of_the_keys_that_matter():
    keys = collector_classes()["ODataCollectorConfiguration"]["keys"]
    assert [keys[k]["group"] for k in ("interface_name", "odata_query_filter", "download_file_pattern", "extractor", "backup")] == [
        "identity", "remote_query", "download", "extraction", "backup"]

def test_backup_types():
    s3, local = backup_types()["BackupS3"]["keys"], backup_types()["BackupLocal"]["keys"]
    assert set(s3) == {"name", "description", "type", "directory", "calendar_tree", "enable_gzip", "report_model",
                       "buckets", "s3_endpoint_url", "s3_region", "s3_signature_version", "credentials"}
    assert set(local) == {"name", "description", "type", "directory", "calendar_tree", "enable_gzip", "report_model"}
    assert s3["credentials"]["required"] and s3["buckets"]["required"] and s3["name"]["required"] and local["directory"]["required"]
    assert s3["buckets"]["type"] == "string_list" and s3["calendar_tree"]["default"] is False
    assert not ({"s3_access_key", "s3_secret_key"} & (set(s3) | set(local)))     # secrets are in no definition

def test_archive_rule_keys():
    keys = archive_rule()["keys"]
    assert set(keys) == {"backup", "duplicates", "fields", "download"}          # the four editable keys; backup_version and force are written by prepare (Task 8)
    assert keys["duplicates"]["default"] == "skip" and keys["duplicates"]["choices"] == ["skip", "replace_if_changed"]
    assert keys["fields"]["default"] == {"product_id": "product_id", "product_name": "product_name", "size": "content_length",
                                         "checksum": "checksum", "checksum_algorithm": "MD5", "publication_date": "publication_date"}
    assert keys["download"]["default"] == {"url": None, "quote_id": True}
    assert keys["duplicates"]["choices"] == list(DUPLICATES)
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/test_configschema.py -q --no-cov`. Expected: FAIL (`KeyError`/`assert None` on `description`; `NameError: backup_types`).
- [ ] **Step 3: Implement.** Write the catalog for every key of `ODataCollectorConfiguration`, `JiraExtendedCollectorConfiguration` and `FileCollectorConfiguration` (descriptions taken from the dataclass comments and `docs/odatacollector.md`, `docs/jiraxcollector.md`, `docs/filecollector.md`), assigned to groups as follows; every other key of every other class gets a description only when someone writes one (the descriptor keeps `null`).
  - `identity`: `interface_name`, `interface_credentials`, `enabled`, `class` (the pseudo-key of Task 11).
  - `remote_query`: `odata_product_url`, `product_url`, `odata_version`, `protocol_version`, `odata_entities`, `odata_entity_location`, `odata_query_filter`, `odata_query_order_by`, `custom_query_suffix`, `product_per_page`, `refresh_interval`, `odata_start_offset`, `end_date_time_offset`, `max_time_window`, `date_attr`, `filter_date_v3`, `filter_date_v4`, every auth key (`auth_method`, `client_username`, `client_password`, `token`, `token_url`, `client_id`, `client_secret`, `scope`, `grant_type`, `oauth_basic_credential`, `no_credential`, `auth_timeout`, `token_field_header`), `http_query_params`, `disable_insecure_request_warning`; jira `end_point`, `jql_query`, `username`, `password`, `proxy_login`, `proxy_password`.
  - `download`: `download_file_pattern`, `parent_file_pattern`, `list_of_files_to_retrieve*`, `file_pattern`, `file_routing_key`, `ingest_attachements`, `attachement_prefix`, `attachment_extra_fields`, `attachment_date_fields`.
  - `extraction`: `extractor`, `model`, `id_field`, `force_update`, `store_meta`, `routing_key`, `exchange_name`.
  - `backup`: `backup`. `archive`: `archive` (when plan 3 adds it). `monitoring`: `no_probe`, and the monitor class's keys. `other`: whatever remains.

  `backup_types()` describes only `BackupS3` and `BackupLocal` (`factory_type_dict` also holds `BackupSFTP`, which the builder does not offer) and derives their keys from `factory_type_dict[type].CONFIGURATION_CLASS` fields minus `interface_name`, `interface_credentials`, `backup_name` (plan 1 adds it to `CollectorBackupConfiguration`; a definition's key is `name`), the secret fields and `type` (re-added as the fixed key), plus the definition-only keys `name`, `description` (both types) and `credentials` (S3 only); `required` is true for `name`, `type`, `directory`, and for S3 `buckets` and `credentials`. `archive_rule()` is a literal table (contracts §1) of the **four** editable keys `backup`, `duplicates`, `fields`, `download` with `choices` for `duplicates`; `backup_version` and `force` are written by `prepare` (Task 8) from a recollect's archive choice and are never described, which keeps plan 3's schema test passing; its `fields` default is the contract's mapping (plan 3 keeps its own `DEFAULT_FIELDS` equal to it).
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/configschema.py tests/test_configschema.py
git commit -m "feat: :sparkles: Describe the entry keys, backup definitions and archive rule" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: Extractors, the other file shapes and `configuration_schema()` (B3d)

**Files:**
- Modify: `src/maas_collector/rawdata/configschema.py`
- Create: `docs/configuration_schema.md`; Modify: `docs/index.md`, `CHANGELOG.md`
- Test: `tests/test_configschema.py`

**Interfaces:**
- Consumes: Tasks 11 and 12; `maas_collector.rawdata.extractor` namespace (subclasses of `BaseExtractor`), `build_extractor` (`configuration.py:85`: `args` plus an extractor-level `converter_map`), `parse_chunk_config` (`messenger.py:23`), `maas_collector.__version__`; Task 1's `ODATA_PLACEHOLDERS`, `JIRA_CLAUSE`.
- Produces: `extractor_classes() -> dict`, `shapes() -> dict` and

```python
def configuration_schema() -> dict: ...    # contracts §3 shape, plus "shapes"; json.dumps-able; version = maas_collector.__version__
```
`window_rules` is the literal of the contract (`odata`: `date_field_key` `filter_date_v4` and the two placeholders; `jira`: the clause). `secret_keys` is `sorted(SECRET_KEYS)`. `shapes()` returns exactly this structure (plan 5's `ConfigSchema` type reads it; every leaf descriptor has the shape of a key descriptor of Task 11):

```python
{"extractor": {"keys": {"class": {...required...}, "args": {...}, "converter_map": {...}},
               "attr_map_kinds": ["<path string>", "{attr, path?}", "{python}", "{rpython}", "{root_path}"],
               "converter_kinds": ['{type: "regex", expression, format?}', '{type: "python", python}']},
 "model": {"forms": ["name", "definition"],                  # a model is a name, or an inline definition
           "field_types": ["Date", ...every opensearchpy.Field subclass name...],
           "definition": {"keys": {"name": ..., "index": ..., "fields": ...,   # fields: [{name, type}]
                                   "partition_field": ..., "partition_format": ...}}},
 "amqp": {"keys": {"chunk_size": {"type": "integer", ...positive...}}}}   # the file's "amqp": {"<routing key>": {"chunk_size": n}}
```

The file's `models` list is described by `shapes["model"]["definition"]` (no `shapes["models"]`); `models`, `backups` and `amqp` of a file are the three parts a file holds besides `collectors`.

- [ ] **Step 1: Write the failing tests.**

```python
def test_extractor_classes_are_read_from_their_constructors():
    ex = extractor_classes()
    assert set(ex) >= {"XMLExtractor", "JSONExtractor", "CSVExtractor", "LogExtractor", "XLSXExtractor"}
    xml = ex["XMLExtractor"]["args"]
    assert xml["attr_map"]["required"] and xml["attr_map"]["type"] == "object" and not xml["converter_map"]["required"]
    assert (xml["allow_partial"]["type"], xml["allow_partial"]["default"]) == ("boolean", False)
    assert ex["JSONExtractor"]["args"]["parsing_method"]["type"] == "any"          # a callable default is not JSON
    assert ex["EDRSApsExtractor"]["extra_args"] is True and ex["XMLExtractor"].get("extra_args", False) is False
    assert "BaseExtractor" not in ex

def test_shapes():
    s = shapes()
    assert set(s["extractor"]["keys"]) == {"class", "args", "converter_map"} and s["extractor"]["keys"]["class"]["required"]
    assert s["model"]["forms"] == ["name", "definition"] and {"Keyword", "Date", "Integer"} <= set(s["model"]["field_types"])
    assert set(s["model"]["definition"]["keys"]) == {"name", "index", "fields", "partition_field", "partition_format"}
    assert s["amqp"]["keys"]["chunk_size"]["type"] == "integer"
    assert set(s) == {"extractor", "model", "amqp"} and "models" not in s
    assert s["extractor"]["attr_map_kinds"] and s["extractor"]["converter_kinds"]
    assert {k for k, v in s["model"]["definition"]["keys"].items() if v["required"]} == {"name", "index", "fields"}   # configuration.py:60-70: partition keys optional

def test_the_schema_is_json_and_has_the_contract_shape():
    schema = json.loads(json.dumps(configuration_schema()))
    assert set(schema) == {"version", "collector_classes", "extractor_classes", "backup_types", "archive_rule", "secret_keys", "window_rules", "shapes"}
    assert schema["version"] == maas_collector.__version__ and schema["secret_keys"] == sorted(SECRET_KEYS)
    assert schema["window_rules"] == {"odata": {"date_field_key": "filter_date_v4", "placeholders": ["publication_start_date", "publication_end_date"]},
                                      "jira": {"clause": "updated > {date_criteria}"}}
    assert set(schema["backup_types"]) == {"BackupS3", "BackupLocal"}

# an entry with an extractor-level converter_map, the shape production uses (build_extractor merges it into the arguments)
EXTRACTOR_LEVEL_CONVERTER_MAP = {"class": "ODataCollectorConfiguration", "interface_name": "ITF_A", "routing_key": "k", "id_field": "id", "model": "M",
    "extractor": {"class": "XMLExtractor", "args": {"attr_map": {"a": "b"}}, "converter_map": {"a": {"type": "python", "python": "lambda x: x"}}}}
# in tests/conf, read by no code: the loader ignores it, the builder keeps it. A new unknown key fails the test.
UNKNOWN_BUT_LOADED = {"hours_delta"}

def described(schema, entry, service_class):                       # the keys of the entry the schema does not know
    keys = schema["collector_classes"][entry_class_name(entry, service_class)]["keys"]
    unknown = {k for k in entry if k not in keys}
    extractor = entry.get("extractor")
    if isinstance(extractor, dict):
        assert set(extractor) <= set(schema["shapes"]["extractor"]["keys"])
        assert set(extractor.get("args", {})) <= set(schema["extractor_classes"][extractor["class"]]["args"])
    if isinstance(entry.get("backup"), dict):
        assert set(entry["backup"]) <= set(schema["backup_types"][entry["backup"]["type"]]["keys"]) | {"s3_access_key", "s3_secret_key"}
    return unknown

def test_every_key_of_tests_conf_and_of_an_extractor_level_converter_map_is_described():
    schema, unknown = configuration_schema(), set()
    for path in sorted(glob.glob("tests/conf/**/*.json", recursive=True)):
        content = json.load(open(path))
        if not is_config_content(content):
            continue                                               # credential files
        entries = content.get("collectors", [])
        for entry in entries:
            unknown |= described(schema, entry, service_class_of(entries))
        for model in content.get("models", []):
            assert set(model) <= set(schema["shapes"]["model"]["definition"]["keys"])
        for amqp in content.get("amqp", {}).values():
            assert set(amqp) <= set(schema["shapes"]["amqp"]["keys"])
    assert described(schema, EXTRACTOR_LEVEL_CONVERTER_MAP, "ODataCollectorConfiguration") == set()
    assert unknown == UNKNOWN_BUT_LOADED
```

- [ ] **Step 2: Run the tests to verify they fail.** Run: `TZ=UTC .venv/bin/python -m pytest tests/test_configschema.py -q --no-cov`. Expected: FAIL (`NameError: extractor_classes`).
- [ ] **Step 3: Implement.** `extractor_classes()`: every class of `maas_collector.rawdata.extractor` that subclasses `BaseExtractor`, its `__init__` parameters through `inspect.signature` (type from the annotation or the default's type with the Task 11 mapping, `attr_map` and `converter_map` always `object`; a callable default maps to `any`/`None`; `extra_args: true` when the constructor takes `**kwargs`). `window_rules` is built from `ODATA_PLACEHOLDERS`, `JIRA_CLAUSE` and the `filter_date_v4` default. Write `docs/configuration_schema.md` (what the schema is for, the shape, the secret-key rule and how to classify a new credential-looking field: add it to `SECRET_KEYS` sources or `NOT_SECRET_KEYS`, and the entry-class rule of B3d) and a `CHANGELOG.md` "### Configuration schema" bullet.
- [ ] **Step 4: Run the tests to verify they pass.** Run the Step 2 command, then the whole suite: `TZ=UTC .venv/bin/python -m pytest tests -q --no-cov`. Expected: PASS.
- [ ] **Step 5: Commit.**

```bash
git add src/maas_collector/rawdata/configschema.py tests/test_configschema.py docs/configuration_schema.md docs/index.md CHANGELOG.md
git commit -m "feat: :sparkles: Assemble the configuration schema for the builder" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage (my sections).**
- B1 allowlist, `merge_overrides`, archive-choice check: Task 2; structural window checks (odata with the entry's date field, jira): Task 1 and Task 2 (`test_the_odata_window_uses_the_live_entrys_date_field`).
- B3a custom lanes, own queues bound to custom keys only, handler refusing on plain lanes, shared slot: Tasks 4, 5 and 9.
- B3b `@custom`, real name in the request, fail closed: Tasks 3 and 4 (`test_a_worker_without_custom_lanes_finds_no_such_interface`).
- B3c refusals, overrides applied, archive written with `backup_version` pinned, operation id and requester to the child, static validation of the copy, backup refused when gone or untagged: Tasks 6, 7, 8.
- B3d `configuration_schema()`, `SECRET_KEYS` with the explicit list and the name test, class an entry builds with, extractor shape with extractor-level `converter_map`, `model` forms, `models`/`amqp`/`backups`/archive rule shapes, `tests/conf` coverage: Tasks 11, 12, 13.
- `contract.py` (`ReplayRequest` fields, `is_custom`, `routing_key`, `to_body`, custom helpers): Task 3.
- Generic single-lane mode (`OPERATOR_SINGLE_LANE`, one `single` lane, long-lane per-day timeout, other key and custom keys on its queue): Task 10. No task names a service.

**Step scan.** Steps that decide nothing: none; Task 4 to 8 steps name the refusals, the lock, the copy handling and the pinning rule. Bodies are left to the implementer where the signature and tests fix them.

**Type consistency.** `OVERRIDABLE_KEYS`, `DUPLICATES`, `merge_overrides`, `check_odata_window`, `check_jql_window`, `check_archive_choice`, `CUSTOM_SUFFIX`, `replay_custom_*`, `ReplayRequest.is_custom/routing_key`, `SECRET_KEYS`, `configuration_schema` match contracts §3 exactly. The archive choice keys (`backup`, `backup_version`, `duplicates`, `force`) are the keys plan 3's `ArchiveRule.from_entry` accepts; `ReplayArgs.requested_by` is the name plan 3 assumes. `archive_rule()` lists the four editable keys only (plan 3's schema test); `backup_version` and `force` are written by Task 8 and declared in the header's contract note.

**Proportion.** Code blocks hold tests and signatures; the scanner, the lock, the copy rewrite and the reflection are described, not written.

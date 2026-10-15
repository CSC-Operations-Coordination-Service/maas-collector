# OpenSearch-backed collector configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collectors read their configuration files from a versioned OpenSearch index, `maas-collector-config`, and exit with code 75 to be restarted on a valid change, while the file backend stays the default.

**Architecture:** A `ConfigProvider` abstraction (`configprovider.py`) returns `ConfigSource` objects, the content of one configuration file each; `FileConfigProvider` keeps today's `-c` / `-d` behaviour and `OpenSearchConfigProvider` reads the latest documents of one service through the opensearch-py connection that `FileCollector.setup()` creates from the vault-resolved `args.es_url`. `ConfigStore` (`configstore.py`) is the only writer of immutable `<path>@<version>` documents and is driven by an admin CLI (`cli/config.py`). `FileCollector.run()` calls `check_config_changes()` after `refresh_credentials()`, validates a changed set without touching `MODEL_DICT`, and raises `SystemExit(75)`.

**Tech Stack:** Python 3.11, opensearch-py 2.6 (`opensearchpy.helpers.scan`, `connections`), maas-model 3.1.0 (`maas_model.vault`, `maas_model.testing.fakevault.FakeVault`), pytest, tox.

**Spec:** `docs/superpowers/specs/2026-10-12-opensearch-config-design.md` (its line numbers predate 3.11.0: this plan gives the current ones).

## Global Constraints

- Settings, each in `config_parser()` of `cli/lib/args.py` and in `common_parser()`: `--config-backend` / `CONFIG_BACKEND` default `file`, choices `file` and `opensearch`; `--config-index` / `CONFIG_INDEX` default `maas-collector-config`; `--config-service` / `CONFIG_SERVICE` no default, required with `opensearch`; `--config-refresh-period` / `CONFIG_REFRESH_PERIOD` default `300`, `0` = never check.
- `CollectorArgs` gains `config_backend: str = "file"`, `config_refresh_period: int = 300`, `config_store: ConfigStoreArgs = None` (`ConfigStoreArgs(index="maas-collector-config", service="")`).
- With `opensearch`: `CONFIG_SERVICE` and the OpenSearch connection must be set, else `ValueError`; the connection check reads `args.es_url` *after* vault resolution, so `ES_VAULT_PATH` alone satisfies it. `-c` / `-d` are ignored with a warning.
- The OpenSearch provider reads through `es_connections.get_connection("default")`, the connection `FileCollector.setup()` creates from `args.es_url`; `setup()` creates it **before** `load_config()`.
- No documents for the service: `ValueError("no configuration for service <service> in index <index>")`.
- Exit code of a restart on a new configuration: `CONFIG_CHANGED_EXIT_CODE = 75` (EX_TEMPFAIL), raised as `SystemExit` from `run()` so its `finally` stops the health check and flushes messages. No CLI module changes.
- Log lines, verbatim: `configuration changed: <path> v3→v4, <path> added, <path> removed, restarting` (WARNING) and `configuration change rejected: <path>@<version>: <reason>` (ERROR).
- Index `maas-collector-config`, document id `<path>@<version>`, mapping `"dynamic": "strict"`, fields `path` keyword, `version` integer, `latest` boolean, `deleted` boolean, `services` keyword, `content` object `"enabled": false`, `interface_names` keyword, `model_names` keyword, `author` keyword, `comment` text, `created_at` date, `based_on` integer.
- A configuration content's top-level keys are within `{collectors, models, amqp}`; anything else (a credential file, `{"interfaces": ...}`) is refused by `put` and skipped by `import`.
- Validation never registers models: `build_model_class(meta, register=False)`; `MODEL_DICT` stays untouched.
- The file backend stays the default and behaves as today; its fingerprint is `None`, so it never triggers a restart. `configuration.load_json(path, cls)` and `Messenger.load_config_file(path)` keep their signatures (maas-cds and `scripts/generate_interface.py` use them).
- No new dependency.
- The repository is public: no private repository names, internal host names or local paths in code, tests, docs or commit messages. Examples use `opensearch.example`, `bao.internal`, `<registry>`.
- Commits: gitmoji conventional style (`feat: :sparkles: ...`, `test: :white_check_mark: ...`, `refactor: :recycle: ...`, `docs: :memo: ...`, `build: :bookmark: ...`), each message ending with the trailer `Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>`.
- Test commands. Once, from the repository root, build the tox environment and record the baseline (193 passed on 3.11.0):
  `PIP_FIND_LINKS="$MAAS_WHEELS" SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 tox -e default`
  where `MAAS_WHEELS` is a directory holding the `maas_model-3.1.0` wheel (leave it empty when the package index serves maas-model). The steps below then run the working tree against that environment, in seconds:
  `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest <tests> --no-cov -q`
  The final task runs the whole `tox -e default` again.

## Review Focus

1. **OpenSearch unreachable, at startup versus at a refresh.** At startup the collector must stop with the connection error, not start with an empty configuration (Task 2: `test_collector_does_not_start_when_opensearch_is_down`). At a refresh it must keep collecting on its current configuration, log one traceback then one line per check, and pick a change up once OpenSearch is back (Task 4: `test_unreachable_opensearch_keeps_the_collector_running`).
2. **OpenSearch connection from OpenBao (`ES_VAULT_PATH`, no `ES_URL`).** The startup check must accept it and the provider must read through the connection built from the vault-resolved URL (Task 2: `test_configuration_reads_through_the_vault_connection`); the admin CLI must accept the same vault options for its own connection (Task 6: `test_connection_from_the_vault`).
3. **A service left with no file, or dropped from a shared path, while running.** Deleting every file of a service (or a `put` that drops the service tag) must not become a restart loop: the empty set is logged and the collector keeps running (Task 4: `test_service_left_without_configuration_keeps_running`); dropping the service from one of several shared paths is a real change and restarts (Task 4: `test_service_removed_from_a_shared_path_restarts`).
4. **A file whose collectors use a model defined in another file** (29 shared named models). `put` of that file alone must be accepted (Task 3: `test_a_model_defined_in_another_file_is_accepted`), and the collector-side check must resolve names across the whole new set: accepted when the defining file is present (Task 4: `test_model_defined_in_another_document_resolves`), rejected when it is not (Task 4: `test_model_missing_from_the_new_set_is_rejected`).
5. **Concurrent or interrupted publishes.** Two `latest` versions of a path, a tombstone next to its still-`latest` predecessor, and the late step 4 of a slow publish must neither resurrect a deleted path nor clear the flag of a newer version (Task 2: `test_opensearch_provider_hides_a_path_whose_tombstone_is_not_alone`; Task 3: `test_publish_repairs_the_flags_of_a_crashed_publish`, `test_a_late_publish_leaves_a_newer_version_latest`, `test_concurrent_publishes_on_the_same_base_conflict`).

---

## File Structure

| File | Status | Responsibility |
| --- | --- | --- |
| `src/maas_collector/rawdata/configuration.py` | Modify | `load_json_dict()` builds configurations from a dict; `register=` on `build_model_class()` and `build_collector_configuration()`; `load_json()` becomes a wrapper |
| `src/maas_collector/rawdata/messenger.py` | Modify | `Messenger.load_config_dict()` reads chunk sizes from a dict; `load_config_file()` becomes a wrapper |
| `src/maas_collector/rawdata/configprovider.py` | Create | `ConfigSource`, `ConfigStoreArgs`, `FileConfigProvider`, `OpenSearchConfigProvider`, `build_config_provider()`, `validate_config_sources()`, `describe_config_changes()`. Imported by `filecollector.py`, so it must not import any collector module |
| `src/maas_collector/rawdata/configstore.py` | Create | `ConfigStore`, the only writer: index creation, publish protocol, history, rollback, tombstones, per-entry configuration class resolution |
| `src/maas_collector/rawdata/collector/filecollector.py` | Modify | `CollectorArgs` fields, `config_provider`, `config_sources`, connection before `load_config()`, `check_config_changes()` in `run()` |
| `src/maas_collector/rawdata/collector/monitorcollector.py` | Modify | second pass over `config_sources`; `config_classes_for()` also builds probed entries with their own class |
| `src/maas_collector/rawdata/cli/lib/args.py` | Modify | `config_index_parser()`, `config_parser()`, `get_config_store_args()`, wiring in `common_parser()` and `get_collector_args()` |
| `src/maas_collector/rawdata/cli/config.py` | Create | admin CLI: `init`, `import`, `export`, `list`, `get`, `history`, `diff`, `put`, `rollback`, `delete` |
| `tests/fakeopensearch.py` | Create | in-memory opensearch-py client (409 on `op_type=create`, strict mappings, term queries, sort, size, scroll) and `add_config_document()` |
| `tests/conftest.py` | Modify | `fake_opensearch` fixture registered as the `default` connection |
| `tests/test_configuration.py`, `tests/test_messenger.py` | Modify | refactor tests |
| `tests/test_config_provider.py` | Create | providers, settings, collector start-up |
| `tests/test_configstore.py` | Create | publish protocol and validation |
| `tests/test_config_refresh.py` | Create | change detection and exit |
| `tests/test_monitoring.py`, `tests/test_credential_refresh.py` | Modify | monitor through the provider; drop the `monitorcollector.find_configurations` patches |
| `tests/test_config_cli.py` | Create | admin CLI |
| `docs/opensearch_config.md` | Create | document model, settings, restart behaviour, CLI, migration, roles, Dashboards |
| `docs/configuration.md`, `docs/common_options.md`, `docs/index.md` | Modify | pointers and option table |
| `CHANGELOG.md`, `pyproject.toml`, `sonar-project.properties` | Modify | 3.12.0 |

---

### Task 1: Load configurations and chunk sizes from dicts

**Files:**
- Modify: `src/maas_collector/rawdata/configuration.py:20-28` (`build_model_class` signature and docstring), `:75-79` (registration), `:105-112` and `:124` (`build_collector_configuration`), `:160-184` (`load_json`)
- Modify: `src/maas_collector/rawdata/messenger.py:150-180` (`load_config_file`)
- Test: `tests/test_configuration.py`, `tests/test_messenger.py`

**Interfaces:**
- Consumes: nothing new.
- Produces:
  - `configuration.build_model_class(meta: dict, register: bool = True) -> MAASRawDocument`
  - `configuration.build_collector_configuration(meta: dict, configuration_class, register=True)`
  - `configuration.load_json_dict(json_dict: dict, configuration_class, source: str = "", register: bool = True)`: generator of configuration instances; the `models` are built (and registered when `register`) once the collector entries are consumed, like `load_json`
  - `Messenger.load_config_dict(json_dict: dict, source: str) -> None`

- [ ] **Step 1: Write the failing tests**

In `tests/test_configuration.py`, add `import json` above `import os` (line 1), then append:

```python
def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def test_load_json_dict_builds_the_same_configurations_as_load_json():
    from_file = list(configuration.load_json(CONF1, FileCollectorConfiguration))

    from_dict = list(
        configuration.load_json_dict(
            read_json(CONF1), FileCollectorConfiguration, "conf1.json"
        )
    )

    assert [config.name for config in from_dict] == [
        config.name for config in from_file
    ]
    assert from_dict[0].model_meta == from_file[0].model_meta


def test_load_json_dict_registers_the_models_of_a_model_only_content():
    content = {"models": [{"name": "DictOnlyModel", "index": "", "fields": []}]}

    configs = list(
        configuration.load_json_dict(content, FileCollectorConfiguration, "models")
    )

    assert configs == []
    assert configuration.MODEL_DICT["DictOnlyModel"].__name__ == "DictOnlyModel"


def test_unregistered_models_leave_model_dict_untouched():
    before = dict(configuration.MODEL_DICT)
    content = {
        "collectors": [
            {
                "id_field": "",
                "routing_key": "",
                "model": {"name": "InlineCheckedModel", "index": "", "fields": []},
                "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
            }
        ],
        "models": [{"name": "SharedCheckedModel", "index": "", "fields": []}],
    }

    configs = list(
        configuration.load_json_dict(
            content, FileCollectorConfiguration, "check", register=False
        )
    )

    assert configs[0].model.__name__ == "InlineCheckedModel"
    assert configuration.MODEL_DICT == before
```

Append to `tests/test_messenger.py`:

```python
def test_load_config_dict_reads_the_chunk_sizes():
    instance = Messenger("amqp://localhost:5672")

    instance.load_config_dict(
        {"amqp": {"new.raw.data.a": {"chunk_size": 12}, "new.raw.data.b": {}}},
        "odata/a.json",
    )

    assert instance.chunk_config == {"new.raw.data.a": 12}


def test_load_config_dict_ignores_a_content_without_amqp():
    instance = Messenger("amqp://localhost:5672")

    instance.load_config_dict({"models": []}, "models.json")

    assert instance.chunk_config == {}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_configuration.py tests/test_messenger.py --no-cov -q`
Expected: 5 FAILED, with `AttributeError: module 'maas_collector.rawdata.configuration' has no attribute 'load_json_dict'` and `AttributeError: 'Messenger' object has no attribute 'load_config_dict'`.

- [ ] **Step 3: Add `register=` to the model and configuration builders**

In `src/maas_collector/rawdata/configuration.py`, replace the head of `build_model_class` (lines 20-28):

```python
def build_model_class(meta: dict, register: bool = True) -> MAASRawDocument:
    """opensearch document class factory

    Args:
        meta (dict): meta-information from configuration file
        register (bool, optional): add the class to MODEL_DICT. False only checks
            that the class builds. Defaults to True.

    Returns:
        model.MAASRawDocument: Document class object
    """
```

Replace its end (lines 75-79):

```python
    model_class = type(meta["name"], (MAASRawDocument,), class_dict)

    if register:
        add_model(model_class)

    return model_class
```

Replace the head of `build_collector_configuration` (lines 105-112):

```python
def build_collector_configuration(meta: dict, configuration_class, register=True):
    """build CollectorConfiguration instance from json dict

    Args:
        meta (dict): configuration meta information
        configuration_class: configuration dataclass to instanciate
        register (bool, optional): register an inline model in MODEL_DICT.
            Defaults to True.

    Returns:
        CollectorConfiguration: build configuration instance
    """
```

and line 124 `args["model"] = build_model_class(meta["model"])` with:

```python
            args["model"] = build_model_class(meta["model"], register=register)
```

- [ ] **Step 4: Extract `load_json_dict` from `load_json`**

Replace `load_json` (lines 160-184) with:

```python
def load_json_dict(
    json_dict: dict, configuration_class, source: str = "", register: bool = True
):
    """load a list of collector configuration from the content of a configuration
    file, and register its models

    Args:
        json_dict (dict): configuration content, {collectors?, models?, amqp?}
        configuration_class: configuration dataclass to instanciate
        source (str, optional): name of the content, for the logs. Defaults to "".
        register (bool, optional): register the models in MODEL_DICT. Defaults to
            True.

    Returns:
        typing.Iterator[CollectorConfiguration]: configuration instances
    """
    if "collectors" in json_dict:
        for conf_meta in json_dict["collectors"]:
            try:
                yield build_collector_configuration(
                    conf_meta, configuration_class, register=register
                )
            except Exception as error:
                LOGGER.critical(
                    "Cannot instanciate configuration from %s: %s", source, conf_meta
                )
                raise error

    if "models" in json_dict:
        for model_meta in json_dict["models"]:
            LOGGER.debug("Registering model %s", model_meta["name"])
            build_model_class(model_meta, register=register)


def load_json(path: str, configuration_class):
    """load a list of collector configuration from a json file

    Args:
        path (str): path a json file

    Returns:
        typing.List[CollectorConfiguration]: list of configuration instances
    """
    LOGGER.info("Loading configuration file: %s", path)
    with open(path, encoding="UTF-8") as conf_fd:
        json_dict = json.load(conf_fd)

    yield from load_json_dict(json_dict, configuration_class, path)
```

- [ ] **Step 5: Extract `load_config_dict` from `Messenger.load_config_file`**

In `src/maas_collector/rawdata/messenger.py`, replace `load_config_file` (lines 150-180) with:

```python
    def load_config_file(self, path: str):
        """Load amqp configuration from file"""
        with open(path, "r", encoding="UTF-8") as config_fd:
            json_config = json.load(config_fd)

        self.load_config_dict(json_config, path)

    def load_config_dict(self, json_dict: dict, source: str):
        """Load amqp configuration from the content of a configuration file

        Args:
            json_dict (dict): configuration content, {collectors?, models?, amqp?}
            source (str): name of the content, for the logs
        """
        if "amqp" not in json_dict:
            # not amqp setting
            return

        for routing_key, routing_info in json_dict["amqp"].items():
            if not "chunk_size" in routing_info:
                continue

            # log overwrite
            if routing_key in self.chunk_config:
                # log redeclare
                self.logger.warning(
                    "%s: overwrite chunk size config for %s from %s to %s",
                    source,
                    routing_key,
                    self.chunk_config[routing_key],
                    routing_info["chunk_size"],
                )

            self.logger.info(
                "Set chunk configuration for %s: %d",
                routing_key,
                routing_info["chunk_size"],
            )

            self.chunk_config[routing_key] = routing_info["chunk_size"]
```

- [ ] **Step 6: Run the whole suite**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `198 passed` (193 existing, among them `test_configuration`, `test_meta_conf`, `test_extractor_lta_xls` and `test_messenger`, plus 5 new).

- [ ] **Step 7: Commit**

```bash
git add src/maas_collector/rawdata/configuration.py src/maas_collector/rawdata/messenger.py tests/test_configuration.py tests/test_messenger.py
git commit -m "refactor: :recycle: Load collector configurations and chunk sizes from dicts" -m "load_json_dict() and Messenger.load_config_dict() build from the content of a configuration file, so a configuration stored elsewhere than in a file can be loaded. load_json() and load_config_file() keep their signatures and wrap them. build_model_class() and build_collector_configuration() take register=False to check that a model builds without adding it to MODEL_DICT." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Configuration providers, settings and the in-memory OpenSearch

The OpenSearch fake lands here, not with `ConfigStore`: the OpenSearch provider is the first code that needs it.

**Files:**
- Create: `tests/fakeopensearch.py`
- Modify: `tests/conftest.py:17-28` (import and `fake_opensearch` fixture)
- Create: `src/maas_collector/rawdata/configprovider.py`
- Modify: `src/maas_collector/rawdata/cli/lib/args.py:13-14` (imports), `:1157` (new parsers before `common_parser`), `:1164-1175` (parents), `:1359` (`get_collector_args`)
- Modify: `src/maas_collector/rawdata/collector/filecollector.py:33-37` (imports), `:100` (`CollectorArgs`), `:326-358` (`__init__` end, `get_configs`, `load_config` head), `:398-441` (`config_files`, `setup`)
- Test: `tests/test_config_provider.py`

**Interfaces:**
- Consumes: `configuration.load_json_dict()`, `Messenger.load_config_dict()` (Task 1); `configuration.find_configurations(directory_path) -> list[str]`; `args.get_es_credentials_url()` (vault resolution, existing).
- Produces:
  - `configprovider.DEFAULT_CONFIG_INDEX = "maas-collector-config"`
  - `@dataclass ConfigStoreArgs(index: str = DEFAULT_CONFIG_INDEX, service: str = "")`
  - `@dataclass(frozen=True) ConfigSource(name: str, content: dict, version: int | None = None)` with property `label -> str` (`name` or `name@version`)
  - `ConfigProvider.load() -> list[ConfigSource]` (abstract), `ConfigProvider.fingerprint(sources: list[ConfigSource]) -> frozenset | None`
  - `FileConfigProvider(config_file: str = "", config_dir: str = "")`, fingerprint `None`
  - `OpenSearchConfigProvider(index: str, service: str, using: str = "default")`, property `client`, fingerprint `frozenset((path, version))`
  - `build_config_provider(args) -> ConfigProvider` (raises `ValueError`)
  - `args.config_index_parser() -> ArgumentParser`, `args.config_parser() -> ArgumentParser`, `args.get_config_store_args(namespace) -> ConfigStoreArgs`
  - `CollectorArgs.config_backend`, `.config_refresh_period`, `.config_store`
  - `FileCollector.config_provider` (cached property), `FileCollector.config_sources: list[ConfigSource]`, `FileCollector._config_fingerprint`, `FileCollector.get_configs(source: ConfigSource) -> list`
  - `tests/fakeopensearch.py`: `FakeOpenSearch` (attributes `documents`, `mappings`, `outage`; methods `indices.exists/create`, `index`, `get`, `update`, `search`, `scroll`, `clear_scroll`), `add_config_document(client, path, version, services, content, latest=True, deleted=False, index="maas-collector-config")`
  - conftest fixture `fake_opensearch` (a `FakeOpenSearch` registered as the `default` connection)

- [ ] **Step 1: Create the in-memory OpenSearch**

Create `tests/fakeopensearch.py`:

```python
"""
In-memory stand-in for the opensearch-py client, for the configuration tests.

It covers what the configuration provider, the configuration store and the admin
CLI call: indices.exists / create, index (op_type=create answers 409 on an existing
id), get, update, search with a bool query of term filters, sort and size, and the
scroll calls of opensearchpy.helpers.scan. An index created with a "dynamic":
"strict" mapping refuses unmapped top-level fields, like OpenSearch does.
"""

import copy
import itertools

from opensearchpy.exceptions import ConflictError, NotFoundError, RequestError

# OpenSearch answers 10 hits when a search sets no size
DEFAULT_SIZE = 10


class FakeIndices:
    """client.indices"""

    def __init__(self, client: "FakeOpenSearch"):
        self.client = client

    def exists(self, index: str, **_kwargs) -> bool:
        """True if the index exists"""
        self.client.check_available()
        return index in self.client.documents

    def create(self, index: str, body: dict = None, **_kwargs) -> dict:
        """create an index, with its mapping"""
        self.client.check_available()
        if index in self.client.documents:
            raise RequestError(400, "resource_already_exists_exception", index)
        self.client.documents[index] = {}
        self.client.mappings[index] = copy.deepcopy((body or {}).get("mappings", {}))
        return {"acknowledged": True, "index": index}


class FakeOpenSearch:
    """opensearch-py client keeping its documents in dictionaries"""

    def __init__(self):
        # index name -> document id -> source
        self.documents: dict = {}

        # index name -> mappings given at creation
        self.mappings: dict = {}

        # set to an exception instance to make every call raise it
        self.outage: Exception | None = None

        self.indices = FakeIndices(self)

        self._scroll_ids = itertools.count(1)

    def check_available(self):
        """raise the outage exception, if any"""
        if self.outage is not None:
            raise self.outage

    def _check_mapping(self, index: str, fields):
        mapping = self.mappings.get(index, {})
        if mapping.get("dynamic") != "strict":
            return
        unknown = set(fields) - set(mapping.get("properties", {}))
        if unknown:
            raise RequestError(400, "strict_dynamic_mapping_exception", sorted(unknown))

    def index(
        self, index: str, body: dict, id: str = None, op_type: str = None, **_kwargs
    ):
        """write a document. op_type=create refuses an existing id with a 409"""
        # pylint: disable=redefined-builtin
        self.check_available()
        self._check_mapping(index, body)
        documents = self.documents.setdefault(index, {})
        if op_type == "create" and id in documents:
            raise ConflictError(409, "version_conflict_engine_exception", id)
        documents[id] = copy.deepcopy(body)
        return {"_index": index, "_id": id, "result": "created"}

    def get(self, index: str, id: str, **_kwargs) -> dict:
        """read a document by id"""
        # pylint: disable=redefined-builtin
        self.check_available()
        if index not in self.documents:
            raise NotFoundError(404, "index_not_found_exception", index)
        if id not in self.documents[index]:
            raise NotFoundError(404, "not_found", id)
        return {
            "_index": index,
            "_id": id,
            "found": True,
            "_source": copy.deepcopy(self.documents[index][id]),
        }

    def update(self, index: str, id: str, body: dict, **_kwargs) -> dict:
        """merge body["doc"] into a document"""
        # pylint: disable=redefined-builtin
        self.check_available()
        self._check_mapping(index, body["doc"])
        if id not in self.documents.get(index, {}):
            raise NotFoundError(404, "document_missing_exception", id)
        self.documents[index][id].update(copy.deepcopy(body["doc"]))
        return {"_index": index, "_id": id, "result": "updated"}

    def search(
        self,
        index: str = None,
        body: dict = None,
        size: int = None,
        scroll: str = None,
        **_kwargs,
    ) -> dict:
        """bool / term / terms / match_all queries, sort, size. With scroll, every
        hit comes in the first page, and scroll() ends the iteration"""
        self.check_available()
        if index not in self.documents:
            raise NotFoundError(404, "index_not_found_exception", index)
        body = body or {}
        hits = [
            {"_index": index, "_id": doc_id, "_source": copy.deepcopy(source)}
            for doc_id, source in self.documents[index].items()
            if matches(source, body.get("query", {"match_all": {}}))
        ]
        hits = sort_hits(hits, body.get("sort", []))
        if scroll is None:
            hits = hits[: body.get("size", size if size is not None else DEFAULT_SIZE)]
        response = {
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {"total": {"value": len(hits)}, "hits": hits},
        }
        if scroll is not None:
            response["_scroll_id"] = f"scroll-{next(self._scroll_ids)}"
        return response

    def scroll(self, body: dict = None, **_kwargs) -> dict:
        """next page of a scroll: always empty, search() returned every hit"""
        self.check_available()
        return {
            "_scroll_id": body["scroll_id"],
            "_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
            "hits": {"hits": []},
        }

    def clear_scroll(self, **_kwargs) -> dict:
        """release a scroll"""
        return {"succeeded": True}


def matches(source: dict, query: dict) -> bool:
    """evaluate the subset of the query DSL the configuration code uses"""
    if "match_all" in query:
        return True

    if "term" in query:
        ((field, value),) = query["term"].items()
        if isinstance(value, dict):
            value = value["value"]
        stored = source.get(field)
        if isinstance(stored, list):
            return value in stored
        return field in source and stored == value

    if "terms" in query:
        ((field, values),) = query["terms"].items()
        return any(matches(source, {"term": {field: value}}) for value in values)

    if "bool" in query:
        clauses = query["bool"]

        def as_list(name):
            value = clauses.get(name, [])
            return value if isinstance(value, list) else [value]

        return all(
            matches(source, clause) for clause in as_list("filter") + as_list("must")
        ) and not any(matches(source, clause) for clause in as_list("must_not"))

    raise NotImplementedError(f"FakeOpenSearch does not support the query {query}")


def sort_hits(hits: list, sort) -> list:
    """apply a sort clause: "_doc", {field: order} or {field: {"order": order}}"""
    if isinstance(sort, (str, dict)):
        sort = [sort]
    for clause in reversed(sort):
        if clause == "_doc":
            continue
        ((field, order),) = clause.items()
        if isinstance(order, dict):
            order = order.get("order", "asc")
        hits = sorted(
            hits, key=lambda hit: hit["_source"][field], reverse=order == "desc"
        )
    return hits


def add_config_document(
    client: FakeOpenSearch,
    path: str,
    version: int,
    services: list,
    content: dict,
    latest: bool = True,
    deleted: bool = False,
    index: str = "maas-collector-config",
):
    """write a configuration document as a publish would, without validation"""
    client.index(
        index=index,
        id=f"{path}@{version}",
        body={
            "path": path,
            "version": version,
            "latest": latest,
            "deleted": deleted,
            "services": services,
            "content": content,
        },
    )
```

In `tests/conftest.py`, after `from maas_model.testing.fakevault import FakeVault` (line 17), add:

```python

from fakeopensearch import FakeOpenSearch
```

and after the `mock_client` fixture (ends line 26), add:

```python
@pytest.fixture
def fake_opensearch():
    """an in-memory OpenSearch, registered as the default connection"""
    client = FakeOpenSearch()
    es_connections.add_connection("default", client)
    yield client
    es_connections._conn = {}
    es_connections._kwargs = {}
```

- [ ] **Step 2: Write the failing tests**

Create `tests/test_config_provider.py`:

```python
"""Tests for the configuration providers: files and OpenSearch documents"""

import json
import logging
import os
from unittest.mock import patch

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.cli.lib.args import common_parser, get_collector_args
from maas_collector.rawdata.collector.filecollector import (
    CollectorArgs,
    FileCollector,
    FileCollectorConfiguration,
)
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    ConfigStoreArgs,
    FileConfigProvider,
    OpenSearchConfigProvider,
    build_config_provider,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

COLLECTOR_D = os.path.join(CONF_DIR, "collector.d")

MODEL_RESOLVE = os.path.join(CONF_DIR, "model_resolve")

INDEX = "maas-collector-config"


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def collector_entry(model_name: str, interface_name: str = "") -> dict:
    """a collector entry building with FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": {"name": model_name, "index": "", "fields": []},
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
    }


def opensearch_args(**kwargs) -> CollectorArgs:
    return CollectorArgs(
        es_url="http://opensearch.example:9200",
        config_backend="opensearch",
        config_store=ConfigStoreArgs(index=INDEX, service="odata"),
        **kwargs,
    )


# ---------------------------------------------------------------- file backend


def test_file_provider_reads_the_visible_files_of_a_tree():
    provider = FileConfigProvider(config_dir=COLLECTOR_D)

    sources = provider.load()

    assert sorted(source.name for source in sources) == sorted(
        configuration.find_configurations(COLLECTOR_D)
    )
    assert len(sources) == 5
    assert all(".hidden" not in source.name for source in sources)
    for source in sources:
        assert source.content == read_json(source.name)
        assert source.version is None


def test_file_provider_reads_the_single_file_first():
    single = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")

    sources = FileConfigProvider(single, MODEL_RESOLVE).load()

    assert sources[0].name == single
    assert len(sources) == 4


def test_file_provider_never_changes():
    provider = FileConfigProvider(config_dir=COLLECTOR_D)

    assert provider.fingerprint(provider.load()) is None


@pytest.mark.parametrize("conf_dir", [COLLECTOR_D, MODEL_RESOLVE])
def test_file_backend_builds_the_same_configurations_as_before(conf_dir):
    collector = FileCollector(CollectorArgs(rawdata_config_dir=conf_dir))
    collector.load_config()

    expected = []
    for path in configuration.find_configurations(conf_dir):
        expected.extend(
            config.name
            for config in configuration.load_json(path, FileCollectorConfiguration)
        )

    assert sorted(config.name for config in collector.configs) == sorted(expected)
    assert all(not isinstance(config.model, str) for config in collector.configs)


def test_model_only_and_amqp_only_files_load(tmp_path):
    (tmp_path / "models.json").write_text(
        json.dumps({"models": [{"name": "OnlyAModel", "index": "", "fields": []}]}),
        encoding="UTF-8",
    )
    (tmp_path / "chunks.json").write_text(
        json.dumps({"amqp": {"new.raw.data.only": {"chunk_size": 64}}}),
        encoding="UTF-8",
    )

    collector = FileCollector(CollectorArgs(rawdata_config_dir=str(tmp_path)))
    collector.load_config()

    assert collector.configs == []
    assert collector._messenger.chunk_config == {"new.raw.data.only": 64}
    assert "OnlyAModel" in configuration.MODEL_DICT


# ---------------------------------------------------------- opensearch backend


def test_opensearch_provider_loads_the_latest_documents_of_its_service(
    fake_opensearch,
):
    add_config_document(fake_opensearch, "odata/a.json", 1, ["odata"], {}, latest=False)
    add_config_document(fake_opensearch, "odata/a.json", 2, ["odata"], {"amqp": {}})
    add_config_document(fake_opensearch, "sftp/b.json", 1, ["sftp"], {"amqp": {}})
    add_config_document(fake_opensearch, "shared/c.json", 4, ["sftp", "odata"], {})
    add_config_document(
        fake_opensearch, "odata/gone.json", 1, ["odata"], {}, latest=False
    )
    add_config_document(
        fake_opensearch, "odata/gone.json", 2, ["odata"], {}, deleted=True
    )

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert sources == [
        ConfigSource("odata/a.json", {"amqp": {}}, 2),
        ConfigSource("shared/c.json", {}, 4),
    ]


def test_opensearch_provider_keeps_the_highest_of_two_latest_versions(
    fake_opensearch,
):
    """a publish flags the new version latest before it clears the old one"""
    add_config_document(fake_opensearch, "odata/a.json", 2, ["odata"], {"amqp": {}})
    add_config_document(fake_opensearch, "odata/a.json", 3, ["odata"], {"models": []})

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert sources == [ConfigSource("odata/a.json", {"models": []}, 3)]


def test_opensearch_provider_hides_a_path_whose_tombstone_is_not_alone(
    fake_opensearch,
):
    """between the two steps of a delete, the previous version is still latest"""
    add_config_document(fake_opensearch, "odata/a.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "odata/b.json", 1, ["odata"], {})
    add_config_document(fake_opensearch, "odata/b.json", 2, ["odata"], {}, deleted=True)

    sources = OpenSearchConfigProvider(INDEX, "odata").load()

    assert [source.name for source in sources] == ["odata/a.json"]


def test_opensearch_provider_order_is_stable(fake_opensearch):
    for path in ("odata/z.json", "odata/a.json", "odata/m/b.json"):
        add_config_document(fake_opensearch, path, 1, ["odata"], {})

    provider = OpenSearchConfigProvider(INDEX, "odata")

    names = [source.name for source in provider.load()]

    assert names == ["odata/a.json", "odata/m/b.json", "odata/z.json"]
    assert provider.fingerprint(provider.load()) == frozenset(
        {("odata/a.json", 1), ("odata/m/b.json", 1), ("odata/z.json", 1)}
    )


def test_opensearch_provider_raises_without_document(fake_opensearch):
    add_config_document(fake_opensearch, "sftp/b.json", 1, ["sftp"], {})

    with pytest.raises(ValueError, match="no configuration for service odata"):
        OpenSearchConfigProvider(INDEX, "odata").load()


def test_opensearch_provider_raises_without_index(fake_opensearch):
    with pytest.raises(ValueError, match="the index does not exist"):
        OpenSearchConfigProvider(INDEX, "odata").load()


# ------------------------------------------------------------------ settings


def test_opensearch_backend_needs_a_service():
    args = opensearch_args()
    args.config_store.service = ""

    with pytest.raises(ValueError, match="CONFIG_SERVICE"):
        build_config_provider(args)


def test_opensearch_backend_needs_the_opensearch_connection():
    args = opensearch_args()
    args.es_url = ""

    with pytest.raises(ValueError, match="ES_URL or ES_VAULT_PATH"):
        build_config_provider(args)


def test_opensearch_backend_ignores_the_configuration_files(caplog):
    args = opensearch_args(rawdata_config_dir=COLLECTOR_D)

    with caplog.at_level(logging.WARNING):
        provider = build_config_provider(args)

    assert isinstance(provider, OpenSearchConfigProvider)
    assert "RAWDATA_CONFIG_DIR" in caplog.text


def test_unknown_backend_is_refused():
    with pytest.raises(ValueError, match="expected file or opensearch"):
        build_config_provider(CollectorArgs(config_backend="consul"))


def test_file_backend_is_the_default():
    provider = build_config_provider(CollectorArgs(rawdata_config_dir=COLLECTOR_D))

    assert isinstance(provider, FileConfigProvider)


CONFIG_ENV = (
    "CONFIG_BACKEND",
    "CONFIG_INDEX",
    "CONFIG_SERVICE",
    "CONFIG_REFRESH_PERIOD",
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
)


@pytest.fixture
def clean_env(monkeypatch):
    for name in CONFIG_ENV:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


def test_configuration_settings_default_to_the_files(clean_env):
    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))

    assert args.config_backend == "file"
    assert args.config_refresh_period == 300
    assert args.config_store == ConfigStoreArgs(index=INDEX, service="")


def test_configuration_settings_come_from_the_environment(clean_env):
    clean_env.setenv("CONFIG_BACKEND", "opensearch")
    clean_env.setenv("CONFIG_INDEX", "my-config")
    clean_env.setenv("CONFIG_SERVICE", "monitor-main")
    clean_env.setenv("CONFIG_REFRESH_PERIOD", "10")

    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))

    assert args.config_backend == "opensearch"
    assert args.config_refresh_period == 10
    assert args.config_store == ConfigStoreArgs(
        index="my-config", service="monitor-main"
    )


# ------------------------------------------------------- collector start-up


def test_collector_loads_its_configuration_from_opensearch(fake_opensearch):
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        1,
        ["odata"],
        {"collectors": [collector_entry("OpenSearchModelA", "ITF_A")]},
    )

    calls = []

    def create_connection(**kwargs):
        # the provider reads through the connection: it must exist by then
        assert collector.config_sources == []
        calls.append(kwargs)

    collector = FileCollector(opensearch_args())
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection",
        side_effect=create_connection,
    ):
        collector.setup()

    assert calls[0]["hosts"] == ["http://opensearch.example:9200"]
    assert collector.get_interface_names() == ["ITF_A"]
    assert collector.config_sources[0].label == "odata/a.json@1"


def test_collector_does_not_start_when_opensearch_is_down(fake_opensearch):
    fake_opensearch.outage = OpenSearchConnectionError("N/A", "unreachable", None)

    collector = FileCollector(opensearch_args())
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection"
    ), pytest.raises(OpenSearchConnectionError):
        collector.setup()


OPENSEARCH_SECRET = {
    "url": "https://opensearch.example:9200",
    "username": "maas",
    "password": "s3cr3t-opensearch",
}


def test_configuration_reads_through_the_vault_connection(
    fake_vault, fake_opensearch, clean_env, tmp_path
):
    """ES_VAULT_PATH alone gives the connection: no ES_URL is needed"""
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")
    fake_vault.put("secret", "maas/cds/opensearch", OPENSEARCH_SECRET)
    for name, value in {
        "VAULT_ADDR": fake_vault.url,
        "VAULT_AUTH_METHOD": "token",
        "VAULT_TOKEN_FILE": str(token_file),
        "ES_VAULT_PATH": "maas/cds/opensearch",
        "CONFIG_BACKEND": "opensearch",
        "CONFIG_SERVICE": "odata",
    }.items():
        clean_env.setenv(name, value)
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        1,
        ["odata"],
        {"collectors": [collector_entry("VaultConnectionModel", "ITF_VAULT")]},
    )

    args = get_collector_args(CollectorArgs, common_parser().parse_args([]))
    collector = FileCollector(args)
    with patch(
        "opensearchpy.connection.connections.Connections.create_connection"
    ) as create_connection:
        collector.setup()

    assert create_connection.call_args.kwargs["hosts"] == [
        "https://maas:s3cr3t-opensearch@opensearch.example:9200"
    ]
    assert collector.get_interface_names() == ["ITF_VAULT"]
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_config_provider.py --no-cov -q`
Expected: collection error `ModuleNotFoundError: No module named 'maas_collector.rawdata.configprovider'`.

- [ ] **Step 4: Create the providers**

Create `src/maas_collector/rawdata/configprovider.py`:

```python
"""
Configuration providers: where the collectors read their configuration from.

A provider returns configuration sources. A source is the content of one
configuration file, {collectors?, models?, amqp?}, with its name: the file path for
the file backend, the document path for the OpenSearch backend.
"""

import abc
import dataclasses
import json
import logging

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.exceptions import NotFoundError
from opensearchpy.helpers import scan

from maas_collector.rawdata.configuration import find_configurations

LOGGER = logging.getLogger("ConfigProvider")

DEFAULT_CONFIG_INDEX = "maas-collector-config"


@dataclasses.dataclass
class ConfigStoreArgs:
    """OpenSearch configuration settings of the collectors"""

    # index holding the configuration documents
    index: str = DEFAULT_CONFIG_INDEX

    # service tag of the documents the collector loads
    service: str = ""


@dataclasses.dataclass(frozen=True)
class ConfigSource:
    """content of one configuration file"""

    # file path, or document path
    name: str

    # {collectors?, models?, amqp?}
    content: dict

    # document version, None for a file
    version: int | None = None

    @property
    def label(self) -> str:
        """name and version, for the logs: odata/prip.json@3"""
        if self.version is None:
            return self.name
        return f"{self.name}@{self.version}"


class ConfigProvider(abc.ABC):
    """Source of collector configurations"""

    @abc.abstractmethod
    def load(self) -> list[ConfigSource]:
        """Read the configuration sources, in a deterministic order"""

    def fingerprint(self, sources: list[ConfigSource]) -> frozenset | None:
        """Identify a set of sources, to notice a change

        Args:
            sources (list[ConfigSource]): sources returned by load()

        Returns:
            frozenset | None: None when the sources never change while running
        """
        return None


class FileConfigProvider(ConfigProvider):
    """Configuration files: a single file and / or a directory tree"""

    def __init__(self, config_file: str = "", config_dir: str = ""):
        self.config_file = config_file

        self.config_dir = config_dir

    def paths(self) -> list[str]:
        """the configuration file, then the files of the directory tree, hidden
        files and directories excluded"""
        paths = []

        if self.config_file:
            paths.append(self.config_file)

        if self.config_dir:
            paths.extend(find_configurations(self.config_dir))

        return paths

    def load(self) -> list[ConfigSource]:
        """Read every configuration file"""
        sources = []

        for path in self.paths():
            LOGGER.info("Loading configuration file: %s", path)
            with open(path, encoding="UTF-8") as conf_fd:
                sources.append(ConfigSource(path, json.load(conf_fd)))

        return sources


class OpenSearchConfigProvider(ConfigProvider):
    """Latest configuration documents of a service, in a maas-collector-config index.

    The provider uses the opensearch-py connection registered under the alias using:
    the one FileCollector.setup() creates from the collector arguments, after the
    vault has resolved ES_VAULT_PATH.
    """

    def __init__(self, index: str, service: str, using: str = "default"):
        self.index = index

        self.service = service

        self.using = using

    @property
    def client(self):
        """opensearch-py client of the collector"""
        return es_connections.get_connection(self.using)

    def load(self) -> list[ConfigSource]:
        """Read the current version of every path tagged with the service

        A path keeps its highest version among the documents flagged latest: two of
        them exist between the two steps of a publish. A path whose current version
        is a tombstone is left out.

        Raises:
            ValueError: when the index holds no configuration for the service
            opensearchpy.exceptions.OpenSearchException: when the index cannot be read

        Returns:
            list[ConfigSource]: sources sorted by path
        """
        query = {
            "query": {
                "bool": {
                    "filter": [
                        {"term": {"latest": True}},
                        {"term": {"services": self.service}},
                    ]
                }
            }
        }

        newest = {}

        try:
            for hit in scan(self.client, index=self.index, query=query):
                document = hit["_source"]
                current = newest.get(document["path"])
                if current is None or document["version"] > current["version"]:
                    newest[document["path"]] = document
        except NotFoundError as error:
            raise ValueError(
                f"no configuration for service {self.service} in index {self.index}:"
                " the index does not exist"
            ) from error

        sources = [
            ConfigSource(path, newest[path]["content"], newest[path]["version"])
            for path in sorted(newest)
            if not newest[path].get("deleted")
        ]

        if not sources:
            raise ValueError(
                f"no configuration for service {self.service} in index {self.index}"
            )

        return sources

    def fingerprint(self, sources: list[ConfigSource]) -> frozenset:
        """the (path, version) pairs"""
        return frozenset((source.name, source.version) for source in sources)


def build_config_provider(args) -> ConfigProvider:
    """Create the configuration provider of a collector

    Args:
        args (CollectorArgs): collector arguments

    Raises:
        ValueError: on an unknown backend or incomplete OpenSearch settings

    Returns:
        ConfigProvider: provider
    """
    backend = args.config_backend or "file"

    if backend == "file":
        return FileConfigProvider(args.rawdata_config, args.rawdata_config_dir)

    if backend == "opensearch":
        store_args = args.config_store or ConfigStoreArgs()

        if not store_args.service:
            raise ValueError(
                "CONFIG_BACKEND=opensearch needs the service to load:"
                " set CONFIG_SERVICE or --config-service"
            )

        if not args.es_url:
            raise ValueError(
                "CONFIG_BACKEND=opensearch needs the OpenSearch connection:"
                " set ES_URL or ES_VAULT_PATH"
            )

        if args.rawdata_config or args.rawdata_config_dir:
            LOGGER.warning(
                "RAWDATA_CONFIG / -c and RAWDATA_CONFIG_DIR / -d are ignored:"
                " the configuration comes from the OpenSearch index %s",
                store_args.index,
            )

        return OpenSearchConfigProvider(store_args.index, store_args.service)

    raise ValueError(
        f"Unknown configuration backend {backend}: expected file or opensearch"
    )
```

- [ ] **Step 5: Add the settings**

In `src/maas_collector/rawdata/cli/lib/args.py`, after `from maas_collector.rawdata.collector.credentialprovider import VaultArgs` (line 13), add:

```python
from maas_collector.rawdata.configprovider import DEFAULT_CONFIG_INDEX, ConfigStoreArgs
```

Before `def common_parser()` (line 1158), add:

```python
def config_index_parser() -> ArgumentParser:
    """configuration index CLI argument parser factory, shared by the collectors
    and the configuration admin CLI

    Returns:
        ArgumentParser: configuration index argument parser
    """
    parser = ArgumentParser(add_help=False)

    parser.add_argument(
        "--config-index",
        dest="config_index",
        help="OpenSearch index holding the collector configurations"
        " (default: %(default)s)",
        action=EnvDefault,
        envvar="CONFIG_INDEX",
        required=False,
        default=DEFAULT_CONFIG_INDEX,
        type=str,
    )

    return parser


def config_parser() -> ArgumentParser:
    """collector configuration source CLI argument parser factory

    Returns:
        ArgumentParser: configuration source argument parser
    """
    parser = ArgumentParser(add_help=False, parents=[config_index_parser()])

    parser.add_argument(
        "--config-backend",
        dest="config_backend",
        help="Where the configuration is read from: file (-c, -d) or opensearch"
        " (default: %(default)s)",
        action=EnvDefault,
        envvar="CONFIG_BACKEND",
        required=False,
        default="file",
        type=str,
        choices=["file", "opensearch"],
    )

    parser.add_argument(
        "--config-service",
        dest="config_service",
        help="Service tag of the configuration documents to load, required with"
        " the opensearch backend (default: %(default)s)",
        action=EnvDefault,
        envvar="CONFIG_SERVICE",
        required=False,
        default="",
        type=str,
    )

    parser.add_argument(
        "--config-refresh-period",
        dest="config_refresh_period",
        help="Seconds between two checks for a configuration change, 0 never"
        " checks (default: %(default)s)",
        action=EnvDefault,
        envvar="CONFIG_REFRESH_PERIOD",
        required=False,
        default=300,
        type=int,
    )

    return parser


def get_config_store_args(namespace: Namespace) -> ConfigStoreArgs:
    """ConfigStoreArgs factory

    Args:
        namespace (Namespace): parsed arguments

    Returns:
        ConfigStoreArgs: configuration index settings
    """
    return ConfigStoreArgs(
        index=namespace.config_index,
        service=namespace.config_service,
    )
```

In `common_parser()`, add `config_parser()` to the parents (lines 1164-1175):

```python
    parser = ArgumentParser(
        add_help=False,
        parents=[
            es_parser(),
            amqp_parser(),
            backup_parser_general(),
            backup_parser_s3(),
            backup_parser_sftp(),
            replay_parser(),
            vault_parser(),
            config_parser(),
        ],
    )
```

In `get_collector_args()`, after `vault=get_vault_args(namespace),` (line 1359), add:

```python
        config_backend=namespace.config_backend,
        config_refresh_period=namespace.config_refresh_period,
        config_store=get_config_store_args(namespace),
```

- [ ] **Step 6: Load the configuration through the provider**

In `src/maas_collector/rawdata/collector/filecollector.py`, replace the `configuration` import (lines 33-37):

```python
from maas_collector.rawdata.configuration import load_json_dict, get_model
from maas_collector.rawdata.configprovider import (
    ConfigProvider,
    ConfigSource,
    ConfigStoreArgs,
    build_config_provider,
)
```

At the end of `CollectorArgs`, after `es_ignore_certs_verification: bool = False` (line 100), add:

```python

    # where the configuration is read from: file (rawdata_config and
    # rawdata_config_dir) or opensearch (config_store)
    config_backend: str = "file"

    # seconds between two checks for a configuration change, 0 never checks
    config_refresh_period: int = 300

    config_store: ConfigStoreArgs = None
```

Replace from `# list of configuration files` in `__init__` through the first loop of `load_config` (lines 326-358) with the code below. The emptiness check of `get_configs` goes away: it tested a generator, which is always truthy, so it never fired, and making it fire would reject model-only and amqp-only files.

```python
        # configuration sources the configurations were built from
        self.config_sources: list[ConfigSource] = []

        # fingerprint of config_sources, None when they cannot change
        self._config_fingerprint = None

    @cached_property
    def config_provider(self) -> ConfigProvider:
        """where the configuration is read from"""
        return build_config_provider(self.args)

    def get_configs(self, source: ConfigSource) -> list:
        """build the configurations of a configuration source, and register its
        models

        Args:
            source (ConfigSource): content of a configuration file

        Returns:
            list: configuration objects, none for a model-only or amqp-only source
        """
        return list(load_json_dict(source.content, self.CONFIG_CLASS, source.name))

    def load_config(self):
        """populate configuration for pattern matching and routing configuration"""
        self.configs.clear()

        sources = self.config_provider.load()

        for source in sources:
            self.configs.extend(self.get_configs(source))

            self._messenger.load_config_dict(source.content, source.name)

        self.config_sources = sources
        self._config_fingerprint = self.config_provider.fingerprint(sources)
```

(The rest of `load_config`, from `# resolve model`, is unchanged.)

Delete the `config_files` property (lines 398-419) and make `setup()` (lines 421-441) create the connection first:

```python
    def setup(self):
        """Connect to opensearch, load rawdata extraction configuration"""
        # the connection comes first: the opensearch configuration backend reads
        # through it. Connections are lazy, so the file backend opens nothing.
        self.logger.info(
            "Setup connection to opensearch to with %d max retries / timeout=%s",
            self.args.es_retries,
            self.args.es_timeout,
        )
        es_connections.create_connection(
            hosts=[self.args.es_url],
            retry_on_timeout=True,
            max_retries=self.args.es_retries,
            timeout=self.args.es_timeout,
            verify_certs=not self.args.es_ignore_certs_verification,
            ssl_show_warn=not self.args.es_ignore_certs_verification,
        )

        # load json configuration
        self.load_config()
```

(The signal handlers and `self._initialized = True` that follow are unchanged.)

- [ ] **Step 7: Run the tests to verify they pass**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `220 passed` (22 new). `test_configuration`, `test_backup_report_integration`, `test_monitoring`, `test_connection_args` and the credential tests stay green.

- [ ] **Step 8: Commit**

```bash
git add tests/fakeopensearch.py tests/conftest.py tests/test_config_provider.py src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/cli/lib/args.py src/maas_collector/rawdata/collector/filecollector.py
git commit -m "feat: :sparkles: Read the collector configuration from files or OpenSearch" -m "A configuration provider returns the content of each configuration file. The file provider keeps -c and -d as they are and never changes. With CONFIG_BACKEND=opensearch and CONFIG_SERVICE, the OpenSearch provider reads the latest documents of the service from CONFIG_INDEX, through the connection setup() now creates before loading the configuration, so ES_VAULT_PATH works too. tests/fakeopensearch.py is an in-memory client for the tests." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: ConfigStore, the only writer

**Files:**
- Modify: `src/maas_collector/rawdata/configprovider.py` (imports, constants, `ConfigValidationError`, `is_config_content`, `validate_config_sources`)
- Create: `src/maas_collector/rawdata/configstore.py`
- Test: `tests/test_configstore.py`

**Interfaces:**
- Consumes: `ConfigSource`, `DEFAULT_CONFIG_INDEX`, `OpenSearchConfigProvider` (Task 2); `build_collector_configuration(..., register=False)`, `build_model_class(..., register=False)` (Task 1); `FakeOpenSearch`, `add_config_document`, fixture `fake_opensearch` (Task 2); `implementation.get_collector_class_by_config_classname(name)` (existing, raises `KeyError`).
- Produces:
  - `configprovider.CONFIG_KEYS = frozenset({"collectors", "models", "amqp"})`
  - `configprovider.ConfigValidationError(label: str, reason: str)`, a `ValueError` whose message is `"<label>: <reason>"`
  - `configprovider.is_config_content(content) -> bool`
  - `configprovider.validate_config_sources(sources: list, config_classes_for: Callable[[dict], list], resolve_models: bool = True) -> None` (raises `ConfigValidationError`)
  - `configstore.CONFIG_INDEX_BODY`, `ConfigConflictError(Exception)`, `ConfigNotFoundError(LookupError)`
  - `configstore.check_path(path: str) -> None`, `document_id(path: str, version: int) -> str`, `config_class_for(meta: dict) -> type`, `config_classes_for(meta: dict) -> list`, `content_names(content: dict) -> tuple[list, list]`
  - `ConfigStore(client, index: str = DEFAULT_CONFIG_INDEX)`: `init() -> bool`, `history(path) -> list[dict]` (oldest first), `get(path, version=None) -> dict | None`, `latest(service="", interface="") -> list[dict]` (tombstones included, sorted by path), `publish(path, content, services, author, comment, base_version=None, deleted=False, based_on=None) -> dict`, `rollback(path, to, author, comment, base_version=None) -> dict`, `delete(path, author, comment, base_version=None) -> dict`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_configstore.py`:

```python
"""Tests for the versioned configuration documents written by ConfigStore"""

import json
import os

import pytest

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.configprovider import (
    ConfigValidationError,
    OpenSearchConfigProvider,
)
from maas_collector.rawdata.configstore import (
    CONFIG_INDEX_BODY,
    ConfigConflictError,
    ConfigNotFoundError,
    ConfigStore,
)

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

INDEX = "maas-collector-config"

PATH = "odata/prip/maas-collector-odata-prip.json"


def read_json(name):
    with open(os.path.join(CONF_DIR, name), encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def entry(model, interface_name="ITF_A", extractor_class="LogExtractor"):
    """a collector entry without class: FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": model,
        "extractor": {"class": extractor_class, "args": {"pattern": ".*"}},
    }


def content(model_name="StoreModelA", **kwargs):
    return {
        "collectors": [entry({"name": model_name, "index": "", "fields": []}, **kwargs)]
    }


@pytest.fixture
def client(fake_opensearch):
    """the fake is also the default connection, read by the provider"""
    return fake_opensearch


@pytest.fixture
def store(client):
    return ConfigStore(client, INDEX)


def latest_flags(store, path=PATH):
    return {doc["version"]: doc["latest"] for doc in store.history(path)}


def test_init_creates_the_strict_index_once(store, client):
    assert store.init() is True
    assert store.init() is False

    assert client.mappings[INDEX]["dynamic"] == "strict"
    assert client.mappings[INDEX] == CONFIG_INDEX_BODY["mappings"]


def test_first_publish_writes_version_one(store, client):
    document = store.publish(PATH, content(), ["odata"], "alice", "first import")

    assert document["version"] == 1
    assert document["latest"] is True
    assert document["deleted"] is False
    assert document["based_on"] is None
    assert document["interface_names"] == ["ITF_A"]
    assert document["model_names"] == ["StoreModelA"]
    assert f"{PATH}@1" in client.documents[INDEX]


def test_second_publish_moves_the_latest_flag(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")

    document = store.publish(
        PATH, content("StoreModelB"), ["odata"], "bob", "v2", base_version=1
    )

    assert document["version"] == 2
    assert document["based_on"] == 1
    assert latest_flags(store) == {1: False, 2: True}
    assert store.get(PATH)["content"] == content("StoreModelB")


def test_publish_on_an_outdated_base_is_refused(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2", base_version=1)

    with pytest.raises(ConfigConflictError, match="version 2 was published after"):
        store.publish(
            PATH, content("StoreModelC"), ["odata"], "carol", "v3", base_version=1
        )

    assert [doc["version"] for doc in store.history(PATH)] == [1, 2]


def test_concurrent_publishes_on_the_same_base_conflict(store, client, monkeypatch):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    stale_history = store.history(PATH)

    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")

    # carol read the history before bob wrote version 2
    racing = ConfigStore(client, INDEX)
    monkeypatch.setattr(racing, "history", lambda path: stale_history)

    with pytest.raises(ConfigConflictError, match="published concurrently"):
        racing.publish(PATH, content("StoreModelC"), ["odata"], "carol", "v2 too")

    assert store.get(PATH)["author"] == "bob"
    assert latest_flags(store) == {1: False, 2: True}


def test_publish_repairs_the_flags_of_a_crashed_publish(store, client):
    """a publish that died between its two steps left two latest versions"""
    add_config_document(client, PATH, 1, ["odata"], content())
    add_config_document(client, PATH, 2, ["odata"], content("StoreModelB"))
    store.init()

    store.publish(PATH, content("StoreModelC"), ["odata"], "alice", "v3")

    assert latest_flags(store) == {1: False, 2: False, 3: True}


def test_a_late_publish_leaves_a_newer_version_latest(store, client):
    """step 4 of a slow publish must not clear the flag of a later publish"""
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")
    add_config_document(client, PATH, 3, ["odata"], content("StoreModelC"))

    store._clear_latest(PATH, 2)

    assert latest_flags(store)[3] is True


def test_history_lists_every_version_oldest_first(store):
    for number in range(1, 13):
        store.publish(PATH, content(f"StoreModel{number}"), ["odata"], "alice", "edit")

    history = store.history(PATH)

    assert [doc["version"] for doc in history] == list(range(1, 13))


def test_rollback_publishes_an_old_version_again(store):
    store.publish(PATH, content(), ["odata", "sftp"], "alice", "v1")
    store.publish(PATH, content("StoreModelB"), ["odata"], "bob", "v2")

    document = store.rollback(PATH, 1, "carol", "back to v1")

    assert document["version"] == 3
    assert document["based_on"] == 1
    assert document["content"] == content()
    assert document["services"] == ["odata", "sftp"]
    assert latest_flags(store) == {1: False, 2: False, 3: True}


def test_rollback_to_an_unknown_version_is_refused(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")

    with pytest.raises(ConfigNotFoundError):
        store.rollback(PATH, 7, "carol", "nope")


def test_delete_publishes_a_tombstone(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.publish("odata/other.json", content("StoreModelB"), ["odata"], "alice", "v1")

    document = store.delete(PATH, "bob", "interface decommissioned")

    assert document["version"] == 2
    assert document["deleted"] is True
    assert document["services"] == ["odata"]
    with pytest.raises(ConfigNotFoundError):
        store.delete(PATH, "bob", "twice")

    # collectors stop loading the path
    sources = OpenSearchConfigProvider(INDEX, "odata").load()
    assert [source.name for source in sources] == ["odata/other.json"]


def test_publish_after_a_delete_brings_the_path_back(store):
    store.publish(PATH, content(), ["odata"], "alice", "v1")
    store.delete(PATH, "bob", "removed")

    document = store.publish(PATH, content(), ["odata"], "carol", "restored")

    assert document["version"] == 3
    assert document["deleted"] is False


def test_credential_file_content_is_refused(store, client):
    credentials = read_json("test-maas-collector-credentials.json")

    with pytest.raises(ConfigValidationError, match="not a collector configuration"):
        store.publish("cred.json", credentials, ["odata"], "alice", "oops")

    assert client.documents.get(INDEX, {}) == {}


def test_unknown_extractor_class_is_refused(store):
    bad = content(extractor_class="NoSuchExtractor")

    with pytest.raises(ConfigValidationError, match=f"{PATH}: AttributeError"):
        store.publish(PATH, bad, ["odata"], "alice", "typo")


def test_unknown_configuration_class_is_refused(store):
    bad = content()
    bad["collectors"][0]["class"] = "NoSuchCollectorConfiguration"

    with pytest.raises(ConfigValidationError, match="unknown configuration class"):
        store.publish(PATH, bad, ["odata"], "alice", "typo")


@pytest.mark.parametrize(
    "name",
    [
        "maas-collector-interface-monitoring.json",
        "test-maas-collector-odata-basic.json",
        "maas-collector-conf-lta-deletion.json",
    ],
)
def test_real_configuration_files_are_accepted(store, name):
    store.publish(name, read_json(name), ["odata"], "alice", "import")

    assert store.get(name)["version"] == 1


def test_a_model_defined_in_another_file_is_accepted(store):
    """29 named models are shared: a file may use a model of another file"""
    uses_shared = {"collectors": [entry("SharedStoreModel")]}

    document = store.publish(PATH, uses_shared, ["odata"], "alice", "v1")

    assert document["model_names"] == ["SharedStoreModel"]


def test_validation_registers_no_model(store):
    before = dict(configuration.MODEL_DICT)

    store.publish(
        PATH,
        {
            **content("NeverRegisteredA"),
            "models": [{"name": "NeverRegisteredB", "index": "", "fields": []}],
        },
        ["odata"],
        "alice",
        "v1",
    )

    assert configuration.MODEL_DICT == before


def test_publish_needs_a_service(store):
    with pytest.raises(ValueError, match="at least one service"):
        store.publish(PATH, content(), [], "alice", "v1")


@pytest.mark.parametrize(
    "path",
    [
        "",
        "/etc/conf.json",
        "odata/../conf.json",
        "odata//conf.json",
        "./conf.json",
        "odata\\conf.json",
        "odata/conf.txt",
        "odata/",
    ],
)
def test_bad_paths_are_refused(store, client, path):
    with pytest.raises(ValueError, match="bad configuration path"):
        store.publish(path, content(), ["odata"], "alice", "v1")

    assert client.documents.get(INDEX, {}) == {}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_configstore.py --no-cov -q`
Expected: collection error `ImportError: cannot import name 'ConfigValidationError' from 'maas_collector.rawdata.configprovider'`.

- [ ] **Step 3: Add the validation to the provider module**

It lives in `configprovider.py` because `check_config_changes()` (Task 4) needs it and `filecollector.py` cannot import `configstore.py`, which imports the collector registry.

In `src/maas_collector/rawdata/configprovider.py`, add `import typing` after `import logging`, and replace `from maas_collector.rawdata.configuration import find_configurations` with:

```python
from maas_collector.rawdata.configuration import (
    build_collector_configuration,
    build_model_class,
    find_configurations,
)
```

After `DEFAULT_CONFIG_INDEX = "maas-collector-config"`, add:

```python

# top-level keys of a configuration file. A credential file, {"interfaces": ...},
# has none of them.
CONFIG_KEYS = frozenset({"collectors", "models", "amqp"})


class ConfigValidationError(ValueError):
    """a configuration source that a collector cannot load"""

    def __init__(self, label: str, reason: str):
        super().__init__(f"{label}: {reason}")

        self.label = label

        self.reason = reason


def is_config_content(content) -> bool:
    """True for the content of a configuration file: a JSON object whose top-level
    keys are within collectors, models and amqp"""
    return isinstance(content, dict) and set(content) <= CONFIG_KEYS
```

At the end of the module, add:

```python


def validate_config_sources(
    sources: list,
    config_classes_for: typing.Callable[[dict], list],
    resolve_models: bool = True,
) -> None:
    """Check that collectors can load configuration sources, without changing the
    models the running process has registered

    Args:
        sources (list[ConfigSource]): sources to check
        config_classes_for (Callable[[dict], list]): configuration classes each
            collector entry must build with
        resolve_models (bool, optional): check that every model named by a collector
            entry or a backup report is defined by one of the sources. Off to check
            a single file, whose models may live in another file. Defaults to True.

    Raises:
        ConfigValidationError: on the first source that cannot be loaded
    """
    defined = set()
    referenced = []

    for source in sources:
        content = source.content

        if not is_config_content(content):
            keys = sorted(content) if isinstance(content, dict) else []
            raise ConfigValidationError(
                source.label,
                f"not a collector configuration: top-level keys {keys},"
                f" expected within {sorted(CONFIG_KEYS)}",
            )

        try:
            for meta in content.get("collectors", []):
                for configuration_class in config_classes_for(meta):
                    build_collector_configuration(
                        meta, configuration_class, register=False
                    )

                if isinstance(meta.get("model"), dict):
                    defined.add(meta["model"]["name"])
                elif meta.get("model"):
                    referenced.append((source.label, meta["model"]))

                backup = meta.get("backup")
                if isinstance(backup, dict) and backup.get("report_model"):
                    referenced.append((source.label, backup["report_model"]))

            for model_meta in content.get("models", []):
                build_model_class(model_meta, register=False)
                defined.add(model_meta["name"])

        # any error makes the source unusable: report it with the source label
        # pylint: disable=W0703
        except Exception as error:
            raise ConfigValidationError(
                source.label, f"{error.__class__.__name__}: {error}"
            ) from error

    if resolve_models:
        for label, model_name in referenced:
            if model_name not in defined:
                raise ConfigValidationError(
                    label,
                    f"model {model_name} is defined in none of the configuration"
                    " files",
                )
```

- [ ] **Step 4: Create the store**

Create `src/maas_collector/rawdata/configstore.py`. Notes on the protocol: `publish` reads the history, refuses a stale `base_version`, validates the content alone (`resolve_models=False`, since a shared named model may live in another file), writes `<path>@<n+1>` with `op_type="create"` (a 409 is a concurrent publish), then clears `latest` on the **lower** versions only, so a slow publish never clears the flag of a later one, and any flag a crashed publish left behind is repaired. `check_path` keeps `export` from writing outside its directory. The monitor configuration class is resolved explicitly because `InterfaceMonitor` is not in the implementation registry (which it imports).

```python
"""
Versioned collector configurations in OpenSearch.

Every version of a configuration file is a document <path>@<version> of the
maas-collector-config index, and the latest flag marks the current version of each
path. ConfigStore is the only writer: the admin CLI uses it, the collectors only read
the index (see configprovider.OpenSearchConfigProvider).
"""

import datetime
import logging

from opensearchpy.exceptions import ConflictError, NotFoundError
from opensearchpy.helpers import scan

from maas_collector.rawdata.collector.filecollector import FileCollectorConfiguration
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitorCollectorConfiguration,
)
from maas_collector.rawdata.configprovider import (
    DEFAULT_CONFIG_INDEX,
    ConfigSource,
    validate_config_sources,
)
from maas_collector.rawdata.implementation import (
    get_collector_class_by_config_classname,
)

LOGGER = logging.getLogger("ConfigStore")

CONFIG_INDEX_BODY = {
    "settings": {"number_of_shards": 1},
    "mappings": {
        "dynamic": "strict",
        "properties": {
            "path": {"type": "keyword"},
            "version": {"type": "integer"},
            "latest": {"type": "boolean"},
            "deleted": {"type": "boolean"},
            "services": {"type": "keyword"},
            # the file JSON verbatim: stored, not indexed
            "content": {"type": "object", "enabled": False},
            "interface_names": {"type": "keyword"},
            "model_names": {"type": "keyword"},
            "author": {"type": "keyword"},
            "comment": {"type": "text"},
            "created_at": {"type": "date"},
            "based_on": {"type": "integer"},
        },
    },
}


class ConfigConflictError(Exception):
    """another version of the path was published in the meantime"""


class ConfigNotFoundError(LookupError):
    """no such path or version"""


def check_path(path: str):
    """A document path is a relative path to a .json file, with "/" separators,
    as under /conf/collector/ in a file deployment

    Raises:
        ValueError: on an empty, absolute or non-normalized path
    """
    segments = path.split("/") if path else []

    if (
        not segments
        or "\\" in path
        or any(segment in ("", ".", "..") for segment in segments)
        or not path.endswith(".json")
    ):
        raise ValueError(
            f"bad configuration path {path!r}: expected a relative path like"
            " odata/prip/conf.json, with / separators and no . or .. segment"
        )


def document_id(path: str, version: int) -> str:
    """identifier of a version document: odata/prip.json@3"""
    return f"{path}@{version}"


def config_class_for(meta: dict) -> type:
    """configuration class a collector entry builds with: the one its class key
    names, FileCollectorConfiguration without class

    Raises:
        ValueError: for an unknown configuration class
    """
    name = meta.get("class")

    if not name:
        return FileCollectorConfiguration

    # the monitor is not in the implementation registry, which it imports
    if name == InterfaceMonitorCollectorConfiguration.__name__:
        return InterfaceMonitorCollectorConfiguration

    try:
        return get_collector_class_by_config_classname(name).CONFIG_CLASS
    except KeyError as error:
        raise ValueError(f"unknown configuration class {name}") from error


def config_classes_for(meta: dict) -> list:
    """validate_config_sources callback: the class the entry names"""
    return [config_class_for(meta)]


def content_names(content: dict) -> tuple[list, list]:
    """interface names and model names of a configuration content, for searching
    in Dashboards"""
    interface_names = set()
    model_names = set()

    for meta in content.get("collectors", []):
        if meta.get("interface_name"):
            interface_names.add(meta["interface_name"])
        if isinstance(meta.get("model"), dict):
            model_names.add(meta["model"]["name"])
        elif meta.get("model"):
            model_names.add(meta["model"])

    for model_meta in content.get("models", []):
        model_names.add(model_meta["name"])

    return sorted(interface_names), sorted(model_names)


class ConfigStore:
    """Writer and reader of the configuration documents"""

    def __init__(self, client, index: str = DEFAULT_CONFIG_INDEX):
        self.client = client

        self.index = index

        self._index_ready = False

    def init(self) -> bool:
        """Create the index with its mapping, when it does not exist

        Returns:
            bool: True if the index was created
        """
        if self.client.indices.exists(index=self.index):
            self._index_ready = True
            return False

        self.client.indices.create(index=self.index, body=CONFIG_INDEX_BODY)
        self._index_ready = True
        LOGGER.info("Created the configuration index %s", self.index)
        return True

    def _scan(self, query: dict) -> list:
        """every document matching a query, none without index"""
        try:
            return [
                hit["_source"]
                for hit in scan(self.client, index=self.index, query={"query": query})
            ]
        except NotFoundError:
            return []

    def history(self, path: str) -> list:
        """every version of a path, oldest first"""
        return sorted(
            self._scan({"term": {"path": path}}), key=lambda doc: doc["version"]
        )

    def get(self, path: str, version: int | None = None) -> dict | None:
        """a version of a path, the current one by default

        Returns:
            dict | None: document, None if there is no such version
        """
        if version is None:
            history = self.history(path)
            return history[-1] if history else None

        try:
            return self.client.get(index=self.index, id=document_id(path, version))[
                "_source"
            ]
        except NotFoundError:
            return None

    def latest(self, service: str = "", interface: str = "") -> list:
        """current version of every path, tombstones included, sorted by path

        Args:
            service (str, optional): only the paths tagged with this service
            interface (str, optional): only the paths declaring this interface
        """
        filters = [{"term": {"latest": True}}]
        if service:
            filters.append({"term": {"services": service}})
        if interface:
            filters.append({"term": {"interface_names": interface}})

        newest = {}
        for doc in self._scan({"bool": {"filter": filters}}):
            current = newest.get(doc["path"])
            if current is None or doc["version"] > current["version"]:
                newest[doc["path"]] = doc

        return [newest[path] for path in sorted(newest)]

    # the audit fields make a long but explicit signature
    # pylint: disable=too-many-arguments
    def publish(
        self,
        path: str,
        content: dict,
        services: list,
        author: str,
        comment: str,
        base_version: int | None = None,
        deleted: bool = False,
        based_on: int | None = None,
    ) -> dict:
        """Write a new version of a path, and make it the current one

        Args:
            path (str): path relative to the service directory
            content (dict): configuration file content
            services (list): services loading the file
            author (str): who publishes
            comment (str): why
            base_version (int, optional): version the edit started from. Refused
                when another version was published since.
            deleted (bool, optional): publish a tombstone. Defaults to False.
            based_on (int, optional): version edited or rolled back to, for the
                audit. Defaults to the current version.

        Raises:
            ConfigConflictError: when base_version is not the current version, or
                when another publish wrote the same version first
            ConfigValidationError: when collectors cannot load the content
            ValueError: without service

        Returns:
            dict: the written document
        """
        check_path(path)

        if not self._index_ready:
            self.init()

        history = self.history(path)
        current = history[-1]["version"] if history else 0

        if base_version is not None and base_version != current:
            raise ConfigConflictError(
                f"{path}: version {current} was published after version {base_version}"
            )

        if not deleted:
            validate_config_sources(
                [ConfigSource(path, content)], config_classes_for, resolve_models=False
            )

        services = sorted(set(services))
        if not services:
            raise ValueError(f"{path}: a configuration needs at least one service")

        version = current + 1
        interface_names, model_names = content_names(content)

        document = {
            "path": path,
            "version": version,
            "latest": True,
            "deleted": deleted,
            "services": services,
            "content": content,
            "interface_names": interface_names,
            "model_names": model_names,
            "author": author,
            "comment": comment,
            "created_at": datetime.datetime.now(tz=datetime.UTC).isoformat(),
            "based_on": based_on if based_on is not None else (current or None),
        }

        try:
            self.client.index(
                index=self.index,
                id=document_id(path, version),
                body=document,
                op_type="create",
                refresh="wait_for",
            )
        except ConflictError as error:
            raise ConfigConflictError(
                f"{path}: version {version} was published concurrently"
            ) from error

        self._clear_latest(path, version)

        LOGGER.info("Published %s", document_id(path, version))

        return document

    def _clear_latest(self, path: str, version: int):
        """clear the latest flag of the versions below version. Also repairs the
        flags a crashed publish left behind. A higher version belongs to a later
        publish and keeps its flag."""
        query = {
            "bool": {"filter": [{"term": {"path": path}}, {"term": {"latest": True}}]}
        }
        for doc in self._scan(query):
            if doc["version"] < version:
                self.client.update(
                    index=self.index,
                    id=document_id(path, doc["version"]),
                    body={"doc": {"latest": False}},
                    refresh="wait_for",
                )

    def rollback(
        self,
        path: str,
        to: int,
        author: str,
        comment: str,
        base_version: int | None = None,
    ) -> dict:
        """Publish the content and services of an older version as a new version

        Raises:
            ConfigNotFoundError: when the version does not exist
        """
        # pylint: disable=invalid-name
        target = self.get(path, to)
        if target is None:
            raise ConfigNotFoundError(f"{document_id(path, to)} does not exist")

        return self.publish(
            path,
            target["content"],
            target["services"],
            author,
            comment,
            base_version=base_version,
            deleted=target.get("deleted", False),
            based_on=to,
        )

    def delete(
        self, path: str, author: str, comment: str, base_version: int | None = None
    ) -> dict:
        """Publish a tombstone: the services stop loading the path

        Raises:
            ConfigNotFoundError: when the path does not exist or is deleted already
        """
        current = self.get(path)
        if current is None or current.get("deleted"):
            raise ConfigNotFoundError(f"{path} does not exist")

        return self.publish(
            path,
            {},
            current["services"],
            author,
            comment,
            base_version=base_version,
            deleted=True,
        )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `249 passed` (29 new).

- [ ] **Step 6: Commit**

```bash
git add src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/configstore.py tests/test_configstore.py
git commit -m "feat: :sparkles: Publish versioned collector configurations to OpenSearch" -m "ConfigStore writes every version of a configuration file as an immutable <path>@<version> document of a strict index, and moves the latest flag to it. A publication based on an outdated version, or racing another one, is refused. Rollbacks and deletions are new versions. Every content is checked before it is written: credential files, unknown classes and broken extractors are refused, and no model is registered while checking." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Restart the collector on a configuration change

**Files:**
- Modify: `src/maas_collector/rawdata/configprovider.py` (append `describe_config_changes`)
- Modify: `src/maas_collector/rawdata/collector/filecollector.py`, line numbers as left by Task 2: `:34-39` (`configprovider` import), `:47` (module constant), `:341` (`__init__`), `:372` (`load_config`), before `:412` (`setup`), `:796` (`run()`)
- Test: `tests/test_config_refresh.py`

**Interfaces:**
- Consumes: `validate_config_sources`, `ConfigValidationError` (Task 3); `ConfigStore` (Task 3, in tests); `FileCollector.config_provider`, `config_sources`, `_config_fingerprint` (Task 2); `CollectorArgs.config_refresh_period` (Task 2).
- Produces:
  - `filecollector.CONFIG_CHANGED_EXIT_CODE = 75`
  - `FileCollector.config_classes_for(meta: dict) -> list` (overridable; `[self.CONFIG_CLASS]`)
  - `FileCollector.check_config_changes() -> None`, raises `SystemExit(75)`
  - `configprovider.describe_config_changes(previous: list, current: list) -> str`

- [ ] **Step 1: Write the failing tests**

Create `tests/test_config_refresh.py`. `FakeClock` replaces the `time` module of `filecollector`: `monotonic` moves on demand, `time` and `sleep` stay real for `run()`.

```python
"""Tests for restarting a collector when its OpenSearch configuration changes"""

import logging
import time
from unittest.mock import patch

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

from fakeopensearch import add_config_document

from maas_collector.rawdata import configuration
from maas_collector.rawdata.collector import filecollector
from maas_collector.rawdata.collector.filecollector import (
    CONFIG_CHANGED_EXIT_CODE,
    CollectorArgs,
    FileCollector,
)
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore

INDEX = "maas-collector-config"


class FakeClock:
    """replace the time module of filecollector: monotonic moves on demand, the
    other functions are the real ones"""

    def __init__(self):
        self.now = 1000.0

    def monotonic(self) -> float:
        """seconds"""
        return self.now

    @staticmethod
    def time() -> float:
        """wall clock"""
        return time.time()

    @staticmethod
    def sleep(seconds: float):
        """real sleep"""
        time.sleep(seconds)


def entry(model, interface_name: str) -> dict:
    """a collector entry building with FileCollectorConfiguration"""
    return {
        "id_field": "",
        "routing_key": "",
        "interface_name": interface_name,
        "model": model,
        "extractor": {"class": "LogExtractor", "args": {"pattern": ".*"}},
    }


def inline(model_name: str, interface_name: str = "ITF_A") -> dict:
    """content with one collector entry declaring its model"""
    return {
        "collectors": [
            entry({"name": model_name, "index": "", "fields": []}, interface_name)
        ]
    }


@pytest.fixture
def clock(monkeypatch):
    fake_clock = FakeClock()
    monkeypatch.setattr(filecollector, "time", fake_clock)
    return fake_clock


@pytest.fixture
def store(fake_opensearch):
    """a configuration index holding odata/a.json v1 and shared/b.json v1"""
    config_store = ConfigStore(fake_opensearch, INDEX)
    config_store.publish("odata/a.json", inline("RefreshModelA"), ["odata"], "a", "v1")
    config_store.publish(
        "shared/b.json",
        inline("RefreshModelB", "ITF_B"),
        ["odata", "sftp"],
        "a",
        "v1",
    )
    return config_store


def start_collector(**kwargs) -> FileCollector:
    collector = FileCollector(
        CollectorArgs(
            es_url="http://opensearch.example:9200",
            config_backend="opensearch",
            config_store=ConfigStoreArgs(index=INDEX, service="odata"),
            **kwargs,
        )
    )
    with patch("opensearchpy.connection.connections.Connections.create_connection"):
        collector.setup()
    return collector


def test_nothing_is_read_before_the_refresh_period(clock, store):
    collector = start_collector()
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")

    clock.now += 299
    with patch.object(
        collector.config_provider, "load", wraps=collector.config_provider.load
    ) as load:
        collector.check_config_changes()

    load.assert_not_called()


def test_unchanged_configuration_keeps_the_collector_running(clock, store):
    collector = start_collector()

    clock.now += 300
    collector.check_config_changes()

    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_changed_configuration_exits_with_code_75(clock, store, caplog):
    collector = start_collector()
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    store.publish(
        "odata/c.json", inline("RefreshModelC", "ITF_C"), ["odata"], "b", "v1"
    )

    clock.now += 300
    with pytest.raises(SystemExit) as exit_info:
        collector.check_config_changes()

    assert exit_info.value.code == CONFIG_CHANGED_EXIT_CODE == 75
    assert (
        "configuration changed: odata/a.json v1→v2, odata/c.json added, restarting"
        in caplog.text
    )


def test_collector_loop_exits_after_its_clean_up(clock, store):
    collector = start_collector(watch_period=0)
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    clock.now += 300

    with patch.object(collector, "start_healthcheck"), patch.object(
        collector, "stop_healthcheck"
    ) as stop_healthcheck, patch.object(collector, "_healthcheck"), patch.object(
        collector, "loop_body"
    ) as loop_body, patch.object(
        collector, "_flush_message_groups"
    ) as flush, pytest.raises(
        SystemExit
    ) as exit_info:
        collector.run([])

    assert exit_info.value.code == 75
    loop_body.assert_not_called()
    stop_healthcheck.assert_called_once()
    flush.assert_called()


def test_invalid_change_is_logged_and_ignored(clock, store, fake_opensearch, caplog):
    collector = start_collector()
    before = dict(configuration.MODEL_DICT)
    # written straight into the index, without the validation of ConfigStore
    add_config_document(
        fake_opensearch,
        "odata/a.json",
        2,
        ["odata"],
        {
            "collectors": [
                {
                    **inline("RejectedModel")["collectors"][0],
                    "extractor": {"class": "NoSuchExtractor"},
                }
            ]
        },
    )

    clock.now += 300
    collector.check_config_changes()

    assert (
        "configuration change rejected: odata/a.json@2: AttributeError" in caplog.text
    )
    assert configuration.MODEL_DICT == before
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_rejected_versions_are_not_validated_again(clock, store, fake_opensearch):
    collector = start_collector()
    add_config_document(
        fake_opensearch, "odata/a.json", 2, ["odata"], {"interfaces": []}
    )

    with patch(
        "maas_collector.rawdata.collector.filecollector.validate_config_sources",
        wraps=filecollector.validate_config_sources,
    ) as validate:
        clock.now += 300
        collector.check_config_changes()
        clock.now += 300
        collector.check_config_changes()

    assert validate.call_count == 1

    # a fixed version is a new change
    store.publish("odata/a.json", inline("RefreshModelA3"), ["odata"], "b", "fix")
    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()


def test_unreachable_opensearch_keeps_the_collector_running(
    clock, store, fake_opensearch, caplog
):
    caplog.set_level(logging.INFO)
    collector = start_collector()
    fake_opensearch.outage = OpenSearchConnectionError("N/A", "unreachable", None)

    clock.now += 300
    collector.check_config_changes()
    clock.now += 300
    collector.check_config_changes()

    errors = [record for record in caplog.records if record.levelno == logging.ERROR]
    assert len(errors) == 2
    assert errors[0].exc_info is not None
    assert errors[1].exc_info is None
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]

    # back online, and changed meanwhile
    fake_opensearch.outage = None
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")
    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()
    assert "Configuration checked again" in caplog.text


def test_service_left_without_configuration_keeps_running(clock, store, caplog):
    """deleting every file of a service must not turn into a restart loop"""
    collector = start_collector()
    store.delete("odata/a.json", "b", "gone")
    store.publish(
        "shared/b.json", inline("RefreshModelB", "ITF_B"), ["sftp"], "b", "sftp only"
    )

    clock.now += 300
    collector.check_config_changes()

    assert "no configuration for service odata" in caplog.text
    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_service_removed_from_a_shared_path_restarts(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "shared/b.json", inline("RefreshModelB", "ITF_B"), ["sftp"], "b", "sftp only"
    )

    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()

    assert "configuration changed: shared/b.json removed, restarting" in caplog.text


def test_model_defined_in_another_document_resolves(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "odata/models.json",
        {"models": [{"name": "SharedRefreshModel", "index": "", "fields": []}]},
        ["odata"],
        "b",
        "shared model",
    )
    store.publish(
        "odata/a.json",
        {"collectors": [entry("SharedRefreshModel", "ITF_A")]},
        ["odata"],
        "b",
        "use the shared model",
    )

    clock.now += 300
    with pytest.raises(SystemExit):
        collector.check_config_changes()


def test_model_missing_from_the_new_set_is_rejected(clock, store, caplog):
    collector = start_collector()
    store.publish(
        "odata/a.json",
        {"collectors": [entry("ModelNobodyDefines", "ITF_A")]},
        ["odata"],
        "b",
        "dangling model",
    )

    clock.now += 300
    collector.check_config_changes()

    assert (
        "configuration change rejected: odata/a.json@2: model ModelNobodyDefines"
        in caplog.text
    )


def test_zero_refresh_period_never_checks(clock, store):
    collector = start_collector(config_refresh_period=0)
    store.publish("odata/a.json", inline("RefreshModelA2"), ["odata"], "b", "v2")

    clock.now += 100_000
    collector.check_config_changes()

    assert collector.get_interface_names() == ["ITF_A", "ITF_B"]


def test_file_backend_never_checks(clock, tmp_path):
    conf = tmp_path / "conf.json"
    conf.write_text('{"amqp": {}}', encoding="UTF-8")
    collector = FileCollector(CollectorArgs(rawdata_config=str(conf)))
    collector.load_config()
    conf.write_text('{"amqp": {"rk": {"chunk_size": 3}}}', encoding="UTF-8")

    clock.now += 100_000
    with patch.object(collector.config_provider, "load") as load:
        collector.check_config_changes()

    load.assert_not_called()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_config_refresh.py --no-cov -q`
Expected: collection error `ImportError: cannot import name 'CONFIG_CHANGED_EXIT_CODE' from 'maas_collector.rawdata.collector.filecollector'`.

- [ ] **Step 3: Describe a change**

Append to `src/maas_collector/rawdata/configprovider.py`:

```python


def describe_config_changes(previous: list, current: list) -> str:
    """Summarize the difference between two sets of sources, for the logs

    Args:
        previous (list[ConfigSource]): sources the collector runs on
        current (list[ConfigSource]): sources read now

    Returns:
        str: like "odata/a.json v3→v4, odata/b.json added, odata/c.json removed"
    """
    before = {source.name: source.version for source in previous}
    after = {source.name: source.version for source in current}

    changes = []
    for name in sorted(before.keys() | after.keys()):
        if name not in before:
            changes.append(f"{name} added")
        elif name not in after:
            changes.append(f"{name} removed")
        elif before[name] != after[name]:
            changes.append(f"{name} v{before[name]}→v{after[name]}")

    return ", ".join(changes)
```

- [ ] **Step 4: Check for changes in `run()`**

In `src/maas_collector/rawdata/collector/filecollector.py`, extend the `configprovider` import of Task 2:

```python
from maas_collector.rawdata.configprovider import (
    ConfigProvider,
    ConfigSource,
    ConfigStoreArgs,
    ConfigValidationError,
    build_config_provider,
    describe_config_changes,
    validate_config_sources,
)
```

After `from maas_collector.rawdata.meta import IngestionMeta` (line 47), add:

```python

# exit code of a collector that stops to restart on a new configuration: EX_TEMPFAIL
CONFIG_CHANGED_EXIT_CODE = 75
```

In `__init__`, after `self._config_fingerprint = None` (line 341), add:

```python

        # time.monotonic() of the last configuration check
        self._config_check_time: float = None

        # fingerprint of the last rejected configuration, not checked again
        self._rejected_config_fingerprint = None

        # True while the configuration checks fail, to log one traceback per outage
        self._config_check_failing = False
```

In `load_config`, after `self._config_fingerprint = self.config_provider.fingerprint(sources)` (line 372), add:

```python
        self._config_check_time = time.monotonic()
```

Before `def setup(self):` (line 412), add:

```python
    def config_classes_for(self, meta: dict) -> list:
        """configuration classes a collector entry of a new configuration must build
        with before the collector restarts on it

        Args:
            meta (dict): collector entry

        Returns:
            list: configuration classes
        """
        return [self.CONFIG_CLASS]

    def check_config_changes(self):
        """read the configuration again once the refresh period has elapsed, and exit
        with CONFIG_CHANGED_EXIT_CODE when it changed, for the orchestrator to
        restart the collector on it.

        A failing read is logged and the collector keeps its configuration: it is
        retried at the next period. A new configuration the collector cannot load
        is logged and ignored until it changes again.

        Raises:
            SystemExit: with CONFIG_CHANGED_EXIT_CODE when the configuration changed
        """
        if (
            self._config_check_time is None
            or self._config_fingerprint is None
            or self.args.config_refresh_period <= 0
        ):
            return

        if time.monotonic() - self._config_check_time < self.args.config_refresh_period:
            return

        self._config_check_time = time.monotonic()

        try:
            sources = self.config_provider.load()

        # a failing configuration source shall not stop the collect
        # pylint: disable=W0703
        except Exception as error:
            if self._config_check_failing:
                self.logger.error(
                    "Cannot check the configuration, keeping the current one: %s",
                    error,
                )
            else:
                self.logger.exception(
                    "Cannot check the configuration, keeping the current one"
                )

            self._config_check_failing = True
            return

        if self._config_check_failing:
            self.logger.info("Configuration checked again")
            self._config_check_failing = False

        fingerprint = self.config_provider.fingerprint(sources)

        if fingerprint in (self._config_fingerprint, self._rejected_config_fingerprint):
            return

        try:
            validate_config_sources(sources, self.config_classes_for)
        except ConfigValidationError as error:
            self.logger.error("configuration change rejected: %s", error)
            self._rejected_config_fingerprint = fingerprint
            return

        self.logger.warning(
            "configuration changed: %s, restarting",
            describe_config_changes(self.config_sources, sources),
        )

        raise SystemExit(CONFIG_CHANGED_EXIT_CODE)
```

In `run()`, right after `self.refresh_credentials()` (line 796), add:

```python

                # restart on a new configuration, between two loops. Not in
                # on_loop_start so overrides cannot skip it
                self.check_config_changes()
```

A provider that finds no document raises `ValueError`, which lands in the `except` above: a service emptied while running is logged and keeps its configuration instead of exiting into a start-up failure loop.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `262 passed` (13 new), `test_credential_refresh` included.

- [ ] **Step 6: Commit**

```bash
git add src/maas_collector/rawdata/configprovider.py src/maas_collector/rawdata/collector/filecollector.py tests/test_config_refresh.py
git commit -m "feat: :sparkles: Restart collectors on a configuration change" -m "Once CONFIG_REFRESH_PERIOD has passed, the loop reads the configuration again right after the credentials. When the documents changed and the collector can load them, it logs the changes and exits with code 75 after its usual clean-up, for the orchestrator to restart it. A set it cannot load is logged once and ignored, and an unreadable index keeps the current configuration until the next check. The file backend never changes." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Monitor second pass through the provider

**Files:**
- Modify: `src/maas_collector/rawdata/collector/monitorcollector.py:38-41` (imports), `:385-431` (second pass), before `:458` (new `config_classes_for`)
- Modify: `tests/test_monitoring.py:1-68`, `tests/test_credential_refresh.py:127-135`, `:422-490`
- Modify: `tests/test_config_refresh.py` (imports, append one test)

**Interfaces:**
- Consumes: `FileCollector.config_sources` (Task 2), `load_json_dict` (Task 1), `FileCollector.config_classes_for` / `check_config_changes` (Task 4), `ConfigStore` (Task 3), `ConfigStoreArgs` (Task 2).
- Produces: `InterfaceMonitor.config_classes_for(meta: dict) -> list`; `InterfaceMeta.configuration_path` holds `ConfigSource.name` (file path or document path).

- [ ] **Step 1: Point the monitor tests at real configuration directories**

The second pass will no longer call `monitorcollector.find_configurations`, so the four tests that patch it would fail with `AttributeError`. Give them a directory holding the probed files instead (same assertions).

In `tests/test_monitoring.py`, add `import shutil` after `import logging`, and replace the decorators, signature and setup of `test_monitoring_disable` (lines 33-56):

```python
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
```

In `tests/test_credential_refresh.py`, replace `start_monitor` (lines 127-135):

```python
@pytest.fixture
def probed_dir(tmp_path):
    """configuration directory of the interfaces the monitor probes"""
    path = tmp_path / "probed"
    path.mkdir()
    shutil.copy(PROBED_CONF, path)
    return path


def start_monitor(credential_file, probed_dir) -> InterfaceMonitor:
    monitor = InterfaceMonitor(
        CollectorArgs(
            rawdata_config=MONITOR_CONF,
            rawdata_config_dir=str(probed_dir),
            credential_file=str(credential_file),
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    monitor.setup()
    return monitor
```

and the heads of the three monitor tests (lines 422-490): remove each `@patch("maas_collector.rawdata.collector.monitorcollector.find_configurations")` decorator, each `find_conf_mock` parameter and each `find_conf_mock.return_value = [PROBED_CONF]` line, add a `probed_dir` parameter, and call `start_monitor(credential_file, probed_dir)`:

```python
def test_monitor_probes_with_the_rotated_credentials(
    clock, credential_file, probed_dir
):
    monitor = start_monitor(credential_file, probed_dir)
```

```python
def test_monitor_reads_the_credentials_once_at_startup(
    credential_file, probed_dir, monkeypatch
):
    loads = []
```

with `monitor = start_monitor(credential_file, probed_dir)` further down that test, and

```python
def test_monitor_probes_interfaces_without_credential_entry(
    credential_file, probed_dir
):
    """some interfaces need no credentials: the monitor still probes them"""
    edit_entries(credential_file, remove_entry("basic_test"))

    monitor = start_monitor(credential_file, probed_dir)
```

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_monitoring.py tests/test_credential_refresh.py --no-cov -q`
Expected: all pass on the current code (the old second pass walks `rawdata_config_dir` too).

- [ ] **Step 2: Write the failing tests**

In `tests/test_monitoring.py`, add `import json` above `import os`, add after the `CollectorArgs` import:

```python
from maas_collector.rawdata.configprovider import ConfigStoreArgs
from maas_collector.rawdata.configstore import ConfigStore
```

and append:

```python


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
```

In `tests/test_config_refresh.py`, add `import json` and `import os` to the standard imports, add after the `filecollector` import block:

```python
from maas_collector.rawdata.collector.monitorcollector import (
    InterfaceMonitor,
    InterfaceMonitorConfiguration,
)
```

add after `INDEX = "maas-collector-config"`:

```python

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)
```

and append:

```python


MONITOR_CONF = os.path.join(CONF_DIR, "maas-collector-interface-monitoring.json")

PROBED_CONF = os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")

CREDENTIAL_FILE = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")


def test_monitor_rejects_a_probed_entry_its_collector_cannot_load(
    clock, fake_opensearch, caplog
):
    """the monitoring class accepts any entry, the probed collector class may not"""
    store = ConfigStore(fake_opensearch, INDEX)
    store.publish(
        "monitor/main.json", read_json(MONITOR_CONF), ["monitor-main"], "a", "v1"
    )
    store.publish(
        "odata/basic.json", read_json(PROBED_CONF), ["monitor-main"], "a", "v1"
    )
    monitor = InterfaceMonitor(
        CollectorArgs(
            es_url="http://opensearch.example:9200",
            config_backend="opensearch",
            config_store=ConfigStoreArgs(index=INDEX, service="monitor-main"),
            credential_file=CREDENTIAL_FILE,
        ),
        InterfaceMonitorConfiguration(interface_name="OMCS_Monitoring"),
    )
    with patch("opensearchpy.connection.connections.Connections.create_connection"):
        monitor.setup()

    broken = read_json(PROBED_CONF)
    broken["collectors"][0]["class"] = "NoSuchCollectorConfiguration"
    add_config_document(
        fake_opensearch, "odata/basic.json", 2, ["monitor-main"], broken
    )

    try:
        clock.now += 300
        monitor.check_config_changes()
    finally:
        monitor.exit_gracefully(9, "")

    assert "configuration change rejected: odata/basic.json@2: KeyError" in caplog.text
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_monitoring.py tests/test_config_refresh.py --no-cov -q`
Expected: 2 FAILED: `test_monitor_finds_the_probed_configurations_in_opensearch` with `AssertionError: assert [] == ['basic_test']`, and `test_monitor_rejects_a_probed_entry_its_collector_cannot_load` with `SystemExit: 75`.

- [ ] **Step 4: Iterate the provider sources in the second pass**

In `src/maas_collector/rawdata/collector/monitorcollector.py`, replace the `configuration` import (lines 38-41):

```python
from maas_collector.rawdata.configuration import load_json_dict
```

Replace the start of the second pass (lines 385-404):

```python
        # load the configurations of the interfaces to monitor, from the sources
        # the monitoring configuration came from
        for source in self.config_sources:
            self.logger.debug("Looking in %s", source.name)

            for collect_conf_dict in source.content.get("collectors", []):
                if not collect_conf_dict.get("class"):
                    self.logger.info(
                        "Class property empty. Ignoring some configuration in %s",
                        source.name,
                    )
                    continue

                if collect_conf_dict["class"] == self.CONFIG_CLASS.__name__:
                    self.logger.debug(
                        "Ignoring InterfaceMonitorConfiguration: %s", source.name
                    )
                    continue
```

Replace the configuration lookup (lines 410-416):

```python
                config = [
                    config
                    for config in load_json_dict(
                        source.content, collector_class.CONFIG_CLASS, source.name
                    )
                    if config.interface_name == collect_conf_dict["interface_name"]
                ][0]
```

and `configuration_path=config_path,` (line 426) with:

```python
                    configuration_path=source.name,
```

Before `def load_credentials(self):` (line 458), add:

```python
    def config_classes_for(self, meta: dict) -> list:
        """a probed entry of a new configuration must also build with the
        configuration class of its own collector, as setup() builds it so

        Args:
            meta (dict): collector entry

        Returns:
            list: configuration classes
        """
        classes = [self.CONFIG_CLASS]

        name = meta.get("class")

        if name and name != self.CONFIG_CLASS.__name__:
            classes.append(get_collector_class_by_config_classname(name).CONFIG_CLASS)

        return classes
```

`json` stays imported: the module still uses it to write probe results.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `264 passed` (2 new).

- [ ] **Step 6: Commit**

```bash
git add src/maas_collector/rawdata/collector/monitorcollector.py tests/test_monitoring.py tests/test_credential_refresh.py tests/test_config_refresh.py
git commit -m "feat: :sparkles: Find the probed interfaces through the configuration provider" -m "The interface monitor looks for the configurations of the interfaces it probes in the sources it loaded, files or OpenSearch documents, instead of walking RAWDATA_CONFIG_DIR again. Before restarting on a new configuration, it also checks that each probed entry builds with the configuration class of its own collector." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Admin CLI

**Files:**
- Create: `src/maas_collector/rawdata/cli/config.py`
- Test: `tests/test_config_cli.py`

**Interfaces:**
- Consumes: `ConfigStore`, `ConfigConflictError`, `ConfigNotFoundError`, `config_classes_for` (Task 3); `ConfigSource`, `is_config_content`, `validate_config_sources` (Tasks 2-3); `args.es_parser()`, `args.vault_parser()`, `args.get_es_credentials_url(namespace)` (existing), `args.config_index_parser()` (Task 2); `configuration.find_configurations()`.
- Produces: `cli.config.config_main(argv: list) -> int` (0 on success, 1 on a refused or failed command), `build_parser() -> ArgumentParser`, `build_client(namespace) -> OpenSearch`, `read_deploy_tree(deploy_dir: str) -> tuple[dict, list, dict]`, `@dataclass DeployFile(content: dict, services: list)`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_config_cli.py`:

```python
"""Tests for the configuration admin CLI: python -m maas_collector.rawdata.cli.config"""

import json
import os
import shutil
from unittest.mock import patch

import pytest

from maas_collector.rawdata.cli.config import config_main
from maas_collector.rawdata.configstore import ConfigStore

CONF_DIR = os.path.join(os.path.dirname(__file__), "conf")

INDEX = "maas-collector-config"

ENV = (
    "ES_URL",
    "ES_USERNAME",
    "ES_PASSWORD",
    "ES_VAULT_PATH",
    "CONFIG_INDEX",
    "USER",
    "VAULT_ADDR",
    "VAULT_AUTH_METHOD",
    "VAULT_TOKEN_FILE",
    "VAULT_TOKEN",
)


def read_json(path):
    with open(path, encoding="UTF-8") as conf_fd:
        return json.load(conf_fd)


def write_json(path, content):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="UTF-8") as conf_fd:
        json.dump(content, conf_fd)


@pytest.fixture
def opensearch_class(fake_opensearch):
    """the OpenSearch class of the CLI, answering the fake"""
    with patch(
        "maas_collector.rawdata.cli.config.OpenSearch", return_value=fake_opensearch
    ) as opensearch:
        yield opensearch


@pytest.fixture
def env(monkeypatch, opensearch_class):
    for name in ENV:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("ES_URL", "http://opensearch.example:9200")
    monkeypatch.setenv("USER", "alice")
    return monkeypatch


@pytest.fixture
def store(fake_opensearch):
    return ConfigStore(fake_opensearch, INDEX)


@pytest.fixture
def deploy_dir(tmp_path):
    """<dir>/<service>/... like the collector directory of a deploy tree"""
    root = tmp_path / "collector"
    conf1 = read_json(os.path.join(CONF_DIR, "collector.d", "conf1.json"))
    write_json(root / "odata" / "shared" / "conf1.json", conf1)
    write_json(root / "sftp" / "shared" / "conf1.json", conf1)
    write_json(root / "sftp" / ".hidden" / "conf1.json", conf1)
    for name in ("models.json", "conf1.json"):
        write_json(
            root / "odata" / "resolve" / name,
            read_json(os.path.join(CONF_DIR, "model_resolve", name)),
        )
    os.makedirs(root / "odata" / "credentials")
    shutil.copy(
        os.path.join(CONF_DIR, "test-maas-collector-credentials.json"),
        root / "odata" / "credentials",
    )
    return root


def run(*argv) -> int:
    return config_main([str(arg) for arg in argv])


def test_init_creates_the_index_once(env, fake_opensearch, capsys):
    assert run("init") == 0
    assert run("init") == 0

    assert capsys.readouterr().out.splitlines() == [
        f"created index {INDEX}",
        f"index {INDEX} exists already",
    ]
    assert fake_opensearch.mappings[INDEX]["dynamic"] == "strict"


def test_import_merges_the_services_of_a_shared_path(env, store, deploy_dir, capsys):
    assert run("import", deploy_dir) == 0

    out = capsys.readouterr().out
    assert "skipped odata/credentials/test-maas-collector-credentials.json" in out
    assert "created shared/conf1.json v1 (odata, sftp)" in out
    assert [doc["path"] for doc in store.latest()] == [
        "resolve/conf1.json",
        "resolve/models.json",
        "shared/conf1.json",
    ]
    assert store.get("shared/conf1.json")["services"] == ["odata", "sftp"]
    assert store.get("shared/conf1.json")["author"] == "alice"


def test_import_dry_run_writes_nothing(env, store, deploy_dir, capsys):
    assert run("import", deploy_dir, "--dry-run") == 0

    assert "would create shared/conf1.json v1" in capsys.readouterr().out
    assert store.latest() == []


def test_import_again_leaves_equal_paths_alone(env, store, deploy_dir, capsys):
    run("import", deploy_dir)

    assert run("import", deploy_dir) == 0

    assert "unchanged shared/conf1.json v1" in capsys.readouterr().out
    assert store.get("shared/conf1.json")["version"] == 1


def test_import_reports_drift_and_leaves_the_index_untouched(
    env, store, deploy_dir, capsys
):
    run("import", deploy_dir)
    models = deploy_dir / "odata" / "resolve" / "models.json"
    write_json(models, {"models": []})

    assert run("import", deploy_dir) == 0

    assert "drift resolve/models.json: v1 in the index" in capsys.readouterr().out
    assert store.get("resolve/models.json")["version"] == 1
    assert store.get("resolve/models.json")["content"]["models"] != []


def test_import_refuses_a_path_whose_copies_differ(env, store, deploy_dir, capsys):
    write_json(deploy_dir / "sftp" / "shared" / "conf1.json", {"amqp": {}})

    assert run("import", deploy_dir) == 1

    assert "refused shared/conf1.json: the copies" in capsys.readouterr().out
    assert store.get("shared/conf1.json") is None
    assert store.get("resolve/models.json")["version"] == 1


def test_import_refuses_an_invalid_file(env, store, deploy_dir, capsys):
    write_json(
        deploy_dir / "odata" / "bad.json",
        {"collectors": [{"extractor": {"class": "NoSuchExtractor"}}]},
    )

    assert run("import", deploy_dir) == 1

    assert "refused bad.json: bad.json: AttributeError" in capsys.readouterr().out
    assert store.get("bad.json") is None


def test_export_after_import_reproduces_the_tree(env, deploy_dir, tmp_path):
    run("import", deploy_dir)
    out_dir = tmp_path / "out"

    assert run("export", out_dir) == 0

    def tree(root):
        return {
            os.path.relpath(path, root): read_json(path)
            for path in (
                os.path.join(directory, name)
                for directory, dirs, names in os.walk(root)
                if "/." not in directory
                for name in names
            )
            if "credentials" not in path
        }

    assert tree(out_dir) == tree(deploy_dir)


def test_export_of_one_service(env, deploy_dir, tmp_path):
    run("import", deploy_dir)

    run("export", tmp_path / "out", "--service", "sftp")

    assert os.listdir(tmp_path / "out") == ["sftp"]
    assert os.path.isfile(tmp_path / "out" / "sftp" / "shared" / "conf1.json")


def test_put_a_new_path_needs_its_services(env, store, tmp_path, capsys):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {}})

    assert run("put", "odata/new.json", conf, "-m", "new") == 1
    assert "give its services with --services" in capsys.readouterr().err

    assert (
        run("put", "odata/new.json", conf, "-m", "new", "--services", "odata,sftp") == 0
    )
    assert store.get("odata/new.json")["services"] == ["odata", "sftp"]


def test_put_keeps_the_services_and_checks_the_base_version(
    env, store, tmp_path, capsys
):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {}})
    run("put", "odata/a.json", conf, "-m", "v1", "--services", "odata,sftp")
    write_json(conf, {"amqp": {"rk": {"chunk_size": 5}}})

    assert run("put", "odata/a.json", conf, "-m", "v2", "--base-version", "1") == 0
    assert run("put", "odata/a.json", conf, "-m", "v3", "--base-version", "1") == 1

    assert "version 2 was published after version 1" in capsys.readouterr().err
    assert store.get("odata/a.json")["services"] == ["odata", "sftp"]
    assert store.get("odata/a.json")["version"] == 2


def test_put_refuses_a_credential_file(env, store, capsys):
    credentials = os.path.join(CONF_DIR, "test-maas-collector-credentials.json")

    assert (
        run("put", "odata/c.json", credentials, "-m", "x", "--services", "odata") == 1
    )

    assert "not a collector configuration" in capsys.readouterr().err
    assert store.get("odata/c.json") is None


def test_get_history_diff_rollback_and_delete(env, store, tmp_path, capsys):
    conf = tmp_path / "conf.json"
    write_json(conf, {"amqp": {"rk": {"chunk_size": 5}}})
    run("put", "odata/a.json", conf, "-m", "first", "--services", "odata")
    write_json(conf, {"amqp": {"rk": {"chunk_size": 50}}})
    env.setenv("USER", "bob")
    run("put", "odata/a.json", conf, "-m", "bigger chunks")
    capsys.readouterr()

    assert run("get", "odata/a.json", "--version", "1") == 0
    output = capsys.readouterr()
    assert json.loads(output.out) == {"amqp": {"rk": {"chunk_size": 5}}}
    assert "odata/a.json@1 services=odata author=alice" in output.err

    assert run("diff", "odata/a.json") == 0
    diff = capsys.readouterr().out
    assert "--- odata/a.json@1" in diff
    assert '-                "chunk_size": 5' in diff
    assert '+                "chunk_size": 50' in diff

    assert run("rollback", "odata/a.json", "--to", "1", "-m", "too big") == 0
    assert store.get("odata/a.json")["content"] == {"amqp": {"rk": {"chunk_size": 5}}}

    assert run("delete", "odata/a.json", "-m", "decommissioned") == 0
    capsys.readouterr()

    assert run("history", "odata/a.json") == 0
    lines = capsys.readouterr().out.splitlines()
    assert [line.split("\t")[0] for line in lines] == ["v1", "v2", "v3", "v4"]
    assert lines[1].split("\t")[2] == "bob"
    assert lines[2].endswith("too big (based on v1)")
    assert lines[3].split("\t")[3] == "latest,deleted"

    assert run("list") == 0
    assert capsys.readouterr().out == ""


def test_list_filters_by_service_and_interface(env, store, capsys):
    store.publish(
        "odata/a.json",
        read_json(os.path.join(CONF_DIR, "test-maas-collector-odata-basic.json")),
        ["odata"],
        "alice",
        "v1",
    )
    store.publish("sftp/b.json", {"amqp": {}}, ["sftp"], "alice", "v1")

    run("list", "--service", "sftp")
    assert capsys.readouterr().out.startswith("sftp/b.json\tv1\tsftp\talice\t")

    run("list", "--interface", "basic_test")
    assert capsys.readouterr().out.startswith("odata/a.json\tv1\todata\talice\t")


def test_unknown_path_is_reported(env, capsys):
    assert run("get", "odata/none.json") == 1

    assert "odata/none.json does not exist" in capsys.readouterr().err


def test_no_connection_is_reported(env, capsys):
    env.delenv("ES_URL")

    assert run("list") == 1

    assert "set ES_URL or ES_VAULT_PATH" in capsys.readouterr().err


def test_connection_from_the_vault(env, fake_vault, opensearch_class, tmp_path):
    """the admin CLI reads its OpenSearch connection like the collectors"""
    token_file = tmp_path / "token"
    token_file.write_text(fake_vault.root_token, encoding="UTF-8")
    fake_vault.put(
        "secret",
        "maas/cds/opensearch-admin",
        {
            "url": "https://opensearch.example:9200",
            "username": "config-admin",
            "password": "s3cr3t/admin",
        },
    )
    env.delenv("ES_URL")
    env.setenv("VAULT_ADDR", fake_vault.url)
    env.setenv("VAULT_AUTH_METHOD", "token")
    env.setenv("VAULT_TOKEN_FILE", str(token_file))
    env.setenv("ES_VAULT_PATH", "maas/cds/opensearch-admin")

    assert run("init") == 0

    assert opensearch_class.call_args.kwargs["hosts"] == [
        "https://config-admin:s3cr3t%2Fadmin@opensearch.example:9200"
    ]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests/test_config_cli.py --no-cov -q`
Expected: collection error `ModuleNotFoundError: No module named 'maas_collector.rawdata.cli.config'`.

- [ ] **Step 3: Write the CLI**

Create `src/maas_collector/rawdata/cli/config.py`. Logs go to stderr because stdout carries `get`, `diff` and `list`. `import` publishes new paths with `base_version=0`, so a path created meanwhile by someone else is refused rather than overwritten; `--dry-run` still validates.

```python
"""
Administration of the collector configurations stored in OpenSearch.

    python -m maas_collector.rawdata.cli.config <command> ...

The OpenSearch connection comes from the same settings as the collectors: ES_URL,
ES_USERNAME and ES_PASSWORD, or ES_VAULT_PATH with the VAULT_* settings. Writing
needs an OpenSearch user allowed to write to the configuration index.
"""

import argparse
import dataclasses
import difflib
import json
import logging
import os
import sys

from opensearchpy import OpenSearch

from maas_collector.rawdata.cli.lib.args import (
    config_index_parser,
    es_parser,
    get_es_credentials_url,
    vault_parser,
)
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    is_config_content,
    validate_config_sources,
)
from maas_collector.rawdata.configuration import find_configurations
from maas_collector.rawdata.configstore import (
    ConfigConflictError,
    ConfigNotFoundError,
    ConfigStore,
    config_classes_for,
)

LOGGER = logging.getLogger("ConfigAdmin")


@dataclasses.dataclass
class DeployFile:
    """a configuration file of a deploy tree, and the services that load it"""

    content: dict

    services: list = dataclasses.field(default_factory=list)


def build_parser() -> argparse.ArgumentParser:
    """command line parser of the configuration admin CLI"""
    parser = argparse.ArgumentParser(
        prog="python -m maas_collector.rawdata.cli.config",
        description="Manage the collector configurations stored in OpenSearch",
        parents=[es_parser(), vault_parser(), config_index_parser()],
    )

    parser.add_argument(
        "--author",
        default=os.environ.get("USER", ""),
        help="author of the published versions (default: $USER)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        action="store_const",
        const=logging.INFO,
        default=logging.WARNING,
        help="log what is done",
    )

    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="create the index, unless it exists")

    command = commands.add_parser(
        "import", help="publish the files of a deploy tree <dir>/<service>/**/*.json"
    )
    command.add_argument("deploy_dir", help="deploy tree")
    command.add_argument(
        "--dry-run", action="store_true", help="check and report, write nothing"
    )

    command = commands.add_parser(
        "export", help="write the current versions as <out>/<service>/<path>"
    )
    command.add_argument("out_dir", help="output directory")
    command.add_argument("--service", default="", help="only this service")

    command = commands.add_parser("list", help="list the current versions")
    command.add_argument("--service", default="", help="only this service")
    command.add_argument("--interface", default="", help="only this interface name")

    command = commands.add_parser(
        "get", help="print a version: content on stdout, details on stderr"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("--version", type=int, help="version (default: current)")

    command = commands.add_parser("history", help="list every version of a path")
    command.add_argument("path", help="configuration path")

    command = commands.add_parser("diff", help="unified diff between two versions")
    command.add_argument("path", help="configuration path")
    command.add_argument(
        "--from", dest="from_version", type=int, help="(default: previous version)"
    )
    command.add_argument(
        "--to", dest="to_version", type=int, help="(default: current version)"
    )

    command = commands.add_parser("put", help="publish a new version from a file")
    command.add_argument("path", help="configuration path")
    command.add_argument("file", help="JSON configuration file")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument(
        "--services",
        default="",
        help="comma-separated services loading the file"
        " (default: the services of the current version, required for a new path)",
    )
    command.add_argument(
        "--base-version",
        type=int,
        help="refuse to publish if the current version is not this one",
    )

    command = commands.add_parser(
        "rollback", help="publish the content of an older version again"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("--to", type=int, required=True, help="version to restore")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument("--base-version", type=int, help="see put")

    command = commands.add_parser(
        "delete", help="publish a tombstone: the services stop loading the path"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument("--base-version", type=int, help="see put")

    return parser


def build_client(namespace: argparse.Namespace) -> OpenSearch:
    """OpenSearch client from the ES_* settings, or from the vault with
    ES_VAULT_PATH

    Raises:
        ValueError: without OpenSearch connection settings
    """
    url = get_es_credentials_url(namespace)

    if not url:
        raise ValueError("no OpenSearch connection: set ES_URL or ES_VAULT_PATH")

    return OpenSearch(
        hosts=[url],
        timeout=namespace.es_timeout,
        max_retries=namespace.es_retries,
        retry_on_timeout=True,
        verify_certs=not namespace.es_ignore_certs_verification,
        ssl_show_warn=not namespace.es_ignore_certs_verification,
    )


def get_author(namespace: argparse.Namespace) -> str:
    """author of a write

    Raises:
        ValueError: without author
    """
    if not namespace.author:
        raise ValueError("no author: set --author or USER")
    return namespace.author


def read_deploy_tree(deploy_dir: str) -> tuple[dict, list, dict]:
    """Read the configuration files of a deploy tree <deploy_dir>/<service>/**/*.json,
    hidden files and directories excluded

    Returns:
        tuple[dict, list, dict]: path -> DeployFile; the skipped files, which are
            not configuration files, like credential files; path -> reason for the
            refused paths
    """
    files = {}
    skipped = []
    refused = {}

    for service in sorted(os.listdir(deploy_dir)):
        service_dir = os.path.join(deploy_dir, service)

        if service.startswith(".") or not os.path.isdir(service_dir):
            continue

        for file_path in sorted(find_configurations(service_dir)):
            path = os.path.relpath(file_path, service_dir).replace(os.sep, "/")

            if path in refused:
                continue

            try:
                with open(file_path, encoding="UTF-8") as conf_fd:
                    content = json.load(conf_fd)
            except ValueError as error:
                refused[path] = f"{service}/{path} is not valid JSON: {error}"
                continue

            if not is_config_content(content):
                skipped.append(f"{service}/{path}")
                continue

            if path in files and files[path].content != content:
                refused[path] = f"the copies of {path} differ between services"
                continue

            files.setdefault(path, DeployFile(content)).services.append(service)

    for path in refused:
        files.pop(path, None)

    return files, skipped, refused


def init_command(store: ConfigStore, _namespace) -> int:
    """create the index"""
    if store.init():
        print(f"created index {store.index}")
    else:
        print(f"index {store.index} exists already")
    return 0


def import_command(store: ConfigStore, namespace) -> int:
    """publish the new paths of a deploy tree, report the others"""
    files, skipped, refused = read_deploy_tree(namespace.deploy_dir)
    status = 0

    for name in skipped:
        print(f"skipped {name}: not a configuration file")

    for path, reason in sorted(refused.items()):
        print(f"refused {path}: {reason}")
        status = 1

    author = get_author(namespace) if not namespace.dry_run else namespace.author

    for path in sorted(files):
        deploy_file = files[path]
        current = store.get(path)

        if current is None:
            try:
                if namespace.dry_run:
                    validate_config_sources(
                        [ConfigSource(path, deploy_file.content)],
                        config_classes_for,
                        resolve_models=False,
                    )
                else:
                    store.publish(
                        path,
                        deploy_file.content,
                        deploy_file.services,
                        author,
                        "initial import",
                        base_version=0,
                    )
            except (ConfigConflictError, ValueError) as error:
                print(f"refused {path}: {error}")
                status = 1
                continue

            verb = "would create" if namespace.dry_run else "created"
            print(f"{verb} {path} v1 ({', '.join(deploy_file.services)})")

        elif (
            not current["deleted"]
            and current["content"] == deploy_file.content
            and current["services"] == sorted(deploy_file.services)
        ):
            print(f"unchanged {path} v{current['version']}")

        else:
            print(
                f"drift {path}: v{current['version']} in the index differs from the"
                " deploy tree, left untouched"
            )

    return status


def export_command(store: ConfigStore, namespace) -> int:
    """write the current versions back as a deploy tree"""
    count = 0

    for document in store.latest(service=namespace.service):
        if document["deleted"]:
            continue

        for service in document["services"]:
            if namespace.service and service != namespace.service:
                continue

            target = os.path.join(
                namespace.out_dir, service, *document["path"].split("/")
            )
            os.makedirs(os.path.dirname(target), exist_ok=True)

            with open(target, "w", encoding="UTF-8") as out_fd:
                json.dump(document["content"], out_fd, indent=4, ensure_ascii=False)
                out_fd.write("\n")

            count += 1

    print(f"exported {count} files to {namespace.out_dir}")
    return 0


def list_command(store: ConfigStore, namespace) -> int:
    """print path, version, services, author and date of the current versions"""
    for document in store.latest(
        service=namespace.service, interface=namespace.interface
    ):
        if document["deleted"]:
            continue

        print(
            "\t".join(
                [
                    document["path"],
                    f"v{document['version']}",
                    ",".join(document["services"]),
                    document.get("author") or "",
                    document.get("created_at") or "",
                ]
            )
        )
    return 0


def find_version(store: ConfigStore, path: str, version: int | None) -> dict:
    """a version of a path

    Raises:
        ConfigNotFoundError: when it does not exist
    """
    document = store.get(path, version)

    if document is None:
        name = path if version is None else f"{path}@{version}"
        raise ConfigNotFoundError(f"{name} does not exist")

    return document


def get_command(store: ConfigStore, namespace) -> int:
    """content on stdout, version, services and author on stderr"""
    document = find_version(store, namespace.path, namespace.version)

    print(json.dumps(document["content"], indent=4, ensure_ascii=False))

    deleted = " (deleted)" if document["deleted"] else ""
    print(
        f"{document['path']}@{document['version']}{deleted}"
        f" services={','.join(document['services'])}"
        f" author={document.get('author')} created_at={document.get('created_at')}",
        file=sys.stderr,
    )
    return 0


def history_command(store: ConfigStore, namespace) -> int:
    """one line per version, oldest first"""
    history = store.history(namespace.path)

    if not history:
        raise ConfigNotFoundError(f"{namespace.path} does not exist")

    for document in history:
        flags = [
            flag
            for flag, value in (
                ("latest", document["latest"]),
                ("deleted", document["deleted"]),
            )
            if value
        ]
        based_on = (
            f" (based on v{document['based_on']})" if document.get("based_on") else ""
        )
        print(
            f"v{document['version']}\t{document.get('created_at')}"
            f"\t{document.get('author')}\t{','.join(flags)}"
            f"\t{document.get('comment')}{based_on}"
        )
    return 0


def diff_lines(document: dict) -> list:
    """lines of a version compared by diff: services, tombstone flag and content"""
    shown = {
        "services": document["services"],
        "deleted": document["deleted"],
        "content": document["content"],
    }
    text = json.dumps(shown, indent=4, sort_keys=True, ensure_ascii=False) + "\n"
    return text.splitlines(keepends=True)


def diff_command(store: ConfigStore, namespace) -> int:
    """unified diff between two versions, the current one and its previous one by
    default"""
    to_document = find_version(store, namespace.path, namespace.to_version)

    from_version = namespace.from_version
    if from_version is None:
        from_version = to_document["version"] - 1

    from_document = find_version(store, namespace.path, from_version)

    sys.stdout.writelines(
        difflib.unified_diff(
            diff_lines(from_document),
            diff_lines(to_document),
            fromfile=f"{namespace.path}@{from_version}",
            tofile=f"{namespace.path}@{to_document['version']}",
        )
    )
    return 0


def put_command(store: ConfigStore, namespace) -> int:
    """publish a new version from a file"""
    with open(namespace.file, encoding="UTF-8") as conf_fd:
        content = json.load(conf_fd)

    if namespace.services:
        services = [
            service.strip()
            for service in namespace.services.split(",")
            if service.strip()
        ]
    else:
        current = store.get(namespace.path)
        if current is None:
            raise ValueError(
                f"{namespace.path} is a new path: give its services with --services"
            )
        services = current["services"]

    document = store.publish(
        namespace.path,
        content,
        services,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )

    print(
        f"published {namespace.path}@{document['version']}"
        f" ({', '.join(document['services'])})"
    )
    return 0


def rollback_command(store: ConfigStore, namespace) -> int:
    """publish an older version again"""
    document = store.rollback(
        namespace.path,
        namespace.to,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )
    print(
        f"published {namespace.path}@{document['version']}"
        f" with the content of v{namespace.to}"
    )
    return 0


def delete_command(store: ConfigStore, namespace) -> int:
    """publish a tombstone"""
    document = store.delete(
        namespace.path,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )
    print(f"deleted {namespace.path}: tombstone v{document['version']}")
    return 0


COMMANDS = {
    "init": init_command,
    "import": import_command,
    "export": export_command,
    "list": list_command,
    "get": get_command,
    "history": history_command,
    "diff": diff_command,
    "put": put_command,
    "rollback": rollback_command,
    "delete": delete_command,
}


def config_main(argv: list) -> int:
    """entry point

    Args:
        argv (list): command line arguments

    Returns:
        int: exit code, 1 on a refused or failed command
    """
    namespace = build_parser().parse_args(argv)

    # stderr: stdout carries the output of get, diff and export
    logging.basicConfig(
        level=namespace.loglevel,
        stream=sys.stderr,
        format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
    )

    try:
        store = ConfigStore(build_client(namespace), namespace.config_index)
        return COMMANDS[namespace.command](store, namespace)
    except (ConfigConflictError, ConfigNotFoundError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(config_main(sys.argv[1:]))
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `PYTHONPATH=src TZ=UTC .tox/default/bin/python -m pytest tests --no-cov -q`
Expected: `281 passed` (17 new).

Run: `PYTHONPATH=src .tox/default/bin/python -m maas_collector.rawdata.cli.config --help`
Expected: usage listing `{init,import,export,list,get,history,diff,put,rollback,delete}` and the `--es-vault-path`, `--vault-*`, `--config-index` and `--author` options.

- [ ] **Step 5: Commit**

```bash
git add src/maas_collector/rawdata/cli/config.py tests/test_config_cli.py
git commit -m "feat: :sparkles: Add the collector configuration admin CLI" -m "python -m maas_collector.rawdata.cli.config creates the index, imports a deploy tree once (merging the services of shared paths, skipping credential files, reporting drift without overwriting), exports it back, and lists, shows, diffs, publishes, rolls back and deletes versions. It reads its OpenSearch connection like the collectors, ES_VAULT_PATH included." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Documentation

**Files:**
- Create: `docs/opensearch_config.md`
- Modify: `docs/configuration.md:1-6`, `docs/common_options.md:42-51`, `docs/index.md:9`

**Interfaces:**
- Consumes: the settings, log lines, exit code and CLI commands of Tasks 2-6, verbatim.
- Produces: `docs/opensearch_config.md` with the anchors `#roles` and `#moving-to-the-index`, linked from the other pages and from `CHANGELOG.md` (Task 8).

- [ ] **Step 1: Write the page**

Create `docs/opensearch_config.md`:

````markdown
# Configuration in OpenSearch

By default, a collector reads its configuration from JSON files, given with `-c` and
`-d` (see [configuration](configuration.md)). With `CONFIG_BACKEND=opensearch`, it
reads the same files from an OpenSearch index instead. The index then becomes the
single source of truth for the collector configurations:

- every edit is a new version, with its author, date and comment, so the history is
  complete and a rollback is one command;
- a running collector notices a change and restarts itself to apply it;
- Dashboards shows which services load which files, and their content.

The file backend stays the default. Keep it for the tests, local runs and replay
services.

## Documents and versions

The index, `maas-collector-config` by default (`CONFIG_INDEX`), holds **one document
per version of a configuration file**. The document id is `<path>@<version>`, like
`odata/prip/maas-collector-odata-prip.json@3`.

- `path` is the path of the file relative to the service directory of a file
  deployment: the path under `/conf/collector/` in the containers.
- `content` is the file JSON, verbatim: `{"collectors": [...], "models": [...],
  "amqp": {...}}`, each key optional. It is stored, not indexed.
- `services` lists the services that load the file. Several services may share a
  file: one document serves them all.
- The current version of a path is flagged `latest`. Versions are never modified,
  apart from this flag: an edit, a rollback or a deletion writes a new version.
- A deletion writes a **tombstone**: a version flagged `deleted`, with an empty
  content. The services stop loading the path. Publishing the path again brings it
  back.

| Field                        | Type                   | Meaning                                            |
| ---------------------------- | ---------------------- | -------------------------------------------------- |
| `path`                       | keyword                | relative path of the file                          |
| `version`                    | integer                | 1, 2, ... for each path                            |
| `latest`                     | boolean                | current version of the path                        |
| `deleted`                    | boolean                | tombstone: the file was removed                    |
| `services`                   | keyword                | services loading the file                          |
| `content`                    | object, not indexed    | the file JSON                                      |
| `interface_names`            | keyword                | `interface_name` of the collector entries          |
| `model_names`                | keyword                | models declared or used by the file                |
| `author`, `comment`          | keyword, text          | who published the version, and why                 |
| `created_at`                 | date                   | publication time                                   |
| `based_on`                   | integer                | version edited, or version restored by a rollback |

The mapping is strict: OpenSearch refuses any other field. Only credential-free
configuration files are accepted: a file whose top-level keys are not within
`collectors`, `models` and `amqp`, like a credential file, is refused.

## Settings of the collectors

| CLI option              | Environment variable  | Description                                                                     |
| ----------------------- | --------------------- | ------------------------------------------------------------------------------- |
| --config-backend        | CONFIG_BACKEND        | Where the configuration is read from: `file` or `opensearch` (default: file)    |
| --config-index          | CONFIG_INDEX          | Index holding the configurations (default: maas-collector-config)               |
| --config-service        | CONFIG_SERVICE        | Service tag of the files to load, required with `opensearch` (default: none)    |
| --config-refresh-period | CONFIG_REFRESH_PERIOD | Seconds between two checks for a change, 0 never checks (default: 300)          |

- With `CONFIG_BACKEND=opensearch`, the collector stops at startup when
  `CONFIG_SERVICE` is not set, when the OpenSearch connection is not set (neither
  `ES_URL` nor `ES_VAULT_PATH`), when OpenSearch cannot be read, or when the index
  holds no file for the service.
- `-c` / `RAWDATA_CONFIG` and `-d` / `RAWDATA_CONFIG_DIR` are then ignored, with a
  warning.
- The configuration is read through the collector's OpenSearch connection, so it
  also works when the connection comes from the vault with `ES_VAULT_PATH` (see
  [vault](vault.md#opensearch-and-amqp-connections)).
- The interface monitor finds the configurations of the interfaces it probes among
  the files of its own service: tag them with the monitor service too.

## Applying a change: the collector restarts

A collector checks its files at the start of a loop, once `CONFIG_REFRESH_PERIOD`
seconds have passed since the last check. A change is picked up within about
`CONFIG_REFRESH_PERIOD` plus `WATCH_PERIOD` plus the duration of the current loop.

- **A file changed, was added for the service, or was removed from it:** the
  collector checks that it can load the new set of files, logs
  `configuration changed: odata/prip.json v3→v4, odata/new.json added, restarting`,
  sends its pending messages, and **exits with code 75** (`EX_TEMPFAIL`). The
  orchestrator restarts it, and the new process loads the new files. Swarm restarts
  it with `restart_policy` `any` or `on-failure`, Kubernetes with
  `restartPolicy: Always`.
- **The new set cannot be loaded** (a bad extractor class, a model defined nowhere,
  a document written by hand with a wrong shape): the collector logs
  `configuration change rejected: <path>@<version>: <reason>` and keeps running
  with its current files. It checks this set only once: fix the file with a new
  version or a rollback.
- **OpenSearch cannot be read**, or the service has no file left: the collector logs
  the error and keeps running with its current files. It checks again at the next
  period. The first failure logs a traceback, the next ones a single line.
- With `CONFIG_REFRESH_PERIOD=0`, the collector never checks: a change applies at
  the next restart.

## Administration CLI

Edits go through the administration CLI, which checks every version before
publishing it. It reads the OpenSearch connection from the same settings as the
collectors: `ES_URL`, `ES_USERNAME` and `ES_PASSWORD`, or `ES_VAULT_PATH` with the
`VAULT_*` settings, and the index from `CONFIG_INDEX` / `--config-index`. Its user
needs write access to the index (see [Roles](#roles)). `--author` defaults to
`$USER`. The general options go before the command.

```shell
python -m maas_collector.rawdata.cli.config [--config-index INDEX] [--author NAME] <command> ...
```

| Command                                                                  | What it does                                                                                                         |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `init`                                                                   | Create the index with its mapping. Does nothing if it exists.                                                        |
| `import <deploy-dir> [--dry-run]`                                        | Publish the files of a deploy tree `<deploy-dir>/<service>/**/*.json` (see [Moving to the index](#moving-to-the-index)). |
| `export <out-dir> [--service S]`                                         | Write the current versions back as `<out-dir>/<service>/<path>`.                                                     |
| `list [--service S] [--interface I]`                                     | Path, version, services, author and date of the current versions.                                                    |
| `get <path> [--version N]`                                               | Content on stdout; version, services and author on stderr.                                                           |
| `history <path>`                                                         | Every version, with its date, author, flags and comment.                                                             |
| `diff <path> [--from N] [--to M]`                                        | Unified diff between two versions, by default the current one and the previous one.                                  |
| `put <path> <file> -m <comment> [--services a,b] [--base-version N]`     | Publish a new version. `--services` defaults to the services of the current version, and is required for a new path. |
| `rollback <path> --to N -m <comment>`                                    | Publish the content and services of version N as a new version.                                                      |
| `delete <path> -m <comment>`                                             | Publish a tombstone.                                                                                                 |

- `put`, `rollback` and `delete` accept `--base-version N`: the command is refused
  when the current version is no longer N, because someone published in the
  meantime. Two publications of the same version at the same time are refused the
  same way: the second one gets a conflict.
- A refused command exits with code 1 and prints the reason on stderr.

Editing a file:

```shell
python -m maas_collector.rawdata.cli.config get odata/prip/prip-s1a.json > prip-s1a.json
# edit prip-s1a.json, then
python -m maas_collector.rawdata.cli.config put odata/prip/prip-s1a.json prip-s1a.json \
    -m "refresh every 10 minutes" --base-version 3
python -m maas_collector.rawdata.cli.config diff odata/prip/prip-s1a.json
```

## Moving to the index

1. Create the index: `config init`.
2. Check the import of the collector directory of the deploy tree, which holds one
   directory per service:
   `config import <deploy-tree>/collector --dry-run`.
   - Files are grouped by path relative to their service directory, and a path
     shared by several services becomes one document listing all of them. A path
     whose copies differ between services is refused: make the copies equal first.
   - Files that are not configuration files, like credential files, are skipped and
     listed. Hidden files and directories are ignored.
   - Every file is checked like a `put`.
3. Import: `config import <deploy-tree>/collector`. A path that is not in the index
   yet gets version 1. A path already in the index is never overwritten: when it
   differs from the deploy tree, the import reports a `drift` and leaves it
   untouched. From then on, the index is the source of truth.
4. Check a service: `config list --service odata`.
5. Switch the services one at a time: set `CONFIG_BACKEND=opensearch` and
   `CONFIG_SERVICE=<service directory name>`, and remove the configuration mounts
   and `-d` / `RAWDATA_CONFIG_DIR`.

To go back to files, `config export <dir>` writes the current versions as a deploy
tree, which can also be compared with the repository.

```yaml
services:
  collector-odata:
    image: <registry>/maas-collector:<version>
    environment:
      CONFIG_BACKEND: opensearch
      CONFIG_SERVICE: odata
      ES_VAULT_PATH: maas/cds/opensearch
      AMQP_VAULT_PATH: maas/cds/amqp
      CREDENTIAL_BACKEND: vault
      # VAULT_* settings: see vault.md
    deploy:
      restart_policy:
        condition: any
```

## Roles

**Configurations run code**: the `{"python": "lambda ..."}` values of a
configuration are evaluated by the collectors. Write access to the index therefore
means code execution in every collector that reads it.

- Collectors get **read-only** access to the index.
- Only the administrators running the CLI get write access.
- Check that no other role of the collector users grants write access to the index,
  for example through a `*` index pattern.

With the OpenSearch security plugin:

```json
PUT _plugins/_security/api/roles/maas_collector_config_read
{
  "cluster_permissions": ["cluster_composite_ops_ro"],
  "index_permissions": [
    {"index_patterns": ["maas-collector-config"], "allowed_actions": ["read"]}
  ]
}

PUT _plugins/_security/api/roles/maas_collector_config_admin
{
  "cluster_permissions": ["cluster_composite_ops"],
  "index_permissions": [
    {
      "index_patterns": ["maas-collector-config"],
      "allowed_actions": ["crud", "create_index"]
    }
  ]
}
```

Map the first role to the users of the collectors, and the second one to the
administrators, with `PUT _plugins/_security/api/rolesmapping/<role>`.

Secrets never belong in the index: the CLI refuses credential files, and the
credentials stay in the vault or the credential file.

## Dashboards

Dashboards is meant for inspection: hand-written versions in Dev Tools skip the
checks of the CLI.

1. Create an index pattern `maas-collector-config` with `created_at` as time field.
2. In Discover, save the search `latest:true and not deleted:true`: the current
   files, with `path`, `version`, `services`, `interface_names` and `author` as
   columns. The content shows in the document details.
3. `path:"odata/prip/prip-s1a.json"` sorted by `version` shows the history of a file.

## Trying it locally

```shell
docker compose -f docker-compose-opensearch.yml up -d
export ES_URL=http://localhost:9200
python -m maas_collector.rawdata.cli.config init
python -m maas_collector.rawdata.cli.config put odata/basic.json \
    tests/conf/test-maas-collector-odata-basic.json -m "local test" --services odata

CONFIG_BACKEND=opensearch CONFIG_SERVICE=odata CONFIG_REFRESH_PERIOD=10 \
    WATCH_PERIOD=5 CREDENTIAL_FILE=tests/conf/test-maas-collector-credentials.json \
    python -m maas_collector.rawdata.cli.odata -v
```

Then `put` a changed version from another shell: the collector logs
`configuration changed: odata/basic.json v1→v2, restarting` and exits with code 75.
Recent OpenSearch images need `OPENSEARCH_INITIAL_ADMIN_PASSWORD` set.
````

- [ ] **Step 2: Add the pointers**

In `docs/configuration.md`, replace lines 3-6:

```markdown
maas-collector uses JSON file to store:

- a list of configurations in the root key `collectors`
- named models shared by several configurations in the root key `models`
- amqp fine-tuning in the root key `amqp`

The files are read from `-c` / `-d` by default. They can also be stored as versioned
documents in an OpenSearch index, see [configuration in OpenSearch](opensearch_config.md).
```

In `docs/common_options.md`, after `See [configuration](configuration.md)` (line 51), add:

```markdown

The configuration can also come from an OpenSearch index, see
[configuration in OpenSearch](opensearch_config.md):

| CLI option              | Environment variable  | Description                                                                  |
| ----------------------- | --------------------- | ---------------------------------------------------------------------------- |
| --config-backend        | CONFIG_BACKEND        | Where the configuration is read from: `file` or `opensearch` (default: file) |
| --config-index          | CONFIG_INDEX          | Index holding the configurations (default: maas-collector-config)            |
| --config-service        | CONFIG_SERVICE        | Service tag of the files to load, required with `opensearch` (default: none) |
| --config-refresh-period | CONFIG_REFRESH_PERIOD | Seconds between two checks for a change, 0 never checks (default: 300)       |

> With `opensearch`, `-c` and `-d` are ignored, and a collector whose configuration
> changed exits with code 75 for its orchestrator to restart it.
```

In `docs/index.md`, after `- [Configuration](configuration)` (line 9), add:

```markdown

- [Configuration in OpenSearch](opensearch_config)
```

- [ ] **Step 3: Check the page against the code and the public repository rule**

Run: `PYTHONPATH=src .tox/default/bin/python -c "from maas_collector.rawdata.cli.lib.args import common_parser; n = common_parser().parse_args([]); print(n.config_backend, n.config_index, repr(n.config_service), n.config_refresh_period)"`
Expected: `file maas-collector-config '' 300`, the defaults the two tables state.

Run: `git add docs/opensearch_config.md docs/configuration.md docs/common_options.md docs/index.md && git diff --cached -- docs | grep '^+' | grep -o -E 'https?://[^ )]*' | grep -v -E 'example|internal|localhost|127\.0\.0\.1'`
Expected: no output: every URL the pages add is an example one. Read the new page once more for repository names, host names or local paths of a real deployment, and replace any with a placeholder.

Run: `grep -c -E "configuration changed: |configuration change rejected: |code 75" docs/opensearch_config.md`
Expected: `3` or more.

- [ ] **Step 4: Commit**

```bash
git add docs/opensearch_config.md docs/configuration.md docs/common_options.md docs/index.md
git commit -m "docs: :memo: Document the collector configuration in OpenSearch" -m "New page on the versioned documents, the collector settings, the restart on change and its exit code, the admin CLI, the move from the deploy tree, the read-only and admin roles, Dashboards, and a local trial. The configuration and common options pages point to it." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

### Task 8: Release preparation for 3.12.0

**Files:**
- Modify: `CHANGELOG.md:1-3`
- Modify: `pyproject.toml:11`
- Modify: `sonar-project.properties:3`

**Interfaces:**
- Consumes: everything above.
- Produces: the 3.12.0 changelog entry; `fallback_version = "3.12.0"`; `sonar.projectVersion=3.12.0`.

- [ ] **Step 1: Add the changelog entry**

In `CHANGELOG.md`, insert between `# Changelog` (line 1) and `## Version 3.11.0` (line 3):

```markdown
## Version 3.12.0

### Collector configuration in OpenSearch

- Collectors can read their configuration files from an OpenSearch index instead of
  `-c` / `-d`: set `CONFIG_BACKEND=opensearch` (`--config-backend`) and
  `CONFIG_SERVICE` (`--config-service`), the service whose files to load. The files
  stay the default (`CONFIG_BACKEND=file`).
- The index, `maas-collector-config` by default (`CONFIG_INDEX`, `--config-index`),
  holds one document per version of a file, `<path>@<version>`, with its services,
  author, date and comment. The current version of each path is flagged `latest`, and
  a deletion is a tombstone version. The mapping is strict, and the file content is
  stored without being indexed.
- A running collector checks its files at the start of a loop, once
  `CONFIG_REFRESH_PERIOD` seconds (`--config-refresh-period`, default 300) have passed.
  When they changed and it can load the new ones, it logs
  `configuration changed: <path> v3→v4, ... restarting` and exits with code 75, for
  its orchestrator to restart it on the new files. A new set it cannot load is logged
  as `configuration change rejected: <path>@<version>: <reason>` and ignored until it
  changes again. When OpenSearch cannot be read, the collector keeps its files and
  checks again at the next period. `CONFIG_REFRESH_PERIOD=0` turns the checks off.
- At startup, a collector with `CONFIG_BACKEND=opensearch` stops when
  `CONFIG_SERVICE` or the OpenSearch connection is not set, when OpenSearch cannot be
  read, or when the index has no file for its service. The configuration is read
  through the collector's OpenSearch connection, so `ES_VAULT_PATH` works too.
- New administration CLI, `python -m maas_collector.rawdata.cli.config`: `init`,
  `import` of a deploy tree (with `--dry-run`), `export`, `list`, `get`, `history`,
  `diff`, `put`, `rollback` and `delete`. It checks every version before publishing
  it, refuses credential files, and refuses a publication based on an outdated
  version. It reads the OpenSearch connection like the collectors, including
  `ES_VAULT_PATH`.
- The interface monitor finds the configurations of the interfaces it probes among
  the files of its own service.
- Write access to the index means code execution in the collectors, as
  configurations hold Python lambdas: give the collectors read-only access. New
  `docs/opensearch_config.md` covers the documents, the CLI, the move from files, the
  roles, Dashboards and the restart behaviour.

### Changes

- The check meant to refuse a configuration file without collector entries never
  ran, as it tested a generator. It is removed, so files holding only `models` or
  only `amqp` keep loading, from files and from the index alike.
- `FileCollector.get_configs()` takes a `ConfigSource` instead of a path, and the
  `config_files` property is replaced by `config_provider` and `config_sources`.
  `configuration.load_json()` and `Messenger.load_config_file()` are unchanged, and
  rely on the new `load_json_dict()` and `load_config_dict()`.

### Requirements and upgrade

- No new dependency. A collector that sets none of the new variables runs as before.
- The deployments move one service at a time: see `docs/opensearch_config.md`.
```

- [ ] **Step 2: Bump the fallback and SonarQube versions**

In `pyproject.toml` line 11, `fallback_version = "3.11.0"` becomes:

```toml
fallback_version = "3.12.0"
```

In `sonar-project.properties` line 3, `sonar.projectVersion=3.11.0` becomes:

```properties
sonar.projectVersion=3.12.0
```

- [ ] **Step 3: Run the full suite in tox**

Run: `PIP_FIND_LINKS="$MAAS_WHEELS" SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 tox -e default`
Expected: `281 passed`, `default: OK`; `configprovider.py`, `configstore.py` and `cli/config.py` covered at 95% or more in the coverage table.

Run: `git diff develop -- src tests docs CHANGELOG.md | grep '^+' | grep -n -E '/home/|/Users/|https?://[^ )"]*' | grep -v -E 'example|internal|localhost|127\.0\.0\.1|opensearch\.org|tox\.wiki'`
Expected: no output: the branch adds no local path and no real host.

- [ ] **Step 4: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: :memo: Changelog for 3.12.0" -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
git add pyproject.toml sonar-project.properties
git commit -m "build: :bookmark: Set the fallback and SonarQube versions to 3.12.0" -m "A build without git metadata is labelled with the fallback version, and SonarQube shows the project version for every analysis." -m "Co-Authored-By: Claude Opus 5.5 (1M context) <noreply@anthropic.com>"
```

---

## Spec coverage

| Spec section | Task |
| --- | --- |
| 1. Index, id `<path>@<version>`, strict mapping, fields | 3 (`CONFIG_INDEX_BODY`, `publish`), settings in 2 |
| 2. Write protocol, conflicts, repair, history/get/rollback/delete, validation | 3 |
| 3. `ConfigSource`, providers, fingerprints, `build_config_provider`, refactors, connection before `load_config`, monitor second pass | 1, 2, 5 |
| 4. `check_config_changes`, time gate, failure, rejection, exit 75 | 4 (monitor class check in 5) |
| 5. Admin CLI commands | 6 |
| 6. Settings, `ConfigStoreArgs`, start-up checks, `-c` / `-d` warning | 2 |
| 7. Security: read-only collectors, credential files refused | 3, 6, 7 |
| 8. Docs | 7 |
| Verification list (fake, provider, store, import/export, change check, monitor, existing suites) | 2-6, full run in 8 |
| Release 3.12.0 | 8 |

The manual end-to-end check of the spec (`docker-compose-opensearch.yml`, `config init` / `import` / `put` / `rollback`, a collector exiting with code 75) is described in `docs/opensearch_config.md#trying-it-locally` and runs after Task 8, against a real OpenSearch.

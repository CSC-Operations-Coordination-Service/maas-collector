# Design: read collector configuration from OpenSearch

> Status: approved 2026-10-12. Implementation on branch `feature/opensearch-config`, cut from develop after 3.11.0 (it builds on the vault `run()` and args changes released there).

## Context

Each collector reads its configuration from JSON files given with `-c` / `-d` (`RAWDATA_CONFIG` / `RAWDATA_CONFIG_DIR`). The files are read once in `FileCollector.setup()` (`filecollector.py:421-424` → `load_config()` 352-396). In a typical deployment, a deploy tree `collector/<service>/…` holds a few hundred files, and each one is mounted into its service as a config entry under `/conf/collector/<path>`. Changing a config means a commit to the deploy repository, a new config object, and a service update.

**Goal:** collectors read their configuration from an OpenSearch index. This index becomes the single source of truth, with full history and rollback. A collector notices a change and restarts itself to apply it. The file backend stays the default, for tests, local runs and replay services.

### Decisions taken with the user

| Question | Decision |
|---|---|
| Why | Central source of truth, history and audit, change without a manual restart, inspection in Dashboards |
| Where config is authored | Git seeds the index once. After that, OpenSearch wins: a re-import never overwrites a document. |
| How a running collector applies a change | **Clean exit.** The orchestrator restarts it (Swarm uses `restart_policy: any`). No in-process hot reload. |
| Index layout | A **new index owned by maas-collector, one document per config file**. Not the backend's `maas-config-collector`. |
| History | **Immutable versions**: each edit is a new document `<path>@<n>`, and a `latest` flag marks the current one. |
| Editing in Dashboards | Dashboards is for **inspection**. Edits go through the admin CLI, because hand-writing versioned documents in Dev Tools is error-prone. |

### What the deploy tree looks like (surveyed in a real deployment)

- There are **100 distinct relative paths**. 77 of them are shared by several services (for example `odata`, `sftp`, `monitor-main`), and **no copies differ**. So the document id is the path relative to the service directory, which is also the path under `/conf/collector/` today, and the document lists the services that load it.
- **A service is not selected by `class`.** sftp, filesystem and monitor load other collectors' entries, and 21 entries have no `class` at all. Selection is therefore by an explicit `services` tag.
- **Files hold `collectors`, `models` (29 shared named models) and `amqp` (30 chunk-size files).** Storing whole files keeps all three without any change of shape.
- **Credential JSONs sit in the same tree.** The import must never ingest them.

## Design

### 1. Index `maas-collector-config` (name settable with `CONFIG_INDEX`)

One document per **version of a file**, with id `<path>@<version>`, for example `odata/prip/maas-collector-odata-prip-s1a-01-serco.json@3`. The mapping is `dynamic: strict`:

| Field | Type | Meaning |
|---|---|---|
| `path` | keyword | relative path, the same as under `/conf/collector/` today |
| `version` | integer | 1, 2, … for each path |
| `latest` | boolean | the current version of the path |
| `deleted` | boolean | tombstone version: the file was removed |
| `services` | keyword[] | deploy service names that load the file (`odata`, `sftp`, `monitor-main`, …) |
| `content` | object, `enabled: false` | the file JSON verbatim: `{collectors?, models?, amqp?}`. Stored, not indexed, so there is no mapping explosion and no type clash |
| `interface_names`, `model_names` | keyword[] | taken from `content` at write time, for searching in Dashboards |
| `author`, `comment`, `created_at`, `based_on` | keyword, text, date, integer | audit fields; `based_on` is the version that was edited or rolled back to |

### 2. Write protocol: new `src/maas_collector/rawdata/configstore.py`

`ConfigStore(client, index)` is the only writer. It is used by the admin CLI and by `import`.

`publish(path, content, services, author, comment, base_version=None, deleted=False)`:
1. Read the highest version `n` of the path. If `base_version` is given and differs from `n`, refuse with a conflict: someone published in the meantime.
2. Validate `content`, unless this is a tombstone (see the end of this section).
3. Write `<path>@<n+1>` with `op_type=create` and `latest: true`. A 409 means a concurrent publish, which is reported as a conflict.
4. Set `latest: false` on every other `latest` version of the path, using a search and then `update` per document. This step also repairs anything a crashed earlier publish left behind.

Readers stay correct in the short window between steps 3 and 4: for each path they keep the highest `version` among the `latest` documents.

Other operations:
- `history(path)`, `get(path, version=None)`.
- `rollback(path, to, …)` publishes the content and services of version `to` as a new version.
- `delete(path, …)` publishes a tombstone.

**Validation:**
- The top-level keys must be within `{collectors, models, amqp}`. This rejects credential files, which are `{"interfaces": …}`.
- Every collector entry must build with `build_collector_configuration`. The class used is the one named in `class`, via `get_collector_class_by_config_classname`, or `FileCollectorConfiguration` when `class` is missing or empty.
- Every model must build without being registered (see section 4).

### 3. Provider in collectors: new `src/maas_collector/rawdata/configprovider.py`

This follows the shape of `credentialprovider.py`.

```python
@dataclass(frozen=True)
class ConfigSource:
    name: str              # file path, or the document path
    content: dict          # {collectors?, models?, amqp?}
    version: int | None = None

class ConfigProvider(abc.ABC):
    def load(self) -> list[ConfigSource]: ...
    def fingerprint(self, sources) -> frozenset | None: ...   # None = never changes
```

- **`FileConfigProvider(config_file, config_dir)`** contains today's `config_files` logic (`filecollector.py:398-419`) plus `find_configurations`. Its fingerprint is `None`, so the file backend never triggers a restart and behaves exactly as today.
- **`OpenSearchConfigProvider(index, service)`**:
  - **Query:** `scan` with the filters `latest=true`, `services=<service>`, `deleted=false`. For each path it keeps the highest version, and it sorts by path so the load order is deterministic.
  - **Fingerprint:** `frozenset((path, version))`.
  - **No documents:** at startup this raises `ValueError("no configuration for service X in index Y")`.
- **`build_config_provider(args)`** switches on `args.config_backend`.

**Refactors so that dicts can be loaded without files:**
- `configuration.py`: extract `load_json_dict(json_dict, configuration_class, source)` from `load_json` (160-184). `load_json(path, cls)` becomes a thin wrapper, so maas-cds `track_deletion.py`, `generate_interface.py` and the tests are untouched.
- `messenger.py`: extract `load_config_dict(json_dict, source)` from `load_config_file` (150-177), and keep the wrapper.
- `filecollector.py`:
  - `get_configs(source)` takes a `ConfigSource`.
  - `load_config()` iterates `self.config_provider.load()`.
  - Delete the emptiness check at 345-348. It can never fire, because a generator is always truthy, and making it fire would reject model-only and amqp-only files.
- `setup()`: create the OpenSearch connection **before** `load_config()`, because the OpenSearch provider needs it. `create_connection` is lazy, so nothing changes for the file backend.
- `monitorcollector.py:385-415`: the second pass iterates the provider's sources instead of `find_configurations` plus `json.load`, and uses `load_json_dict` instead of `load_configuration_json(path, …)`. `InterfaceMeta.configuration_path` holds the source name.

### 4. Detecting a change and exiting: `FileCollector.check_config_changes()`

- It is called in `run()` right after `refresh_credentials()` (`filecollector.py:806`), for the same reason that call sits there: overrides cannot skip it, and it runs on the main thread between loops.
- It is time-gated by `CONFIG_REFRESH_PERIOD` using `time.monotonic`, as `refresh_credentials` is. When the period is `0` or the fingerprint is `None`, it does nothing.
- **Loading fails** (OpenSearch is down): log the error, keep running on the current config, and retry at the next period.
- **Fingerprint unchanged:** do nothing.
- **Fingerprint changed:**
  1. Validate the new sources. Collectors are built with the process's `CONFIG_CLASS`, and models with `build_model_class(meta, register=False)`. That flag is a new keyword, so the running process's `MODEL_DICT` is not touched. Model names must resolve within the new set.
  2. **Invalid:** log `configuration change rejected: <path>@<v>: <error>` and keep running. The same versions are not re-validated until the fingerprint changes again. This avoids a crash loop on a bad edit.
  3. **Valid:** log `configuration changed: <path> v3→v4, <path> added, … restarting`, then `raise SystemExit(CONFIG_CHANGED_EXIT_CODE)` with code `75` (EX_TEMPFAIL). The existing `finally` in `run()` stops the health check and flushes messages. None of the 13 CLI modules need a change.

### 5. Admin CLI: new `src/maas_collector/rawdata/cli/config.py`

Run it as `python -m maas_collector.rawdata.cli.config <command>`. It reuses `es_parser()` (ES_URL and friends) and `--config-index`. `--author` defaults to `$USER`.

| Command | What |
|---|---|
| `init` | Create the index with the mapping. Does nothing if the index exists. |
| `import <deploy-dir> [--dry-run]` | Walk `<deploy-dir>/<service>/**/*.json`, skipping hidden files and directories. **Skip any file whose top-level keys are not within the config keys** (credential files) and log its name. Group files by relative path and merge their `services`; refuse a path whose copies differ. A path with no versions gets v1. If it has versions and is equal (content plus services), skip it. If it is different, **report drift and leave it untouched**. |
| `export <out-dir> [--service S]` | Write the latest versions back as `<out>/<service>/<path>`. This is the way back to the file backend, and lets you diff against git. |
| `list [--service S] [--interface I]` | path, version, services, author, date |
| `get <path> [--version N]` | Content to stdout; version, services and author to stderr. |
| `history <path>` / `diff <path> [--from N] [--to M]` | Audit trail, and a unified diff between versions. |
| `put <path> <file> -m <comment> [--services a,b] [--base-version N]` | Publish a new version. `services` defaults to the current version's services, and is required for a new path. |
| `rollback <path> --to N -m <comment>` / `delete <path> -m <comment>` | New version with old content / tombstone. |

### 6. Settings: `cli/lib/args.py`

Add a `config_parser()` and add it to the `common_parser()` parents, on the model of `vault_parser()` (946-1033). Then add a `ConfigStoreArgs` dataclass plus `config_backend` and `config_refresh_period` to `CollectorArgs` (`filecollector.py:50-100`), and fill them in `get_collector_args()` (1228-1285).

| Option / env | Default | Purpose |
|---|---|---|
| `--config-backend` / `CONFIG_BACKEND` | `file` | `file` or `opensearch` |
| `--config-index` / `CONFIG_INDEX` | `maas-collector-config` | index name |
| `--config-service` / `CONFIG_SERVICE` | – | service tag to load; required with `opensearch` |
| `--config-refresh-period` / `CONFIG_REFRESH_PERIOD` | `300` | seconds between change checks; `0` = never |

**Startup checks:**
- With `opensearch`, `CONFIG_SERVICE` and `ES_URL` must be set; otherwise raise `ValueError` with a clear message.
- If `-c` or `-d` is also given, log a warning that it is ignored.

### 7. Security

- **Configs run code.** `{"python": "lambda …"}` strings are passed to `eval()` (`extractor/base.py:85-88,134-148`). So **write access to `maas-collector-config` means code execution in every collector.**
  - Collector OpenSearch users get **read-only** access to the index.
  - Only admins running the CLI may write.
  - The docs ship both role definitions.
- `import` and `put` refuse files that are not config files, so credential JSONs can never land in the index. Secrets stay in Vault or the credential file.

### 8. Docs

- New `docs/opensearch_config.md`:
  - document model and versioning;
  - CLI reference;
  - migration steps;
  - roles;
  - a Dashboards index pattern with a saved search `latest:true and not deleted:true`;
  - the restart behaviour and its exit code;
  - a Swarm snippet: `CONFIG_BACKEND=opensearch`, `CONFIG_SERVICE=<dir>`, and no `configs:` mounts or `-d`.
- Pointers from `docs/configuration.md` and `docs/common_options.md`.

## Files

- **New:**
  - `src/maas_collector/rawdata/configprovider.py`
  - `src/maas_collector/rawdata/configstore.py`
  - `src/maas_collector/rawdata/cli/config.py`
  - `docs/opensearch_config.md`
  - `tests/fakeopensearch.py`
  - `tests/test_config_provider.py`
  - `tests/test_configstore.py`
  - `tests/test_config_refresh.py`
  - `tests/test_config_cli.py`
- **Modified:**
  - `configuration.py`: `load_json_dict`, and `register=` on `build_model_class`
  - `messenger.py`: `load_config_dict`
  - `filecollector.py`: provider, `CollectorArgs` fields, connection before `load_config`, `check_config_changes()` in `run()`
  - `monitorcollector.py`: second pass through the provider
  - `cli/lib/args.py`: new options
  - `docs/configuration.md` and `docs/common_options.md`

## Out of scope (possible follow-ups)

- **Two copies of the OData configs.** maas-cds `TRACK_DELETION` keeps reading `maas-config-collector`, which ocs-operator-backend writes, so the OData configs then live in two places. Follow-up: point TRACK_DELETION at this index, or have `ConfigStore` mirror the OData entries.
- In-process hot reload.
- `scripts/generate_interface.py` stays on files.
- The deployment changes: done in the deployment repository, one service at a time.
- An editing UI.

## Sequencing

1. Create branch `feature/opensearch-config` from `feature/vault-credentials`.
2. Copy this design to `docs/superpowers/specs/2026-10-12-opensearch-config-design.md` and commit it.
3. Invoke **superpowers:writing-plans** for the step-by-step implementation plan.
4. Implement test-first, in this order:
   1. the `load_json_dict` / `load_config_dict` refactor (existing tests stay green);
   2. the providers;
   3. `ConfigStore`;
   4. `check_config_changes`;
   5. the monitor;
   6. the CLI;
   7. the docs.

## Verification

- **Unit tests** (`pytest tests/`). They use a small in-memory OpenSearch fake that supports `index(op_type=create)` → 409, `get`, `update`, term-filter `search`/`scan` and sort. They follow the style of `tests/fakevault.py`.
  - **`FileConfigProvider`** gives the same configs as today on `tests/conf/collector.d` and `tests/conf/model_resolve` (names, models, amqp chunk sizes).
  - **`OpenSearchConfigProvider`:**
    - filters by service, `latest` and `deleted`;
    - keeps the highest version while two documents are both `latest`;
    - returns a stable order;
    - raises when no documents are found.
  - **`ConfigStore`:**
    - v1, then v2, flips `latest`;
    - two concurrent publishes on the same base: the second gets a conflict;
    - `base_version` mismatch, rollback, tombstone, history order;
    - validation rejects credential-shaped content and a bad extractor class.
  - **Import:**
    - deduplicates across services and merges `services`;
    - skips `tests/conf/test-maas-collector-credentials.json`;
    - reports drift and does not overwrite;
    - `export` after `import` reproduces the tree byte for byte, once both sides are parsed as JSON.
  - **Change check** (the `FakeClock` pattern from `test_credential_refresh.py`):
    - nothing happens before the period;
    - no change: no exit;
    - a valid change raises `SystemExit(75)` after the `finally` clean-up;
    - an invalid change is logged and the collector keeps running, and `MODEL_DICT` is untouched;
    - a provider error is logged and the collector keeps running.
  - **Monitor:** finds the probed interfaces' configs through the OpenSearch provider.
  - The existing suites still pass: `test_configuration`, `test_meta_conf`, `test_monitoring`, `test_backup_report_integration`, `test_messenger` and the credential tests.
- **Manual end-to-end** with `docker-compose-opensearch.yml`. Recent images may need `OPENSEARCH_INITIAL_ADMIN_PASSWORD` set.
  1. `config init`, then `config import <deploy-dir>/collector --dry-run`, then the same import for real. Check that 100 paths were written and the credential files were skipped.
  2. `config list --service odata`.
  3. Start the OData collector with `CONFIG_BACKEND=opensearch`, `CONFIG_SERVICE=odata`, `CONFIG_REFRESH_PERIOD=10` and `WATCH_PERIOD=5`.
  4. `config put` a changed `refresh_interval`. Check that the log shows `configuration changed … v1→v2` and that the process exits with code 75. After a restart it should load v2.
  5. Write an invalid version straight into the index. Check that the collector logs the rejection and keeps collecting.
  6. Run `config rollback`, then `config history`, and check that all three versions are listed with authors and comments.

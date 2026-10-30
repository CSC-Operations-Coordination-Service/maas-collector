# Backups, recollect configurations and the archive — cross-plan contracts

**Spec:** [2026-10-29-maas-operator-backups-recollect-dm-design.md](../specs/2026-10-29-maas-operator-backups-recollect-dm-design.md)

The plans of this feature are written in parallel, one per subsystem. This document fixes every
name, signature, message body, route and index field that crosses a plan or a repository
boundary. A plan uses these names exactly. When the code makes one of them impossible, the plan
says so in a "Contract deviation" note at its top instead of choosing another name.

| plan | file | repository | base |
| --- | --- | --- | --- |
| 1 | `2026-10-29-archive-1-collector-backups.md` | maas-collector | `feature/satu-xml-extractor` (from `origin/develop`, 3.13.0) |
| 2 | `2026-10-29-archive-2-collector-recollects.md` | maas-collector | plan 1's branch |
| 3 | `2026-10-29-archive-3-collector-archive.md` | maas-collector | plan 2's branch |
| 4 | `2026-10-29-archive-4-backend.md` | maas-operator-backend | `develop` (0.1.1) |
| 5 | `2026-10-29-archive-5-app.md` | maas-operator-app | `feature/operator-app` (0.3.1) |
| 6 | `2026-10-29-archive-6-deploy.md` | csc-ocs-ansible, download-manager, maas-cds | their current integration branches |

maas-collector paths are relative to `src/maas_collector/`. Plans 4 to 6 consume maas-collector
through a release (3.14.0) or a dev build of plans 1 to 3.

## 1. Configuration content (plan 1 defines, everyone reads)

- `CONFIG_KEYS` (in `rawdata/configprovider.py`) becomes `{"collectors", "models", "amqp", "backups"}`.
- A backup definition (one per file `backups/<name>.json`, content `{"backups": [definition]}`):

  | key | type | notes |
  | --- | --- | --- |
  | `name` | str | `[a-z0-9][a-z0-9-]*`, at most 64 characters |
  | `description` | str | optional |
  | `type` | str | `"BackupS3"` or `"BackupLocal"` |
  | `directory` | str | mandatory |
  | `calendar_tree` | bool | default `false` |
  | `enable_gzip` | bool | default `false` |
  | `report_model` | str or null | collector file backups only |
  | `buckets` | list[str] | S3 only, mandatory for S3 |
  | `s3_endpoint_url`, `s3_region`, `s3_signature_version` | str or null | S3 only |
  | `credentials` | str | S3 only, mandatory for S3, starts with `backup-` |

- **Destination keys**: `type`, `s3_endpoint_url`, `directory`, `calendar_tree`, `enable_gzip`, and
  a removed bucket.
- **Secret fields of a backup block**: `s3_access_key`, `s3_secret_key`. Refused in any backup block
  or definition.
- A collector entry's `backup` is a name (`str`) or an inline block (`dict`, without secret fields;
  an inline S3 block names `credentials`).
- A collector entry's `archive` (plan 3 reads, plan 1 lets it through validation):

  ```json
  {
    "backup": "<name>",
    "duplicates": "skip",
    "fields": {"product_id": "product_id", "product_name": "product_name", "size": "content_length",
               "checksum": "checksum", "checksum_algorithm": "MD5", "publication_date": "publication_date"},
    "download": {"url": null, "quote_id": true}
  }
  ```

  `duplicates` is `"skip"` (default) or `"replace_if_changed"`. `download.url` is a template with
  `{id}`; `null` means the collector's form `<odata_product_url>/odata/v1/Products('<id>')/$value`
  (`quote_id` decides the quotes around the id).
- An `interface_name` containing `@` is refused at validation.

## 2. maas-collector — plan 1 (named backups, safe validation, reports)

Module `backup/named.py`:

```python
BACKUP_NAME: re.Pattern               # [a-z0-9][a-z0-9-]*
MAX_NAME_LENGTH = 64
CREDENTIAL_PREFIX = "backup-"
SECRET_FIELDS = ("s3_access_key", "s3_secret_key")
DESTINATION_KEYS = ("type", "s3_endpoint_url", "directory", "calendar_tree", "enable_gzip", "buckets")

def backup_definitions(contents: Iterable[dict]) -> dict[str, dict]: ...
    # name -> definition, from the "backups" of every content; ValueError on a duplicate name
def check_backup_block(block: dict, where: str) -> None: ...
    # ValueError naming `where` for a secret field, a bad name, a bad credentials name
def resolve_backup(reference: str | dict, definitions: dict[str, dict]) -> tuple[dict, str | None]: ...
    # (block for instanciate_collector_backup, without name/description/credentials;
    #  name of the credential entry or None); KeyError for an unknown name
```

`rawdata/configprovider.py`:

```python
def validate_config_sources(sources, config_classes_for, resolve_models: bool = True,
                            evaluate: bool = True) -> None: ...
    # evaluate=False: no eval of any lambda or converter (see rawdata/extractor/static.py)
class ConfigIntegrityError(ConfigValidationError):
    problems: list[str]        # one human-readable line per dangling reference
class BackupDestinationError(ConfigValidationError): ...
```

Module `rawdata/extractor/static.py`:

```python
def check_lambda_source(code: str) -> None: ...      # ValueError unless a Lambda without defaults
def check_extractor_static(extractor_meta: dict) -> None: ...
    # class exists, argument names match its signature, every lambda/converter checked statically
```

`rawdata/configstore.py`: the constructor gains `evaluate: bool = True` and
`report_index: str = "raw-data-backup"`. With `evaluate=False`, `publish` and `rollback` validate
statically. `publish`, `rollback` and `delete` raise `ConfigIntegrityError` and
`BackupDestinationError` (A1, A2 of the spec).

`backup/backup.py`:

```python
OK, KO, REPLACED, SKIPPED, REQUESTED = "OK", "KO", "REPLACED", "SKIPPED", "REQUESTED"
# BackupReport gains (all with defaults): origin: str = "collector", backup_name: str = "",
# operation_id: str = "", product_id: str = "", product_name: str = "",
# written_md5: str = "", catalogue_checksum: str = ""
```

`rawdata/collector/filecollector.py`: `BACKUP_FAILED_EXIT_CODE` (a value distinct from
`REPLAY_FAILED_EXIT_CODE`), and the child's last line on that exit:
`backup failed: <count> files to <backup name>`.

`rawdata/collector/credentialmixin.py`: configurations gain `backup_credentials: dict` (field
metadata `sensitive`), filled from the entry named by the backup.

## 3. maas-collector — plan 2 (custom recollects, schema)

Module `rawdata/operator/overrides.py`:

```python
OVERRIDABLE_KEYS: dict[str, frozenset[str]] = {
    "ODataCollectorConfiguration": frozenset({"odata_query_filter", "download_file_pattern",
                                              "product_per_page", "interface_credentials"}),
    "JiraExtendedCollectorConfiguration": frozenset({"jql_query", "interface_credentials"}),
}
DUPLICATES = ("skip", "replace_if_changed")
class OverrideRefused(ValueError): ...
def merge_overrides(collector_class: str, live_entry: dict, overrides: dict) -> dict: ...
def check_odata_window(odata_filter: str, date_field: str) -> None: ...   # OverrideRefused
def check_jql_window(jql: str) -> None: ...                               # OverrideRefused
def check_archive_choice(collector_class: str, archive: dict | None) -> None: ...
```

Archive choice (request, record, saved configuration):
`{"backup": str, "backup_version": int | None, "duplicates": "skip" | "replace_if_changed", "force": bool}`.

`rawdata/operator/contract.py`:

```python
CUSTOM_SUFFIX = "@custom"
def replay_custom_routing_key(service: str, lane: str) -> str: ...  # maas.operator.<service>.replay-custom.<lane>
def replay_custom_queue_name(service: str, lane: str) -> str: ...   # maas-operator-<service>-replay-custom-<lane>
# ReplayRequest gains: overrides: dict | None = None, configuration: str = "",
#                      archive: dict | None = None
# property is_custom -> bool  (any of the three set)
# def routing_key(self, service: str) -> str   (plain or custom key for its lane)
# to_body(): when is_custom, "interface_name" is "<real>@custom" and "interface" is the real name;
#            "overrides", "configuration", "archive" are added; otherwise unchanged
```

Lanes: plain `short`, `long`; custom `custom-short`, `custom-long`; in single-lane mode (generic
worker setting `OPERATOR_SINGLE_LANE=true`) one lane named `single`, custom-capable.

**No service-specific code (user decision):** maas-collector code, tests and docs never name a
particular service or interface (no `cdse`, no CDSE download host). Which service runs single-lane,
which interfaces are archived and where their products are downloaded from are deployment
configuration (plan 6).

Module `rawdata/configschema.py`:

```python
SECRET_KEYS: frozenset[str]
def configuration_schema() -> dict: ...
```

Shape of `configuration_schema()`:

```json
{
  "version": "<maas-collector version>",
  "collector_classes": {
    "<ClassName>": {"replayable": true, "archivable": true,
      "keys": {"<key>": {"type": "string|integer|number|boolean|string_list|list|object|any",
                         "default": null, "required": false, "sensitive": false,
                         "overridable": false, "group": null, "description": null}}}
  },
  "extractor_classes": {"<ClassName>": {"args": {"<arg>": {"type": "...", "default": null,
                                                            "required": false, "description": null}}}},
  "backup_types": {"BackupS3": {"keys": {}}, "BackupLocal": {"keys": {}}},
  "archive_rule": {"keys": {}},
  "secret_keys": ["..."],
  "window_rules": {"odata": {"date_field_key": "filter_date_v4",
                             "placeholders": ["publication_start_date", "publication_end_date"]},
                   "jira": {"clause": "updated > {date_criteria}"}}
}
```

## 4. maas-collector — plan 3 (the archive)

Package `rawdata/transfer/`:

```python
# contract.py
TRANSFER_EXCHANGE = "maas-transfer-exchange"
def transfer_routing_key(service: str) -> str: ...   # maas.transfer.<service>
def transfer_queue_name(service: str) -> str: ...    # maas-transfer-<service>
@dataclass(frozen=True)
class TransferRequest:
    service: str
    interface_name: str
    documents: tuple[tuple[str, str], ...]   # (document id, index)
    backup: str
    backup_version: int | None
    duplicates: str                          # "skip" | "replace_if_changed"
    force: bool = False
    operation_id: str = ""
    requested_by: str = ""
    origin: str = "live"                     # "live" | "recollect" | "download"
    def to_body(self) -> dict: ...
    @classmethod
    def from_body(cls, body) -> "TransferRequest": ...   # ValueError on a bad body

# rule.py
@dataclass(frozen=True)
class ArchiveRule:            # from a collector entry's "archive"
    backup: str
    duplicates: str
    fields: dict[str, str]
    download_url: str | None
    quote_id: bool
    @classmethod
    def from_entry(cls, entry: dict) -> "ArchiveRule | None": ...

# reports.py
def archived_state(client, backup: str, interface: str, product_name: str) -> dict[str, dict]: ...
    # destination -> last archive report (OK, REPLACED or KO), for the duplicate check
def requested_reports(request: TransferRequest, products: list[dict]) -> list: ...
    # BackupReport objects with status REQUESTED, origin "archive"

# conflicts.py
CONFLICT_INDEX = "maas-archive-conflict"
def conflict_id(backup: str, interface: str, product_name: str) -> str: ...

# gaps.py
def find_gaps(client, entry: dict, backup: str, start, end, delay) -> list[dict]: ...
    # [{"product_id", "product_name", "document_index", "indexed_at"}]
```

CLI: `python -m maas_collector.rawdata.cli.transfer` (configuration and credentials as the
collectors: `CONFIG_BACKEND`, `CONFIG_SERVICE`, `CREDENTIAL_BACKEND`).

`maas-archive-conflict` document (id `conflict_id(...)`): `backup_name`, `interface_name`,
`product_name`, `product_id`, `document_index`, `status` (`OPEN` | `RESOLVED`), `archived_md5`,
`archived_at`, `catalogue_checksum`, `publication_date`, `first_seen`, `last_seen`.

## 5. Backend API — plan 4 defines, plan 5 consumes

All under the existing `/api/v1` prefix; admin profile unless said otherwise.

| route | body / query | response |
| --- | --- | --- |
| `GET /capabilities` (both) | | `features` gains `custom_recollect`, `downloads`, `backups` (false on public); `limits` gains `download_max_products` (100) |
| `GET /schema` | | `configuration_schema()` plus `"maas_collector_version"` |
| `POST /configs/validate` | `{path, content}` | `{ok: bool, error: str \| null}` |
| `DELETE /configs/{path}` | `{comment, base_version}` | the tombstone document |
| `GET /backups` | | `[{name, type, description, destination: {endpoint, buckets, directory}, services, version, author, created_at, credentials, credential_status: {service: "present" \| "missing"}, usage: {entries: [str], archive_rules: [str], recollect_configs: [str]}}]` |
| `GET /recollect-configs` | `?interface=` | `[{name, interface_name, overrides, archive, description, version, author, created_at, live_path, live_version, stale: {live_version_now, changed_keys: [str]}}]` |
| `GET /recollect-configs/{name}` | | one of the above |
| `GET /recollect-configs/{name}/history`, `/versions/{version}` | | as the configuration routes |
| `PUT /recollect-configs/{name}` | `{interface_name, overrides, archive, description, comment, base_version}` | the written version |
| `DELETE /recollect-configs/{name}` | `{comment, base_version}` | the tombstone |
| `POST /actions/recollect` | adds `configuration: {name, version?}`, `overrides: {}`, `archive: {backup, duplicates, force}` | adds `applied: {configuration: str \| null, overridden_keys: [str], archive: {backup, backup_version, duplicates, force} \| null}` |
| `POST /actions/download` | `{interface_name, products: [str], backup?, duplicates?, force, reason, dry_run}` | dry run: `{plan: [{product, outcome: "transfer" \| "skip" \| "conflict" \| "not_found", archived_md5, catalogue_checksum}]}`; run: `{operation_id}` |
| `GET /archive/gaps` | `?interface=&backup=&start=&end=` | `[{product_id, product_name, document_index, indexed_at}]` |
| `GET /operations/{operation_id}/transfers` | | `{requested, transferred, replaced, skipped, conflicts, failed, pending}` |

The app's `plugin.json` gains a DELETE proxy route for `admin/api/*`.

`maas-operation` new fields (all scalar): `configuration_name`, `configuration_version`,
`overrides` (JSON string, not indexed), `backup_name`, `backup_version`, `duplicates`, `force`,
`document_index`, `product_count`. New `operation_type` values: `RECOLLECT_CONFIG_SAVE`,
`RECOLLECT_CONFIG_DELETE`, `DOWNLOAD`.

`maas-recollect-config` documents: `name`, `version`, `latest`, `deleted`, `interface_name`,
`overrides` (JSON string, not indexed), `archive` (JSON string, not indexed), `description`,
`author`, `comment`, `created_at`, `based_on`, `live_path`, `live_version`.

Backend settings: `ADMIN_ONLY_SERVICES` (default empty; the deployment sets it), `SERVICE_COLLECTOR_CLASSES` (default:
taken from the service's entries that name a class), `ARCHIVE_GAP_DELAY_HOURS` (default 6).

## 6. Index templates and where they live (plan 6, plan 4)

| index | template owner |
| --- | --- |
| `maas-operation`, `maas-recollect-config`, `maas-archive-conflict` | maas-operator-backend `resources/templates/api/`, installed by `init-indices` |
| `raw-data-backup`, `raw-data-dm-cdse-product` | maas-cds `resources/templates/`, next to the other raw-data templates |

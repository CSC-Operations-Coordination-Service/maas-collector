# MAAS operator: named backups, recollect configurations, the continuous archive — design

Extends [the MAAS operator design](2026-10-16-maas-operator-design.md). Repositories:
maas-collector, maas-operator-backend, maas-operator-app, csc-ocs-ansible; the
`download-manager` repository is retired by this work.

Code references are to the versions in production or about to be: maas-collector **3.13.0**
(`origin/develop`, the worktree `maas-collector-satu`; its operator worker runs on maas-engine
3.2.0's generic replay worker, `rawdata/operator/handler.py`), maas-operator-backend 0.1.1,
maas-operator-app 0.3.1. The `maas-collector` checkout on `feature/telemetry` has diverged and
is not a reference. Two review rounds (62 and 60 verified findings) are folded in.

## Context

maas-operator (deployed in production on 2026-10-23) lets operators recollect an odata or jira
interface over a window, check a product, edit the collector configuration stored in
`maas-collector-config`, and write the collectors' credentials in OpenBao. A recollect runs on
the interface's live configuration: `CollectorReplayHandler.prepare` copies the service's current
files from the index and runs the collector in replay mode on that copy. No replay has been run
in production yet.

Three needs do not fit.

1. **A recollect with another query.** The Satellite-Unavailability fix of 2026-10-29 needs the
   S1 REP__SUP reports published since their format changed collected again. They come from the
   `AUXIP_Exprivia` odata interface; replayed as it is, that interface lists every AUXIP product of
   the window and downloads every MPL_TIMELINE file too. What is wanted is the same interface with
   a narrower query, for that recollect only:

   ```json
   {
     "odata_query_filter": "PublicationDate ge {publication_start_date} and PublicationDate le {publication_end_date} and contains(Name,'REP__SUP')",
     "download_file_pattern": ["*OPER_REP__SUP__*.EOF*"]
   }
   ```

2. **Backups that people share.** A collector backup is an inline block on each entry
   (`docs/backup.md`) with its S3 keys inline (`backup/s3_backup.py`). Nothing names a backup, so
   nobody can say "archive it in X", extend X, or review who changed X. No collector backs anything
   up today.

3. **Archiving products.** The `download-manager` repository (dev only, its operator code
   uncommitted) watches the CDSE catalogue through a metadata-only collector, then archives each
   product once to one S3 target per environment: an engine downloads it (streamed, size and MD5
   checked, retried), uploads it, and records the outcome in `raw-data-backup`; operators
   redownload, force a re-transfer or recollect from `maas-dm-app`.

**A security issue found on the way, already in production.** Every configuration write of the
backend (`configs.py`, `ConfigStore.publish`) validates the content by building its extractors,
which evaluates their `python` lambdas (`extractor/base.py`, `eval`) inside `backend-admin`. That
process holds the OpenBao token that reads every collector credential, and the `operator` role
(Grafana Editors) holds `config_write`. Part B1's static validation removes it; Rollout 0 asks for
a mitigation until then.

## User decisions

From the requests and answers of 2026-10-29:

- Recollect with a custom configuration, such as a specific query; start from an existing
  configuration, create a new one from it or use it as is; name it.
- Several named backups, shared, extendable ("eases storage management and collaborative work");
  create one on a filesystem or on S3 from maas-operator.
- Example operation: recollect a specific period with a specific query into a specific backup.
- A complete maas-collector configuration builder.
- Named backups are used by collector entries and by recollects.
- A filesystem backup is a path in the container; mounting it is the deployment's business and out
  of scope for now.
- Products are downloaded from maas-operator; the download manager has no reason to exist any
  more.
- The archive is **continuous and in two stages**: collect, then transfer each product into the
  wanted backup, after checking that it is not already there. Another service consumes the backup.
- **No duplicate by default**; sometimes a **forced** transfer, because content may change.
- **The duplicate rule is configurable**: keep the archived product when the name is the same, or
  replace it when its checksum changed. A **dashboard** shows products archived under the same name
  with another checksum, from which the operator forces the transfer by hand.
- How consumers will consume the archive is not known: keep a note on how to trigger something
  external; do not implement it.
- The first end-to-end run is in production: a one-day recollect, then the full window.

## Goals / non-goals / anti-goals

Goals

- Named backups (S3 or filesystem): created, extended, derived, reviewed and rolled back from the
  app; S3 keys in OpenBao, written but never read back; referenced by name by collector entries,
  archive rules and recollects.
- A recollect may use a modified query of its interface (saved under a name, or used once) and
  archive every product it finds into a chosen backup.
- A continuous archive: every new product of an archived interface lands once in its backup,
  replaced only by rule or by force; conflicts visible and forced by hand; products never archived
  visible too.
- Downloads from maas-operator: by product list or by period and query, into a chosen backup.
- A configuration builder: every part of a collector configuration file is edited through forms
  generated from maas-collector itself.
- Every action records who, when, why, with which configuration and backup, and how it ended,
  transfers included.
- The SATU recollect works.

Non-goals (v1)

- Archiving products of non-odata interfaces.
- Mounting filesystem backups in the containers; `BackupSFTP` (not implemented in maas-collector).
- Changing anything but the query, the downloads, the page size and the credential entry in a
  recollect configuration (B1).
- Running an extractor on a sample file from the UI.
- Custom recollects, backups, downloads and archive actions on the public deployment.
- Notifying the archive's consumers (C8 keeps the note).
- Migrating `dm-download` or `dm-operation` (dev-only history).
- docker-jobs using recollect configurations, backups or archive rules.

Anti-goals (a failure even if it works)

- An S3 key, or any secret field of a collector entry, readable back: in a configuration, a record,
  the API, the UI or a log.
- Operator-supplied code (the `python` lambdas of extractors) evaluated by the backend or by the
  worker's parent process.
- A recollect configuration loaded by a live collector (it is stored outside
  `maas-collector-config`).
- A recollect that writes what the live interface would not (another index, other document ids,
  another interface name, another routing or exchange) or that sends credentials to another host.
- A custom recollect run by a worker that does not understand it, as a live replay.
- An unbounded replay: a custom query that escapes the replay window.
- A duplicate in a backup, unless the rule or a force says so; mirrors of one backup that diverge.
- An operation reported done when products it had to transfer were not, or a product the archive
  should hold that nothing shows as missing.
- A configuration write that leaves a collector service unable to load.
- The builder losing a key it does not know.

## Part A — named backups (maas-collector, backend, app)

### A1. What a named backup is

- A configuration file may hold a new top-level key, `backups`: a list of backup definitions.
  `is_config_content` accepts it next to `collectors`, `models` and `amqp`.
- **One file per backup** (my call): `backups/<name>.json`, content `{"backups": [{...}]}`, tagged
  with every service that may use it. Each backup has its own versions, history, review and
  rollback, and two people editing two backups never conflict.
- A definition: `name` (`[a-z0-9][a-z0-9-]*`, at most 64 characters, unique among the files a
  service loads), `description`, `type` (`BackupS3` or `BackupLocal`), `directory`,
  `calendar_tree`, `enable_gzip`, `report_model` (collector file backups only, A3); for S3 also
  `buckets`, `s3_endpoint_url`, `s3_region`, `s3_signature_version`, and `credentials`: the name of
  a credential entry, which must start with `backup-`.
- **Several buckets are mirrors**: an S3 backup writes every file to every bucket. Adding a bucket
  duplicates every new write; it does not add capacity.
- **Extend** = a new version of the file: description, report model, an added bucket.
  **Destination changes** (type, endpoint, directory, removed bucket, `calendar_tree`,
  `enable_gzip`) are refused once the backup has reports: derive a new backup instead (my call).
  The guard lives in `ConfigStore` (publish, rollback, delete then re-create), so `cli.config` and
  the backend share it.
- **Derive** = duplicate under a new name.
- **Who**: operators (`config_write`) create filesystem backups and edit the description of any
  backup. Creating or deleting an S3 backup, or changing its destination, `credentials`, `buckets`,
  `s3_region` or `s3_signature_version`, needs `credential_write` (admin role): otherwise an
  operator could aim the keys an admin wrote at another endpoint or bucket (my call, made stricter
  while planning: an added bucket also aims those keys elsewhere).
- **Filesystem**: `directory` is a path in the container (user decision). `BackupLocal` changes:
  it creates folders below `directory` but no longer `directory` itself; when it does not exist,
  the backup fails (KO), so an unmounted path does not silently fill the container's own disk.

### A2. Referencing and building a backup

- A collector entry's `backup` may be a name: `"backup": "<name>"`. An archive rule names one too
  (C1). Names are resolved at load, for every entry (enabled or not), against the `backups` of the
  files the service loads; an unknown name is a load error. Inline blocks stay accepted.
- **No secret in any backup block**, named or inline: `s3_access_key` and `s3_secret_key` are
  refused by `validate_config_sources` and by the loader itself (collector load, worker, transfer
  stage), since validation does not run at load. An inline S3 block names a `credentials` entry
  too. Local files with inline keys fail at load; `docs/backup.md` changes accordingly.
- **Where the keys live at run time**: resolving a name sets the entry's backup to the definition
  without `name`, `description` and `credentials`. `CredentialMixin` adds the backups' credential
  entries to its credential names, and copies only their `s3_access_key` and `s3_secret_key` into a
  separate field of the configuration marked `sensitive` (`backup_credentials`), refreshed on
  rotation like interface credentials. They are merged into a copy only when the backup object is
  built. An entry holding any other field fails the backup at load, naming the field.
  `CollectorBackupS3Configuration` marks both keys sensitive and masks them in its repr.
- **One resolution function**, in a module that does not import the extractors, used by the file
  collector and by the transfer stage (C3).
- **Reference integrity**, in `ConfigStore` for every write (publish, rollback, delete): refused
  when, with the write applied, an entry or archive rule of an affected service (enabled or not)
  names a backup the service does not load, or a backup names a `report_model` the service does not
  define. This covers deleting a backup, untagging a service, renaming, rolling back to an older
  version or a tombstone.

### A3. Reports

- Two writers, one index (`raw-data-backup`), told apart by `origin`:
  - **collector** file backups (`backup`), written when the backup names a `report_model`;
  - the **archive** (C3), always, whatever `report_model` says.
- Fields added to `BackupReport`: `origin`, `backup_name`, `operation_id`, `interface_name`,
  `product_id`, `product_name`, the MD5 written, the catalogue checksum. The backup sinks receive the
  backup's name to put in their reports.
- Statuses: `OK`, `KO`; for the archive also `REPLACED`, `SKIPPED` and `REQUESTED` (C5).
  Conflicts are not reports: they live in `maas-archive-conflict` (C4).
- **Already archived** (the archive's duplicate check): the last archive report of
  `(backup, interface, product name, destination)` has status `OK` or `REPLACED`.
- An entry may not use the same backup as its file `backup` and in its `archive` rule (refused at
  validation): the two would write each other's objects.
- The `raw-data-backup` template carries those fields and moves next to the other raw-data templates
  (left to the builder where); it is installed in production before the first report (Rollout).

### A4. Backup failures end a replay in failure

In replay mode, the collector counts every file whose collector backup did not end with OK reports
for all its destinations, the "Cannot backup" path included. When the count is not zero it exits
with a new exit code after collecting everything it could, its last line giving the count and the
backup. `CollectorReplayHandler.describe_failure` turns that into `N files could not be backed up to
<backup>`. Live mode is unchanged.

### A5. Backend

- Backups are configuration files: the existing routes (`GET/PUT /configs/{path}`, history, diff,
  versions, rollback) manage them. The backend checks A1 (name matches path, who may change what)
  and maps `ConfigStore`'s refusals to 409, listing the entries, archive rules and recollect
  configurations concerned. It applies the same integrity check to recollect configurations.
- `GET /backups`: every backup (latest, not deleted) with type, destination, services, version,
  author, date, credential status per service (present or missing, never values) and usage.
- **Deletion** of a configuration file is a new route, with its proxy route in the app's
  `plugin.json`.
- **S3 keys**: written through the Credentials route and page (`credential_write`). The credential
  usage scan learns the entries named by backups: an entry is written under
  `…/maas-collector/<service>/<entry>` for every service the backup is tagged with. A Credentials
  PUT on an entry named only by backups accepts only `s3_access_key` and `s3_secret_key`. When a
  backup loses a service, its key copy under that service is deleted (the admin policy gains
  `delete` on `…/maas-collector/*/backup-*`).
- Tagging a backup with a service makes its keys readable by every identity of that service
  (`odata` includes the docker-jobs `maas-jobs` AppRole); the Backups page says so.
- `GET /configs/...` masks secret keys (A2, B1) in what it returns; a PUT that sends a masked value
  back keeps the stored one.

### A6. App

A **Backups** page (admin profile): the list of `GET /backups`; New backup (filesystem, or S3 for
the admin role, a form from the schema); open a backup (its form, history, diff, rollback); Duplicate
as (derive); Delete (refused while used, the page shows by what); Write keys (the Credentials page
for its entry, admin role).

## Part B — recollect configurations and the builder

### B1. What a recollect configuration is

- It belongs to one interface, its **base**, and holds **overrides** of that interface's live
  entry. (My call: overrides rather than a frozen copy, so a saved configuration follows the live
  entry for every key it does not override.)
- **Only these keys may be overridden** (my call: an allowlist, which the schema marks
  `overridable`):
  - odata: `odata_query_filter`, `download_file_pattern`, `product_per_page` (1 to the live value or
    1000, whichever is higher), `interface_credentials` (an entry that exists for the service);
  - jira: `jql_query`, `interface_credentials`.

  Every other key stays live: identity, model, routing, exchange, extractor, URLs and hosts,
  `custom_query_suffix`, offsets, backups. A recollect therefore writes the same documents to the
  same places, and sends credentials only to the live host.
- It may name an **archive choice** (C5): backup, duplicate rule, force (odata only).
- **The window stays**, checked on the structure:
  - `odata_query_filter`: the top-level `and` terms must include
    `<date field> ge {publication_start_date}` and `<date field> le {publication_end_date}`, the
    date field being the entry's (`filter_date_v4`, `PublicationDate` by default); no `or` at
    parenthesis depth 0 outside quoted strings.
  - `jql_query`: the clause `updated > {date_criteria}` in a top-level `AND` term, and no `OR` at
    depth 0 outside quotes before any `ORDER BY`.
- **Name**: unique, `[a-z0-9][a-z0-9-]*`, at most 64 characters; a description.
- **Versions**, with `ConfigStore`'s rules (my call): `<name>@<version>`, one `latest`, author,
  mandatory comment, `created_at`, `based_on`, tombstone, history, 409 on a stale save. Each version
  records the live file (path, version) it was checked against.
- **Store**: a backend index, `maas-recollect-config` (my call; the `maas-` prefix puts it under the
  snapshot policies). `overrides` stored as a JSON string in a field neither indexed nor aggregated.
- **Use once**: a recollect may carry overrides inline without saving them.
- **Validation never evaluates**: a new static mode of `validate_config_sources` (maas-collector),
  used by `ConfigStore.publish` and `rollback` when the backend calls them, by
  `POST /configs/validate`, by recollect-configuration saves, by dry runs, and by the worker's
  `prepare`. Each `python` lambda and converter is parsed (`ast.parse(..., mode="eval")`, a `Lambda`
  without default arguments) and never evaluated; an extractor's class and argument names are
  checked against its signature without building it. Lambdas are evaluated only where they run:
  in the collectors and in the replay child.

The first saved configuration: `auxip-exprivia-satu-only`, base `AUXIP_Exprivia`, with the
overrides of the Context.

### B2. A recollect with a configuration

- `POST /actions/recollect` gains, all optional: `configuration` (`{name, version?}`) or inline
  `overrides`, and `archive` (C5). An archive choice on a non-odata interface is refused (422).
- The dry run returns the merged entry's changes against live, the archive choice resolved, and the
  validation outcome; the response echoes what the backend applied, and the app refuses to confirm
  when its choice is missing from the echo (a new app against an older backend would otherwise run
  a live recollect).
- `GET /capabilities` gains `custom_recollect` and `downloads` features; the app shows the custom
  controls only when they are on (both off on the public profile).
- The record (`maas-operation`) gets `configuration_name`, `configuration_version`, `overrides`
  (JSON string, not indexed), `backup_name`, `backup_version`, `duplicates`, `force`.
- The public profile refuses custom recollects, archive choices and downloads (403); its
  `/operations` lists only plain recollects and product checks, without configuration, backup or
  archive fields.

### B3. maas-collector

a. **Custom lanes.** `cli/operator_worker.py` declares, next to each lane, a custom lane: its own
   queue (`maas-operator-<service>-replay-custom-<lane>`) bound only to
   `maas.operator.<service>.replay-custom.<lane>`. maas-engine passes the lane's name to
   `prepare`; the handler applies `overrides` and `archive` only on a custom lane and refuses them on
   a plain one (`ReplayNotPossible`). A custom lane shares its plain lane's slot, so a worker runs at
   most one short and one long replay at a time, as today. The backend publishes custom requests
   with the custom routing key (mandatory: a worker without custom lanes binds no such key, so the
   publish fails with 503).

b. **Fail closed.** A custom request's `interface_name` is `<interface>@custom`, the real name in
   the request's extra fields; `@` is refused in interface names at validation. A worker that does
   not know custom requests records FAILED "not found" instead of replaying the live configuration.
   The record keeps the real name.

c. **Prepare**, on a custom lane, after `sync_configs`: refuse a key outside the allowlist, a window
   that escapes, an unknown backup, an archive choice on a non-odata entry (`ReplayNotPossible`);
   apply the overrides to the interface's entry in the copy; write the archive choice into its
   `archive` (backup, `backup_version`, duplicates, force); pass the operation id (the replay
   suffix, as today) and the requester to the child; validate the copied files statically (B1). The
   live entry is resolved at run time (a saved configuration follows live by design); the backup is
   the version the request names, refused when gone or no longer tagged with the service.

d. **Configuration schema** (for the builder), a new module returning for the installed version:
   - every collector entry class with its JSON keys: type, default, required, `sensitive`,
     `overridable`, description and group when written; whether it can be replayed or archived;
   - which class an entry builds with: the **collector class of its service**, not its `class` key
     (entries without `class`, such as the Satellite-Unavailability and MP_* sub-entries, build as
     the service's odata class); the backend keeps a service-to-class map, by default taken from the
     service's entries that name a class;
   - the shape of `extractor`, `{class, args?, converter_map?, …}` (production uses an
     extractor-level `converter_map`), each extractor class with its constructor arguments;
   - `model` as a name or an inline model definition;
   - the shapes of `models`, `amqp`, `backups` and the archive rule;
   - the **secret keys**: every field marked `sensitive` plus an explicit list of credential fields
     that are not marked (for example `oauth_basic_credential`), with a test that fails on any
     field whose name looks like a credential (password, secret, token, credential, key) and is in
     neither;
   - the window rules.

   Keys and types come from the dataclasses and signatures; descriptions and groups from a catalog
   in the same module, written first for the replayable classes, the file entries, backups and
   archive rules. A test checks that every key used by the configurations under `tests/conf` (plus a
   fixture with an extractor-level `converter_map`) is described.

### B4. Backend

- `maas-recollect-config` routes, admin profile: list (`?interface=`, with staleness: live file
  version now against the recorded one, and the overridden keys whose live value changed since),
  get, history, versions, `PUT /recollect-configs/{name}` (checks of B1; records
  `RECOLLECT_CONFIG_SAVE`), delete as a tombstone (records `RECOLLECT_CONFIG_DELETE`). Verbs (my
  call): `read` to look, `recollect` to save or delete.
- `GET /schema` (admin): the schema of B3d and the maas-collector version the backend runs.
- `POST /configs/validate` `{path, content}` → `{ok, error}`: the static validation plus
  `ConfigStore`'s checks, without writing (admin).
- Publishing a live configuration refuses a secret key whose value is new or changed (files that
  already hold one can still be republished).

### B5. App — the builder and the Recollect page

a. **The builder**, driven by `GET /schema`, edits every part of a configuration file:
   - collector entries (`EntryForm`): groups (identity, remote and query, download, extraction,
     backup, archive, monitoring, other), typed inputs, defaults as placeholders, descriptions as
     help; `extractor` as a class select then its arguments, `attr_map` and `converter_map` as table
     editors (field, kind: path, root path, attribute, python; value), other object arguments as
     JSON; `backup` and `archive.backup` as selects of named backups; `interface_credentials` as a
     select of credential entries; secret keys never shown, only "set" or "absent" with a link to the
     Credentials page; unknown keys kept and editable as JSON, inside `extractor` too; a JSON tab on
     the whole entry;
   - `models` (a table of fields with type selects, partition settings), `amqp`, `backups`;
   - **variant mode** for recollect configurations: starts from the live entry or a saved
     configuration, only overridable keys editable, changed keys marked "overridden" with "back to
     live"; its result is the overrides.
b. **Configuration page**: entries and models open in the builder (the JSON modal stays as "Edit as
   JSON"); "Duplicate as" starts a new entry from an existing one, with a new `interface_name`.
c. **Recollect page**: interface; configuration (Live, a saved one of that interface, or Custom
   opening the builder in variant mode, then "Save as" or "Use for this recollect only"); archive
   choice (C5); window; reason; check; confirm.
d. **Recollect configurations**: a tab of the Recollect page listing name, interface, overridden
   keys, archive choice, version, author, date, a "live changed since" marker; open, edit, history,
   delete.
e. **Operations page**: a recollect shows its configuration `name@version`, overrides and archive
   choice, and its transfer progress (C5); an operation still PUBLISHED after its lane's timeout is
   flagged "not picked up".

## Part C — the continuous archive

### C1. Archive rules (maas-collector)

- An odata collector entry may declare `archive`:

  ```json
  {
    "backup": "<name>",
    "duplicates": "skip",
    "fields": {"product_id": "product_id", "product_name": "product_name",
               "size": "content_length", "checksum": "checksum",
               "checksum_algorithm": "MD5", "publication_date": "publication_date"},
    "download": {"url": "<template with {id}>", "quote_id": true}
  }
  ```

  Every product indexed through that entry is then archived into that backup by the transfer stage.
- `duplicates` (user decision: configurable), default `skip`:
  - `skip`: a product already archived (A3) is not transferred again. When its checksum differs
    from the archived one, a **conflict** is recorded (C4).
  - `replace_if_changed`: same checksum → skipped; another checksum → transferred again,
    **replacing** the archived object.
  - A **forced** transfer always transfers and replaces.
  - A product without a checksum can only be compared by name: `replace_if_changed` behaves like
    `skip`, and no conflict can be detected.
- `fields`: where the raw document holds the product's id, name, size, checksum (and its algorithm)
  and publication date; the defaults above. The CDSE model's checksum is `origin_checksum_md5`.
- `download`: how to fetch the product's bytes. Default: the collector's own form,
  `<odata_product_url>/odata/v1/Products('<id>')/$value`. CDSE serves bytes from another host
  (`https://download.dataspace.copernicus.eu/odata/v1/Products(<id>)/$value`, id unquoted); the
  transfer stage never follows a redirect while sending a bearer token (requests drops the header
  on a cross-host redirect).
- **Stable object keys**: an archived product is written at
  `<directory>/<interface_name>/[YYYY/MM/DD/]<product file name>`, the date being the product's
  publication date. The backup sinks gain an optional date and an explicit key for this (today they
  file by the transfer day). A replacement therefore overwrites the object the consumers read.
- **Mirrors stay equal**: under `skip`, a destination still missing a product is filled only when
  the product's checksum matches the archived one; otherwise it is a conflict.

### C2. Transfer requests

- After a run indexes documents through an entry with `archive`, the collector publishes **transfer
  requests**:
  - **live**: for the documents created or updated;
  - **in replay** (an operation id is present): for **every document the run extracted**, created,
    updated or unchanged, because a recollect over products already indexed must still archive
    them. These requests go on the transfer path only; the ordinary product messages are not sent
    again (no `-f`, no `--force-message`). The collector keeps the stored index of unchanged
    documents (`ActionIterator` holds it) so the request names `(id, index)` pairs.
- A request carries the documents' ids and index (several per request), the interface and service,
  `backup`, `backup_version`, `duplicates`, `force` (false unless set), `operation_id` (empty for the
  live flow), `requested_by`, `origin` (`live`, `recollect`, `download`). An explicit contract in
  maas-collector, not the ordinary product message, whose `force` defaults to true.
- Exchange `maas-transfer-exchange` (topic, durable), routing key `maas.transfer.<service>`, queue
  `maas-transfer-<service>`. Published mandatory and confirmed: a failed publish fails a replay
  (FAILED) and is retried by the live collector at its next loop.
- For an operation, the publisher (the collector in replay, the backend for a download) writes one
  `REQUESTED` archive report per product when it publishes, so the operation knows what it sent.
- The maas-operator backend publishes the same requests for a download by product list (C5).

### C3. The transfer stage (maas-collector)

A new service, `python -m maas_collector.rawdata.cli.transfer`, one per collector service, reading
configurations from the index and credentials from OpenBao like the collectors, its concurrency set
by its replicas (one product at a time per replica; for CDSE, within the account's rate limit). It
resolves each product's entry and archive rule from the live configuration (URLs and credentials are
never overridden, B1), and the backup at the version the request names (latest for the live flow).
For each product:

1. Read the raw document from the index the request names.
2. Unless forced, apply `duplicates` (C1) against the archive reports (A3). An operation's skips are
   reported (`SKIPPED`), live skips are not.
3. Download the product, streamed in chunks to a working directory made with `mkdtemp` (never built
   from the product's id or name), size and checksum checked against the document; retried with
   backoff on transient errors.
4. Upload it to every destination still missing it (all of them when forced or replacing), retried
   per destination.
5. Write one report per destination (`OK`, `REPLACED` or `KO`), with the A3 fields.
6. Bounded retries everywhere: a permanent error (4xx, a checksum mismatch that persists) ends as a
   `KO` report and the request is acknowledged; nothing requeues forever.

The transfer stage is the "engine" of the standalone system (Part D): the second stage of the
archive, shipped in the maas-collector image because it needs the collector's configurations,
credentials, authentication and backups.

The download manager's engine (`transfer/source.py`, `transfer/runner.py`,
`engines/reports/transfer.py`) is the reference, not code to move as it is: it is built on the
maas-engine engine base, reads `dm-download` documents and has a CDSE-specific source and OAuth. The
collector's own download (`odata_download_product`) is unchanged: it reads a file into memory and
suits the small files it extracts.

### C4. Conflicts, gaps and the dashboard

- A **conflict**: a product archived in a backup, seen again under the same name with another
  checksum, under `skip` (or a mirror that cannot be filled, C1). The transfer stage keeps one
  document per `(backup, interface, product name)` in a new index, `maas-archive-conflict` (my
  call): status `OPEN` or `RESOLVED`, the new product's id and raw index, archived MD5 and date, new
  checksum and publication date, first and last seen. A later `OK` or `REPLACED` transfer of that
  product into that backup sets it `RESOLVED`.
- A **gap**: a product of an archived interface indexed more than a set delay ago (default 6 hours)
  with no archive report in its backup: a lost request, a stage down. The backend lists them for a
  window (`GET /archive/gaps`).
- An **Archive dashboard** in the monitoring Grafana: transfers per backup and interface over time
  (OK, REPLACED, KO, SKIPPED), failed transfers, the **open conflicts** and the gap count, each with
  a link to the maas-operator Download page, prefilled (product, backup, `force` for a conflict).

### C5. Downloading from maas-operator

- **By product list**: a **Download** page (admin profile): interface, products (ids or names, at
  most 100, or `?products=`), backup (default: the entry's archive backup), duplicate rule (default:
  the rule's), force. The plan per product: transfer, skip (already archived, with both checksums),
  conflict (replaced only when forced), or not found in the interface's raw index. Run: one
  `DOWNLOAD` operation, its requests carrying its id.
- **Missing in a window**: the same page lists the gaps of an interface and a window (C4) and sends
  them as one `DOWNLOAD` operation.
- **By period and query**: a recollect (Part B) with an **archive choice**: backup, duplicate rule,
  force. Every product the replay finds goes to the transfer stage with the recollect's operation id
  (C2). A recollect of an archived interface without that choice sends its products to the rule's
  backup with the rule's settings.
- **Progress and outcome**: an operation's transfers are counted from the reports carrying its id:
  requested, transferred, replaced, skipped, conflicts, failed, and pending (requested without an
  outcome). A recollect's record says DONE when its replay ended; the Operations page shows its
  transfer progress beside it, and "complete" only when nothing is pending. A download is complete
  when every product has an outcome.
- Verbs (my call): `recollect` for downloads, archive choices and forced transfers.

### C6. The continuous archive in production

- The CDSE collector file (`odata/maas-collector-odata-cdse-s1-ocn.json`, interface `CDSE_S1_OCN`)
  moves into `maas-collector-config` under its own service, **`cdse`** (my call on the name; tagging
  it `odata` would make the live odata collectors load it and fight over its journal):
  - without its top-level `_comment*` keys (they fail `is_config_content`; their text goes into the
    publish comment);
  - without `exchange_name` (`dm-collect-exchange`): with the download manager retired, nothing
    consumes its ordinary product messages; its transfer requests replace them;
  - with `archive` set to the production archive backup, and the CDSE download host (C1).
- Its CDSE account moves into OpenBao under `…/maas-collector/cdse/`. New AppRole and policy
  `maas-collector-cdse` (its service prefix plus the shared connection paths), for its collector, its
  transfer stage and its operator worker.
- **A stack of its own, `maas-archive`** (my call): the `cdse` collector and the transfer stages
  (`cdse`, and `odata` for archives of AUXIP and other odata interfaces), reading the index and
  OpenBao like the maas-operator workers. It is deployable without waiting for the maintenance window
  that will move the `maas-collector-cds` collectors onto the index.
- **`worker-cdse`** in the maas-operator stack runs one replay at a time (the CDSE rate limit)
  through a **generic** worker setting, `OPERATOR_SINGLE_LANE=true`: exactly one maas-engine `Lane`
  (one thread, one child), with the long lane's per-day timeout; `cli/operator_worker.py` binds the
  other lane's key and the custom keys to its queue. Its single lane is custom-capable; for it the
  defences are the backend's refusal, B3b and the broker permissions below.
- **Public profile**: a backend setting `ADMIN_ONLY_SERVICES` (default empty; the deployment sets
  `cdse`) hides and refuses the interfaces of those services on the public profile.
- **No service-specific code (user decision):** maas-collector and the backend never name a service
  or interface in code, tests or defaults. Everything CDSE-specific (the single lane, the download
  host, the archive rule, the admin-only service) is deployment configuration.
- **Templates**: `raw-data-dm-cdse-product` (the CDSE raw index) moves out of the retired repository
  with `raw-data-backup`, and both are installed in production.
- Prerequisites: the production archive bucket and its keys (the definition's `credentials` entry,
  written from the app).

### C7. Retired

The `download-manager` repository: the engine (consolidation into `dm-download`, transfer, forced
queue), the replay worker, `download-manager-backend`, `maas-dm-app`, the deploy files, the indices
`dm-download` and `dm-operation`. `raw-data-backup` stays (A3). The download manager's uncommitted
operator code is committed to a branch first, as a reference. The dev download manager keeps
running until the continuous archive runs in production (my call): nothing is retired before its
replacement works.

### C8. Note: triggering consumers (not implemented)

How consumers will consume the archive is not known yet. The integration points kept open:

- **An event per archived product.** After writing an `OK` or `REPLACED` report, the transfer stage
  could publish on a topic exchange (for example `maas-archive-exchange`, routing key
  `archived.<backup>.<interface>`) the backup, the interface, the product id and name, the object
  keys, size, MD5, `replaced`, `operation_id` and the time. Each consumer binds its own queue to the
  keys it wants. This is the natural hook: the report is written when the object is known complete.
- **The report index.** A consumer can poll `raw-data-backup` for a backup's new `OK` and `REPLACED`
  archive documents (by `ingestionTime`), with no change to the transfer stage.
- **Storage notifications.** If the storage offers bucket notifications, consumers can react to
  object writes without MAAS; whether the OVH endpoints offer them is to be checked.
- Whatever is chosen: emit only once the object is complete, and make a replacement distinguishable
  from a first arrival.

## Part D — a standalone system (to be designed)

User note (2026-10-29): this system will also be deployed as a whole, from scratch: install
Docker, a one-node Swarm cluster, a one-node OpenSearch, then a collector, an engine and a Grafana
to operate it, maybe monitoring. The download manager was a demo; its deployment is not a
reference to keep.

What Parts A–C need from such a system (to be confirmed):

- Docker and a one-node Swarm.
- OpenSearch, one node: the raw indices, `maas-collector-config`, `maas-operation`,
  `maas-recollect-config`, `raw-data-backup`, `maas-archive-conflict`, with their templates.
- **RabbitMQ**, not in the user's list but required: collector → transfer stage
  (`maas-transfer-exchange`), backend → workers (`maas-operator-exchange`).
- **OpenBao**, not in the user's list: the backend writes interface and backup keys there and the
  services read them. Without it, keys could only sit in credential files and the app could not
  write them. Recommended, one node.
- maas-collector services: the collectors (stage 1), the transfer stages (stage 2, the "engine"),
  the operator workers.
- The maas-operator backend (admin profile; public optional) and Grafana with `maas-operator-app`
  and the Archive dashboard.
- The backups' storage: S3 endpoints, or filesystem paths mounted into the transfer stages and
  collectors.
- Monitoring, optional.

Existing material: csc-ocs-ansible's roles `docker_host`, `docker_swarm_cluster`,
`docker_swarm_stack` and `openbao_config`; ocs-maas-deploy's compose files (OpenSearch, RabbitMQ,
Garage, Grafana, monitoring with Prometheus, Loki and Mimir, maas-collector, maas-engine), whose
README asks for them to move into Ansible.

Open, to brainstorm before planning this part: who runs it and where; what its hosts can reach
(registry, the remote interfaces, S3); the operating system; how it is installed (csc-ocs-ansible,
ocs-maas-deploy or a new repository); sizing on one host; OpenSearch snapshots; access to Grafana
(authentication, TLS); the monitoring scope.

## Records (`maas-operation`)

New types: `RECOLLECT_CONFIG_SAVE`, `RECOLLECT_CONFIG_DELETE`, `DOWNLOAD`. New fields, all scalar
(the generated `/operations` schema does not take lists): `configuration_name`,
`configuration_version`, `overrides` (JSON string, not indexed), `backup_name`, `backup_version`,
`duplicates`, `force`, `document_index`, `product_count`. The products of a download are in its
`REQUESTED` reports, not in the record. The template changes, the existing index's mapping is
updated, and the generated schemas are regenerated with the maas-model generator branch the backend
already pins (`fastapigen`), until it is released.

## Deployment and security notes

- **backend-public** holds today the broker's default user and the OpenSearch `admin` account, as
  `backend-admin` does. Before the custom features are enabled (Rollout 4), give it least-privilege
  accounts: OpenSearch read on what it lists and write on `maas-operation` only; a broker user that
  may publish only the plain replay and check keys of `odata` and `jirax`.
- A rollback of the workers: B3b makes queued custom requests fail when an older worker reads them;
  requests sitting in custom queues no worker consumes are flagged "not picked up" (B5e); the
  runbook deletes those queues and marks their records FAILED.

## Rollout

0. **On the current production stack, before anything else:**
   - **mitigate the code-execution path** of configuration writes until B1 ships: the user fixes
     it the week after 2026-10-29;
   - the Satellite-Unavailability entry of `odata/auxip/maas-collector-odata-auxip-exprivia.json`
     published to the index with the XMLExtractor args and the `SatUnavailabilityProduct` model's
     new fields; no file of the index names `SatUnavailabilityExtractor` any more (3.14.0 cannot load
     it, and every odata replay would fail);
   - the docker-jobs nightly `rc-auxip-exprivia-1d` job rolled to the new configuration, or off;
   - the first production replay: a plain one-hour odata recollect from the app (PUBLISHED, then
     RUNNING, then DONE) and a product check, recorded in the deploy runbook.
1. **maas-collector 3.14.0**: A1–A4, B1's static validation, B3, C1–C3, the removal of
   `SatUnavailabilityExtractor` (branch `feature/satu-xml-extractor`).
2. **maas-operator-backend** and **maas-operator-app** releases.
3. **Indices**: `raw-data-backup` (A3 fields), `raw-data-dm-cdse-product` and
   `maas-archive-conflict` installed in production before the first report; check first that no
   `raw-data-backup-*` index exists with dynamic mappings.
4. **csc-ocs-ansible**: least-privilege accounts for backend-public; workers on 3.14.0 with custom
   lanes; backend and app; `ADMIN_ONLY_SERVICES`; the admin policy's `delete` on backup keys; the
   not-yet-run maintenance window's collector version raised to 3.14.0 or later.
5. **A lasting rule, not a one-off check**: no service is tagged on a backups file while any
   collector, worker or backend that loads that service, now or after a planned deploy or rollback,
   runs a version below 3.14.0.
6. **First custom run, in production (user decision)**: save `auxip-exprivia-satu-only`; recollect
   one day (with an archive choice if the reports are wanted in a backup); check the operation: only
   REP__SUP products listed, SATU documents with comments, transfers complete. Then the full window,
   as consecutive recollects of at most 92 days, each checked before the next. An archive choice
   needs the `odata` transfer stage of step 7 running.
7. **The `maas-archive` stack** (C6): the `cdse` service with its bucket, keys, AppRole, collector
   and transfer stage, the `odata` transfer stage, `worker-cdse`; then the download manager's
   retirement (C7).

## Left to the builder

Index mappings beyond the fields stated; route, module and component names beyond those stated; the
UI layout; how the schema derives an entry's JSON keys; the catalog's groups and wording; the exit
code of A4 and the format of its last line; batching of transfer requests; where the
`raw-data-backup` and `raw-data-dm-cdse-product` templates live; the gap delay; the Archive
dashboard's panels; error messages.

## Verification

- **maas-collector**: backups content (names, secrets refused in any block and at load, destination
  guard on publish, rollback and re-create, who may change what); name resolution with credentials
  from the credential file and from OpenBao, rotation, no other field accepted, masking in reprs;
  integrity refusals (publish, untag, rollback, tombstone, disabled entries); reports with the new
  fields; A4 counting failures without a KO report; static validation never evaluates (a lambda with
  a side effect is parsed, not run); custom lanes (applied on a custom lane, refused on a plain one,
  slot shared), `@` refused, fail-closed names, allowlist, window structure (date field bound,
  `or` at depth 0, jira clause), backup version pinning; schema (sub-entries built as their service's
  class, extractor-level `converter_map`, secret-key test, every key of `tests/conf` described);
  archive rules, transfer requests (live: created and updated; replay: every extracted document,
  including one already indexed and unchanged), mandatory publish, `REQUESTED` reports; the transfer
  stage (skip, replace, force, conflict, mirror kept equal, CDSE download host without redirect,
  stable keys by publication date, bounded retries, KO without requeue); conflicts opened and
  resolved; gaps.
- **Backend**: no lambda evaluated on any write; backups list, usage, integrity 409s, S3 changes
  admin-only, credential writes limited to the two keys, key copy deleted on untag, masking in
  `GET /configs`; recollect configurations (versions, conflict, tombstone, staleness, allowlist);
  recollect with a configuration, inline overrides and an archive choice (dry run echo, public
  refusal, capabilities, unroutable publish); downloads (plan with checksums, run, gaps, progress
  from reports); `ADMIN_ONLY_SERVICES`; records with their new fields through `/operations`, and the
  public list stripped.
- **App**: the builder keeps unknown keys (entry and extractor), hides secret keys, edits `attr_map`,
  models and backups; variant mode yields only allowed overrides; the Backups, Recollect and Download
  pages; the confirmation refused without the echo; "not picked up" flag.
- **Production**: Rollout 0, 6 and 7, each recorded in the deploy runbook.

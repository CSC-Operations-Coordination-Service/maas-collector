# MAAS operator (app + backend) — design

## Context

Today, when an operator spots a gap or a wrong collector setup in Grafana, they
ask a developer, who runs a replay playbook / `maas_collect_yesterday.sh` /
the `cli.config` CLI by hand. The goal is that operators do it themselves from
Grafana:

- **recollect** an interface over a window (mostly odata, sometimes jira,
  sometimes long windows),
- **update collector configuration** — typically add an interface, disable
  one, switch credentials, occasionally split one interface in two,
- **switch credentials** — both "point the interface at another credential
  entry" and "write new secret values in OpenBao",
- **product check** — is product X (by name or id) on the remote interface,
  is it in maas, do they differ; then recollect around it.

It ships as **two deployments of the same code**: an *admin* one in the
monitoring Grafana (everything, long windows) and a *public* one (short
recollect windows, product check, nothing that writes config or secrets).

Recollect reuses the download-manager pattern
(`download-manager/download-manager-backend/.../actions.py`,
`maas-engine-download-manager/.../replay/worker.py`, `maas-dm-app/src/pages/Recollect.tsx`):
validate → record an operation → publish on RabbitMQ → a worker runs
`maas_collector … --replay-*` with its own replay journal → status updates
on the operation document.

## Goals / non-goals / anti-goals

Goals
- Operators self-serve recollect, config edits, credential switches, product
  checks, without a developer.
- Replays collect **exactly** the requested window.
- Jira interfaces can be replayed.
- Every action leaves a record: who, when, what, why, outcome.

Non-goals (v1)
- Cancelling a running replay (my call; record says RUNNING until it ends).
- Product check for non-odata interfaces (my call; jira tickets aren't
  "products"). Recollect still covers jira.
- Replacing docker-jobs nightly systematic replays.
- Keycloak (headers trusted from Grafana, like ocs-operator / DM).

Anti-goals (would be a failure even if it works)
- The public deployment able to write configs or secrets — must be impossible
  by construction (routes not mounted, no write credentials), not just hidden.
- Secret values ever readable back through the API or UI.
- A config edit that bypasses the validation `cli.config put` applies, or
  silently overwrites someone else's newer version.
- A replay that interferes with the live collector journal.

## Parts and decisions

### 1. maas-collector changes (this repo)

a. **Exact replay window** (user decision). When running in replay mode:
   - ignore `odata_start_offset` — `src/maas_collector/rawdata/collector/odata/query_strategy.py:54`
   - ignore `end_date_time_offset` — `src/maas_collector/rawdata/collector/http/abstract_query_strategy.py:54`
   The strategy already receives the journal (`CollectorReplayJournal` vs
   `CollectorJournal`), so replay is detectable there. Normal runs unchanged.

b. **Jira replay** (user decision). `jiraxcollector.py:194` (`# TODO handle
   start/stop for replay`): in replay mode bound the JQL by replay start and end.
   Every existing `jql_query` uses `updated > {date_criteria}`; how to add the
   upper bound (e.g. `date_criteria = '"<start>" AND updated <= "<end>"'` or a
   new optional `{end_criteria}` placeholder) is left to the builder, as long as
   existing configs keep working unchanged. Uses the replay journal like the
   other collectors.

c. **`enabled` flag on collector entries** (my call — "disable an interface"
   today means deleting the entry, which loses it). Entry with
   `"enabled": false` is loaded, validated, but not collected, not replayed,
   and not probed by the interface monitor. Default `true`; existing configs
   unchanged.

e. **Config sync** (user decision: the index becomes the source of truth, plus a
   sync that brings the index's configuration into a local folder). New
   `cli.config sync <dir> [--service S ...]`: writes the latest version of
   every path, removes files whose path is deleted (tombstone) or gone, and is
   idempotent (unchanged files untouched). `export` stays as it is. Used to
   refresh `csc-ocs-ansible/configs/` and by anything that still needs files.

d. **Operator worker** — new CLI `maas_collector.rawdata.cli.operator_worker`
   (my call: lives in maas-collector because product check needs collector
   classes; generalises the DM `replay/worker.py`). One worker per collector
   service (`CONFIG_SERVICE`), consuming two queues for its service:
   - `replay` requests in **two lanes per service** (my call, so a 92-day
     admin replay can't block a 1-hour public one): `short` (window ≤ 24 h)
     and `long`, each one at a time, each with its own timeout (long scales
     with the window, unlike the DM's fixed 6 h). Each lane materialises the service's latest config
     from `maas-collector-config` (via `ConfigStore` export) into a temp dir,
     runs the matching CLI module as a child process with the file backend
     (as `docs/opensearch_config.md` recommends for replays):
     `python -m maas_collector.rawdata.cli.<module> -d <tmp> --replay-interface-name … --replay-start-date … --replay-end-date … --replay-retry <OPERATOR_REPLAY_RETRY, default 3> --replay-suffix=<operation_id>`.
     Module chosen from the entry's `class` (`ODataCollectorConfiguration`→`odata`,
     `JiraExtendedCollectorConfiguration`→`jirax`; table extensible). Updates
     the operation record PUBLISHED→RUNNING (tail of output in `details`)→DONE/FAILED.
   - `check` requests (RPC, `reply_to` + correlation id): build the odata
     collector for the interface with its config + credentials, query the remote
     interface by name or id, run the collector's own extraction to get the
     document maas *would* store, read the stored doc from `model.index` by
     `id_field`, reply `{remote, stored, diff}`. Answers checks even while a
     replay runs (separate thread/consumer).
   Credentials come from OpenBao exactly like the collector (`CREDENTIAL_BACKEND=vault`).

f. **A failed replay exits non-zero** (found while planning). Today
   `HttpCollector.ingest_replay` only logs "Failed replay" when retries run out,
   and `cli/odata.py` returns nothing, so the process exits 0 and a worker would
   record DONE. The odata and jirax CLIs exit 1 when any replayed interface
   failed; an unknown interface still raises before collecting.

### Message and record contract (shared by worker and backend)

- Exchange `maas-operator-exchange`, topic, durable.
- Replay request: routing key `maas.operator.<service>.replay.<lane>`, queue
  `maas-operator-<service>-replay-<lane>`, `lane` ∈ `short` (window ≤ 24 h) |
  `long`. Body JSON:
  `{"operation_id", "interface_name", "start_date", "end_date", "requested_by", "lane"}`,
  dates ISO 8601 UTC with `Z`.
- Check request: routing key `maas.operator.<service>.check`, queue
  `maas-operator-<service>-check`, message properties `reply_to` +
  `correlation_id`. Body `{"operation_id", "interface_name", "product_name"?, "product_id"?}`
  (exactly one of the two). Reply on the default exchange to `reply_to`, same
  `correlation_id`:
  `{"status": "ok"|"error", "remote": {..}|null, "stored": {..}|null, "diff": [{"field", "remote", "stored"}], "error": str|null}`.
- `maas-operation` index: mapping owned by the backend (template + generated
  CRUD). The backend writes the full record (status PUBLISHED) **before**
  publishing; the worker only sends partial updates by id
  (`status` RUNNING|DONE|FAILED, `updated_at`, `exit_code`, `details` = last 20
  output lines), with `retry_on_conflict`; a missing record is logged, not
  created.

### 2. maas-operator-backend (new repo `/home/aagazzi/work/maas-operator-backend`)

Python 3.11, FastAPI, pydantic v2, opensearch-py, kombu — layout copied
from `download-manager-backend` (`settings.py`, `auth.py`, `messaging.py`,
`store.py`, `actions.py`, `scripts/generate.sh`, committed `generated/`).
Generated parts use **maas-model `fastapigen`** (user decision), which today is
only on maas-model branch `feature/DEVOCS-XXX-fastapigen-crud-api-cerbos-authz`.
That branch forks before 3.1.0 and lacks `maas_model.vault`, which maas-collector
(imported by the backend for `ConfigStore`) needs. **User decision: merge
maas-model `develop` into the fastapigen branch**; the backend pins that branch
(git dependency / dev build, usable in Docker and CI, not only the sibling
checkout) until it is released. Requires `maas-model[api,vault]`.

- Generated (templates in `src/maas_operator_backend/resources/templates/api/`):
  `maas-operation` index (operation records) — list/get only for clients.
- Hand-written routers under `/api/v1`:
  - `GET /capabilities` — profile, limits, enabled features (app uses it to
    decide which pages exist).
  - `GET /interfaces` — interfaces from `maas-collector-config` (latest, not
    deleted): name, service, path, collector class, enabled, credential entry.
  - `POST /actions/recollect` `{interface_name, start_date, end_date, reason, dry_run}`
    — checks interface exists and is replay-capable, start<end, end≤now,
    window ≤ profile max; resolves service via `interface_names`; record then
    publish (same `_record_then_publish` semantics as DM).
  - `POST /actions/product-check` `{interface_name, product_name | product_id}`
    — RPC to the service's worker, timeout (default 30 s) → 504 with a clear message.
  - **admin profile only** (routers not mounted in public):
    - `GET /configs`, `GET /configs/{path}` (+ `history`, `diff`, `{version}`)
    - `PUT /configs/{path}` `{content, comment, based_on}` → uses maas-collector
      `ConfigStore.publish` so validation/conflict rules are the CLI's;
      `ConfigConflictError`→409, validation error→422 with the reason.
    - `POST /configs/{path}/rollback` `{version, comment}`.
    - `PUT /configs/{path}` also creates a new file (my call — "add an
      interface" can need one): body then carries `services`, as the CLI's
      `--services`.
    - `GET /credentials` — entry names + field names + which service prefixes
      hold them, never values.
    - `PUT /credentials/{entry}` `{fields: {name: value}, comment}` — writes a new
      KV v2 version, returns only version numbers. OpenBao keeps one copy per
      service (`odata/X`, `monitor/X`); **my call:** the write goes to every
      service prefix whose configs use the entry (listed in the UI before
      confirming), and creates the entry when it doesn't exist (policy needs
      `create`). Collectors pick it up via `CREDENTIAL_REFRESH_PERIOD`.
    - Pointing an interface at another `interface_credentials` entry: the
      backend refuses unless that entry exists under every service prefix of
      the document (55 paths are shared by `odata`/`jirax` and `monitor`) (my call).
  Every write (config, rollback, credential, recollect) also writes a
  `maas-operation` record (`operation_type` CONFIG_PUBLISH / CONFIG_ROLLBACK /
  CREDENTIAL_WRITE / RECOLLECT / PRODUCT_CHECK, `requested_by`, `reason`, target, outcome).
- Profiles (`OPERATOR_PROFILE=admin|public`) (my call on mechanism):
  - public: `RECOLLECT_MAX_HOURS` default 24, optional `RECOLLECT_INTERFACES`
    allowlist, no OpenBao client, read-only OpenSearch user except
    `maas-operation`.
  - admin: `RECOLLECT_MAX_HOURS` default 2208 (92 days), OpenBao AppRole with
    write policy limited to the collectors' KV prefix, write access to
    `maas-collector-config`.
- Auth (my call, follows `ocs-operator-backend/src/ocs_operator_backend/principal.py`):
  trusts `X-Grafana-User` (401 without) and `X-Maas-Roles` set by the plugin
  proxy route per Grafana role. **No Cerbos for now** (user decision,
  2026-10-16): roles are checked against a fixed table in the backend
  (`user` → read + product check; `operator` → also recollect + config edit;
  `admin` → also credential writes); Cerbos can replace the table later without
  changing the API.
- Messaging: exchange `maas-operator-exchange`, routing keys
  `maas.operator.<service>.replay.short|long` / `.check`, queues per service.
- The public backend lists only RECOLLECT and PRODUCT_CHECK records from
  `maas-operation` (my call): config and credential records stay admin-only.
- Accounts (user decision: ship anyway): the admin Grafana still uses a shared
  `admin` login, so `requested_by` will often be "admin". `reason` / `comment`
  are mandatory on every write so the record still says why.

Relation to `ocs-operator-backend`: its generated `maas-config-collector` CRUD is
a different, free-form index, not the collector's `maas-collector-config`; not
touched.

### 3. maas-operator-app (`/home/aagazzi/work/maas-operator-app`)

Rebuild on the `maas-dm-app` / `docker-jobs-app` pattern (my call): Grafana
≥13.2, React 19, `@grafana/*` ~13.2.3, `"backend": false`, proxy routes in
`plugin.json` to `{{ .JsonData.backendUrl }}/api` with `X-Maas-Roles` per
`reqRole`; drop the Go backend plugin and template pages from the 11.5 scaffold.
`.config/` copied from maas-dm-app; Jest + RTL tests next to pages. The
prototype pages (Dataflow, Acquisition, Completeness, MaaS Configuration) depend
on the removed Go backend and are dropped (my call).

Pages (shown according to `GET /capabilities` and the user's role):
- **Operations** — `maas-operation` list with filters, status, details (like DM `Operations.tsx`).
- **Recollect** — DM `Recollect.tsx` generalised: interface combobox from
  `/interfaces`, From/To, reason, Check (dry run) → confirm → Recollect; max
  window shown from capabilities.
- **Product check** — interface + name/id → remote vs maas side by side with
  field diff; "Recollect around this product" prefills Recollect with a small
  window around the product's date.
- **Configuration** (admin) — files by service; JSON editor with diff against
  current, mandatory comment, publish (409 → "someone published v N, reload"),
  history, rollback; shortcuts on an interface: enable/disable, change
  `interface_credentials`.
- **Credentials** (admin, Admin role) — entries with field names; write-only
  form for new values.

### 4. Deployment

Swarm stack `csc-ocs-ansible/stacks/apps/maas-operator/` like `download-manager`:
nginx serving the plugin zip, `maas-operator-backend` ×2 (admin, public — each
on its Grafana's network), one `operator_worker` per collector service.
Each Grafana provisions the app with its own `backendUrl`;
`GF_DATAPROXY_SEND_USER_HEADER=true`.

- **Admin**: the monitoring Grafana (grafana-ocs, 13.2, swarm).
- **Public**: the **TDH Grafana** (user decision). It is 11.5.3 and a
  standalone container, so this work includes upgrading it to ≥13.2 and giving
  it a network path to the public backend. Precondition to verify there: its
  users log in with named accounts, since the backend refuses requests without
  `X-Grafana-User` (anonymous access would refuse everything).

**Source of truth** (user decision): `maas-collector-config` and OpenBao.
`csc-ocs-ansible` follows: `maas-collector-alerts.py` (the "interface not
collected" rules) and the docker-jobs recollect jobs read from the index (or
from a `sync`ed folder), and the repo `configs/` + ansible-vault credential
files are retired as sources, kept only as a `sync` target.

## Carried from part 1 (maas-collector) into the later parts

- Backend: `CheckRequest.to_body` emits both `product_name` and `product_id`, one
  of them empty; treat `""` as absent.
- Backend: a check can wait up to 5 s (idle poll, `OPERATOR_POLL_INTERVAL`) plus
  20 s (`OPERATOR_CHECK_TIMEOUT`) before its reply; the backend's 30 s RPC wait
  leaves little margin, so keep it at 30 s or more.
- Backend: compute `lane` with `contract.lane_for`; the worker uses the queue's
  lane for timeouts, not the body's.
- Deployment: `stop_grace_period` of the worker must exceed the 10 s child
  terminate grace (Docker's default is 10 s), or interrupted replays keep
  RUNNING records.
- Deployment: the worker has no health endpoint of its own.
- Not covered by the worker: mpip still shifts its start date on replay, S3
  replay is not implemented; neither is in `REPLAY_MODULES`.

## Carried from part 2 (maas-operator-backend) into the later parts

- App: `GET /api/v1/capabilities` drives which pages exist (`features`, `allowed`
  per verb, `max_window_hours`). Config `PUT` and rollback need `base_version`
  (409 on a stale edit); credential `PUT` needs `expected_versions` for exactly
  the services listed by `GET /credentials` (rows carry `kind`
  shared|interface|both, `services` → field names or null, `versions`).
  Interface-own entries override the shared entry field by field.
- App: errors arrive as `{"detail": ...}`; recollect/check failures also carry
  `operation_id`; 504 = no reply from the worker, 503 = no worker / broker, 502 =
  OpenBao.
- Deployment: the backend's AMQP user needs *configure* + write on
  `maas-operator-exchange` (it declares the exchange, never a queue); workers
  declare their queues.
- Deployment: run `maas-operator-backend init-indices` once before first use
  (otherwise `maas-operation` gets a dynamic mapping and filters stop matching).
- Deployment: OpenBao AppRole of the admin backend: read + create/update on
  `<mount>/data/<VAULT_PATH_ROOT>/*`, read on `<mount>/metadata/<VAULT_PATH_ROOT>/*`.
- Deployment: verify once on the real stack that a recollect with no worker for
  the service gives 503/FAILED, and that the worker's dates without milliseconds
  are accepted by the `maas-operation` mapping (`strict_date_optional_time`).
- Deployment: the image needs the maas-model fastapigen build (pending push of
  the merged branch) and maas-collector ≥ the operator merge; pin git refs once
  pushed.

## Carried from part 3 (maas-operator-app) into deployment

- Grafana needs `GF_DATAPROXY_SEND_USER_HEADER=true` and `GF_DATAPROXY_TIMEOUT` ≥ 45
  (a product check waits up to 30 s); `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=maas-operator-app`
  unless the zip is signed; install with `GF_PLUGINS_PREINSTALL_SYNC` from the app's nginx image.
- Verify once on a real Grafana 13.2: the proxy takes the first route matching path + method
  (a Viewer can POST `api/v1/actions/product-check` and nothing else); the config editor's
  "Leave without saving?" prompt fires on the side menu, breadcrumbs and Back.
- The app picks its route family from the Grafana org role (Viewer → `user`, Editor →
  `operator`, Admin → `admin` in `X-Maas-Roles`).

## Left to the builder

Index template details for `maas-operation`; check-reply payload shape and
diff format; the JQL upper-bound technique; worker temp-dir lifecycle and
concurrency between replay and check consumers; UI layout and editor component;
CI wiring via `workflow-catalog`; how the services list for workers is derived.

## Implementation order

1. This design lives in `maas-collector/docs/superpowers/specs/` next to the
   other collector specs (the backend repo doesn't exist yet); the backend and
   app plans link to it.
2. maas-model: merge `develop` into the fastapigen branch, tests green.
3. maas-collector: 1a, 1b, 1c, 1e (each with tests, CHANGELOG), then 1d.
4. maas-operator-backend: scaffold, generated `maas-operation`, recollect +
   product check, then admin config + credentials routers, profiles.
5. maas-operator-app rebuild, pages in the order above.
6. csc-ocs-ansible: stack, admin Grafana provisioning, TDH Grafana upgrade +
   network, alert script and docker-jobs reading the index / synced folder.

A detailed plan per part is written with superpowers:writing-plans after approval.

## Verification

- maas-collector: `tox` — new tests for replay ignoring both offsets (normal
  run still applies them), jira replay JQL bounds, `enabled: false` skipped,
  worker command building / module mapping / record transitions with fakes.
- Backend: pytest with in-memory fakes (as `ocs-operator-backend/tests/fakes.py`);
  public profile test asserts config/credential routes return 404 and no OpenBao
  client is built; conflict → 409; credential GET never contains values.
  OpenSearch tests behind an env URL.
- App: `npm run lint && npm run typecheck && npm test`; docker-compose Grafana
  13.2 with `dist/` mounted against a local backend.
- End to end on a dev stack: recollect a 1 h odata window → operation goes
  PUBLISHED→RUNNING→DONE, replay journal `<interface>_<operation_id>`
  covers exactly the window; product check on a known product; publish a
  config edit and see the collector restart (exit 75) on the new version.

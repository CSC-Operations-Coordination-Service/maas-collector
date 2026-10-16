# MAAS operator — part 3: maas-operator-app Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild `maas-operator-app` as a frontend-only Grafana 13.2 app that lets operators
recollect interfaces, check products, follow operations, edit collector configurations
(with history and rollback) and write collector credentials through `maas-operator-backend`.

**Architecture:** Same shape as `download-manager/maas-dm-app` and `docker-jobs-app`: a
`backend: false` app plugin, React 19 + `@grafana/ui` 13.2, plugin proxy routes to the
backend's `/api`, one route family per Grafana role (Viewer → `user`, Editor → `operator`,
Admin → `admin` in `X-Maas-Roles`). Pages adapt to `GET /api/v1/capabilities` (profile,
features, allowed verbs). The configuration editor reproduces the Dataflow edition UX of the
old prototype (always-editable table, row actions, JSON modal, filters, selection + batch,
single Save) on plain `@grafana/ui`, adding a review step, the required comment and 409 handling.

**Tech Stack:** TypeScript 5.5, React 19.2.8, `@grafana/{data,runtime,schema,ui}` ~13.2.3,
react-router-dom 6, rxjs, @emotion/css, webpack/SWC from `@grafana/create-plugin` 5.19.1
(`.config/`), Jest 29 + React Testing Library 16 (fireEvent), Node 22.

**Spec:** `/home/aagazzi/work/maas-collector/docs/superpowers/specs/2026-10-16-maas-operator-design.md`
(section 3, "Carried from part 2"). Backend contract: `/home/aagazzi/work/maas-operator-backend/README.md`
and its `src/maas_operator_backend/*.py` (the code wins over any doc).

## Global Constraints

- Repo `/home/aagazzi/work/maas-operator-app` (remote `origin`). Work in a NEW worktree
  `/home/aagazzi/work/maas-operator-app-rebuild` on branch `feature/operator-app` created from
  `origin/develop`. Never touch the main checkout: it holds the user's uncommitted Dataflow
  work on `feature/bootstrap-the-app`.
- Plugin id stays `maas-operator-app`; `"backend": false`; `grafanaDependency ">=13.2.0"`.
- Dependencies and tooling exactly as `maas-dm-app` (package.json versions, `.cprc.json`
  `{"features":{"bundleGrafanaUI":false,"useReactRouterV6":true}}`, jest.config.js ESM list,
  jest-setup.js MessageChannel stub) plus docker-jobs-app's canvas `getContext` stub for Combobox.
  No @tanstack, no dnd-kit, no react-hook-form, no playwright, no Go.
- Proxy routes (plugin.json), each `url: "{{ .JsonData.backendUrl }}/api"` and a static
  `X-Maas-Roles` header:
  `api/*` GET, reqRole Viewer, roles `user`;
  `operator/api/*` GET/POST/PUT, reqRole Editor, roles `operator`;
  `admin/api/*` GET/POST/PUT, reqRole Admin, roles `admin`.
  The client picks its base from `config.bootData.user.orgRole` (Admin → `admin/api/v1`,
  Editor → `operator/api/v1`, else `api/v1`) under `/api/plugin-proxy/maas-operator-app/`.
  `X-Grafana-User` comes from Grafana (`GF_DATAPROXY_SEND_USER_HEADER=true`).
- Pages: Operations (default, Viewer), Recollect (Editor), Product check (Viewer),
  Configuration (Editor), Credentials (Admin), app config page (Admin). A page whose
  feature is off in `/capabilities` (public profile) shows an info Alert
  "Not available on this deployment", never a broken call. Buttons the caller's `allowed`
  verbs forbid are hidden or disabled with a tooltip.
- Every request uses `showErrorAlert: false`; errors are shown inline with
  `errorMessage()` (detail string, list of `{loc,msg}`, `{written,failed,error}` bodies).
- Secret values typed by the user live only in component state, are sent once in the PUT
  body, never logged, never written to storage, cleared after a successful write.
- Dates sent as `toISOString()` (UTC `Z`); shown in UTC with the `Z` suffix.
- Tests: Jest + RTL with `fireEvent`, mock `../api` in page tests, mock `getBackendSrv` in
  `api.test.ts`; `isDisabled(el)` helper (Grafana buttons use aria-disabled).
- Commands (in the worktree): `npm ci`, `npm run typecheck`, `npm run lint`, `npm run test:ci`,
  `npm run build` — all green before each commit.
- Commit messages: gitmoji style as the repos (`feat: :sparkles: …`), ending with the
  Co-Authored-By line the committing session's system reminder gives. Never push.

## Review Focus

- A config edit raced by another operator: Save answers 409 → an Alert offers "Reload" (drops
  local edits) or "Keep my edits" (rebases `base_version` on the reloaded version and keeps
  the draft for a second review); local edits are never silently lost. Test in Task 6.
- A credential write that is half applied (502 `{written, failed, error}`): the page names the
  written and the failed services, reloads the versions, and keeps the typed values only until
  the user dismisses the error. Test in Task 8.
- A Viewer opening Recollect / Configuration / Credentials by URL: no write control is enabled
  and no write request can be sent. Test in Tasks 4, 6 and 8.
- A product check that takes close to the backend's 30 s: the button shows a busy state, a
  second click sends nothing, a 504 is shown as "no reply from the worker", and the operation
  id is shown so the user can find the record. Test in Task 5.
- Public profile (`features.configs/credentials` false): those pages render the "not
  available" Alert and send no `/configs` or `/credentials` request. Test in Task 6 and 8.

---

### Task 0: Worktree, clean scaffold, tooling, CI

**Files:** delete `pkg/`, `Magefile.go`, `go.mod`, `go.sum`, `.go-version`, `.vscode/`,
`.config/{Dockerfile,docker-compose-base.yaml,entrypoint.sh,supervisord}`, `playwright.config.ts`,
`tests/`, `.github/workflows/*`, all of `src/` except `img/logo.svg`; replace `package.json`,
`package-lock.json`, `jest.config.js`, `jest-setup.js`, `.cprc.json`, `docker-compose.yaml`,
`provisioning/`; create `scripts/package.sh`, `Dockerfile`, `docker/nginx.conf`,
`.github/workflows/maas-operator-app.yml`, `README.md`, `CHANGELOG.md`, `src/{module.tsx,plugin.json,constants.ts,settings.ts}`,
`src/components/App/App.tsx`, `src/components/AppConfig/AppConfig.tsx`, `src/pages/Placeholder.tsx`.

- [ ] **Step 1:** `git -C /home/aagazzi/work/maas-operator-app worktree add -b feature/operator-app /home/aagazzi/work/maas-operator-app-rebuild origin/develop`.
- [ ] **Step 2:** Clean and copy: tooling files from `/home/aagazzi/work/download-manager/maas-dm-app`
  (package.json deps/devDeps/scripts with name `maas-operator-app`, version `0.1.0`; jest files; `.cprc.json`;
  `.config/` compared with `diff -rq` — keep the frontend scaffold, drop backend extras);
  packaging and CI from `/home/aagazzi/work/docker-jobs-app` (nginx-only Dockerfile serving
  `release/maas-operator-app-*.zip`, `scripts/package.sh`, workflow calling workflow-catalog
  `node-workflow.yml@develop` with `working-directory: .`, node `22.23.3`, then `package` and
  `docker` jobs as docker-jobs-app). `docker-compose.yaml` = Grafana 13.2.0 with `dist/` mounted,
  `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS=maas-operator-app`, `GF_DATAPROXY_SEND_USER_HEADER=true`,
  `GF_DATAPROXY_TIMEOUT=60`; `provisioning/plugins/apps.yaml` with `backendUrl: http://host.docker.internal:18000`.
  **No credentials anywhere** (no OAuth secret, no tokens).
- [ ] **Step 3:** Skeleton: `plugin.json` (routes and includes per Global Constraints; pages
  `operations` defaultNav, `recollect`, `product-check`, `configuration`, `credentials`),
  `constants.ts` (`PLUGIN_ID`, `PLUGIN_BASE_URL`, `ROUTES` enum, `prefixRoute`),
  `settings.ts` (`AppJsonData {backendUrl?: string}`, context + `useAppSettings`),
  `AppConfig.tsx` (backend URL as maas-dm-app's, with the send-user-header and
  `GF_DATAPROXY_TIMEOUT ≥ 45` notes), `App.tsx` (missing backendUrl → warning Alert; lazy routes
  to a `Placeholder` page per route for now).
- [ ] **Step 4: Tests** — `App.test.tsx`: missing backendUrl shows the Alert; each route renders
  its placeholder. `plugin.test.ts`: the three route families with their reqRole, methods and
  `X-Maas-Roles` header values; no `secureJsonData` anywhere.
- [ ] **Step 5:** `npm ci && npm run typecheck && npm run lint && npm run test:ci && npm run build` → green.
  `grep -rn "SECRET\|client_secret\|apiKey" --exclude-dir=node_modules .` → nothing.
- [ ] **Step 6:** Commit `feat: :fire: Rebuild the app as a frontend for the operator backend`.

### Task 1: API client and capabilities

**Files:** Create `src/api.ts`, `src/capabilities.tsx`, `src/test-utils.tsx`, `src/test-fixtures.ts`;
Test `src/api.test.ts`, `src/capabilities.test.tsx`.

**Interfaces — Produces** (every later page imports these exact names):

```ts
export type Role = 'user' | 'operator' | 'admin';
export function apiBase(orgRole?: string): string        // '/api/plugin-proxy/maas-operator-app/{api|operator/api|admin/api}/v1'
export class ApiError extends Error { status: number; data: unknown; operationId?: string }
export function errorMessage(error: unknown): string      // detail string | detail[] → "loc: msg" joined "; " | {written,failed,error} → "written: a, b; failed: c: <error>" | status text
// types mirroring the backend (field names exactly as the backend's JSON):
export interface Capabilities { profile: 'admin'|'public'; user: string; roles: string[]; max_window_hours: number;
  recollect_interfaces: string[]; features: Record<'recollect'|'product_check'|'configs'|'credentials'|'operations', boolean>;
  allowed: Record<'read'|'check'|'recollect'|'config_write'|'credential_write', boolean> }
export interface Interface { name; path; services: string[]; collector_class; enabled: boolean; credential_entry;
  replayable: boolean; checkable: boolean; worker_service: string | null }
export interface Operation { document_id; operation_type; status; requested_by; requested_at; updated_at; reason; interface_name;
  service; lane; exchange; routing_key; start_date; end_date; product_name; product_id; config_path; config_version: number|null;
  credential_entry; credential_services; details; exit_code: number|null }          // all nullable strings unless typed
export interface RecollectRequest { interface_name; start_date; end_date; reason; dry_run: boolean }
export interface RecollectResponse { dry_run; interface_name; service; lane; start_date; end_date; operation_id: string|null; status: string|null; error: string|null }
export interface ProductCheckRequest { interface_name; product_name: string; product_id: string; reason }
export interface DiffItem { field: string; remote: unknown; stored: unknown }
export interface ProductCheckResponse { operation_id; status: 'ok'|'error'; remote: Record<string, unknown>|null; stored: Record<string, unknown>|null; diff: DiffItem[]; error: string|null }
export interface ConfigSummary { path; version: number; services: string[]; interface_names: string[]; deleted: boolean; author; created_at }
export interface ConfigDocument extends ConfigSummary { latest: boolean; content: Record<string, unknown>; model_names: string[]; comment; based_on: number|null }
export interface ConfigVersion { version; author; comment; created_at; deleted; based_on }
export interface PublishRequest { content: Record<string, unknown>; comment: string; base_version: number; services?: string[]|null }
export interface CredentialRow { entry; kind: 'shared'|'interface'|'both'; services: Record<string, string[]|null>; versions: Record<string, number>; interfaces: string[] }
export interface CredentialWrite { fields: Record<string, string>; comment: string; expected_versions: Record<string, number> }
// calls (all through one fetch helper with showErrorAlert:false; non-2xx → ApiError):
getCapabilities(); listInterfaces(); listOperations(f: {offset?; size?; status?; operation_type?; interface_name?});
recollect(r); productCheck(r);
listConfigs(); getConfig(path); getConfigVersion(path, v); getConfigHistory(path); getConfigDiff(path, from?, to?);
publishConfig(path, body) → {document: ConfigDocument; created: boolean}; rollbackConfig(path, {version, comment, base_version});
listCredentials(); writeCredential(entry, body) → {entry; versions}
// config paths are sent with literal '/' (each segment encodeURIComponent'd, joined with '/')

// capabilities.tsx
export function CapabilitiesProvider({children}): JSX.Element   // loads once; Loading/Alert states
export function useCapabilities(): Capabilities
export function FeatureGate({feature, children}): JSX.Element   // Alert "Not available on this deployment" when off
export function canDo(caps: Capabilities, verb): boolean
```

- [ ] **Step 1: Failing tests** — `apiBase` for Admin/Editor/Viewer/undefined; every call's
  method, URL and body (getBackendSrv mocked as docker-jobs-app `api.test.ts`); config path
  `odata/sub/a.json` → `/configs/odata/sub/a.json` and a segment with a space encoded;
  `errorMessage` for each body shape incl. the 502 credentials body and FastAPI 422 lists;
  `ApiError.operationId` taken from a 503/504 body; `publishConfig` 201 → `created: true`;
  `FeatureGate` shows the Alert when off, children when on; `CapabilitiesProvider` shows an
  error Alert when capabilities fail.
- [ ] **Step 2–4:** FAIL → implement → PASS (full checks). **Step 5:** Commit `feat: :sparkles: Talk to the operator backend`.

### Task 2: Shared UI pieces

**Files:** Create `src/components/JsonEditModal.tsx`, `src/components/StatusBadge.tsx`,
`src/components/EditableTable.tsx`, `src/components/SecretFieldsEditor.tsx` (used in Task 8),
`src/utils.ts`; Tests next to them.

**Interfaces — Produces:**

```ts
// JsonEditModal: Dataflow's "Edit record as JSON" modal
export function JsonEditModal(props: { title: string; value: unknown; isOpen: boolean;
  onApply(value: unknown): void; onDismiss(): void }): JSX.Element
  // 20-row monospace TextArea with JSON.stringify(value, null, 2); Apply parses; invalid → inline
  // <Alert severity="error" title="Invalid JSON"> and the modal stays open
export function StatusBadge({status}: {status: string|null}): JSX.Element   // PUBLISHED blue, RUNNING orange, DONE green, FAILED red
// EditableTable: the Dataflow table machinery on InteractiveTable
export interface EditableColumn<T> { id: string; header: string; cell(row: T, index: number): ReactNode;
  hideable?: boolean; filter?: { kind: 'text'|'multi'; options?: string[]; value(row: T): string } }
export function EditableTable<T>(props: { rows: T[]; rowId(row: T): string; columns: Array<EditableColumn<T>>;
  rowActions?(row: T, index: number): ReactNode; selectable?: boolean;
  renderSelectionActions?(selected: number[], clear: () => void): ReactNode; pageSize?: number }): JSX.Element
  // filters row ("Filters", text = case-insensitive substring, multi = MultiCombobox), "Clear filters";
  // "Columns visibility" MultiCombobox with enableAllOption over hideable columns;
  // leading Checkbox column with page select-all (indeterminate) when selectable;
  // batch bar "<n> selected" + renderSelectionActions; filters read the CURRENT rows (not a snapshot);
  // pagination: InteractiveTable pageSize (default 15) + "Rows per page" Combobox [10, 15, 25, 50, 100];
  // `index` passed to cell/rowActions is the index in `rows` (stable under filter/paging)
export function formatUtc(value: string|null): string        // "2026-10-16 10:00:00Z" or "—"
```

- [ ] **Step 1: Failing tests** — JsonEditModal apply/invalid/cancel; StatusBadge colours;
  EditableTable: text filter matches edited values, multi filter, clear filters, column hidden
  via visibility picker, select all on page + batch bar count, row index stable when filtered
  and on page 2, "Rows per page" changes the page size.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Add the editing table and modals`.

### Task 3: Operations page

**Files:** Create `src/pages/Operations.tsx` (+ test); route `operations` (default).

Behaviour (maas-dm-app `Operations.tsx` pattern): InteractiveTable of `listOperations({size: 200, …})`
newest first; columns requested at, type, status (StatusBadge), interface / config path /
credential entry (whichever is set), requested by, updated at; expanded row: every non-null
field as `<dl>` + `details` in `<pre>`; filters: type RadioButtonGroup (all five types on admin,
RECOLLECT/PRODUCT_CHECK on public), status RadioButtonGroup, interface Input; "Auto refresh"
InlineSwitch (15 s) + Refresh button; `?interface=` and `?operation=` query params preset the
interface filter / expand that row. Empty filters are sent as absent.

- [ ] **Step 1: Failing tests** — rows rendered with badges; type options per profile; filters
  passed to `listOperations`; auto refresh polls (jest fake timers) and stops when switched off;
  `?operation=<id>` expands that row; error → inline Alert.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Follow operations`.

### Task 4: Recollect page

**Files:** Create `src/pages/Recollect.tsx` (+ test); route `recollect`.

Behaviour (maas-dm-app `Recollect.tsx`, generalised): interface Combobox from `listInterfaces()`
keeping `replayable && enabled && worker_service`, option description = path + service; From/To
`DateTimePicker`s (default now-6h / now-1h, or `?interface=&from=&to=` query params); required
reason Input; info Alert with `max_window_hours` and the lane the window will use (≤ 24 h short);
**Check** (dry run) → **Recollect** enabled only after a passing check of the current values,
opens ConfirmModal, then sends `dry_run:false`; result Alert with operation id and a link to
`operations?operation=<id>`; 503 body shown with its error and operation id. Hidden write
controls when `!allowed.recollect` (Alert "Your role cannot recollect").

- [ ] **Step 1: Failing tests** — only replayable/enabled/served interfaces offered; check →
  recollect → confirm sends the two requests with `toISOString()` dates; editing after a check
  disables Recollect again; 422 detail shown; 503 shows error + operation link; query params
  prefill; Viewer: no Check/Recollect buttons enabled and no request sent.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Recollect an interface`.

### Task 5: Product check page

**Files:** Create `src/pages/ProductCheck.tsx` (+ test); route `product-check`.

Behaviour: interface Combobox (`checkable && enabled && worker_service`); RadioButtonGroup
"By name | By id" + Input; required reason; **Check** (busy while waiting; disabled while busy);
result: Alert summary — "Missing on the interface and in maas" / "Only in maas" /
"Only on the interface" / "Identical" / "<n> differences"; a table field | interface | maas with
the differing rows highlighted (diff fields first, then the rest of the union of keys); error
reply (`status:"error"`) → error Alert with the worker's message; 504 → "No reply from the
<service> worker" + operation id; **Recollect around this product** (shown when the interface
copy is missing in maas or differs, and `allowed.recollect`): link to
`recollect?interface=<name>&from=<t-1h>&to=<t+1h>` where `t` is the first value among the
remote (else stored) fields whose key matches `/publication/i`, else `/date/i`, that parses as
an ISO date; no date → link without from/to.

- [ ] **Step 1: Failing tests** — each summary case from fixture replies; diff rows first and
  highlighted; error reply; 504 with operation id; double click sends one request; recollect
  link built from `PublicationDate` ±1 h and absent when not allowed.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Check a product against its interface`.

### Task 6: Configuration — files and the entry editor

**Files:** Create `src/pages/Configuration.tsx` (list + routing), `src/pages/ConfigFile.tsx`
(editor), `src/config/draft.ts` (pure draft logic), tests for each; routes
`configuration` and `configuration/file/*` (path after `file/`).

**Interfaces — Produces** (`draft.ts`, used by Task 7 too):

```ts
export interface Entry { _id: string; value: Record<string, unknown> }   // _id client-side only, never sent
export interface Draft { base: ConfigDocument; entries: Entry[]; rest: Record<string, unknown>; services: string[] }
export function toDraft(doc: ConfigDocument): Draft          // entries = content.collectors, rest = content minus collectors
export function toContent(d: Draft): Record<string, unknown>  // {...rest, collectors: entries.map(e => e.value)}
export interface Change { kind: 'added'|'removed'|'changed'; interface: string; fields: string[] }
export function changes(d: Draft): { entries: Change[]; restChanged: boolean; servicesChanged: boolean }
  // matched by interface_name (then position for unnamed entries)
export function isDirty(d: Draft): boolean
```

Behaviour:
- **List** (`Configuration`): InteractiveTable of `listConfigs()` — path (link), services, interfaces count,
  version, deleted badge, author, created at; text filter on path/interface; **New file** (needs
  `config_write`): Modal with path Input (`.json`), services MultiCombobox (union of known
  services, custom values allowed), optional "Copy from existing file" Combobox (`isClearable`),
  comment → `publishConfig(path, {content: source?.content ?? {collectors: []}, comment,
  base_version: 0, services})` → navigate to the file.
- **Editor** (`ConfigFile`, Dataflow UX): header — path, version, services (MultiCombobox,
  editable), author/created at, **History** link (Task 7); a deleted file shows an Alert and
  offers only History. `EditableTable` of entries, every cell always an editor:
  `interface_name` Input, `class` Combobox (classes found in all files + custom), `enabled`
  Switch (missing = true; writes `true/false`), `interface_credentials` Combobox (entries from
  `listCredentials()` when allowed, else free Input; empty clears the key); row actions
  `minus-circle` Remove, `pen` Edit as JSON (JsonEditModal on the whole entry), `copy`
  Duplicate below (name suffixed `_copy`) — all local; **Add entry** opens JsonEditModal on
  `{class: <class of the first entry>, interface_name: ""}`; selection batch: "Set enabled"
  Switch + Apply, "Set credentials" Combobox + Apply, Remove selected; filters: name (text),
  class (multi), enabled (multi true/false); "Other sections" collapsible JSON TextArea for
  `rest` (models, amqp) with inline parse error; a "n unsaved changes" Badge when dirty;
  **Discard** (ConfirmModal) → back to base; **Review & save** (disabled when clean or
  `!allowed.config_write`) → Modal listing `changes()` + required comment → `publishConfig`
  with `base_version: base.version`, `services` only when changed → on success `toDraft(response)`
  + success Alert "Saved as version N"; 422/400 → error Alert with detail, draft kept;
  **409** → Alert "Version N was published meanwhile by <author>" with **Reload** (drop edits)
  and **Keep my edits** (fetch the new base, keep entries/rest/services, set base to it, reopen
  review); `beforeunload` warning while dirty.
- Public profile or `!features.configs`: FeatureGate Alert, no request.

- [ ] **Step 1: Failing tests** — `draft.ts` units (toDraft/toContent round trip keeps every
  unknown field; changes for add/remove/rename/field edit/rest/services); list: new file
  request with base 0 and services; copy-from content; editor: cell edits mark dirty and the
  Badge counts; Edit as JSON applies; duplicate inserts below; batch enabled/credentials;
  filter by class; review shows the changes and requires a comment; save sends base_version
  and toContent; **409 → Reload drops edits; Keep my edits rebases base_version and keeps the
  entries**; 422 detail shown and draft kept; Viewer (no config_write): no Save, no request;
  public: Alert and no `/configs` call.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Edit collector configurations`.

### Task 7: Configuration history and rollback

**Files:** Create `src/pages/ConfigHistory.tsx` (+ test), `src/components/DiffView.tsx` (+ test);
route `configuration/history/*`.

Behaviour: InteractiveTable of `getConfigHistory(path)` newest first (version, author, comment,
created at, deleted badge, based on); selecting a version shows `getConfigDiff(path, v-1, v)` in
`DiffView` (unified diff rendered in a monospace block, `+` lines green, `-` lines red, `@@`
dim; "No change" when empty; version 1 → diff against nothing: show the full version as
JSON via `getConfigVersion`); **Roll back to this version** (needs `config_write`, not on the
current version): ConfirmModal with required comment, warning text when the target version is
a deletion ("this deletes the file again"), → `rollbackConfig(path, {version, comment,
base_version: current})`; 409 → Alert + reload history.

- [ ] **Step 1: Failing tests** — DiffView colouring and empty case; history newest first;
  diff requested for v-1..v; v1 shows the JSON; rollback sends base_version = current;
  tombstone warning; 409 reloads; Viewer: no rollback button.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Review history and roll back`.

### Task 8: Credentials page

**Files:** Create `src/pages/Credentials.tsx` (+ test); `SecretFieldsEditor` from Task 2; route `credentials`.

Behaviour: InteractiveTable of `listCredentials()` — entry, kind Badge (shared/interface/both),
interfaces (truncated list), one cell per service: "✓ username, password" or a red "missing"
Badge; text filter on entry/interface; expanding/selecting an entry opens the editor panel:
`SecretFieldsEditor` rows (field name Input prefilled with the union of known field names,
value `SecretInput`/password Input, empty means "not changed" and is not sent; **Add field**;
remove row), services shown with their current version, required comment, **Write**
(needs `credential_write`) → ConfirmModal "Write <n> field(s) to <services>" →
`writeCredential(entry, {fields, comment, expected_versions: row.versions})`; success →
values cleared, list reloaded, Alert with new versions; **409** → Alert "changed meanwhile" +
reload, values kept; **502 `{written, failed, error}`** → error Alert naming written and failed
services + reload, values kept until the Alert is dismissed; 422 list → field-level messages;
values never rendered back, never in console, never in storage.

- [ ] **Step 1: Failing tests** — rows with kinds and missing badges; only non-empty values
  sent and expected_versions = row.versions; success clears values and reloads; 409 keeps
  values; 502 names written/failed; a planted secret string never appears in the DOM after
  success nor in a `console` spy; Admin-only: Editor sees no Write; public: Alert, no call.
- [ ] **Step 2–4:** FAIL → implement → PASS. **Step 5:** Commit `feat: :sparkles: Write collector credentials`.

### Task 9: Docs, packaging, branch review

- [ ] README (what the app does; pages × roles × profiles table; settings; proxy routes and
  `X-Maas-Roles`; `GF_DATAPROXY_SEND_USER_HEADER=true`, `GF_DATAPROXY_TIMEOUT ≥ 45` so a 30 s
  product check is not cut by Grafana; local dev with docker compose + the backend on 18000;
  packaging), CHANGELOG `## Version 0.1.0`.
- [ ] `./scripts/package.sh` produces `maas-operator-app-0.1.0.zip` + `.sha256`; `docker build`
  only if the local docker context works (state the result).
- [ ] Full checks green; superpowers:requesting-code-review on the whole branch; fix confirmed
  findings; superpowers:finishing-a-development-branch (base `develop`; pushing is the user's call).

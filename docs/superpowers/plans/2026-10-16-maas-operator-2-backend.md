# MAAS operator — part 2: maas-model merge + maas-operator-backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A FastAPI service, deployable as an `admin` or a `public` profile, that lets
Grafana operators recollect interfaces, check products, edit collector configurations
and write collector credentials, recording every action in `maas-operation`.

**Architecture:** First make one maas-model build carry both `fastapigen`/`authz` and
`vault` (merge `origin/develop` into the fastapigen branch). Then a new repo
`/home/aagazzi/work/maas-operator-backend` in the layout of
`download-manager/download-manager-backend` (settings, opensearch, messaging, store,
actions, committed `generated/`), with the header-principal pattern of
`ocs-operator-backend` (`principal.py`, `install_authz`). **No Cerbos for now** (user
decision): roles are checked against a fixed table in code; the generated routers get
maas-model's `AllowAllAuthorizer`. The backend imports maas-collector for
`ConfigStore`, validation and the operator contract.

**Tech Stack:** Python 3.11, FastAPI ≥0.115, pydantic v2 + pydantic-settings, kombu,
opensearch-py via maas-model, hvac via maas-model[vault], maas-model[api] (fastapigen,
authz), maas-collector (3.13.0 dev, from develop), pytest + tox.

**Spec:** `/home/aagazzi/work/maas-collector/docs/superpowers/specs/2026-10-16-maas-operator-design.md`
(sections 2, "Message and record contract", "Carried from part 1").

## Global Constraints

- Backend repo: new local git repo `/home/aagazzi/work/maas-operator-backend`, git-flow
  (`main` + `develop`, features on `feature/*`); package `maas_operator_backend`; no
  remote is created by this plan.
- maas-model and maas-collector come from sibling checkouts in tox, as
  ocs-operator-backend does: `-e {env:MAAS_MODEL_PATH:{toxinidir}/../maas-model-operator}[api,vault]`
  and `-e {env:MAAS_COLLECTOR_PATH:{toxinidir}/../maas-collector}`.
- API under `/api/v1`; `GET /health` outside it.
- Headers: `X-Grafana-User` (missing → 401), `X-Maas-Roles` (comma list; default
  `DEFAULT_ROLES=user`). Personas `user`, `operator`, `admin`.
- No Cerbos (user decision): no `cerbos/` folder, no `AUTHZ_MODE`/`CERBOS_URL`
  settings, no Cerbos container. Permissions are a fixed table in `principal.py`:
  `user` → `read`, `check`; `operator` → also `recollect`, `config_write`;
  `admin` → also `credential_write`. Unknown roles grant nothing.
- Profiles `OPERATOR_PROFILE=admin|public`. Public: config and credential routers
  are **not mounted**, no OpenBao client is built, recollect window ≤ 24 h, optional
  `RECOLLECT_INTERFACES` allowlist, `/operations` lists only `RECOLLECT` and
  `PRODUCT_CHECK`. Admin: window ≤ 2208 h (92 days).
- Operation records: index `maas-operation` (single index, no partition — the worker
  updates by id on that name), written in full with `status=PUBLISHED` **before**
  publishing; `operation_type` ∈ `RECOLLECT`, `PRODUCT_CHECK`, `CONFIG_PUBLISH`,
  `CONFIG_ROLLBACK`, `CREDENTIAL_WRITE`.
- Messages: use `maas_collector.rawdata.operator.contract` (`exchange()`,
  `replay_routing_key`, `replay_queue_name`, `check_routing_key`, `check_queue_name`,
  `lane_for`, `ReplayRequest`, `CheckRequest`) — never re-spell names. `CheckRequest`
  bodies carry both `product_name` and `product_id`; `""` means absent.
- Product check waits `CHECK_TIMEOUT=30` s for the reply → 504 with a clear message.
- `reason` (recollect, check) and `comment` (config, credential writes) are required,
  1..2000 chars.
- Secret values never appear in any response or log.
- OpenBao layout: KV v2 mount `VAULT_KV_MOUNT=ocs`, service prefix
  `f"{VAULT_PATH_ROOT}/{service}"` with `VAULT_PATH_ROOT=apps/maas-collector`, entry
  path `<prefix>/<entry name>`.
- Commit messages end with the Co-Authored-By line the committing session's system
  reminder gives.

## Review Focus

- Two operators edit the same config file: the second `PUT` carries the version it
  started from and gets 409 naming the newer version, never a silent overwrite. Test
  in Task 9.
- A credential write races another writer (or the entry was created meanwhile):
  check-and-set on the KV version → 409, nothing half-written across services; a
  write that fails on the 2nd of 3 services reports which services were written.
  Test in Task 10.
- The worker for the interface's service is not deployed (no queue bound): the
  record goes `FAILED` with "no worker", the API answers 503, not 200. Test in Task 6.
- A recollect with naive or non-UTC datetimes, `end` in the future, or a window of
  exactly 24 h on public → UTC normalised; future refused; exactly 24 h accepted on
  public (lane `short`). Test in Task 6.
- Public deployment: every admin route answers 404 even with `X-Maas-Roles: admin`,
  and building the app with `OPERATOR_PROFILE=public` never touches OpenBao settings.
  Test in Task 11.

---

### Task 0: Merge maas-model develop into the fastapigen branch

**Files (in a new maas-model worktree):** `setup.cfg`, `tox.ini`.

- [ ] **Step 1:** In `/home/aagazzi/work/maas-model`, create a new worktree
  `/home/aagazzi/work/maas-model-operator` on the existing branch
  `feature/DEVOCS-XXX-fastapigen-crud-api-cerbos-authz`. Leave the existing
  `maas-model-fastapigen` (detached) and `maas-model-vault` worktrees alone — they are
  not ours.
- [ ] **Step 2:** `git fetch origin`, then `git merge --no-ff origin/develop` (**not**
  the local `develop`, which carries another session's unpushed commits).
  Expected: one conflict, `setup.cfg` extras.
- [ ] **Step 3:** Resolve by keeping both extras, `api` then `vault`; add `vault` to
  `tox.ini` extras (`testing`, `api`, `vault`).
- [ ] **Step 4:** Run maas-model's tests in a fresh venv:
  `python3.11 -m venv .venv && .venv/bin/pip install -e '.[testing,api,vault]' && .venv/bin/pytest -q`.
  Expected: all pass (authz, fastapigen, vault suites).
- [ ] **Step 5:** Commit the merge (`Merge remote-tracking branch 'origin/develop' into
  feature/DEVOCS-XXX-fastapigen-crud-api-cerbos-authz`). **Do not push** — pushing a
  shared branch is the user's call; ask them.
- [ ] **Step 6:** Tell session `maas-collector-23` the merge is committed at
  `/home/aagazzi/work/maas-model-operator` (its ocs-operator-backend tox can set
  `MAAS_MODEL_PATH` to it).

### Task 1: Repo scaffold, settings, health

**Files:**
- Create: `pyproject.toml`, `tox.ini`, `.gitignore`, `.env.example`, `README.md`,
  `CHANGELOG.md`, `Dockerfile`, `src/maas_operator_backend/{__init__,settings,opensearch,main}.py`
- Test: `tests/conftest.py`, `tests/test_settings.py`, `tests/test_health.py`

**Interfaces — Produces:**

```python
# settings.py
Profile = Literal["admin", "public"]
class Settings(BaseSettings):            # env_file=".env", extra="ignore"; env = upper-case field name
    operator_profile: Profile = "admin"
    root_path: str = ""
    es_url: str = "http://localhost:9200"; es_username: str = ""; es_password: str = ""   # ES_PASSWORD_FILE wins
    es_timeout: int = 30; ignore_certs_verification: bool = False
    amqp_url: str = "amqp://localhost:5672"; amqp_username: str = ""; amqp_password: str = ""  # AMQP_PASSWORD_FILE wins
    user_header: str = "X-Grafana-User"; roles_header: str = "X-Maas-Roles"
    default_roles: Annotated[List[str], NoDecode] = ["user"]
    config_index: str = "maas-collector-config"; operation_index: str = "maas-operation"
    worker_services: Annotated[List[str], NoDecode] = ["odata", "jirax"]
    recollect_interfaces: Annotated[List[str], NoDecode] = []    # empty = all
    recollect_max_hours: int | None = None                         # None → 24 public / 2208 admin
    check_timeout: float = 30.0
    vault_url: str = ""; vault_kv_mount: str = "ocs"; vault_auth_method: str = "approle"
    vault_role_id: str = ""; vault_secret_id_file: str = ""; vault_token_file: str = ""
    vault_path_root: str = "apps/maas-collector"
    @property
    def max_window(self) -> datetime.timedelta
@functools.lru_cache
def get_settings() -> Settings

# opensearch.py
def connect(settings: Settings) -> None        # as download-manager-backend/opensearch.py

# main.py
API_PREFIX = "/api/v1"
def create_app(settings: Settings | None = None, connect: bool = True,
               vault: "CredentialStore | None" = None) -> FastAPI
app = create_app()
```
  Comma lists (`DEFAULT_ROLES`, `WORKER_SERVICES`, `RECOLLECT_INTERFACES`) split by a
  `mode="before"` validator as in DM's `recollect_interfaces`.

- [ ] **Step 1:** `git init`, `main` + `develop` (git-flow), `feature/backend` from
  `develop`. pyproject per DM (setuptools_scm, `requires-python >=3.11`, deps
  `maas-model[api,vault]`, `maas-collector`, `fastapi>=0.115`, `uvicorn[standard]>=0.32`,
  `pydantic>=2.9`, `pydantic-settings>=2.7`, `kombu>=5.3`, `httpx`; extra `testing`;
  `[project.scripts] maas-operator-backend = "maas_operator_backend.cli:main"`;
  coverage omits `*/generated/*`). tox per OCS with the two sibling paths of the
  Global Constraints. Dockerfile per DM (wheel install, non-root, `/health`
  healthcheck, uvicorn `--proxy-headers`).
- [ ] **Step 2: Failing tests**

```python
def test_profile_defaults_give_the_window():
    assert Settings(_env_file=None).max_window == timedelta(hours=2208)
    assert Settings(_env_file=None, operator_profile="public").max_window == timedelta(hours=24)
    assert Settings(_env_file=None, operator_profile="public", recollect_max_hours=6).max_window == timedelta(hours=6)
def test_comma_lists_are_split(monkeypatch):
    monkeypatch.setenv("WORKER_SERVICES", "odata, jirax ,mpip")
    assert Settings(_env_file=None).worker_services == ["odata", "jirax", "mpip"]
def test_password_file_wins(tmp_path, monkeypatch): ...      # ES_PASSWORD_FILE and AMQP_PASSWORD_FILE
def test_unknown_profile_is_refused(): pytest.raises(ValidationError)
def test_health(client): assert client.get("/health").json() == {"status": "ok"}
```
  `conftest.py`: `settings` fixture (`Settings(_env_file=None)`),
  `client` = `TestClient(create_app(settings, connect=False), headers={"X-Grafana-User": "alice", "X-Maas-Roles": "operator"})`.
- [ ] **Step 3:** Run → FAIL. **Step 4:** implement. **Step 5:** `tox` → PASS.
- [ ] **Step 6:** Commit `feat: :tada: Start the maas operator backend`.

### Task 2: Operation records template, generated code, `init-indices`

**Files:**
- Create: `src/maas_operator_backend/resources/templates/api/maas-operation_template.json`,
  `scripts/generate.sh`, `src/maas_operator_backend/generated/**` (generator output,
  committed), `src/maas_operator_backend/cli.py`
- Test: `tests/test_generated.py`, `tests/test_cli.py`

**Interfaces — Produces:** generated `MaasOperationDocument` (pygen, in
`generated/documents.py`), schema `MaasOperation`, router `generated.routers.router`
(`/maas-operation/…`, actions `maas-operation:list|read|create|update|delete`);
`cli.main(argv: list[str] | None = None) -> int` with command `init-indices` (puts the
index template and creates `maas-operation` if missing; idempotent).

Template `index_patterns: ["maas-operation"]`, 1 shard, **no** `partition_field`.
Properties (keyword unless noted): `operation_type`, `status` (PUBLISHED | RUNNING |
DONE | FAILED), `requested_by`, `interface_name`, `service`, `lane`, `exchange`,
`routing_key`, `product_name`, `product_id`, `config_path`, `config_version` (integer),
`credential_entry`, `credential_services`; dates (`date_time`, utc) `requested_at`,
`updated_at`, `start_date`, `end_date`; text `reason`, `details`; integer `exit_code`.

- [ ] **Step 1: Failing tests** — `test_the_generated_router_exposes_list_and_get`
  (paths `/maas-operation/` GET and `/maas-operation/{item_id}` GET present),
  `test_generated_document_maps_every_template_field`,
  `test_init_indices_is_idempotent` (fake client: first run creates, second run no-op,
  exit 0 both), `test_the_template_has_no_partition`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** template + `generate.sh` (as OCS's, output
  `src/maas_operator_backend/generated`, package `maas_operator_backend.generated`,
  `--document-module maas_operator_backend.generated.documents`, no `--policies`) +
  run it + `cli.py`. **Step 4:** PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: Generate the operation record API`.

### Task 3: Principal, role table, capabilities

**Files:**
- Create: `src/maas_operator_backend/principal.py`, `src/maas_operator_backend/capabilities.py`
- Modify: `main.py` (`install_authz(app, principal=current_principal, authorizer=AllowAllAuthorizer())`
  for the generated routers; mount the generated router — admin profile only — and capabilities)
- Test: `tests/test_principal.py`, `tests/test_capabilities.py`

**Interfaces — Produces:**

```python
VERBS = ("read", "check", "recollect", "config_write", "credential_write")
ROLE_PERMISSIONS: dict[str, frozenset[str]] = {
    "user": frozenset({"read", "check"}),
    "operator": frozenset({"read", "check", "recollect", "config_write"}),
    "admin": frozenset(VERBS),
}
def current_principal(request: Request, settings: Settings = Depends(get_settings)) -> Principal   # maas_model.authz.Principal; 401 without user header
@dataclass(frozen=True)
class Caller:
    principal: Principal
    def can(self, verb: str) -> bool              # union of the principal's roles in ROLE_PERMISSIONS
    def require(self, verb: str) -> None          # 403 "<verb> denied for roles <roles>"
    def allowed(self) -> dict[str, bool]          # {verb: can(verb)} for every VERBS
def get_caller(principal: Principal = Depends(current_principal)) -> Caller
# GET /api/v1/capabilities -> {"profile", "user", "roles", "max_window_hours",
#     "recollect_interfaces", "features": {"recollect", "product_check", "configs", "credentials", "operations"}, "allowed": {verb: bool}}
```
  `features.configs`/`credentials` are False on public. The `AllowAllAuthorizer`
  logs maas-model's "DEV/TEST ONLY" warning: silence it with a comment-documented
  logger filter or accept it — say which in the report; the generated routes are
  read-only and mounted on admin only.

- [ ] **Step 1: Failing tests** — header → Principal; default roles; 401;
  `test_role_table` parametrized over user/operator/admin/`stranger` × the 5 verbs
  (expected per Global Constraints; `stranger` gets nothing); `require` 403 message
  names the verb; a principal with roles `user,operator` gets the union;
  `test_capabilities_of_a_public_viewer` (features.configs False, allowed.recollect
  False); `test_capabilities_of_an_admin_profile`.
- [ ] **Step 2:** Run → FAIL. **Step 3:** implement. **Step 4:** PASS; `tox`.
- [ ] **Step 5:** README "Who is calling" + the role table. Commit
  `feat: :lock: Check operator roles`.

### Task 4: Operation store and publisher

**Files:**
- Create: `src/maas_operator_backend/store.py`, `src/maas_operator_backend/messaging.py`
- Test: `tests/test_store.py`, `tests/test_messaging.py`, `tests/fakes.py`

**Interfaces — Produces:**

```python
# store.py
PUBLISHED, RUNNING, DONE, FAILED = "PUBLISHED", "RUNNING", "DONE", "FAILED"
@dataclass
class OperationRecord:
    operation_id: str; operation_type: str; status: str; requested_by: str; requested_at: datetime
    reason: str | None = None; interface_name: str | None = None; service: str | None = None
    lane: str | None = None; exchange: str | None = None; routing_key: str | None = None
    start_date: datetime | None = None; end_date: datetime | None = None
    product_name: str | None = None; product_id: str | None = None
    config_path: str | None = None; config_version: int | None = None
    credential_entry: str | None = None; credential_services: list[str] | None = None
    details: str | None = None
class OperationStore:
    def save(self, record: OperationRecord) -> None                       # op_type="create", refresh="wait_for"; None fields omitted; updated_at = requested_at
    def finish(self, operation_id: str, status: str, details: str | None = None) -> None   # partial update + updated_at
    def list(self, offset: int, size: int, operation_types: Sequence[str] = (), status: str | None = None,
             interface_name: str | None = None) -> list[dict]             # newest requested_at first
def get_store() -> OperationStore

# messaging.py
class UnroutableMessage(Exception): ...
class CheckTimeout(Exception): ...
class Publisher:
    def __init__(self, settings: Settings, connection_factory: Callable[[], kombu.Connection] | None = None)
    def publish(self, body: dict, routing_key: str, declare: Sequence[kombu.Queue] = ()) -> None
        # persistent, mandatory, confirm_publish, retry policy as DM; returned → UnroutableMessage("... is the worker deployed?")
    def request(self, body: dict, routing_key: str, timeout: float) -> dict
        # exclusive auto-delete reply queue "maas-operator-reply-<uuid4>", reply_to + correlation_id;
        # drains until the matching correlation_id or timeout → CheckTimeout; unroutable → UnroutableMessage
def get_publisher(settings: Settings = Depends(get_settings)) -> Publisher
def record_then_publish(store, publisher, record: OperationRecord, body: dict, routing_key: str,
                        declare: Sequence[kombu.Queue] = ()) -> tuple[str, str | None]
    # DM's _record_then_publish semantics: save PUBLISHED, publish; on error finish(FAILED, message) and return (FAILED, message)
```

- [ ] **Step 1: Failing tests** — `FakeStore`/`FakePublisher` in `tests/fakes.py` as DM's
  conftest (publisher asserts the record exists first); messaging against kombu
  `memory://`: `test_publish_reaches_a_bound_queue`, `test_unbound_routing_key_is_unroutable`,
  `test_request_returns_the_reply_with_the_same_correlation_id` (a thread answers on
  `reply_to`), `test_request_ignores_a_reply_for_another_request`,
  `test_request_times_out` (0.2 s), `test_record_then_publish_marks_failed_on_unroutable`;
  store against a fake OpenSearch client: save omits None, list filters types and sorts.
- [ ] **Step 2:** Run → FAIL. **Step 3:** implement. **Step 4:** PASS; `tox`.
- [ ] **Step 5:** Commit `feat: :sparkles: Record operations and talk to the workers`.

### Task 5: Interfaces endpoint

**Files:** Create `src/maas_operator_backend/interfaces.py`; Test `tests/test_interfaces.py`;
copy maas-collector's `tests/fakeopensearch.py` into `tests/fakeopensearch.py` (header
comment naming its origin) so the real `ConfigStore` runs in tests.

**Interfaces — Produces:**

```python
REPLAYABLE = frozenset(REPLAY_MODULES)                  # from maas_collector.rawdata.operator.replay
CHECKABLE = frozenset({"ODataCollectorConfiguration"})
@dataclass(frozen=True)
class Interface:
    name: str; path: str; services: tuple[str, ...]; collector_class: str; enabled: bool
    credential_entry: str                                # interface_credentials or name
    replayable: bool; checkable: bool; worker_service: str | None   # first of services in settings.worker_services
def list_interfaces(store: ConfigStore, settings: Settings) -> list[Interface]   # latest, non-deleted docs; allowlist applied on public
def find_interface(store: ConfigStore, settings: Settings, name: str) -> Interface   # KeyError if absent
def get_config_store(settings: Settings = Depends(get_settings)) -> ConfigStore
# GET /api/v1/interfaces  (verb "read")
```

- [ ] **Step 1: Failing tests** — an odata entry shared by `["monitor", "odata"]` →
  `worker_service == "odata"`; a jira entry → replayable, not checkable; an s3 entry →
  neither; `"enabled": false` reported; tombstoned path absent; public allowlist hides
  others; `credential_entry` falls back to the name.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: List the interfaces operators can act on`.

### Task 6: Recollect

**Files:** Create `src/maas_operator_backend/actions.py` (router `actions`); Test `tests/test_recollect.py`.

**Interfaces — Consumes:** Tasks 3–5. **Produces:**

```python
class RecollectRequest(BaseModel):
    interface_name: str; start_date: datetime; end_date: datetime
    reason: str = Field(min_length=1, max_length=2000); dry_run: bool = True
class RecollectResponse(BaseModel):
    dry_run: bool; interface_name: str; service: str; lane: str; start_date: datetime; end_date: datetime
    operation_id: str | None = None; status: str | None = None; error: str | None = None
# POST /api/v1/actions/recollect   (verb "recollect")
```
  Order: `require("recollect")` → interface exists (404) → enabled, replayable,
  has `worker_service` (422 each, distinct messages) → dates to UTC (naive = UTC) →
  `start < end`, `end <= now`, `end - start <= settings.max_window` (422) → dry run
  returns here → `operation_id = uuid4()`, `lane = lane_for(start, end)`,
  body `ReplayRequest(...).to_body()`, `record_then_publish(..., replay_routing_key(service, lane), declare=[kombu.Queue(replay_queue_name(service, lane), exchange(), routing_key=…, durable=True)])`.
  FAILED → 503 with the message (body still carries `operation_id`).

- [ ] **Step 1: Failing tests** — publishes `maas.operator.odata.replay.short` with a
  `ReplayRequest`-parsable body whose dates end in `Z`; 30-day window → lane `long`;
  public 24 h exact → accepted `short`; public 24 h + 1 s → 422; naive dates taken as
  UTC; `+02:00` converted; future end → 422; disabled / non-replayable / no worker
  service → 422; unknown → 404; dry run records nothing; unroutable → record FAILED
  "is the worker deployed?" and 503; `X-Maas-Roles: user` → 403; missing reason → 422.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: Recollect an interface over a window`.

### Task 7: Product check

**Files:** Modify `actions.py`; Test `tests/test_product_check.py`.

**Interfaces — Produces:**

```python
class ProductCheckRequest(BaseModel):
    interface_name: str; product_name: str = ""; product_id: str = ""
    reason: str = Field(min_length=1, max_length=2000)     # exactly one of name/id non-blank → else 422
class ProductCheckResponse(BaseModel):
    operation_id: str; status: str; remote: dict | None; stored: dict | None
    diff: list[dict]; error: str | None
# POST /api/v1/actions/product-check   (verb "check")
```
  Interface must be checkable and enabled with a `worker_service` (422 otherwise).
  Save record (`PRODUCT_CHECK`, PUBLISHED), `publisher.request(CheckRequest(...).to_body(),
  check_routing_key(service), settings.check_timeout)`; reply `status == "ok"` →
  `finish(DONE)`, else `finish(FAILED, reply error)` and still 200 with the reply;
  `CheckTimeout` → `finish(FAILED, "no reply from the <service> worker within 30 s")`
  and 504; `UnroutableMessage` → FAILED and 503.

- [ ] **Step 1: Failing tests** — happy path returns diff, record DONE; worker error
  reply → 200, record FAILED with the error; timeout → 504 + FAILED; both name and id
  → 422; neither → 422; whitespace-only name → 422; non-checkable jira → 422.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: Check a product against its interface`.

### Task 8: Operations list

**Files:** Create `src/maas_operator_backend/operations.py`; Test `tests/test_operations.py`.

**Interfaces — Produces:** `GET /api/v1/operations?offset=0&size=50&status=&operation_type=&interface_name=`
(verb `read`, `offset` 0..9000, `size` 1..1000) returning `list[MaasOperation]`
(generated schema). Public profile always restricts to `RECOLLECT`, `PRODUCT_CHECK`; a
public request for another type returns `[]`.

- [ ] **Step 1: Failing tests** — admin sees all types; public sees only the two;
  public `operation_type=CREDENTIAL_WRITE` → `[]`; filters pass through to the store;
  newest first.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: List operations for each profile`.

### Task 9: Configuration routes (admin)

**Files:** Create `src/maas_operator_backend/configs.py`; Test `tests/test_configs.py`.

**Interfaces — Consumes:** `ConfigStore` (`latest`, `get`, `history`, `publish`,
`rollback`), `ConfigConflictError`, `ConfigNotFoundError`,
`maas_collector.rawdata.configprovider.ConfigValidationError`,
`maas_collector.rawdata.cli.config.diff_lines`. **Produces:**

```python
# all under /api/v1/configs ; GET → verb "read", writes → verb "config_write"
GET  /configs                          -> [{"path","version","services","interface_names","deleted","author","created_at"}]
GET  /configs/{path:path}              -> full document (latest, tombstone included)
GET  /configs/{path:path}/history      -> [{"version","author","comment","created_at","deleted","based_on"}]
GET  /configs/{path:path}/versions/{version} -> full document
GET  /configs/{path:path}/diff?from_version=&to_version=  -> {"diff": "<unified diff text>"}   # defaults as cli.config diff
PUT  /configs/{path:path}  {"content": {...}, "comment": str, "base_version": int, "services": [str] | None}
     -> written document ; base_version 0 = new path (services required)
POST /configs/{path:path}/rollback {"version": int, "comment": str, "base_version": int} -> written document
```
  Errors: `ConfigConflictError` → 409 (message names the newer version);
  `ConfigNotFoundError` → 404; `ConfigValidationError` → 422 with `.reason`; other
  `ValueError` (bad path/service) → 400. Every successful write saves a
  `CONFIG_PUBLISH`/`CONFIG_ROLLBACK` record (status DONE, `config_path`,
  `config_version`, `reason` = comment). Before publishing, a credential check runs:

```python
class CredentialCheck(Protocol):
    def __call__(self, content: dict, services: list[str], current: dict | None) -> None: ...
        # raises ValueError(message) → 422; `current` = content of the version being replaced, None for a new path
def get_credential_check() -> CredentialCheck      # dependency; Task 9 returns a no-op, Task 10 the real one
```

- [ ] **Step 1: Failing tests** (real `ConfigStore` on `tests/fakeopensearch.py`) —
  list/get/history/diff; publish v2 with `base_version=1` → 200, record saved;
  **two edits from v1: second → 409 "version 2"**; invalid content → 422 naming the
  entry; credential-file content (`{"interfaces": …}`) → 422; new path with
  `base_version=0` and services → 201/200; new path without services → 400; rollback
  to v1 → v3 equal to v1 content; operator role allowed, viewer 403.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: Edit collector configurations from the API`.

### Task 10: Credentials (admin)

**Files:** Create `src/maas_operator_backend/credentials.py`; Modify `configs.py`
(real credential check), `main.py` (build the store on admin only); Test
`tests/test_credentials.py`.

**Interfaces — Produces:**

```python
class CredentialConflict(Exception): ...
class CredentialStore:
    def __init__(self, client: maas_model.vault.VaultClient, kv_mount: str, path_root: str)
    def fields(self, service: str, entry: str) -> list[str] | None   # sorted keys, None if absent; never values
    def version(self, service: str, entry: str) -> int               # 0 if absent
    def write(self, service: str, entry: str, fields: dict[str, str], expected_version: int) -> int
        # read current data, merge `fields`, kv.v2.create_or_update_secret(..., cas=expected_version); CAS failure → CredentialConflict
def build_credential_store(settings: Settings) -> CredentialStore   # VaultClient(VaultSettings(...)); ValueError if vault_url empty
def get_credential_store() -> CredentialStore                        # dependency, overridden in create_app
# GET /api/v1/credentials  (verb "read") -> [{"entry", "services": {service: [field, ...] | null}, "interfaces": [name, ...]}]
#     entries = every credential_entry referenced by enabled+disabled interfaces; services = the interface docs' services
# PUT /api/v1/credentials/{entry}  (verb "credential_write")
#     {"fields": {name: value}, "comment": str, "expected_versions": {service: int}}
#     -> {"entry", "versions": {service: int}}
```
  Write order: refuse (409) up front if any service's current version differs from
  `expected_versions`; then write service by service; a failure after ≥1 write →
  502 `{"written": [...], "failed": service, "error": msg}` and a FAILED record; full
  success → record `CREDENTIAL_WRITE` DONE (`credential_entry`, `credential_services`,
  `reason` = comment) — no values in record or logs. Config check
  (`get_credential_check` now returns the real one): a `PUT /configs` that **changes**
  an entry's `interface_credentials` is refused (422) unless that entry exists under
  every service of the document. From this task on, `create_app` on the admin profile
  builds the store from settings unless `vault=` is passed; `tests/conftest.py` passes
  a `FakeCredentialStore` (in `tests/fakes.py`) so earlier tests keep running.

- [ ] **Step 1: Failing tests** — store against `maas_model.testing.fakevault.FakeVault`
  (write + read round trip; fields hide values); CAS via a stub hvac client
  (`create_or_update_secret` raising `hvac.exceptions.InvalidRequest` "check-and-set")
  → `CredentialConflict`; API: write to `odata` and `monitor` → versions for both;
  stale `expected_versions` → 409, nothing written; second service fails → 502 lists
  `written=["monitor"]`; response and caplog never contain a written value;
  operator (not admin) → 403; config PUT switching `interface_credentials` to an
  entry missing under `monitor` → 422; unchanged `interface_credentials` → no check.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`. **Step 5:** Commit
  `feat: :sparkles: Write collector credentials to OpenBao`.

### Task 11: Profiles end to end, docs

**Files:** Modify `main.py`, `README.md` (Routes table, Configuration env table,
profiles, local dev with sibling checkouts, `.env.example`), `CHANGELOG.md`
(`## Version 0.1.0`); Test `tests/test_profiles.py`.

- [ ] **Step 1: Failing tests** — public app: `/api/v1/configs`, `/api/v1/credentials`,
  `/api/v1/maas-operation/` → 404 with `X-Maas-Roles: admin`;
  `create_app(Settings(operator_profile="public", vault_url=""))` builds without
  touching OpenBao (patch `VaultClient` to raise if constructed); admin app with empty
  `vault_url` → startup error naming `VAULT_URL`; `/capabilities` matches the mounted
  routes on both profiles; `/api/v1/permissions` lists only mounted actions.
- [ ] **Step 2–4:** FAIL → implement → PASS; `tox`.
- [ ] **Step 5:** Commit `docs: :memo: Describe the two operator profiles`.

### Task 12: Branch review

- [ ] `tox` on the tip; superpowers:requesting-code-review on the whole branch; fix
  confirmed findings; superpowers:finishing-a-development-branch (merge
  `feature/backend` into the backend's `develop`; nothing is pushed — the repo has no
  remote yet).

# Design: OpenSearch and AMQP connections from OpenBao / Vault (maas-model, maas-collector, maas-engine)

> Status: approved 2026-10-12. Follows [the vault credentials design](2026-10-12-vault-credentials-design.md).

## Context

Phase 1 is on maas-collector branch `feature/vault-credentials`. It reads per-interface collector credentials from OpenBao / Vault ([credentialprovider.py](../../../src/maas_collector/rawdata/collector/credentialprovider.py)). Its spec explicitly deferred ES / AMQP.

Today both services take `ES_URL`, `ES_USERNAME`, `ES_PASSWORD`, `AMQP_URL`, `AMQP_USERNAME` and `AMQP_PASSWORD` from env vars, so the connection passwords live in the deployment configuration.

**Goal:** maas-collector and maas-engine read their whole OpenSearch and AMQP connection (url, username, password) from the vault. Both read the same secrets, so a password lives in one place. Env vars stay the default for local runs and tests.

**Decisions made with the user:**
- **Scope:** the url, username and password all come from the vault.
- **Rotation:** the settings are read **once at startup**. To rotate, write a new secret version, then run `docker service update --force`. Connections are not rebuilt live.
- **Shared code:** the vault client lives in **maas-model**, which both services already depend on. maas-engine does not depend on maas-collector; its `cli/args.py` is a forked copy.

## Vault layout and behaviour (same in both services)

Each connection is one KV v2 secret, shared by every MAAS service of a stack:

```text
<VAULT_KV_MOUNT>/<ES_VAULT_PATH>     e.g. secret/maas/cds/opensearch → {"url": ..., "username": ..., "password": ...}
<VAULT_KV_MOUNT>/<AMQP_VAULT_PATH>   e.g. secret/maas/cds/amqp       → {"url": ..., "username": ..., "password": ...}
```

| Option / env | Default | Purpose |
|---|---|---|
| `--es-vault-path` / `ES_VAULT_PATH` | empty | Secret holding the OpenSearch connection. Empty means the env vars are used, as today. |
| `--amqp-vault-path` / `AMQP_VAULT_PATH` | empty | Secret holding the AMQP connection. Empty means the env vars are used, as today. |

- The vault access uses the phase-1 settings: `VAULT_ADDR`, `VAULT_KV_MOUNT`, `VAULT_AUTH_METHOD` (approle or token), `VAULT_ROLE_ID`, `VAULT_SECRET_ID_FILE`, `VAULT_TOKEN_FILE`/`VAULT_TOKEN`, and `VAULT_CACERT`.
- The paths are full paths inside the mount. They do not use the collector-only `VAULT_PATH_PREFIX`.
- They do not depend on `CREDENTIAL_BACKEND`.

**Rules when a vault path is set:**
- The secret must exist and hold a non-empty `url`. `username` and `password` are optional.
- The secret's values replace `*_URL`, `*_USERNAME` and `*_PASSWORD`.
  - If one of these is also set, a warning names the ignored variable, never its value. This lets compose files migrate step by step.
- **Fail fast:** startup raises a clear error if the vault is unreachable or refuses the request, the secret is missing, or `url` is missing. A service cannot run without ES or AMQP, and Swarm restarts it.
- Non-secret options stay as env vars, for example `ES_TIMEOUT`, `AMQP_RETRIES`, `AMQP_HEARTBEAT` and `IGNORE_CERTS_VERIFICATION`.

## 1. maas-model: shared vault client

**Branch:** `feature/vault-client` from `develop`, in a separate git worktree.

**New `src/maas_model/vault.py`** (contents mostly moved out of the collector's `credentialprovider.py`):
- **`VaultSettings` dataclass:** `url`, `kv_mount="secret"`, `auth_method="approle"`, `role_id`, `secret_id_file`, `token_file`.
- **`VaultClient(settings)`:**
  - The constructor validates the settings (code moved from `VaultCredentialProvider.__init__`) and creates `hvac.Client(url=...)`.
  - `login()`: AppRole, re-reading the secret id file at each login; or a token from `token_file` or `VAULT_TOKEN`.
  - `read_secret(path) -> dict | None`:
    - Logs in lazily.
    - Calls `kv.v2.read_secret_version(..., raise_on_deleted_version=True)`.
    - On `Forbidden`, logs in again and retries once.
    - Returns `None` on `InvalidPath`.
    - Other hvac errors propagate unchanged, so the existing collector tests that expect `hvac.exceptions.*` keep passing.
- **`ConnectionSettings` dataclass:** `url`, `username`, `password` (`field(repr=False)`).
- **`read_connection(client, path, service) -> ConnectionSettings`:**
  - Raises `ValueError` when the secret is missing or has no `url`.
  - `service` (`"OpenSearch"` or `"AMQP"`) is used only in messages, which name the path and never a value.

**Supporting changes:**
- **Fake vault:** move `tests/fakevault.py` from the collector to **`src/maas_model/testing/fakevault.py`**. It is stdlib only. The model, collector and engine tests then share one fake instead of three copies.
- **`setup.cfg`:** add an extra on the model of the existing `api` extra: `vault = hvac>=2.3,<3.0`. The data-model install stays free of hvac.
- **Tests:** add `tests/test_vault.py`. It covers the AppRole login, the token from a file, `read_secret` returning `None` on 404, the re-login on an expired token, the settings validation errors, and `read_connection` (ok, missing secret, missing url, no password in `repr`).
- **Docs:** add a README section.

## 2. maas-collector (this branch, `feature/vault-credentials`)

- **[credentialprovider.py](../../../src/maas_collector/rawdata/collector/credentialprovider.py):**
  - `VaultArgs` becomes `VaultArgs(VaultSettings)`. It only adds `path_prefix`, so the existing imports and `CollectorArgs.vault` stay unchanged.
  - `VaultCredentialProvider` keeps a `VaultClient`. Its `_read_entries` calls `client.read_secret(f"{prefix}/{name}")`.
  - The login and retry code is removed from the collector.
- **[args.py](../../../src/maas_collector/rawdata/cli/lib/args.py):**
  - Add `--es-vault-path` to `es_parser()` and `--amqp-vault-path` to `amqp_parser()`.
  - When the path is set, `get_es_credentials_url()` and `get_amqp_credentials_url()` (lines 99 and 624):
    1. Build a `VaultClient(get_vault_args(namespace))`.
    2. Call `read_connection`.
    3. Warn about the overridden ES or AMQP env values.
    4. Feed the result into the existing `get_credentials_url()`.
  - Every collector CLI and `scripts/generate_interface.py` already go through `get_collector_args` and these two functions, so **no entry point changes**.
- **Leak fix:** [messenger.py:145](../../../src/maas_collector/rawdata/messenger.py#L145) logs the AMQP URL with its password at debug level. Mask the password.
- **`setup.cfg`:** replace `maas-model` and `hvac` with `maas-model[vault]`.
- **Tests:**
  - Use `maas_model.testing.fakevault` in `conftest.py` and delete `tests/fakevault.py`.
  - Add `tests/test_connection_args.py`:
    - The vault secrets are turned into `args.es_url` and `args.amqp_url`.
    - Env values are ignored, and the warning names the variable without its value.
    - A missing secret or a missing `url` raises.
    - With no path set, the URLs are exactly as today.
  - Add a messenger test: the debug log does not contain the password.
- **Docs:**
  - `docs/vault.md`: add an "OpenSearch and AMQP connections" section with the layout, `bao kv put`, a policy granting `read` on `secret/data/maas/<stack>/opensearch` and `/amqp` to both the collector and the engine AppRoles, a compose snippet, and the rotation-by-restart procedure.
  - `docs/common_options.md`: add the two new rows.
  - Copy this design to `docs/superpowers/specs/2026-10-12-vault-connections-design.md` and commit it first, as phase 1 did.

## 3. maas-engine

**Branch:** `feature/vault-connections` from `develop`. The current branch, `feature/configurable-report-exchange`, is clean and is left as is.

- **`src/maas_engine/cli/args.py`** (the fork of the collector's file):
  - Add a `vault_parser()` with the six `VAULT_*` options above. There is no path prefix, because the engine has no credential entries.
  - Add `--es-vault-path` and `--amqp-vault-path`.
  - Make `get_es_credentials_url()` and `get_amqp_credentials_url()` (lines 159 and 253) vault-aware, exactly as in the collector.
  - Add `vault_parser()` to the parents of `engine_parser()` (line 377).
- **`update/migration.py:536-540`:** add `vault_parser()` to the migration parser's parents.
- **Call sites:** all six URL call sites already use those two functions, so they need **no change**:
  - `consumer/engine_consumer.py:450,452`
  - `cli/run.py:71`
  - `update/migration.py:69`
  - `cli/backup_queue.py:62`
  - `cli/delete_all_queues.py:22`
  - `cli/publish_message_folder.py:48`
- **Merge first:** the unmerged branch `bugfix/mask-es-credentials-in-migration-log` fixes `migration.py:71`, which logs `es_url` with its credentials. Without it, the vault-sourced passwords would leak there.
- **`setup.cfg`:** use `maas-model[vault]`.
- **Tests:** add `tests/test_vault_connections.py` with the same cases as the collector's, using `maas_model.testing.fakevault`. Also check that `MaasEngineConsumer.main`'s namespace receives the vault URLs. kombu connects lazily, as in `test_amqp_heartbeat.py`.
- **Docs:** add a new `docs/vault.md` that is engine-specific and links to the collector page for the vault set-up. Add pointers in `docs/configuration.md` and the README option tables.

## Sequencing (test-first in each repo)

1. **maas-model:**
   - In the worktree: `vault.py`, the fake vault, the extra, and the tests.
   - Point the collector and engine `.venv`s at that worktree with an editable install (`pip install -e <worktree>[vault]`), and point them back at the end.
2. **maas-collector:** the spec doc, the refactor onto `VaultClient` (the existing vault tests must stay green), then the connection args, the messenger mask, and the docs.
3. **maas-engine:** the args, the migration parser, the tests, and the docs.
4. **Release order:**
   - Merge maas-model to `develop` first. CI publishes the dev build to Nexus, and the collector and engine CI resolve `maas-model` unpinned from there.
   - Their CI fails until that build exists.
5. Commits are made per repo. Nothing is pushed and no PR is opened without asking.

## Out of scope

- Live rotation of the ES / AMQP credentials. Restart is the agreed procedure.
- Editing the deployment compose files. That is a deployment change made after release; the docs give the snippet. Rotate the connection passwords after the migration.
- Inbox SFTP, S3 and backup passwords.
- The engine's `/environment` health endpoint, which uses a non-existent `amqp_connection` attribute (`health/health.py:46`). It is unrelated, so it gets noted, not fixed.

## Verification

- **Unit tests:**
  - `pytest` in maas-model, then in maas-collector (the full suite, including `test_credential_provider.py`, `test_credential_refresh.py` and `test_credentials_to_vault.py`, after the refactor), then in maas-engine.
  - All use the in-process fake vault. No real vault, ES or broker is needed.
- **Manual end-to-end check** against `docker run --rm -p 8200:8200 openbao/openbao server -dev`:
  1. `bao kv put -mount=secret maas/dev/opensearch url=... username=... password=...`, and the same for `maas/dev/amqp`.
  2. Run a collector (`python -m maas_collector.rawdata.cli.filesystem ...`) and `python -m maas_engine` with `ES_VAULT_PATH`, `AMQP_VAULT_PATH`, `VAULT_ADDR`, `VAULT_AUTH_METHOD=token` and `VAULT_TOKEN`, and no `ES_*` or `AMQP_*` credentials. Check that both reach OpenSearch and RabbitMQ.
  3. Run both again at `-vv` and grep the logs: the password must not appear.
  4. Delete the secret and start again: the service must exit with the "secret … missing" error.

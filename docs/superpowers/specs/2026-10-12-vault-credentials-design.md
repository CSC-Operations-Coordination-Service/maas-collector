# Design: read collector credentials from OpenBao / HashiCorp Vault

> Status: approved 2026-10-12. Implementation on branch `feature/vault-credentials`.

## Context

Today every collector reads its secrets from one JSON file (`--credential-file` / `CREDENTIAL_FILE`). `CredentialMixin` ([credentialmixin.py](src/maas_collector/rawdata/collector/credentialmixin.py)) loads it once in `FileCollector.load_config()` (`filecollector.py:386-388`). Each entry is keyed by `interface_credentials`, then `interface_name`, and every key in the entry is `setattr`'d onto the config.

In deployment the file arrives as a Docker Swarm secret. Rotating a secret means editing the file, updating the Swarm secret, and restarting the service.

**Goal:** collectors read the same per-interface entries from OpenBao or HashiCorp Vault (KV v2). They authenticate themselves, and pick up rotated secrets **without a restart**. The file backend stays the default, for tests, local runs and sites without a vault.

**Decisions made with the user:** OpenBao or HashiCorp Vault; a native in-app provider rather than an agent-rendered file; rotation without restart.

## Design

### 1. Pluggable provider: new `src/maas_collector/rawdata/collector/credentialprovider.py`

```python
class CredentialProvider(abc.ABC):
    @abc.abstractmethod
    def load(self, names: set[str]) -> dict[str, dict]:
        """Return {entry_name: {field: value}} for the requested names (missing names are omitted)."""
```

- **`FileCredentialProvider(path)`**: the current `load_credential_dict` logic, moved here. It ignores `names` and returns every entry.
- **`VaultCredentialProvider(client_factory, kv_mount, path_prefix)`**, using `hvac`, which also works with OpenBao:
  - **Storage layout:** one KV v2 secret per entry, at `<kv_mount>/<path_prefix>/<entry_name>`. The secret's data is exactly what a file entry holds today, minus `"name"`. This means no change to how configs consume credentials, and a 1:1 migration. Nested values (SimpleHttp's `credentials` dict) and base64 strings (`client_keyfile`) are stored as JSON values.
  - **Reads:** only the names the collector needs, via `kv.v2.read_secret_version(..., raise_on_deleted_version=True)`. A 404 (`hvac.exceptions.InvalidPath`) means "no entry", which matches today's warning-and-skip behaviour. Reading only these names keeps the policy read-only and least-privilege; no `list` capability is needed.
  - **Auth:**
    - `approle` (production): `VAULT_ROLE_ID` plus `VAULT_SECRET_ID_FILE`, where the file is a Swarm secret.
    - `token` (dev): `VAULT_TOKEN`, or `VAULT_TOKEN_FILE`.
    - Standard hvac env vars are reused: `VAULT_ADDR` and `VAULT_CACERT`.
  - **Token lifecycle:** log in lazily. Before each `load`, if `not client.is_authenticated()`, log in again. On `hvac.exceptions.Forbidden`, log in again and retry once.
- **`build_credential_provider(args) -> CredentialProvider | None`**: a factory that reads `args.credential_backend` (`file` | `vault`). It returns `None` when the backend is `file` and no file is set (today's "optional credential file").
- Keep `CredentialMixin.load_credential_dict()` as a thin wrapper over `FileCredentialProvider`, so `scripts/generate_interface.py` and the tests keep working.

### 2. Refresh without restart: changes to `CredentialMixin`

- `credential_targets(self) -> list`: the configs that receive credentials. By default this is `self.configs`.
- `load_credentials()` (called at setup):
  1. Collect the needed names: `interface_credentials` and `interface_name` of every target.
  2. Call `provider.load(names)`.
  3. Store the result as a snapshot, then apply it with the existing `set_credential_attributes`.
  4. **Fail fast** on any provider error, as a missing file does today.
- New `refresh_credentials()`:
  1. Return early unless `credential_refresh_period > 0` and that many seconds have passed since the last load.
  2. Load again and compare each entry with the snapshot.
  3. For each changed entry, apply it again to the configs that use it. Then call `self.on_credentials_changed(interface_names)`, a no-op hook by default. Log the changed **entry names only, never values**.
  4. On provider error, log the error, **keep the last-known-good credentials**, and retry at the next loop. A vault outage must not stop collection.
  5. If a field disappears from an entry, log a warning that it stays set until restart. This avoids tracking a per-config baseline.
- Call `self.refresh_credentials()` in `FileCollector.run()` (`filecollector.py:792-797`), right before `self.on_loop_start()`. It is called there rather than inside `on_loop_start`, so a subclass that overrides the template cannot skip it. It runs on the main thread between loops, so it never races with the monitor's `ThreadPool` probes.

**Why the rest needs no work:** FTP, ROSFTP, S3, JIRA, HTTP/OAuth, WebDAV and MPIP clients are all rebuilt from the config at each loop (see the exploration notes), so mutating the config in place is enough. Two exceptions:
- ~~**`ODataCollector.__auth_cache`**: override `on_credentials_changed` to evict the changed interfaces.~~ Dropped after review: the cache only serves `probe_item`, which only maas-cds calls. The monitor probes through `HttpCollector.probe`, which builds the authentication at each call.
- **`InterfaceMonitor`** (`monitorcollector.py:373,416-422`):
  - Replace its direct `load_credential_dict(self.args.credential_file)` call with the provider.
  - Override `credential_targets()` to return `[meta.config for meta in self.meta_dict.values()]` plus `self.configs`.
  - ~~On change, call the probed collector class's `on_credentials_changed`.~~ Dropped with the OData cache hook.
  - ~~Make the missing-entry case actually skip the interface.~~ Not done, deliberately: the monitor keeps probing an interface without a credential entry, as some interfaces need no credentials. The dead `KeyError` handling is removed instead.

### 3. Settings: [args.py](src/maas_collector/rawdata/cli/lib/args.py) `common_parser()` and `CollectorArgs`

Each option is set by a CLI flag or an env var through `EnvDefault`, next to `--credential-file` (1078-1086).

| Option / env | Default | Purpose |
|---|---|---|
| `--credential-backend` / `CREDENTIAL_BACKEND` | `file` | `file` or `vault` |
| `--credential-refresh-period` / `CREDENTIAL_REFRESH_PERIOD` | `300` | seconds between refreshes; `0` = startup only |
| `--vault-kv-mount` / `VAULT_KV_MOUNT` | `secret` | KV v2 mount point |
| `--vault-path-prefix` / `VAULT_PATH_PREFIX` | `maas-collector` | per-stack prefix, e.g. `maas-collector/cds` |
| `--vault-auth-method` / `VAULT_AUTH_METHOD` | `approle` | `approle` or `token` |
| `--vault-role-id` / `VAULT_ROLE_ID` | – | AppRole role id |
| `--vault-secret-id-file` / `VAULT_SECRET_ID_FILE` | – | path to the AppRole secret id (Swarm secret) |

- Add `credential_backend`, `credential_refresh_period` and a `vault: VaultArgs = None` dataclass to `CollectorArgs` (`filecollector.py:79`), on the model of `ReplayArgs`. Fill them in `get_collector_args()` (args.py:1117).
- Validate at startup: if the backend is `vault`, `VAULT_ADDR` must be set, plus the role id and secret id file for `approle`. A missing value raises `ValueError` with a clear message.
- Add `hvac>=2.3,<3` to `install_requires` in `setup.cfg`. It is small (it only needs `requests`), and both images need it, so it is not worth an optional extra.

### 4. Migration helper: new `scripts/credentials_to_vault.py`

- Reads an existing credentials JSON and writes each entry to `<mount>/<prefix>/<name>`.
- Has `--dry-run`, which prints entry names and field names only, never values.
- Used once per stack to move the existing credential files into the vault.

### 5. Docs: new `docs/vault.md`, plus a pointer in `docs/common_options.md`

- How to set up the vault:
  - Enable KV v2.
  - Add a read-only policy on `secret/data/maas-collector/<stack>/*`.
  - Create an AppRole with short token TTLs and optionally `secret_id_bound_cidrs`.
  - Deliver the secret id as a Swarm secret. This is the single "secret zero": scoped, revocable and audited.
- A Swarm compose snippet.
- How to rotate a secret: write a new version in the vault; collectors apply it within `CREDENTIAL_REFRESH_PERIOD` plus one loop.
- **Security note:** a credential file that was ever committed to a repository should be considered exposed. Rotate its secrets as part of the migration, then remove the file from that repository.

## Files

- **New:**
  - `src/maas_collector/rawdata/collector/credentialprovider.py`
  - `scripts/credentials_to_vault.py`
  - `docs/vault.md`
  - `tests/test_credential_provider.py`
  - `tests/test_credential_refresh.py`
- **Modified:**
  - `credentialmixin.py`: provider, snapshot, `refresh_credentials`, `credential_targets`, `on_credentials_changed`
  - `filecollector.py`: `CollectorArgs` fields, `refresh_credentials()` call in `run()`, `load_config` via the provider
  - `monitorcollector.py`: provider, `credential_targets`
  - ~~`odatacollector.py`: `on_credentials_changed` evicts `__auth_cache`~~ (dropped after review)
  - `cli/lib/args.py`: new options
  - `setup.cfg`: `hvac`
  - `docs/common_options.md`

## Out of scope (possible follow-ups)

- ES / AMQP / inbox-SFTP passwords from env vars (`args.py:36-53,120-137,287-305`). These are built into URLs at startup. A later phase could read them from a `_service` vault entry.
- S3 keys inside the rawdata `backup` block.
- Existing secret leaks in logs: some config dataclasses are not declared `repr=False` (MPIP, SimpleHttp, Loki…), `rosftpcollector.py:180` logs the full config, and S3 `document()` outputs `access_key`. Worth a separate fix, because vault-sourced secrets would leak the same way.

## Sequencing

1. Commit the current uncommitted interface-isolation work, together with `tests/test_interface_isolation.py`, separately first. It is related: a bad or rotated credential on one interface no longer stops the loop.
2. Copy this design to `docs/superpowers/specs/2026-10-12-vault-credentials-design.md`.
3. Implement test-first: provider, then refresh, then monitor and OData, then the CLI, then the script and docs.

## Verification

- **Unit tests** (`pytest tests/`). hvac is mocked; no real vault is needed.
  - `FileCredentialProvider` gives the same result as today's `load_credential_dict` on `tests/conf/test-maas-collector-credentials.json`.
  - `VaultCredentialProvider`:
    - reads only the requested names, and omits names that return 404;
    - logs in with approle using the secret id file;
    - logs in again and retries once on `Forbidden`.
  - Refresh:
    - nothing is reloaded before the period has passed;
    - a changed value is applied to every config that shares the entry (`interface_credentials` plus override by `interface_name`);
    - a provider error keeps the old values and logs no secret;
    - a change evicts the OData `__auth_cache` entry.
  - The existing `test_commun_credentials.py`, `test_monitoring.py` and `test_interface_isolation.py` still pass.
- **Manual end-to-end check** against a dev OpenBao server (`docker run --rm -p 8200:8200 openbao/openbao server -dev`):
  1. Run the migration script on `tests/conf/test-maas-collector-credentials.json`.
  2. Start an HTTP or OData collector with `CREDENTIAL_BACKEND=vault`, `CREDENTIAL_REFRESH_PERIOD=10` and `WATCH_PERIOD=5`.
  3. Write a new version of one secret.
  4. Check that the log shows "credentials changed for <entry>" with no values, and that the next loop uses the new value.
  5. Stop OpenBao and check that the collector keeps collecting with the last-known-good credentials.

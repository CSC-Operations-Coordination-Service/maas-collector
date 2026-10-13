# Changelog

## Version 3.11.0

### Credentials from OpenBao / HashiCorp Vault

- Collectors can read their credentials from an OpenBao or HashiCorp Vault KV version 2
  engine instead of the credential file: set `CREDENTIAL_BACKEND=vault`
  (`--credential-backend`). The credential file stays the default
  (`CREDENTIAL_BACKEND=file`).
- Each entry of the credential file becomes one secret,
  `<VAULT_KV_MOUNT>/<VAULT_PATH_PREFIX>/<entry name>`, holding the same attributes
  without `name`. Interfaces find their entries as before, by `interface_credentials`
  then `interface_name`.
- New settings, each with a matching `--vault-*` option: `VAULT_ADDR`, `VAULT_KV_MOUNT`
  (default `secret`), `VAULT_PATH_PREFIX` (default `maas-collector`),
  `VAULT_AUTH_METHOD` (`approle`, the default, or `token`), `VAULT_ROLE_ID`,
  `VAULT_SECRET_ID_FILE`, and `VAULT_TOKEN_FILE` (without it, `VAULT_TOKEN` is used).
  To check the vault certificate against a private CA, set `VAULT_CACERT` or
  `VAULT_CAPATH`.
- Behind an egress proxy, list the vault host in `NO_PROXY` and `no_proxy`. The vault
  client follows `https_proxy`, and an egress proxy usually refuses the vault port,
  which stops the collector at startup.
- At startup, the collector stops when the vault settings are incomplete, when the
  vault cannot be read, or when the vault policy denies one of its entries. When none
  of its entries is found, it logs an error naming `VAULT_KV_MOUNT` and
  `VAULT_PATH_PREFIX`, then starts without credentials.
- The interface monitor reads the credentials of every interface it probes, so its
  vault policy has to cover them.
- New `scripts/credentials_to_vault.py` copies a credential file to the vault, one
  secret per entry, and keeps the value types. It uses `VAULT_ADDR` and `VAULT_TOKEN`
  (a token allowed to write under the prefix), and its `--kv-mount` and `--path-prefix`
  default to `VAULT_KV_MOUNT` and `VAULT_PATH_PREFIX`. `--dry-run` lists what it would
  write. It prints entry and attribute names, never values. Run it where maas-collector
  is installed.
- New `docs/vault.md` covers the secret layout, the vault policies and AppRole set-up,
  deployment, the OpenSearch and AMQP connections, the move from a credential file,
  rotation and vault outages.

### Credential refresh without restart

- Collectors read their credentials again at the start of a loop, once
  `CREDENTIAL_REFRESH_PERIOD` seconds (`--credential-refresh-period`, default 300) have
  passed since the last read, so they use a rotated secret without a restart. A replay
  also refreshes them between its retries once the period has passed, and the
  interface monitor refreshes the interfaces it probes. The collector logs the names of
  the changed entries, never their values: `Credentials changed for <entries>`.
- The refresh applies to the credential file too, when the file changes in place. Set
  `CREDENTIAL_REFRESH_PERIOD=0` to read the credentials only at startup, as before.
- When a refresh fails, for example during a vault outage, the collector keeps
  collecting with the last credentials it read and tries again at its next loop. It
  logs one traceback, then one line per loop.
- An entry the vault policy no longer allows the collector to read keeps its current
  values, with a warning at each refresh. The other entries are still refreshed.
- An attribute removed from an entry keeps its value until the collector restarts, and
  a warning names it.

### OpenSearch and AMQP connections from the vault

- `ES_VAULT_PATH` and `AMQP_VAULT_PATH` (`--es-vault-path`, `--amqp-vault-path`) name
  the vault secret holding the `url`, `username` and `password` of each connection, in
  place of `ES_URL`, `ES_USERNAME`, `ES_PASSWORD` and their `AMQP_*` counterparts. The
  path is the full path in the `VAULT_KV_MOUNT` engine; `VAULT_PATH_PREFIX` does not
  apply. The vault is read with the same `VAULT_*` settings as the credentials,
  whatever the `CREDENTIAL_BACKEND`.
- The connections are read once, at startup. After changing a connection password in
  the vault, restart the services. A missing secret, a secret without `url` or an
  unreachable vault stops the collector at startup.
- When a vault path is set, the url, username and password variables of that
  connection are ignored, and a warning names the ones still set (never their values).
  Without a vault path, the collector uses them as before.

### Fixes

- A failing interface, for example one with bad credentials or an unreachable
  endpoint, no longer makes the collector skip the interfaces after it in the loop. The
  collector logs `Cannot collect interface <name>` with the traceback and moves on to
  the next interface. This applies to the HTTP-based collectors (OData, MPIP, Simple
  HTTP, Loki, Weather, Disclaimer) and to the S3, FTP, JIRA extended and read-only SFTP
  collectors. A replay still retries a failing interface.
- The debug message of `Messenger.setup()` no longer shows the AMQP password. The
  collector entry points do not call this method, so this message never reached their
  logs.
- `docs/common_options.md` named `ES_IGNORE_CERTS_VERIFICATION`, which nothing reads.
  The variable is `IGNORE_CERTS_VERIFICATION`. Any non-empty value turns the OpenSearch
  certificate verification off, even `False`.

### Requirements and upgrade

- maas-collector now requires maas-model 3.1.0 or later, below 4, with its `vault`
  extra, which brings `hvac`. The requirement reads `maas-model[vault]>=3.1.0.dev0,<4`
  so that development builds also accept the maas-model development builds.
- A collector that sets none of the new variables runs as before, apart from the
  credential file refresh and the fixes above.
- To move a stack's credentials to the vault, write its entries with
  `scripts/credentials_to_vault.py` (with `--dry-run` first). Then set
  `CREDENTIAL_BACKEND=vault` and the `VAULT_*` settings, and remove `CREDENTIAL_FILE`,
  which the vault backend does not read.
- To move the connections, write the OpenSearch and AMQP secrets and set
  `ES_VAULT_PATH` and `AMQP_VAULT_PATH`. Then remove the `ES_*` and `AMQP_*` connection
  variables and rotate the passwords they held in plain text. See `docs/vault.md`.

Earlier versions are only recorded in the git history.

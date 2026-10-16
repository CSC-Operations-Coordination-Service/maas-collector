# Changelog

## Version 3.13.0

### Replay

- A replay collects exactly `--replay-start-date` to `--replay-end-date`. The
  `end_date_time_offset` and `odata_start_offset` of the interface configuration
  now apply to live collection only, they no longer shift the window of a replay.
- A replay exits with status 1 when an interface exhausted its retries, so that a
  caller running it as a child process can tell a failed replay from a successful one.
- Jira interfaces can be replayed over a window: the `jql_query` of the interface must
  contain `updated > {date_criteria}`, which a replay turns into the window bounds.

### Collector configuration

- A collector entry accepts `enabled: false` to be disabled without being deleted: the
  collector does not load it, a replay of it fails with "<name> is disabled", and the
  interface monitor does not probe it. A non-boolean `enabled` is rejected by the
  configuration validation.

- `config sync <out-dir> [--service S ...]` brings the configuration index into a
  folder as `<out-dir>/<service>/<path>`: it rewrites only the files whose content
  changed and removes only the files it wrote itself (listed in
  `.maas-config-sync.json` of each service folder) that are no longer current, so
  other files of the folder, such as credentials, are never touched.

- The operator worker replays an interface over a window in a `short` or `long` lane
  of its service: it copies the service configuration from the index to a temporary
  folder, runs the odata or jirax collector on it as a child process, and records
  RUNNING, then DONE or FAILED with the exit code and the last output lines. The child
  is killed at the lane timeout even when it prints nothing, and a disabled, deleted,
  missing or non-replayable interface fails the operation with the reason.

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
- A version is refused when the collectors could not start on it: an `amqp` section
  that is not an object of routing keys to objects with a positive integer
  `chunk_size`, `collectors` or `models` that are not lists, or, for the interface
  monitor, a set without its monitoring entry. A service name is a single directory
  name (`odata`, `monitor-main`), and `export` skips any document that would be
  written outside its output directory.
- The interface monitor finds the configurations of the interfaces it probes among
  the files of its own service. It now stops its probe retries when it exits, so a
  restart on a new configuration does not leave the process hanging.
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
- The chunk sizes of a configuration file are checked: a malformed `amqp` section,
  which used to fail with a `TypeError` or an `AttributeError`, now stops the
  collector with a `ValueError` naming the routing key.

### Requirements and upgrade

- No new dependency. A collector that sets none of the new variables runs as before.
- The deployments move one service at a time: see `docs/opensearch_config.md`.

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
  vault cannot be read, or when the vault policy denies one of the entries it looks up.
  When none of its entries is found, it logs an error naming `VAULT_KV_MOUNT` and
  `VAULT_PATH_PREFIX`, then starts without credentials.
- The collector looks up the entries named after the `interface_credentials` and the
  `interface_name` of every configuration, even names without a secret, and the vault
  refuses a path outside its policy even when nothing is stored there. So the policy
  has to allow reading all these names: a wildcard on the prefix, like
  `secret/data/maas-collector/cds/*`, covers them.
- The interface monitor reads the credentials of every interface it probes, so its
  vault policy has to cover the names of these interfaces too.
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
- The username and password stored in the vault may contain any character, like `/`,
  `#` or `%`: the collector percent-encodes them in the connection URL. The `ES_*` and
  `AMQP_*` variables are still used as they are, so a special character there still
  has to be percent-encoded by hand.

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
- `docs/common_options.md` had wrong entries:
  - It named `ES_IGNORE_CERTS_VERIFICATION`, which nothing reads. The variable is
    `IGNORE_CERTS_VERIFICATION`, and any non-empty value turns the OpenSearch
    certificate verification off, even `False`.
  - It named `WORKING_DIRECTORY`, which nothing reads either. The working directory
    comes from `WORKING_DIR`, and defaults to `/tmp`.
  - The health check port defaults to 80, not 8127. The images set
    `HEALTHCHECK_PORT=8080`.
  - The OpenSearch and AMQP url, username and password have no default value.

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
  `ES_VAULT_PATH` and `AMQP_VAULT_PATH`, with the `VAULT_*` settings and the secret id.
  Then remove `ES_URL`, `ES_USERNAME`, `ES_PASSWORD`, `AMQP_URL`, `AMQP_USERNAME` and
  `AMQP_PASSWORD`. Keep the other `ES_*` and `AMQP_*` settings, like `ES_TIMEOUT` or
  `AMQP_EXCHANGE`.
- Then rotate the passwords these six variables held in plain text. Other services
  often share these credentials: rotate a password once every service using it reads
  it from the vault, or update the other services in the same window. The services
  that read the vault use a new password only after a restart. See `docs/vault.md`.

Earlier versions are only recorded in the git history.

# Changelog

## Unreleased

### Satellite unavailability reports

- `SatUnavailabilityExtractor` is removed: the `REP__SUP__` reports are extracted with
  the `XMLExtractor`, the header and report fields anchored on the root with
  `root_path`. It read the subsystem text from `<Comment>` only, so the reports of the
  new "Unavailability Tool" format, where it is `<Description>`, had no comment. The
  extractor args read both formats, and also extract the file header, the identifier,
  the status and the category of the new one: see `SATU_EXTRACTOR_ARGS` in
  `tests/test_extractor_satu.py`. `start_time` and `end_time` keep their `UTC=` prefix,
  so the documents keep their ids.
- **Before upgrading**, switch the `Satellite-Unavailability` collector to these args
  and add the new fields to the `SatUnavailabilityProduct` model: a configuration still
  naming `SatUnavailabilityExtractor` no longer loads. The `XMLExtractor` has read
  `root_path` since 3.12.0, so the configuration can move first.

### Named backups

- A configuration file may hold `backups`, one definition per file
  (`backups/<name>.json`): a name, a type (`BackupS3` or `BackupLocal`), a directory,
  the S3 buckets, endpoint, region and signature version, and the name of the
  credential entry holding the S3 keys. The rules are in `docs/backup.md` and checked
  by `maas_collector.backup.named`.
- Configuration validation knows `backups`: `s3_access_key` and `s3_secret_key` are
  refused in any backup block, a definition or an inline block (an S3 block names a
  `credentials` entry), an `@` in an `interface_name` is refused, and an entry may not
  use the same backup as its file `backup` and in its `archive` rule. Checked together,
  the files of a service must define every backup named by an entry (enabled or not)
  or an archive rule, and every `report_model` (`ConfigIntegrityError`).

### Safe configuration validation

- `validate_config_sources(..., evaluate=False)` validates a configuration without
  evaluating any lambda or converter: each `python` string is parsed and must be a
  lambda without default argument, and the class and the argument names of an
  extractor are checked against its signature (`rawdata/extractor/static.py`).
  `build_collector_configuration` takes the same `evaluate` flag, and
  `ConfigStore(client, index, evaluate=False)` validates its publications and
  rollbacks that way, for a service that must not execute what it validates.

## Version 3.13.0

### Operator image

- `Dockerfile.local` and `Dockerfile.registry` take a build-arg `PACKAGE_EXTRAS` (empty by
  default), and the CI builds a second image, `maas-collector-operator`, with
  `PACKAGE_EXTRAS=operator`: the operator worker needs maas-engine, which the collectors'
  image does not carry. The collectors' image is unchanged. The operator image goes to the
  Nexus Docker registry only, with no SBOM, scan or signature yet.

### Operator worker

- The replays of the operator worker run on the replay worker of maas-engine
  (`maas_engine.replay`, maas-engine after 3.1.0): the queue polling, the child process with
  its deadline, and the operation record are no longer a copy kept here. What stays is
  specific to the collector: `rawdata/operator/handler.py` (the interface lookup in
  the configuration index, the copy of the configuration, the command and the
  environment of the child), and the product check. The operator worker now needs
  maas-engine: `pip install "maas-collector[operator]"`. Its options, queues, messages
  and records are unchanged.
- A child that ignores the termination asked at a stop is killed after 10 s as before,
  but no longer up to 30 s late: that was the progress interval.
- `maas_collector.rawdata.operator.records` and the `ChildRunner` and `ReplayLane` of
  `operator.replay` are gone, replaced by `maas_engine.replay`'s.

### Replay

- A replay collects exactly `--replay-start-date` to `--replay-end-date`. The
  `end_date_time_offset` and `odata_start_offset` of the interface configuration
  now apply to live collection only, they no longer shift the window of a replay.
- A replay exits with status 1 when an interface exhausted its retries, so that a
  caller running it as a child process can tell a failed replay from a successful one.
  A replay refused before collecting anything, for a disabled or unknown interface or
  a jira one without `end_point`, exits with status 1 too, the error being logged.
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

### Operator worker

- `python -m maas_collector.rawdata.cli.operator_worker` serves the replay and check
  requests of one collector service (`CONFIG_SERVICE`, required) published by the
  operator backend on the `maas-operator-exchange` topic exchange, one thread per
  queue: `maas-operator-<service>-replay-short` (windows of 24 h at most) and `-long`,
  each one replay at a time, and `maas-operator-<service>-check`, answered while
  replays run. A replay copies the service configuration from the index to a temporary
  folder, runs the odata or jirax collector on it as a child process, and records
  RUNNING, then DONE or FAILED with the exit code and the last output lines; the child
  is killed at the lane timeout even when it prints nothing, and a disabled, deleted,
  missing or non-replayable interface, or a configuration file the copy skipped, fails
  the operation with the reason. The child gets `--replay-retry 3`
  (`OPERATOR_REPLAY_RETRY`), so that a dead interface does not hold its lane for hours
  with the 1024 retries of the collector. A replay request is acknowledged on receipt
  and never redelivered: one lost to a crash keeps its record RUNNING. A malformed
  request is logged and skipped, its record FAILED when it has an `operation_id`. A
  check takes a product of an odata v4 interface by name or id, asks the interface
  live with its credentials, runs the extraction of the interface on the answer to get
  the document maas would store, and replies it with the stored one and their
  differing fields, `reportName` and `ingestionTime` left out; a product the interface
  no longer has is still looked up in maas by its `id_field`, and a quote in a name is
  escaped as `''` in the filter. The interface and its token
  request get 20 s at most (`OPERATOR_CHECK_TIMEOUT`), the backend waiting 30 s. A
  missing, disabled, non odata or odata v3 interface, an error answer of the
  interface, a product the extraction refuses or a malformed request is an error
  reply. The reply goes to `reply_to` on the default exchange with the
  `correlation_id` of the request and expires after 60 s; a check without `reply_to`
  is dropped. SIGTERM and SIGINT stop taking requests, terminate the running
  collectors and start no other. The `OPERATOR_*` settings are described in
  `docs/operator_worker.md`.

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

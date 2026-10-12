# Credentials from OpenBao / HashiCorp Vault

Collectors can read their credentials from an [OpenBao](https://openbao.org) or
HashiCorp Vault server instead of a credential file. Once a collector has started, it
reads them again periodically, so a rotated secret is used **without restarting**
the collector.

Collectors and maas-engine can also read their
[OpenSearch and AMQP connections](#opensearch-and-amqp-connections) from the vault.

## Where the credentials are

A credential entry holds exactly what an entry of the credential file holds, minus
its `name`. Each entry is one secret of a KV version 2 engine:

```text
<VAULT_KV_MOUNT>/<VAULT_PATH_PREFIX>/<entry name>
```

The entries are found like in the file. An interface first takes the entry named
after its `interface_credentials` (shared credentials), then the entry named after
its `interface_name`, whose attributes win.

For example, this credential file entry:

```json
{
  "name": "common_cred",
  "auth_method": "Basic",
  "client_username": "collector",
  "client_password": "s3cr3t"
}
```

becomes the secret `secret/maas-collector/cds/common_cred`, when
`VAULT_PATH_PREFIX` is `maas-collector/cds`:

```shell
# client_password=- reads the password from stdin, so it stays out of the shell
# history and of the process list
bao kv put -mount=secret maas-collector/cds/common_cred \
    auth_method=Basic client_username=collector client_password=-
```

The `vault` CLI takes the same commands as `bao`.

`key=value` arguments are always stored as strings. Write an entry holding numbers,
booleans or nested values (like the `credentials` dictionary of the Simple HTTP
collector) from a JSON file, so the values keep their types:
`bao kv put -mount=secret maas-collector/cds/my_portal @my_portal.json`.
`scripts/credentials_to_vault.py` also keeps the types of the credential file.

## Settings

| CLI option                  | Environment variable      | Description                                                                     |
| --------------------------- | ------------------------- | ------------------------------------------------------------------------------- |
| --credential-backend        | CREDENTIAL_BACKEND        | `file` or `vault` (default: file)                                               |
| --credential-refresh-period | CREDENTIAL_REFRESH_PERIOD | Seconds between two reads of the credentials, 0 for startup only (default: 300) |
| --vault-addr                | VAULT_ADDR                | Vault address, like `https://bao.internal:8200`                                 |
| --vault-kv-mount            | VAULT_KV_MOUNT            | Mount point of the KV version 2 engine (default: secret)                        |
| --vault-path-prefix         | VAULT_PATH_PREFIX         | Path of the credential secrets in the engine (default: maas-collector)          |
| --vault-auth-method         | VAULT_AUTH_METHOD         | `approle` or `token` (default: approle)                                         |
| --vault-role-id             | VAULT_ROLE_ID             | AppRole role id                                                                 |
| --vault-secret-id-file      | VAULT_SECRET_ID_FILE      | File holding the AppRole secret id, like a Docker Swarm secret                  |
| --vault-token-file          | VAULT_TOKEN_FILE          | File holding a token for the `token` method. Without it, `VAULT_TOKEN` is used  |

TLS verification of the vault follows `VAULT_CACERT` or `VAULT_CAPATH`, like the
vault CLI.

Use `approle` in production. The `token` method is meant for development, with a
token that has to be renewed outside of the collector.

## Setting up the vault

These steps give one stack read-only access to its own credentials. Here the stack
is `cds`.

1. Enable a KV version 2 engine, if there is none yet:

   ```shell
   bao secrets enable -path=secret kv-v2
   ```

2. Write a policy that allows reading the stack credentials only:

   ```shell
   bao policy write maas-collector-cds - <<EOF
   path "secret/data/maas-collector/cds/*" {
     capabilities = ["read"]
   }
   EOF
   ```

3. Create an AppRole role with short-lived tokens:

   ```shell
   bao auth enable approle
   bao write auth/approle/role/maas-collector-cds \
       token_policies=maas-collector-cds \
       token_ttl=1h token_max_ttl=4h \
       secret_id_num_uses=0
   ```

   - The collector logs in again whenever its token expires. Each login uses the
     secret id, so leave `secret_id_num_uses` at 0 (no limit).
   - Set `secret_id_ttl` if your policy requires it. When the secret id expires,
     the collectors fail at their next login, so renew it before then. The
     collectors read the secret id file at each login, but a Swarm secret cannot
     change: a new secret id is a new Swarm secret, swapped in with
     `docker service update --secret-rm ... --secret-add ...`, which restarts the
     collectors.
   - `secret_id_bound_cidrs` and `token_bound_cidrs` restrict where the secret id
     and the tokens can be used.

4. Get the role id, which is not a secret, and create a secret id as a Swarm
   secret:

   ```shell
   bao read -field=role_id auth/approle/role/maas-collector-cds/role-id
   bao write -f -field=secret_id auth/approle/role/maas-collector-cds/secret-id \
       | docker secret create maas_collector_cds_vault_secret_id -
   ```

The secret id replaces the credential file. Once the
[OpenSearch and AMQP connections](#opensearch-and-amqp-connections) come from the
vault too, it is the only secret the stack holds. It only reads that stack's
credentials, it can be revoked, and the vault audit log records every read.

## Deploying a collector

```yaml
services:
  collector-odata:
    image: <registry>/maas-collector:<version>
    environment:
      CREDENTIAL_BACKEND: vault
      VAULT_ADDR: https://bao.internal:8200
      VAULT_CACERT: /run/secrets/vault_ca
      VAULT_PATH_PREFIX: maas-collector/cds
      VAULT_ROLE_ID: <role id>
      VAULT_SECRET_ID_FILE: /run/secrets/maas_collector_cds_vault_secret_id
      NO_PROXY: bao.internal
      no_proxy: bao.internal
    secrets:
      - maas_collector_cds_vault_secret_id
      - vault_ca

secrets:
  maas_collector_cds_vault_secret_id:
    external: true
  vault_ca:
    external: true
```

The vault client honours the `https_proxy` and `http_proxy` variables. When the
collectors reach the internet through a proxy, list the vault host in `NO_PROXY`
and `no_proxy`: an egress proxy usually refuses port 8200, and the collectors would
then fail at startup with a `ProxyError`.

The interface monitor reads the credentials of every interface it probes, so its
policy has to cover them too.

## OpenSearch and AMQP connections

The OpenSearch and AMQP url, username and password can come from the vault instead
of `ES_URL`, `ES_USERNAME`, `ES_PASSWORD`, `AMQP_URL`, `AMQP_USERNAME` and
`AMQP_PASSWORD`. maas-engine reads them the same way, so every service of a stack
shares one secret per connection.

| CLI option        | Environment variable | Description                                                          |
| ----------------- | -------------------- | -------------------------------------------------------------------- |
| --es-vault-path   | ES_VAULT_PATH        | Secret holding the OpenSearch connection, like `maas/cds/opensearch` |
| --amqp-vault-path | AMQP_VAULT_PATH      | Secret holding the AMQP connection, like `maas/cds/amqp`             |

- The path is the secret's full path in the `VAULT_KV_MOUNT` engine. It does not
  use `VAULT_PATH_PREFIX`, which only applies to the credential entries.
- The vault access uses the same `VAULT_*` settings as the credentials. It works
  whatever the `CREDENTIAL_BACKEND`.
- Without a path, the connection comes from the environment variables, as before.

Each secret holds `url`, and `username` and `password` when the service needs
authentication:

```shell
# password=- reads the password from stdin
bao kv put -mount=secret maas/cds/opensearch \
    url=https://opensearch.internal:9200 username=maas password=-
bao kv put -mount=secret maas/cds/amqp \
    url=amqp://rabbitmq.internal:5672// username=maas password=-
```

Give every AppRole of the stack, collectors and maas-engine alike, a policy to read
them:

```shell
bao policy write maas-cds-connections - <<EOF
path "secret/data/maas/cds/opensearch" {
  capabilities = ["read"]
}
path "secret/data/maas/cds/amqp" {
  capabilities = ["read"]
}
EOF
bao write auth/approle/role/maas-collector-cds \
    token_policies=maas-collector-cds,maas-cds-connections \
    token_ttl=1h token_max_ttl=4h secret_id_num_uses=0
```

Then replace the connection variables of the service:

```yaml
    environment:
      ES_VAULT_PATH: maas/cds/opensearch
      AMQP_VAULT_PATH: maas/cds/amqp
      VAULT_ADDR: https://bao.internal:8200
      VAULT_ROLE_ID: <role id>
      VAULT_SECRET_ID_FILE: /run/secrets/maas_collector_cds_vault_secret_id
      NO_PROXY: bao.internal
      no_proxy: bao.internal
```

- The connections are read **once, at startup**. To rotate a password, write a new
  version of the secret, then restart the services:
  `docker service update --force <service>`.
- A collector stops at startup when the vault is unavailable, the secret is missing,
  or the secret has no `url`.
- When `ES_URL`, `ES_USERNAME` or `ES_PASSWORD` (`AMQP_*` alike) are still set, they
  are ignored, and a warning names them. Remove them from the compose files once the
  secrets are in the vault, and rotate the passwords they held in plain text.

## Moving a credential file to the vault

`scripts/credentials_to_vault.py` writes each entry of a credential file as a
secret. It authenticates with `VAULT_TOKEN`, a token allowed to write under the
prefix. `--kv-mount` and `--path-prefix` default to `VAULT_KV_MOUNT` and
`VAULT_PATH_PREFIX`. Like every script of the folder, it needs an environment where
maas-collector is installed. It prints entry and attribute names, never values.

```shell
export VAULT_ADDR=https://bao.internal:8200
export VAULT_TOKEN=...  # a token allowed to write under the prefix

# check what would be written
python scripts/credentials_to_vault.py maas-api-collector-credentials.json \
    --path-prefix maas-collector/cds --dry-run

python scripts/credentials_to_vault.py maas-api-collector-credentials.json \
    --path-prefix maas-collector/cds
```

> A credential file that was ever committed to a repository should be considered
> exposed. Rotate its secrets while moving them to the vault, then remove the file
> from the repository.

## Rotating a secret

Write a new version of the secret:

```shell
bao kv patch -mount=secret maas-collector/cds/common_cred client_password=-
```

Collectors read the credentials again at the start of their first loop after
`CREDENTIAL_REFRESH_PERIOD` seconds, so a change is used within about
`CREDENTIAL_REFRESH_PERIOD` plus `WATCH_PERIOD` plus the duration of the current
loop. A replay also reads them before each retry. When a collector picks a change
up, it logs the names of the entries that changed, never their values:
`Credentials changed for common_cred`. Changes to entries the collector does not use
are ignored.

- Every interface sharing the entry gets the new values.
- Clients and OAuth tokens are created again from the new values at the next
  collect, including the monitor probe authentications.
- An attribute **removed** from an entry, or from a deleted entry, keeps its
  previous value until the collector restarts. A warning tells which one. When an
  interface's own entry is deleted, the shared entry's values apply, as after a
  restart.

The same refresh applies to the credential file backend, when the file changes in
place.

## When the vault is unavailable

- **At startup**, the collector stops with an error, as it does when the credential
  file is missing. It also stops when the vault policy denies one of its entries.
- **At startup, when none of the entries is found**, the collector logs an error
  naming `VAULT_KV_MOUNT` and `VAULT_PATH_PREFIX`, the usual culprits, and starts
  without credentials. It does not stop, as collectors without credentials may share
  these settings.
- **During a refresh**, the collector logs the error and keeps collecting with the
  credentials it already has. It tries again at its next loop. The first failure
  logs a traceback, the next ones a single line.
- **An entry the policy no longer covers** keeps its current values, with a warning
  at each refresh. The other entries are still refreshed.
- **An expired token** makes the collector log in again with its secret id, then
  read again once. A token that is still valid but refused by the policy does not
  cause a new login.

## Trying it locally

A collector also needs OpenSearch and RabbitMQ: start them with
`docker compose -f docker-compose-opensearch.yml up -d`, and set `ES_URL` and
`AMQP_URL` accordingly.

```shell
docker run --rm -d --name bao -p 8200:8200 \
    -e BAO_DEV_ROOT_TOKEN_ID=root openbao/openbao server -dev
export VAULT_ADDR=http://127.0.0.1:8200 VAULT_TOKEN=root

python scripts/credentials_to_vault.py tests/conf/test-maas-collector-credentials.json

CREDENTIAL_BACKEND=vault VAULT_AUTH_METHOD=token CREDENTIAL_REFRESH_PERIOD=10 \
    WATCH_PERIOD=5 python -m maas_collector.rawdata.cli.odata -v \
    -c tests/conf/test-maas-collector-odata-basic.json
```

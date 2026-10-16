# Configuration in OpenSearch

By default, a collector reads its configuration from JSON files, given with `-c` and
`-d` (see [configuration](configuration.md)). With `CONFIG_BACKEND=opensearch`, it
reads the same files from an OpenSearch index instead. The index then becomes the
single source of truth for the collector configurations:

- every edit is a new version, with its author, date and comment, so the history is
  complete and a rollback is one command;
- a running collector notices a change and restarts itself to apply it;
- Dashboards shows which services load which files, and their content.

The file backend stays the default. Keep it for the tests, local runs and replay
services.

## Documents and versions

The index, `maas-collector-config` by default (`CONFIG_INDEX`), holds **one document
per version of a configuration file**. The document id is `<path>@<version>`, like
`odata/prip/maas-collector-odata-prip.json@3`.

- `path` is the path of the file relative to the service directory of a file
  deployment: the path under `/conf/collector/` in the containers.
- `content` is the file JSON, verbatim: `{"collectors": [...], "models": [...],
  "amqp": {...}}`, each key optional. It is stored, not indexed.
- `services` lists the services that load the file. Several services may share a
  file: one document serves them all.
- The current version of a path is flagged `latest`. Versions are never modified,
  apart from this flag: an edit, a rollback or a deletion writes a new version.
- A deletion writes a **tombstone**: a version flagged `deleted`, with an empty
  content. The services stop loading the path. Publishing the path again brings it
  back.

| Field                        | Type                   | Meaning                                            |
| ---------------------------- | ---------------------- | -------------------------------------------------- |
| `path`                       | keyword                | relative path of the file                          |
| `version`                    | integer                | 1, 2, ... for each path                            |
| `latest`                     | boolean                | current version of the path                        |
| `deleted`                    | boolean                | tombstone: the file was removed                    |
| `services`                   | keyword                | services loading the file                          |
| `content`                    | object, not indexed    | the file JSON                                      |
| `interface_names`            | keyword                | `interface_name` of the collector entries          |
| `model_names`                | keyword                | models declared or used by the file                |
| `author`, `comment`          | keyword, text          | who published the version, and why                 |
| `created_at`                 | date                   | publication time                                   |
| `based_on`                   | integer                | version edited, or version restored by a rollback |

The mapping is strict: OpenSearch refuses any other field. Only credential-free
configuration files are accepted: a file whose top-level keys are not within
`collectors`, `models` and `amqp`, like a credential file, is refused.

## Settings of the collectors

| CLI option              | Environment variable  | Description                                                                     |
| ----------------------- | --------------------- | ------------------------------------------------------------------------------- |
| --config-backend        | CONFIG_BACKEND        | Where the configuration is read from: `file` or `opensearch` (default: file)    |
| --config-index          | CONFIG_INDEX          | Index holding the configurations (default: maas-collector-config)               |
| --config-service        | CONFIG_SERVICE        | Service tag of the files to load, required with `opensearch` (default: none)    |
| --config-refresh-period | CONFIG_REFRESH_PERIOD | Seconds between two checks for a change, 0 never checks (default: 300)          |

- With `CONFIG_BACKEND=opensearch`, the collector stops at startup when
  `CONFIG_SERVICE` is not set, when the OpenSearch connection is not set (neither
  `ES_URL` nor `ES_VAULT_PATH`), when OpenSearch cannot be read, or when the index
  holds no file for the service.
- `-c` / `RAWDATA_CONFIG` and `-d` / `RAWDATA_CONFIG_DIR` are then ignored, with a
  warning.
- The configuration is read through the collector's OpenSearch connection, so it
  also works when the connection comes from the vault with `ES_VAULT_PATH` (see
  [vault](vault.md#opensearch-and-amqp-connections)).
- The interface monitor finds the configurations of the interfaces it probes among
  the files of its own service: tag them with the monitor service too.

## Applying a change: the collector restarts

A collector checks its files at the start of a loop, once `CONFIG_REFRESH_PERIOD`
seconds have passed since the last check. A change is picked up within about
`CONFIG_REFRESH_PERIOD` plus `WATCH_PERIOD` plus the duration of the current loop.

- **A file changed, was added for the service, or was removed from it:** the
  collector checks that it can load the new set of files, logs
  `configuration changed: odata/prip.json v3→v4, odata/new.json added, restarting`,
  sends its pending messages, and **exits with code 75** (`EX_TEMPFAIL`). The
  orchestrator restarts it, and the new process loads the new files. Swarm restarts
  it with `restart_policy` `any` or `on-failure`, Kubernetes with
  `restartPolicy: Always`.
- **The new set cannot be loaded** (a bad extractor class, a model defined nowhere,
  a document written by hand with a wrong shape): the collector logs
  `configuration change rejected: <path>@<version>: <reason>` and keeps running
  with its current files. It checks this set only once: fix the file with a new
  version or a rollback.
- **OpenSearch cannot be read**, or the service has no file left: the collector logs
  the error and keeps running with its current files. It checks again at the next
  period. The first failure logs a traceback, the next ones a single line.
- With `CONFIG_REFRESH_PERIOD=0`, the collector never checks: a change applies at
  the next restart.

## Administration CLI

Edits go through the administration CLI, which checks every version before
publishing it. It reads the OpenSearch connection from the same settings as the
collectors: `ES_URL`, `ES_USERNAME` and `ES_PASSWORD`, or `ES_VAULT_PATH` with the
`VAULT_*` settings, and the index from `CONFIG_INDEX` / `--config-index`. Its user
needs write access to the index (see [Roles](#roles)). `--author` defaults to
`$USER`. The general options go before the command.

```shell
python -m maas_collector.rawdata.cli.config [--config-index INDEX] [--author NAME] <command> ...
```

| Command                                                                  | What it does                                                                                                         |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `init`                                                                   | Create the index with its mapping. Does nothing if it exists.                                                        |
| `import <deploy-dir> [--dry-run]`                                        | Publish the files of a deploy tree `<deploy-dir>/<service>/**/*.json` (see [Moving to the index](#moving-to-the-index)). |
| `export <out-dir> [--service S]`                                         | Write the current versions back as `<out-dir>/<service>/<path>`.                                                     |
| `sync <out-dir> [--service S ...]`                                       | Bring `<out-dir>/<service>/<path>` up to date: rewrite changed files, remove the files a previous sync wrote that are no longer current (listed in `.maas-config-sync.json`), leave any other file alone. |
| `list [--service S] [--interface I]`                                     | Path, version, services, author and date of the current versions.                                                    |
| `get <path> [--version N]`                                               | Content on stdout; version, services and author on stderr.                                                           |
| `history <path>`                                                         | Every version, with its date, author, flags and comment.                                                             |
| `diff <path> [--from N] [--to M]`                                        | Unified diff between two versions, by default the current one and the previous one.                                  |
| `put <path> <file> -m <comment> [--services a,b] [--base-version N]`     | Publish a new version. `--services` defaults to the services of the current version, and is required for a new path. |
| `rollback <path> --to N -m <comment>`                                    | Publish the content and services of version N as a new version.                                                      |
| `delete <path> -m <comment>`                                             | Publish a tombstone.                                                                                                 |

- `put`, `rollback` and `delete` accept `--base-version N`: the command is refused
  when the current version is no longer N, because someone published in the
  meantime. Two publications of the same version at the same time are refused the
  same way: the second one gets a conflict.
- A refused command exits with code 1 and prints the reason on stderr.

Editing a file:

```shell
python -m maas_collector.rawdata.cli.config get odata/prip/prip-s1a.json > prip-s1a.json
# edit prip-s1a.json, then
python -m maas_collector.rawdata.cli.config put odata/prip/prip-s1a.json prip-s1a.json \
    -m "refresh every 10 minutes" --base-version 3
python -m maas_collector.rawdata.cli.config diff odata/prip/prip-s1a.json
```

## Moving to the index

1. Create the index: `config init`.
2. Check the import of the collector directory of the deploy tree, which holds one
   directory per service:
   `config import <deploy-tree>/collector --dry-run`.
   - Files are grouped by path relative to their service directory, and a path
     shared by several services becomes one document listing all of them. A path
     whose copies differ between services is refused: make the copies equal first.
   - Files that are not configuration files, like credential files, are skipped and
     listed. Hidden files and directories are ignored.
   - Every file is checked like a `put`.
3. Import: `config import <deploy-tree>/collector`. A path that is not in the index
   yet gets version 1. A path already in the index is never overwritten: when it
   differs from the deploy tree, the import reports a `drift` and leaves it
   untouched. From then on, the index is the source of truth.
4. Check a service: `config list --service odata`.
5. Switch the services one at a time: set `CONFIG_BACKEND=opensearch` and
   `CONFIG_SERVICE=<service directory name>`, and remove the configuration mounts
   and `-d` / `RAWDATA_CONFIG_DIR`.

To go back to files, `config export <dir>` writes the current versions as a deploy
tree, which can also be compared with the repository.

```yaml
services:
  collector-odata:
    image: <registry>/maas-collector:<version>
    environment:
      CONFIG_BACKEND: opensearch
      CONFIG_SERVICE: odata
      ES_VAULT_PATH: maas/cds/opensearch
      AMQP_VAULT_PATH: maas/cds/amqp
      CREDENTIAL_BACKEND: vault
      # VAULT_* settings: see vault.md
    deploy:
      restart_policy:
        condition: any
```

## Roles

**Configurations run code**: the `{"python": "lambda ..."}` values of a
configuration are evaluated by the collectors. Write access to the index therefore
means code execution in every collector that reads it.

- Collectors get **read-only** access to the index.
- Only the administrators running the CLI get write access.
- Check that no other role of the collector users grants write access to the index,
  for example through a `*` index pattern.

With the OpenSearch security plugin:

```json
PUT _plugins/_security/api/roles/maas_collector_config_read
{
  "cluster_permissions": ["cluster_composite_ops_ro"],
  "index_permissions": [
    {"index_patterns": ["maas-collector-config"], "allowed_actions": ["read"]}
  ]
}

PUT _plugins/_security/api/roles/maas_collector_config_admin
{
  "cluster_permissions": ["cluster_composite_ops"],
  "index_permissions": [
    {
      "index_patterns": ["maas-collector-config"],
      "allowed_actions": ["crud", "create_index"]
    }
  ]
}
```

Map the first role to the users of the collectors, and the second one to the
administrators, with `PUT _plugins/_security/api/rolesmapping/<role>`.

Secrets never belong in the index: the CLI refuses credential files, and the
credentials stay in the vault or the credential file.

## Dashboards

Dashboards is meant for inspection: hand-written versions in Dev Tools skip the
checks of the CLI.

1. Create an index pattern `maas-collector-config` with `created_at` as time field.
2. In Discover, save the search `latest:true and not deleted:true`: the current
   files, with `path`, `version`, `services`, `interface_names` and `author` as
   columns. The content shows in the document details.
3. `path:"odata/prip/prip-s1a.json"` sorted by `version` shows the history of a file.

## Trying it locally

```shell
docker compose -f docker-compose-opensearch.yml up -d
export ES_URL=http://localhost:9200
python -m maas_collector.rawdata.cli.config init
python -m maas_collector.rawdata.cli.config put odata/basic.json \
    tests/conf/test-maas-collector-odata-basic.json -m "local test" --services odata

CONFIG_BACKEND=opensearch CONFIG_SERVICE=odata CONFIG_REFRESH_PERIOD=10 \
    WATCH_PERIOD=5 CREDENTIAL_FILE=tests/conf/test-maas-collector-credentials.json \
    python -m maas_collector.rawdata.cli.odata -v
```

Then `put` a changed version from another shell: the collector logs
`configuration changed: odata/basic.json v1→v2, restarting` and exits with code 75.
Recent OpenSearch images need `OPENSEARCH_INITIAL_ADMIN_PASSWORD` set.

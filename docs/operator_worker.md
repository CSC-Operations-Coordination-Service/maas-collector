# Operator worker

The operator worker runs the replays and the product checks that the operator
backend asks for over RabbitMQ:

```
python -m maas_collector.rawdata.cli.operator_worker --config-service odata
```

**One worker per collector service.** The service, `CONFIG_SERVICE`
(`--config-service`), is required: the worker takes only the requests of that
service, and reads its interfaces from the configuration index
(`CONFIG_INDEX`, `maas-collector-config` by default, see
[configuration in OpenSearch](opensearch_config.md)).

The worker serves three queues of its service at once, each in its own thread: the
`short` and `long` replay lanes, and the checks. A long replay never delays a short
one, and checks are answered while replays run.

**The replays are run by maas-engine.** The lanes are the generic replay worker of
`maas_engine.replay` (poll, acknowledge on receipt, child process under a deadline,
operation record: see its `docs/replay.md`), and what is specific to the collector is
`maas_collector.rawdata.operator.handler.CollectorReplayHandler`: it finds the
interface in the configuration index, copies the configuration of the service into a
folder of its own, and builds the collector command. The check thread stays here, being
about odata products. The operator worker needs maas-engine:

```
pip install "maas-collector[operator]"
```

**The image.** The collectors' image does not have maas-engine. The operator worker runs from its own image, `maas-collector-operator`, which the CI builds from the same wheel with the build-arg `PACKAGE_EXTRAS=operator` (`Dockerfile.local` and `Dockerfile.registry`, jobs `docker-local-operator` and `docker-registry-operator` of `.github/workflows/maas-collector.yml`). Locally:

```
docker build -f Dockerfile.local --build-arg PACKAGE_EXTRAS=operator -t maas-collector-operator:dev \
  --secret id=pip_index_url,env=PIP_INDEX_URL --secret id=netrc,src=$HOME/.netrc .
```

## Options

The worker takes the [common options](common_options.md) of the collectors, for the
OpenSearch connection (`ES_URL`, `ES_USERNAME`, `ES_PASSWORD` or `ES_VAULT_PATH`),
the AMQP connection (`AMQP_URL`, `AMQP_USERNAME`, `AMQP_PASSWORD` or
`AMQP_VAULT_PATH`), the vault (`VAULT_*`) and the credentials. **Credentials come
from OpenBao like the collector**: set `CREDENTIAL_BACKEND=vault` and the `VAULT_*`
settings (see [vault credentials](vault.md)). The replay collectors inherit the
environment of the worker, so they read their credentials the same way.

Its own options:

| Option                            | Environment                     | Default                 | Meaning                                                    |
| --------------------------------- | ------------------------------- | ----------------------- | ---------------------------------------------------------- |
| `--config-service`                | `CONFIG_SERVICE`                | required                | collector service served                                   |
| `--config-index`                  | `CONFIG_INDEX`                  | `maas-collector-config` | configuration index                                        |
| `--operator-operation-index`      | `OPERATOR_OPERATION_INDEX`      | `maas-operation`        | index of the operation records                             |
| `--operator-short-timeout`        | `OPERATOR_SHORT_TIMEOUT`        | 21600 (6 h)             | seconds a short replay may run                             |
| `--operator-long-timeout-per-day` | `OPERATOR_LONG_TIMEOUT_PER_DAY` | 10800 (3 h)             | seconds a long replay may run per day of its window        |
| `--operator-poll-interval`        | `OPERATOR_POLL_INTERVAL`        | 5                       | seconds between two polls of an empty queue                |
| `--operator-healthcheck-port`     | `OPERATOR_HEALTHCHECK_PORT`     | 18081                   | health check port of the short lane collector, +1 for long |
| `--operator-workdir`              | `OPERATOR_WORKDIR`              | system temp folder      | folder of the configuration copies and check payloads      |
| `--operator-check-timeout`        | `OPERATOR_CHECK_TIMEOUT`        | 20                      | seconds a check may wait for the interface                 |
| `--operator-replay-retry`         | `OPERATOR_REPLAY_RETRY`         | 3                       | retries of a failing odata replay, 10 s apart              |
| `--operator-single-lane`          | `OPERATOR_SINGLE_LANE`          | false                   | `true` for one lane that runs every replay, one at a time  |

A long replay may run `max(short timeout, days of its window × long timeout per
day)` seconds. The timeout is the one of the lane, that is of the queue the request
came from, whatever its `lane` field says.

`OPERATOR_REPLAY_RETRY` is given to the child as `--replay-retry`: an odata interface
that keeps failing gives up after that many retries, 10 s apart, instead of the 1024
of the collector, so that a dead interface does not hold its lane for hours. A jira
replay makes one attempt whatever the setting.

## Queues

Every queue is bound to the topic exchange `maas-operator-exchange` (durable):

| Queue                                  | Routing key                             | Requests                    |
| -------------------------------------- | --------------------------------------- | --------------------------- |
| `maas-operator-<service>-replay-short` | `maas.operator.<service>.replay.short`  | replays of 24 h at most     |
| `maas-operator-<service>-replay-long`  | `maas.operator.<service>.replay.long`   | longer replays              |
| `maas-operator-<service>-replay-custom-short` | `maas.operator.<service>.replay-custom.short` | custom replays of 24 h at most |
| `maas-operator-<service>-replay-custom-long`  | `maas.operator.<service>.replay-custom.long`  | longer custom replays          |
| `maas-operator-<service>-check`        | `maas.operator.<service>.check`         | product checks              |

Dates are ISO 8601 UTC with `Z`. A replay request is:

```json
{"operation_id": "...", "interface_name": "PRIP_S3A_ACRI",
 "start_date": "2026-10-01T00:00:00Z", "end_date": "2026-10-02T00:00:00Z",
 "requested_by": "bob", "lane": "short"}
```

A **custom** replay request (see [custom recollects](custom_recollect)) is published with
the key `maas.operator.<service>.replay-custom.<lane>` and carries the fields below. Its
`interface_name` is the real name followed by `@custom`, the real name being repeated in
`interface`; at least one of `overrides`, `configuration` and `archive` is set (`{}`
counts for the two objects, an empty `configuration` does not):

```json
{"operation_id": "...", "interface_name": "PRIP_S3A_ACRI@custom",
 "interface": "PRIP_S3A_ACRI",
 "start_date": "2026-10-01T00:00:00Z", "end_date": "2026-10-02T00:00:00Z",
 "requested_by": "bob", "lane": "short",
 "overrides": {"download_file_pattern": ["*.EOF*"]}, "configuration": "",
 "archive": {"backup": "archive-a", "backup_version": null,
             "duplicates": "skip", "force": false}}
```

## Replays

### Single lane

With `OPERATOR_SINGLE_LANE=true` the worker has **one lane**, `single`, instead of
`short`, `long` and the two custom lanes: one thread, one child at a time, and the queue
`maas-operator-<service>-replay-single`, bound to the four replay keys of the service
(`replay.short`, `replay.long`, `replay-custom.short`, `replay-custom.long`), so plain
and custom requests share it. The lane serves custom requests (it is custom-capable),
takes the health check port `OPERATOR_HEALTHCHECK_PORT`, and its timeout is the one of
the long lane: `max(short timeout, days of the window × long timeout per day)`. The
setting is generic: which service runs single-lane is a deployment choice. A value that
is neither true nor false (`1/true/yes/on`, `0/false/no/off`, any case) stops the
worker at start.

When switching a service to this mode, **the plain `-short` and `-long` queues of an
earlier two-lane worker stay bound and keep receiving copies of the requests: delete
them** (and the custom queues, if the worker had them).

### Custom lanes

Beside `short` and `long`, a worker has two **custom lanes**, `custom-short` and
`custom-long`, each with a queue of its own that only the custom key reaches. A custom
lane takes the timeout of its plain lane, and shares its **slot**: one lock and one
health check port per plain lane, so a custom request waits (its record staying
`RUNNING`) while the child of the other lane of its slot runs, and a service never runs
more children than it has plain lanes.

A worker serves a custom request on a custom lane only. On a plain lane it records the
operation `FAILED` without running anything, and a worker that does not know custom
requests finds no interface called `<name>@custom` and fails the record the same way:
a custom recollect is never run as a live replay. The publisher must publish a custom
request as mandatory: it fails when no worker declared the custom queue.

The child runs the real interface name, on a copy of the configuration where the entry
has the overrides applied and the copy is validated statically (nothing is evaluated).

A lane takes one request at a time. It polls its queue and **acknowledges a request
on receipt**: a replay can run for hours, and a redelivered one would collect its
window twice. No AMQP connection is held while a replay runs.

For each request, the lane copies the configuration of the service from the index to
a temporary folder of the work folder, runs the odata or jirax collector on it as a
child process in replay mode, with `--replay-suffix=<operation_id>`,
`--replay-requested-by=<requester>`, `--replay-retry <OPERATOR_REPLAY_RETRY>` and
`--config-service=<service>`, then removes the folder. The child still reads the copy
of the configuration: `--config-service` only names the service whose transfer stage
receives the [archive](archive) requests of the entries that have an `archive` rule. The child is killed at the lane timeout, even when it prints
nothing.

The operation record, written by the backend in `maas-operation` before it publishes
the request, is updated by id; the worker never creates a record:

- `RUNNING` when the replay starts, then every 30 s with the last 20 output lines in
  `details`, so `updated_at` keeps moving while the replay runs;
- `DONE` with `exit_code` 0, or `FAILED` with the exit code and the last lines. The
  child exits 1 when an interface exhausted its retries, and also when it refuses
  the replay, for an interface disabled or missing in its copy, or a jira one
  without `end_point`. It exits 74 when files could not be backed up, and the record
  says `N files could not be backed up to <backup>` (found in the last line the child
  printed, `backup failed: N files to <backup>`, even when other output follows it).
  It exits 69 when the transfer requests of an archive rule could not all be sent, and
  the record says `N documents could not be sent to the transfer stage` (from
  `transfer requests failed: N documents`); when files could not be backed up as well,
  a second line names them. The transfer code wins over the backup code;
- `FAILED` with the reason, and no child run, when the interface is missing,
  disabled, deleted, of another service or of a class that cannot be replayed, or
  when the copy skipped configuration files of the service (bad paths in the index),
  which are named.

A malformed request is acknowledged, logged as an error and skipped. When it carries
an `operation_id`, its record goes `FAILED` with the reason.

**A replay lost to a crash of the worker is not run again**: its record stays
`RUNNING`, with an `updated_at` that no longer moves. On SIGTERM or SIGINT, the worker
takes no new request, terminates the running collectors and starts no other, even for
a request it was preparing, whose record goes `FAILED`. A terminated collector gets
10 s to exit before it is killed, and its record gets its real outcome: `FAILED` with
its exit code, or `DONE` when it still finishes the replay and exits 0 within those
10 s.

## Checks

A check compares a product as an interface has it now with the document maas stored
for it. The request carries the `reply_to` and `correlation_id` message properties
and names the product by name or by id, exactly one of the two:

```json
{"operation_id": "...", "interface_name": "PRIP_S3A_ACRI", "product_name": "S3A_..."}
{"operation_id": "...", "interface_name": "PRIP_S3A_ACRI", "product_id": "3d5934fa-..."}
```

Only **odata v4** interfaces can be checked. The worker asks the interface for the
product with the credentials of the interface, runs the extraction of the interface
on the answer to get the document maas would store, and reads the stored one. The
reply goes to the default exchange with `routing_key` = `reply_to`, the same
`correlation_id`, and an `expiration` of 60 s, so that a reply nobody waits for any
more does not stay in the queue:

```json
{"status": "ok", "remote": {...}, "stored": {...},
 "diff": [{"field": "content_length", "remote": 12, "stored": 10}], "error": null}
```

- `diff` lists the fields whose values differ, `reportName` and `ingestionTime` left
  out: they change at each collect without the product changing.
- A product the interface no longer has gives `remote: null`, and the stored document
  is still searched by the `id_field` of the interface.
- `status` is `error`, with the message in `error`, when the interface is missing,
  disabled, not odata or odata v3, when the interface answers an error, when the
  extraction refuses the product, or when the request is malformed.
- A check request without `reply_to` is dropped with a warning.

The backend waits 30 s for a reply. The interface and its token request get
`OPERATOR_CHECK_TIMEOUT` seconds, 20 by default, which leaves time for the stored
document and the reply. A check waits at most `OPERATOR_POLL_INTERVAL` seconds in an
idle queue before it is taken: keep the poll interval and the check timeout well
within 30 s together.

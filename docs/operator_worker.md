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

A long replay may run `max(short timeout, days of its window × long timeout per
day)` seconds.

## Queues

Every queue is bound to the topic exchange `maas-operator-exchange` (durable):

| Queue                                  | Routing key                             | Requests                    |
| -------------------------------------- | --------------------------------------- | --------------------------- |
| `maas-operator-<service>-replay-short` | `maas.operator.<service>.replay.short`  | replays of 24 h at most     |
| `maas-operator-<service>-replay-long`  | `maas.operator.<service>.replay.long`   | longer replays              |
| `maas-operator-<service>-check`        | `maas.operator.<service>.check`         | product checks              |

Dates are ISO 8601 UTC with `Z`. A replay request is:

```json
{"operation_id": "...", "interface_name": "PRIP_S3A_ACRI",
 "start_date": "2026-10-01T00:00:00Z", "end_date": "2026-10-02T00:00:00Z",
 "requested_by": "bob", "lane": "short"}
```

## Replays

A lane takes one request at a time. It polls its queue and **acknowledges a request
on receipt**: a replay can run for hours, and a redelivered one would collect its
window twice. No AMQP connection is held while a replay runs.

For each request, the lane copies the configuration of the service from the index to
a temporary folder of the work folder, runs the odata or jirax collector on it as a
child process in replay mode, with `--replay-suffix <operation_id>`, then removes the
folder. The child is killed at the lane timeout, even when it prints nothing.

The operation record, written by the backend in `maas-operation` before it publishes
the request, is updated by id; the worker never creates a record:

- `RUNNING` when the replay starts, then every 30 s with the last 20 output lines in
  `details`, so `updated_at` keeps moving while the replay runs;
- `DONE` with `exit_code` 0, or `FAILED` with the exit code and the last lines;
- `FAILED` with the reason when the interface is missing, disabled, deleted, of
  another service or of a class that cannot be replayed.

A malformed request is acknowledged, logged as an error and skipped. When it carries
an `operation_id`, its record goes `FAILED` with the reason.

**A replay lost to a crash of the worker is not run again**: its record stays
`RUNNING`, with an `updated_at` that no longer moves. On SIGTERM or SIGINT, the worker
takes no new request and terminates the running collectors: their records go
`FAILED`.

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

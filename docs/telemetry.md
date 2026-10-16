# Telemetry

The collectors can send OpenTelemetry traces and metrics over OTLP, through the
`maas_model.telemetry` library. Telemetry is **off by default**: the collector then
behaves, logs and publishes exactly as without it, and `opentelemetry` is not even
imported.

The library itself, its environment variables and its rules are described in the
maas-model documentation, `docs/telemetry.md` of the maas-model repository.

## Enabling it

Install the extra, which pulls `maas-model[telemetry]` and the OpenTelemetry SDK and
OTLP/HTTP exporter:

```console
pip install "maas-collector[telemetry]"
```

Then turn it on with the command line option or the environment:

| CLI option | Environment variable | Description |
| --- | --- | --- |
| --telemetry | MAAS_TELEMETRY | Send traces and metrics. `MAAS_TELEMETRY` accepts `1`, `true`, `yes` or `on` (default: off) |

Without the extra, enabling telemetry logs one warning and the collector runs as
before.

Where the data goes and who sends it come from the environment. In a docker swarm
service:

```yaml
environment:
  MAAS_TELEMETRY: "1"
  OTEL_EXPORTER_OTLP_ENDPOINT: http://alloy:4318
  OTEL_EXPORTER_OTLP_PROTOCOL: http/protobuf
  MAAS_DEPLOYMENT: "{{.Service.Name}}"
  MAAS_INSTANCE_ID: "{{.Service.Name}}.{{.Task.Slot}}"
```

| Variable | Effect |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP receiver, the Alloy agent of the stack |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf`, the only protocol the extra installs |
| `MAAS_DEPLOYMENT` | the `deployment` label of every metric, the swarm service name. Default: the host name |
| `MAAS_INSTANCE_ID` | the `instance` label and `service.instance.id`, one per replica. Default: `<hostname>-<pid>` |

The resource is `service.name=maas-collector`, `service.namespace=maas`,
`maas.component=collector` and `maas.collector.type=<collector class>`, for example
`FileCollector` or `ODataCollector`. Mimir turns it into `job="maas/maas-collector"` and
`instance=<MAAS_INSTANCE_ID>`.

### Replays from the operator worker

A replay run by the [operator worker](operator_worker.md) is a child process with its
own telemetry instance: it gets
`MAAS_INSTANCE_ID=<instance id of the worker>-replay-<lane>`, for example
`collector-odata.1-replay-short`. The host name stands in for the worker's id when
`MAAS_INSTANCE_ID` is not set. The replay's series therefore never merge with those of
the worker that started it.

## What is emitted

### Traces

| Span | When | Attributes |
| --- | --- | --- |
| `collect.file` | one per extracted file | `maas.collector.type`, `maas.interface`, `maas.document.class`, `maas.file.name` (base name), and on exit `maas.documents.created`, `maas.documents.updated`, `maas.documents.errors`, `maas.documents.unmodified` |
| `opensearch <operation>` | each OpenSearch request, from the library | `db.operation.name`, `db.collection.name`, `http.response.status_code` |
| `publish <exchange>` | each AMQP message | `messaging.destination.name`, `messaging.rabbitmq.destination.routing_key`, `maas.message_id`, `maas.document.count`, `maas.document.class` |

`collect.file` carries the origin interface as the baggage `maas.origin.interface`. The
trace context and the baggage travel in the AMQP message **headers**
(`traceparent`, `tracestate`, `baggage`), never in the JSON body, so an engine
consuming the message continues the same trace.

- A configuration **without** `amqp` chunking sends one message per document, inside
  the `collect.file` span: the `publish` span is its child, and the engine's `process`
  span follows in the same trace.
- A **chunked** message (an `amqp` `chunk_size` on the routing key), a chunk sent when
  the buffer is full or the remainder flushed at the end of the loop, gathers documents
  of several files. Its `publish` span starts a **new trace**, linked to the
  `collect.file` span of each of its files (at most 128 links). The origin interface
  baggage does not travel with it.
- The `file_routing_key` message of a file is linked to that file's `collect.file`
  span.

### Metrics

The Prometheus names, as Mimir stores them. Every series also carries `component`,
`deployment` and `instance`.

| Metric | Labels | Meaning |
| --- | --- | --- |
| `maas_collector_files_total` | node, document_class, outcome | files extracted; `outcome` is `ok`, `empty` (no document) or `error` |
| `maas_collector_file_duration_seconds_*` | node, document_class | time to extract and index one file |
| `maas_collector_documents_total` | node, document_class, result | documents by `result`: `created`, `updated`, `unmodified`, `error` |
| `maas_collector_documents_routed_total` | node, exchange, routing_key | documents handed to the message bus |
| `maas_collector_loop_duration_seconds_*` | collector_type | one collection loop over all the configurations |
| `maas_messages_published_total` | exchange, routing_key, outcome | messages published, `ok` or `error` |
| `maas_messages_returned_total` | exchange, routing_key, reason | messages the broker returned, `NO_ROUTE` or `other` |
| `maas_message_documents_bucket` | direction, exchange, routing_key | documents per message |
| `maas_opensearch_time_seconds_total`, `maas_opensearch_requests_total` | node_type, node, operation, index | OpenSearch time and requests per collector node, from the library |
| `maas_opensearch_request_duration_seconds_*` | operation, index, status_class | OpenSearch request duration, from the library |

`node` is the `interface_name` of the configuration, or its `model` when it has none.
`document_class` is the `model`. File names and document ids are span attributes only,
never labels.

### Topology

At start-up, the collector declares its part of the flow map as info gauges:

- `maas_node_info{node_type="collector"}`: one node per loaded configuration, with the
  collector class as `impl`.
- `maas_topology_route_info{node_type="collector", direction="out", source="config"}`:
  one route per configuration that publishes, with its exchange (the configuration's
  `exchange_name`, else `--amqp-exchange`, else `collect-exchange`), its
  `routing_key` and its `document_class`, and one more for its `file_routing_key`.
  A configuration with an empty `routing_key` declares no route.

The interface monitor declares only the interface it monitors.

`maas_build_info` gives the maas-collector and maas-model versions.

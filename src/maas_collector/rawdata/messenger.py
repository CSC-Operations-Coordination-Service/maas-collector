"""Message bus tools"""

import dataclasses
import json
import logging
from typing import Dict, List, Sequence, Tuple
import socket


from kombu import BrokerConnection, Exchange, Producer
from kombu.common import maybe_declare
from kombu.utils.url import maybe_sanitize_url

from maas_model import MAASMessage, telemetry
from maas_model.telemetry import metrics

from maas_collector.rawdata.telemetry import node_name
from maas_collector.queues.queues import (
    DEFAULT_PUBLISH_EXCHANGE_NAME,
    build_publish_exchange,
)


# most file span contexts linked to one aggregated message
MAX_GROUP_LINKS = 128


def _unique_contexts(contexts) -> list:
    """span contexts without None and duplicates, order kept, capped"""
    unique = []
    for context in contexts:
        if context is not None and context not in unique:
            unique.append(context)
            if len(unique) >= MAX_GROUP_LINKS:
                break
    return unique


def parse_chunk_config(json_dict: dict) -> dict:
    """read the chunk sizes of the content of a configuration file

    Args:
        json_dict (dict): configuration content, {collectors?, models?, amqp?}

    Raises:
        ValueError: when the amqp section is not a dict of routing keys to dicts,
            or a chunk_size is not a positive integer

    Returns:
        dict: routing key -> chunk size
    """
    amqp = json_dict.get("amqp", {})

    if not isinstance(amqp, dict):
        raise ValueError(f"amqp must be an object, not {type(amqp).__name__}")

    chunk_config = {}

    for routing_key, routing_info in amqp.items():
        if not isinstance(routing_info, dict):
            raise ValueError(f"amqp.{routing_key} must be an object")

        if "chunk_size" not in routing_info:
            continue

        chunk_size = routing_info["chunk_size"]

        if (
            not isinstance(chunk_size, int)
            or isinstance(chunk_size, bool)
            or chunk_size <= 0
        ):
            raise ValueError(
                f"amqp.{routing_key}.chunk_size must be a positive integer"
            )

        chunk_config[routing_key] = chunk_size

    return chunk_config

class Messenger:
    """
    Messenger encapsulates collector message emitting using groups

    """

    def __init__(
        self,
        url: str,
        priority: int = 0,
        max_retries: int = 0,
        pipeline_name: str = "Collector",
        exchange_name: str = DEFAULT_PUBLISH_EXCHANGE_NAME,
    ):
        self.logger: logging.Logger = logging.getLogger(pipeline_name)

        # rabbitmq url
        self.url: str = url

        # default publishing exchange, used by every configuration that does
        # not name one of its own
        self.exchange_name: str = exchange_name or DEFAULT_PUBLISH_EXCHANGE_NAME

        self.priority: int = priority

        self.max_retries: int = max_retries

        self.pipeline_name: str = pipeline_name

        # rabbitmq connection
        self._connection: BrokerConnection | None = None

        # message emitter
        self._producer: Producer | None = None

        # message group configuration, keyed by routing key: chunking is a
        # property of the message flow, not of its destination exchange
        self.chunk_config: Dict[str, int] = {}

        # grouped messages, keyed by (exchange name, routing key): two
        # configurations may share a routing key and differ in exchange
        self.message_groups = {}

        # span context (or None) of the file that produced each buffered
        # document, aligned with message_groups[...][model]["document_ids"]
        # and keyed (exchange name, routing key, model name)
        self._group_contexts: Dict[Tuple[str, str, str], List] = {}

        # Exchange instances by name, built on demand
        self._exchanges: Dict[str, Exchange] = {}

        # names of the exchanges already declared on the broker
        self._declared: set[str] = set()

    @property
    def connection(self) -> BrokerConnection:
        """
        Lazy build broker connection

        Returns:
            BrokerConnection: established connection
        """
        if self._connection is None:
            self._connection = BrokerConnection(
                self.url,
                transport_options={
                    "client_properties": {
                        "connection_name": f"collector@{socket.gethostname()}"
                    }
                },
            )
        return self._connection

    @property
    def producer(self) -> Producer:
        """
        Lazy build producer

        Returns:
            Producer: initialized producer
        """
        if self._producer is None:
            default_exchange = self.get_exchange(self.exchange_name)
            self._producer = self.connection.Producer(
                exchange=default_exchange, on_return=self._on_return
            )
            # the channel only exists once the producer does, so the default
            # exchange is declared here rather than in get_exchange()
            self._declare(default_exchange, self._producer.channel)
        return self._producer

    def get_exchange(self, name: str = "") -> Exchange:
        """get the Exchange instance for a name, building it on first use

        Nothing is sent to the broker here: declaration needs a channel, and
        the channel needs the producer, which is built from the default
        exchange. See _declare().

        Args:
            name (str, optional): exchange name. Defaults to the messenger one.

        Returns:
            Exchange: durable topic exchange
        """
        name = name or self.exchange_name

        if name not in self._exchanges:
            self._exchanges[name] = build_publish_exchange(name)

        return self._exchanges[name]

    def _declare(self, exchange: Exchange, channel) -> None:
        """declare an exchange on the broker, once per exchange

        Args:
            exchange (Exchange): exchange to declare
            channel: channel to declare it on
        """
        if exchange.name in self._declared:
            return

        maybe_declare(exchange, channel)

        self._declared.add(exchange.name)

        self.logger.debug("AMQP: declared exchange %s", exchange.name)

    def setup(self):
        """Connect to the AMQP broker and setup message producer"""
        self.logger.debug(
            "Setup connection to AMQP broker: %s", maybe_sanitize_url(self.url)
        )

    def load_config_file(self, path: str):
        """Load amqp configuration from file"""
        with open(path, "r", encoding="UTF-8") as config_fd:
            json_config = json.load(config_fd)

        self.load_config_dict(json_config, path)

    def load_config_dict(self, json_dict: dict, source: str):
        """Load amqp configuration from the content of a configuration file

        Args:
            json_dict (dict): configuration content, {collectors?, models?, amqp?}
            source (str): name of the content, for the logs
        """
        for routing_key, chunk_size in parse_chunk_config(json_dict).items():
            # log overwrite
            if routing_key in self.chunk_config:
                # log redeclare
                self.logger.warning(
                    "%s: overwrite chunk size config for %s from %s to %s",
                    source,
                    routing_key,
                    self.chunk_config[routing_key],
                    chunk_size,
                )

            self.logger.info(
                "Set chunk configuration for %s: %d",
                routing_key,
                chunk_size,
            )

            self.chunk_config[routing_key] = chunk_size

    def handle_message(self, config, document_id: str, index: str | None = None):
        """handle a document identifier and decide to send it (or not) on the message
        bus following the collector configuration. Accumulate identifiers according to
        the amqp chunk size in the configuration file to send message groups.

        Args:
            config ([FileCollectorConfiguration]): configuration
            document_id ([str]): document identifier
        """
        if not config.routing_key:
            # don't send any message
            return

        # a configuration may name its own destination exchange; fall back on
        # the messenger default. getattr keeps configuration classes that do
        # not carry the field working.
        exchange_name = getattr(config, "exchange_name", "") or self.exchange_name

        metrics.COLLECTOR_DOCUMENTS_ROUTED.add(
            1,
            {
                "node": node_name(config),
                "exchange": exchange_name,
                "routing_key": config.routing_key,
            },
        )

        document_indices = [index] if index is not None else []

        if not config.routing_key in self.chunk_config:
            # no message grouping: send single document identifier and index
            self.send_to_queue(
                config.routing_key,
                MAASMessage(
                    document_class=config.model_name,
                    document_ids=[document_id],
                    document_indices=document_indices,
                    pipeline=[self.pipeline_name],
                ),
                exchange_name=exchange_name,
            )
            return

        chunk_size = self.chunk_config[config.routing_key]

        # get the message group for this destination
        group_key = (exchange_name, config.routing_key)

        if group_key in self.message_groups:
            group = self.message_groups[group_key]
        else:
            group = self.message_groups[group_key] = {}

        # get the document identifier and index list for the model
        if config.model_name in group:
            info = group[config.model_name]

        else:
            info = group[config.model_name] = {}
            info["document_ids"] = []
            info["document_indices"] = []

        contexts = self._group_contexts.setdefault(
            (exchange_name, config.routing_key, config.model_name), []
        )
        contexts.append(telemetry.current_span_context())

        info["document_ids"].append(document_id)

        if index is not None and index not in info["document_indices"]:
            info["document_indices"].append(index)

        # send messages by chunk depending the configuration
        while len(info["document_ids"]) >= chunk_size:
            self.logger.debug(
                "%s buffer has reach %d limit, sending payload",
                config.routing_key,
                chunk_size,
            )

            # send the chunk, linked to exactly the files of its documents
            self.send_to_queue(
                config.routing_key,
                MAASMessage(
                    document_class=config.model_name,
                    document_ids=info["document_ids"][:chunk_size],
                    document_indices=info["document_indices"][:chunk_size],
                    pipeline=[self.pipeline_name],
                ),
                exchange_name=exchange_name,
                links=_unique_contexts(contexts[:chunk_size]),
                new_trace=True,
            )

            # remove the chunk
            del info["document_ids"][:chunk_size]
            del info["document_indices"][:chunk_size]
            del contexts[:chunk_size]

        if not contexts:
            self._group_contexts.pop(
                (exchange_name, config.routing_key, config.model_name), None
            )

    def flush_message_groups(self):
        """clear the document identifier cache"""
        for (exchange_name, routing_key), group in self.message_groups.items():
            for model_name, info in group.items():
                # filter empty identifier list

                if not info["document_ids"]:
                    continue

                self.send_to_queue(
                    routing_key,
                    MAASMessage(
                        document_class=model_name,
                        document_ids=info["document_ids"],
                        document_indices=info["document_indices"],
                        pipeline=[self.pipeline_name],
                    ),
                    exchange_name=exchange_name,
                    links=_unique_contexts(
                        self._group_contexts.get(
                            (exchange_name, routing_key, model_name), ()
                        )
                    ),
                    new_trace=True,
                )

        self.message_groups.clear()
        self._group_contexts.clear()

    def send_to_queue(
        self,
        routing_key: str,
        payload: MAASMessage,
        exchange_name: str = "",
        links: Sequence = (),
        new_trace: bool = False,
    ):
        """send creation / update message to rabbitmq

        Args:
            routing_key (str): routing key on the publishing exchange
            payload (MAASMessage): payload containing document ids and indices
            exchange_name (str, optional): destination exchange. Defaults to the
                messenger one.
            links (Sequence, optional): span contexts linked to the publish span
            new_trace (bool, optional): start the publish span as a new root
                trace, for messages aggregating several origins
        """
        # Keep only unique index
        payload.document_indices = list(set(payload.document_indices))
        payload.pipeline = [self.pipeline_name]

        body = dataclasses.asdict(payload)

        exchange = self.get_exchange(exchange_name)

        self.logger.info(
            "MSG %s PUBLISHING TO %s/%s",
            payload.message_id,
            exchange.name,
            routing_key,
        )

        self.logger.debug("Message body: %s", body)

        json_body = json.dumps(body)

        try:
            # resolve the producer first: _declare needs its channel
            producer = self.producer

            self._declare(exchange, producer.channel)

            with telemetry.amqp.publish_span(
                exchange.name,
                routing_key,
                message_id=payload.message_id,
                document_count=len(payload.document_ids),
                document_class=payload.document_class,
                body_size=len(json_body),
                links=links,
                new_trace=new_trace,
            ) as headers:
                producer.publish(
                    json_body,
                    content_type="application/json",
                    exchange=exchange,
                    routing_key=routing_key,
                    delivery_mode="persistent",
                    mandatory=True,
                    retry=True,
                    priority=self.priority,
                    headers=headers,
                    retry_policy={
                        "interval_start": 0,  # First retry immediately,
                        "interval_step": 2,  # then increase by 2s for every retry.
                        "interval_max": 30,  # but don't exceed 30s between retries.
                        "max_retries": self.max_retries,  # give up after X tries.
                    },
                )
            self.logger.info(
                "MSG %s PUBLISHED TO %s/%s %d %s",
                payload.message_id,
                exchange.name,
                routing_key,
                len(payload.document_ids),
                payload.document_class,
            )
        except Exception as error:
            self.logger.error(
                "MSG %s FAILED TO PUBLISH TO %s/%s %d %s (%s)",
                payload.message_id,
                exchange.name,
                routing_key,
                len(payload.document_ids),
                payload.document_class,
                error,
            )
            self.logger.error(
                "MSG %s UNPUBLISHED on %s/%s PAYLOAD %s",
                payload.message_id,
                exchange.name,
                routing_key,
                json_body,
            )
            raise error

    def _on_return(self, exception, exchange, routing_key, message):
        """Error callback for message publishing

        see:

        https://docs.celeryproject.org/projects/kombu/en/stable/userguide/producers.html

        for details
        """
        args = getattr(exception, "args", ())
        no_route = len(args) > 1 and args[1] == "NO_ROUTE"
        telemetry.amqp.returned(
            exchange, routing_key, "NO_ROUTE" if no_route else "other"
        )
        if no_route:
            self.logger.warning(
                "AMQP exchange %s has no route for %s", exchange, routing_key
            )
        else:
            self.logger.error(
                "AMQP error on %s:%s : %s", exchange, routing_key, exception
            )
            self.logger.error("Cannot deliver message: %s", message.body)

    def close(self):
        """

        Close resources
        """
        for symbol in ["_producer", "_connection"]:
            value = getattr(self, symbol)
            if value is not None:
                try:
                    self.logger.debug("Closing %s", symbol)
                    value.close()
                # pylint: disable=broad-exception-caught
                # no necessity to raise, only log
                except Exception as error:
                    self.logger.error("Cannot close %s: %s", symbol, error)
                # pylint: enable=broad-exception-caught
                finally:
                    setattr(self, symbol, None)

    def __len__(self):
        return sum(
            [
                len(info["document_ids"])
                for info in [group.values() for group in self.message_groups.values()]
                if info and "document_ids" in info
            ]
        )

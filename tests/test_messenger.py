"""Messenger publishing: routing key and destination exchange"""

import dataclasses
import json
from unittest.mock import MagicMock

import pytest

from maas_collector.queues.queues import DEFAULT_PUBLISH_EXCHANGE_NAME
from maas_collector.rawdata.messenger import Messenger


@dataclasses.dataclass
class FakeConfig:
    """the two attributes Messenger.handle_message reads off a configuration"""

    routing_key: str

    model_name: str = "CdseProduct"

    exchange_name: str = ""


@pytest.fixture
def messenger():
    """a Messenger whose producer is a mock, so nothing touches a broker"""

    def _build(exchange_name=DEFAULT_PUBLISH_EXCHANGE_NAME):
        instance = Messenger("amqp://localhost:5672", exchange_name=exchange_name)
        # bypass the lazy connection/producer construction entirely
        instance._producer = MagicMock()
        instance._producer.channel = MagicMock()
        return instance

    return _build


def published(messenger_instance):
    """(exchange name, routing key, payload dict) of every publish call"""
    return [
        (
            call.kwargs["exchange"].name,
            call.kwargs["routing_key"],
            json.loads(call.args[0]),
        )
        for call in messenger_instance._producer.publish.call_args_list
    ]


def test_default_exchange_when_config_names_none(messenger):
    """a configuration without exchange_name publishes to the messenger default"""
    instance = messenger()

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-1", "index-1")

    exchange, routing_key, payload = published(instance)[0]

    assert exchange == DEFAULT_PUBLISH_EXCHANGE_NAME
    assert routing_key == "new.raw.data.cdse-product"
    assert payload["document_ids"] == ["doc-1"]
    assert payload["document_indices"] == ["index-1"]
    assert payload["document_class"] == "CdseProduct"


def test_process_wide_default_is_honoured(messenger):
    """--amqp-exchange / AMQP_EXCHANGE moves every message"""
    instance = messenger(exchange_name="dev-ab-exchange")

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-1")

    assert published(instance)[0][0] == "dev-ab-exchange"


def test_configuration_overrides_the_default(messenger):
    """the exchange_name key of a collector configuration wins"""
    instance = messenger(exchange_name="dev-ab-exchange")

    instance.handle_message(
        FakeConfig("new.raw.data.cdse-product", exchange_name="etl-exchange"), "doc-1"
    )

    assert published(instance)[0][0] == "etl-exchange"


def test_one_collector_ventilates_over_several_exchanges(messenger):
    """the point of the feature: same process, two destinations"""
    instance = messenger()

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-1")
    instance.handle_message(
        FakeConfig("new.raw.data.s1-ocn-backup", exchange_name="etl-exchange"), "doc-2"
    )

    assert [(exchange, key) for exchange, key, _ in published(instance)] == [
        (DEFAULT_PUBLISH_EXCHANGE_NAME, "new.raw.data.cdse-product"),
        ("etl-exchange", "new.raw.data.s1-ocn-backup"),
    ]


def test_exchange_is_declared_once_per_exchange(messenger):
    """maybe_declare is not re-sent for an exchange already declared"""
    instance = messenger()

    instance.handle_message(FakeConfig("rk.a"), "doc-1")
    instance.handle_message(FakeConfig("rk.a"), "doc-2")
    instance.handle_message(FakeConfig("rk.b", exchange_name="etl-exchange"), "doc-3")

    assert instance._declared == {DEFAULT_PUBLISH_EXCHANGE_NAME, "etl-exchange"}


def test_chunks_do_not_mix_across_exchanges(messenger):
    """two configurations sharing a routing key but not an exchange stay apart

    The message group is keyed by (exchange, routing key). Keyed by routing key
    alone, the two documents below would be merged into a single chunk and sent
    to whichever exchange flushed first.
    """
    instance = messenger()
    instance.chunk_config["new.raw.data.cdse-product"] = 2

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-1")
    instance.handle_message(
        FakeConfig("new.raw.data.cdse-product", exchange_name="etl-exchange"), "doc-2"
    )

    # neither group reached the chunk size of 2
    assert instance._producer.publish.call_count == 0
    assert set(instance.message_groups) == {
        (DEFAULT_PUBLISH_EXCHANGE_NAME, "new.raw.data.cdse-product"),
        ("etl-exchange", "new.raw.data.cdse-product"),
    }

    instance.flush_message_groups()

    assert sorted(
        (exchange, payload["document_ids"]) for exchange, _, payload in published(instance)
    ) == [
        (DEFAULT_PUBLISH_EXCHANGE_NAME, ["doc-1"]),
        ("etl-exchange", ["doc-2"]),
    ]


def test_chunking_still_groups_within_one_exchange(messenger):
    """the pre-existing behaviour is unchanged when no exchange is named"""
    instance = messenger()
    instance.chunk_config["new.raw.data.cdse-product"] = 2

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-1", "index-1")
    assert instance._producer.publish.call_count == 0

    instance.handle_message(FakeConfig("new.raw.data.cdse-product"), "doc-2", "index-1")

    exchange, routing_key, payload = published(instance)[0]
    assert exchange == DEFAULT_PUBLISH_EXCHANGE_NAME
    assert routing_key == "new.raw.data.cdse-product"
    assert payload["document_ids"] == ["doc-1", "doc-2"]


def test_empty_routing_key_publishes_nothing(messenger):
    """unchanged: an empty routing key short-circuits before any broker access"""
    instance = messenger()

    instance.handle_message(FakeConfig(""), "doc-1")

    assert instance._producer.publish.call_count == 0

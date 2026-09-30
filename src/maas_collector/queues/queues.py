"""
Define Exchanges, Queues, Binding
"""

from kombu import Exchange, Queue

COLLECT_EXCHANGE = Exchange("s3-exchange", type="topic", durable=True)

COLLECT_QUEUE_LIST = [
    Queue(
        "collect-new-raw-data",
        COLLECT_EXCHANGE,
        routing_key="new.raw.data",
        durable=True,
        exclusive=False,
        max_priority=9,
    )
]

# Default destination of the messages a collector publishes.
#
# It is only a default: Messenger takes an exchange name, set process-wide by
# --amqp-exchange / AMQP_EXCHANGE, and a collector configuration entry can name
# its own through the "exchange_name" key. That is what allows messages to be
# ventilated per exchange and not only per routing key.
DEFAULT_PUBLISH_EXCHANGE_NAME = "collect-exchange"

# Kept as a module constant so existing imports keep resolving.
PUBLISH_EXCHANGE = Exchange(DEFAULT_PUBLISH_EXCHANGE_NAME, type="topic", durable=True)


def build_publish_exchange(name: str) -> Exchange:
    """build a publishing exchange

    Every exchange a collector publishes to is a durable topic exchange, exactly
    like PUBLISH_EXCHANGE: the engines bind their queues to it by routing key.

    Args:
        name (str): exchange name

    Returns:
        Exchange: durable topic exchange
    """
    return Exchange(name, type="topic", durable=True)

import kombu
import pytest

from maas_collector.rawdata.transfer import amqp
from maas_collector.rawdata.transfer.amqp import (
    TransferPublisher,
    TransferPublishError,
    take_body,
    transfer_connection,
)
from maas_collector.rawdata.transfer.contract import TransferRequest, transfer_queue

REQUEST = TransferRequest("svc", "ITF_A", (("d1", "raw-data-a-2026"),), "archive-a", None, "skip")


def memory():
    # the memory transport shares one broker per process
    return kombu.Connection("memory://")


@pytest.fixture(autouse=True)
def clean_queue():
    with memory() as connection:
        transfer_queue("svc")(connection.default_channel).declare()
        transfer_queue("svc")(connection.default_channel).purge()
    yield


def test_request_reaches_the_queue_of_its_service():
    with memory() as connection:
        queue = transfer_queue("svc")(connection.default_channel)
        queue.declare()
        TransferPublisher(memory).publish(REQUEST)
        message = queue.get()
    assert TransferRequest.from_body(message.body) == REQUEST
    assert message.delivery_info["routing_key"] == "maas.transfer.svc"


def test_take_body_acknowledges_on_receipt():
    TransferPublisher(memory).publish(REQUEST)
    assert TransferRequest.from_body(take_body(memory, "svc")) == REQUEST
    assert take_body(memory, "svc") is None


class FakeProducer:
    """records the publish arguments; can answer with a return or an error"""

    instances = []
    returns = False
    errors = []

    def __init__(self, connection, on_return=None):
        self.connection = connection
        self.on_return = on_return
        self.calls = []
        FakeProducer.instances.append(self)

    def publish(self, body, **kwargs):
        if FakeProducer.errors:
            raise FakeProducer.errors.pop(0)
        self.calls.append((body, kwargs))
        if FakeProducer.returns:
            self.on_return(Exception("NO_ROUTE"), kwargs["exchange"], kwargs["routing_key"], None)


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def fake_producer(monkeypatch):
    FakeProducer.instances = []
    FakeProducer.returns = False
    FakeProducer.errors = []
    monkeypatch.setattr(amqp.kombu, "Producer", FakeProducer)
    return FakeProducer


def test_publish_is_mandatory_persistent_and_confirmed(fake_producer):
    TransferPublisher(FakeConnection).publish(REQUEST)
    [(body, kwargs)] = fake_producer.instances[0].calls
    assert body == REQUEST.to_body()
    assert (kwargs["mandatory"], kwargs["delivery_mode"], kwargs["routing_key"]) == (
        True,
        "persistent",
        "maas.transfer.svc",
    )
    assert kwargs["exchange"].name == "maas-transfer-exchange"
    assert (
        transfer_connection("amqp://u:p@broker//").transport_options["confirm_publish"]
        is True
    )


def test_returned_request_is_an_error(fake_producer):
    fake_producer.returns = True
    connections = []

    def factory():
        connections.append(FakeConnection())
        return connections[-1]

    publisher = TransferPublisher(factory)
    with pytest.raises(TransferPublishError, match="maas.transfer.svc"):
        publisher.publish(REQUEST)
    assert connections[0].closed


def test_broker_error_is_an_error_and_the_next_publish_reconnects(fake_producer):
    fake_producer.errors = [OSError("broker gone")]
    factory_calls = []

    def factory():
        factory_calls.append(1)
        return FakeConnection()

    publisher = TransferPublisher(factory)
    with pytest.raises(TransferPublishError):
        publisher.publish(REQUEST)
    publisher.publish(REQUEST)
    assert len(factory_calls) == 2

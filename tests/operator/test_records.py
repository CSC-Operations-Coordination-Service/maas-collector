import datetime

from opensearchpy.exceptions import NotFoundError

from maas_collector.rawdata.operator.records import DONE, RUNNING, OperationRecorder


class FakeClient:
    def __init__(self, error=None):
        self.error = error
        self.calls = []

    def update(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error


NOW = datetime.datetime(2026, 10, 16, 10, 0, 5, tzinfo=datetime.UTC)


def test_update_sends_only_given_fields():
    client = FakeClient()
    rec = OperationRecorder(client, clock=lambda: NOW)
    assert rec.update("op1", RUNNING)
    assert client.calls == [
        {
            "index": "maas-operation",
            "id": "op1",
            "body": {
                "doc": {"status": "RUNNING", "updated_at": "2026-10-16T10:00:05Z"}
            },
            "retry_on_conflict": 3,
        }
    ]
    rec.update("op1", DONE, exit_code=0, details="ok")
    assert client.calls[1]["body"]["doc"] == {
        "status": "DONE",
        "updated_at": "2026-10-16T10:00:05Z",
        "exit_code": 0,
        "details": "ok",
    }


def test_not_found_returns_false_without_upsert(caplog):
    client = FakeClient(NotFoundError(404, "missing", {}))
    assert OperationRecorder(client, clock=lambda: NOW).update("x", RUNNING) is False
    assert "doc_as_upsert" not in client.calls[0]["body"]
    assert "x" in caplog.text


def test_other_error_returns_false():
    client = FakeClient(RuntimeError("boom"))
    assert OperationRecorder(client).update("x", RUNNING) is False

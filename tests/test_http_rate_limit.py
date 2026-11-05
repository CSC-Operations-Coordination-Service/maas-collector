"""A rate limited (429) or overloaded (503) API is waited for, not failed at once"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

import pytest
import requests
from requests.adapters import HTTPAdapter

from maas_collector.rawdata.collector.httpcollector import (
    HttpConfiguration,
    replay_retry_wait,
)


class ScriptedHandler(BaseHTTPRequestHandler):
    """answers the scripted statuses in turn, then 200"""

    statuses = []
    calls = 0

    def do_GET(self):  # pylint: disable=invalid-name
        cls = type(self)
        status = cls.statuses[cls.calls] if cls.calls < len(cls.statuses) else 200
        cls.calls += 1
        body = b'{"error":"WAF","status":429}' if status != 200 else b"{}"
        self.send_response(status)
        if status != 200:
            self.send_header("Retry-After", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):  # silence the test output
        pass


@pytest.fixture(name="serve")
def fixture_serve():
    """start a local server answering the given statuses, yield its url"""
    servers = []

    def serve(statuses):
        handler = type("Handler", (ScriptedHandler,), {"statuses": statuses})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append(server)
        return handler, f"http://127.0.0.1:{server.server_port}/Products"

    yield serve

    for server in servers:
        server.shutdown()
        server.server_close()


def session_for(config: HttpConfiguration) -> requests.Session:
    """a session mounted like HttpCollector.ingest_http does"""
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=config.status_retry())
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


@pytest.mark.parametrize("status", [429, 503])
def test_come_back_later_answers_are_retried(serve, status):
    handler, url = serve([status, status])

    with session_for(HttpConfiguration(status_backoff_factor=0)) as session:
        response = session.get(url, timeout=5)

    assert response.status_code == 200
    assert handler.calls == 3


def test_last_answer_is_returned_once_retries_are_exhausted(serve):
    """the caller still logs the 429 and its body, as before"""
    handler, url = serve([429] * 10)

    config = HttpConfiguration(status_retries=2, status_backoff_factor=0)
    with session_for(config) as session:
        response = session.get(url, timeout=5)

    assert response.status_code == 429
    assert b"WAF" in response.content
    assert handler.calls == 3


def test_other_errors_are_not_retried(serve):
    handler, url = serve([500])

    with session_for(HttpConfiguration(status_backoff_factor=0)) as session:
        response = session.get(url, timeout=5)

    assert response.status_code == 500
    assert handler.calls == 1


def test_post_is_not_retried():
    """a token request is not replayed behind the caller's back"""
    retry = HttpConfiguration().status_retry()

    assert not retry.is_retry("POST", 429)
    assert retry.is_retry("GET", 429)


def test_timeouts_still_fail_at_once():
    """the files retrieval mode relies on a timeout to skip unavailable files"""
    retry = HttpConfiguration().status_retry()

    assert retry.read == 0
    assert retry.connect == 0


def test_replay_waits_longer_at_each_retry():
    assert [replay_retry_wait(n) for n in range(1, 7)] == [10, 20, 40, 80, 120, 120]

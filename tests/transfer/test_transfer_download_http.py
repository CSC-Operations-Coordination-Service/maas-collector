"""The downloader against a real local server: streaming, the token and redirects"""

import hashlib
import http.server
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests

from maas_collector.rawdata.transfer.download import (
    PermanentTransferError,
    ProductDownloader,
)
from maas_collector.rawdata.transfer.rule import ArchiveRule

CONTENT = bytes(range(256)) * 4000


class Handler(http.server.BaseHTTPRequestHandler):
    seen = []

    def do_GET(self):  # pylint: disable=invalid-name
        Handler.seen.append((self.path, self.headers.get("Authorization")))
        if self.path.startswith("/odata/v1/Products('id-1')"):
            self.send_response(302)
            self.send_header("Location", "/bytes/p1")
            self.end_headers()
        elif self.path.startswith("/odata/v1/Products(id-2)"):
            if self.headers.get("Authorization") != "Bearer token":
                self.send_response(401)
                self.end_headers()
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(CONTENT)))
            self.end_headers()
            self.wfile.write(CONTENT)
        elif self.path == "/bytes/p1":
            self.send_response(200)
            self.send_header("Content-Length", str(len(CONTENT)))
            self.end_headers()
            self.wfile.write(CONTENT)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, *_args):
        pass


@pytest.fixture(name="server")
def fixture_server():
    Handler.seen = []
    httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()


class Authentication:
    @staticmethod
    def get_headers():
        return {"Authorization": "Bearer token"}


def downloader(server, template):
    rule = replace(ArchiveRule.default("archive-a"), download_url=server + template)
    config = SimpleNamespace(interface_name="ITF_A")
    return ProductDownloader(
        config, rule, session=requests.Session(), authentication=Authentication()
    )


def product(identifier):
    return {
        "product_id": identifier,
        "product_name": "P.zip",
        "size": len(CONTENT),
        "checksum": hashlib.md5(CONTENT).hexdigest(),
    }


def test_a_redirect_is_followed_without_the_token(server, tmp_path):
    result = downloader(server, "/odata/v1/Products({id})/$value").download(
        product("id-1"), str(tmp_path)
    )
    assert (tmp_path / "P.zip").read_bytes() == CONTENT and result.size == len(CONTENT)
    assert Handler.seen == [
        ("/odata/v1/Products('id-1')/$value", "Bearer token"),
        ("/bytes/p1", None),
    ]


def test_the_token_goes_to_the_configured_host(server, tmp_path):
    rule_downloader = downloader(server, "/odata/v1/Products({id})/$value")
    rule_downloader.rule = replace(rule_downloader.rule, quote_id=False)
    rule_downloader.download(product("id-2"), str(tmp_path))
    assert Handler.seen == [("/odata/v1/Products(id-2)/$value", "Bearer token")]


def test_a_404_is_permanent(server, tmp_path):
    with pytest.raises(PermanentTransferError, match="404"):
        downloader(server, "/nothing/{id}").download(product("id-3"), str(tmp_path))

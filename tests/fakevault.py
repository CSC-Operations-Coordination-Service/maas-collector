"""In-process stand-in for the parts of the OpenBao / Vault HTTP API the collector
uses: AppRole login and KV v2 read / write. Real hvac clients talk to it over HTTP.
"""

import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class FakeVault:
    """A KV v2 store behind AppRole and token authentication"""

    def __init__(self):
        self.role_id = "collector-role"
        self.secret_id = "collector-secret-id"
        self.root_token = "root-token"

        # "mount/path" -> list of versions, latest last
        self.secrets = {}

        # "mount/path" the tokens are not allowed to read
        self.denied = set()

        self.valid_tokens = {self.root_token}

        # number of successful AppRole logins
        self.logins = 0

        # False answers every request like a sealed vault
        self.available = True

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), self._handler_class())
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )

    @property
    def url(self) -> str:
        """base url of the fake vault"""
        return f"http://127.0.0.1:{self._server.server_port}"

    def start(self):
        """serve requests in a background thread"""
        self._thread.start()

    def stop(self):
        """stop serving requests"""
        self._server.shutdown()
        self._server.server_close()

    def put(self, mount: str, path: str, data: dict):
        """write a new version of a secret"""
        self.secrets.setdefault(f"{mount}/{path}", []).append(dict(data))

    def expire_tokens(self):
        """invalidate every token issued by AppRole logins"""
        self.valid_tokens = {self.root_token}

    def _handler_class(self):
        vault = self

        class Handler(BaseHTTPRequestHandler):
            """answer like the vault HTTP API"""

            def log_message(self, *args):  # pylint: disable=W0221
                pass

            def _reply(self, status: int, body: dict):
                payload = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _body(self) -> dict:
                length = int(self.headers.get("Content-Length") or 0)
                return json.loads(self.rfile.read(length) or b"{}")

            def _kv_path(self):
                # /v1/<mount>/data/<path>
                parts = self.path.split("?")[0][len("/v1/") :].split("/", 2)
                if len(parts) != 3 or parts[1] != "data":
                    return None
                return f"{parts[0]}/{parts[2]}"

            def _authorized(self, kv_path) -> bool:
                return (
                    self.headers.get("X-Vault-Token") in vault.valid_tokens
                    and kv_path not in vault.denied
                )

            def do_POST(self):  # pylint: disable=C0103
                """AppRole login and KV v2 write"""
                if not vault.available:
                    self._reply(503, {"errors": ["Vault is sealed"]})
                    return

                body = self._body()

                if self.path == "/v1/auth/approle/login":
                    if (body.get("role_id"), body.get("secret_id")) != (
                        vault.role_id,
                        vault.secret_id,
                    ):
                        self._reply(400, {"errors": ["invalid role or secret ID"]})
                        return

                    token = f"s.{uuid.uuid4().hex}"
                    vault.valid_tokens.add(token)
                    vault.logins += 1
                    self._reply(
                        200,
                        {
                            "request_id": str(uuid.uuid4()),
                            "lease_id": "",
                            "renewable": False,
                            "lease_duration": 0,
                            "data": None,
                            "wrap_info": None,
                            "warnings": None,
                            "auth": {
                                "client_token": token,
                                "accessor": uuid.uuid4().hex,
                                "policies": ["default", "maas-collector"],
                                "token_policies": ["default", "maas-collector"],
                                "metadata": {"role_name": "maas-collector"},
                                "lease_duration": 3600,
                                "renewable": True,
                                "entity_id": "",
                                "token_type": "service",
                                "orphan": True,
                            },
                        },
                    )
                    return

                kv_path = self._kv_path()
                if kv_path is None:
                    self._reply(404, {"errors": []})
                    return

                if not self._authorized(kv_path):
                    self._reply(403, {"errors": ["permission denied"]})
                    return

                vault.secrets.setdefault(kv_path, []).append(body["data"])
                self._reply(
                    200,
                    {
                        "data": {
                            "created_time": "2026-10-12T10:00:00.000000Z",
                            "custom_metadata": None,
                            "deletion_time": "",
                            "destroyed": False,
                            "version": len(vault.secrets[kv_path]),
                        }
                    },
                )

            def do_GET(self):  # pylint: disable=C0103
                """KV v2 read of the latest version"""
                if not vault.available:
                    self._reply(503, {"errors": ["Vault is sealed"]})
                    return

                kv_path = self._kv_path()

                if not self._authorized(kv_path):
                    self._reply(403, {"errors": ["permission denied"]})
                    return

                if kv_path not in vault.secrets:
                    self._reply(404, {"errors": []})
                    return

                versions = vault.secrets[kv_path]
                self._reply(
                    200,
                    {
                        "request_id": str(uuid.uuid4()),
                        "lease_id": "",
                        "renewable": False,
                        "lease_duration": 0,
                        "data": {
                            "data": versions[-1],
                            "metadata": {
                                "created_time": "2026-10-12T10:00:00.000000Z",
                                "custom_metadata": None,
                                "deletion_time": "",
                                "destroyed": False,
                                "version": len(versions),
                            },
                        },
                        "wrap_info": None,
                        "warnings": None,
                        "auth": None,
                    },
                )

        return Handler

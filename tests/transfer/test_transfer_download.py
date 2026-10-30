import datetime
import hashlib
import os
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests

from maas_collector.rawdata.collector.http.authentication import (
    AuthenticationError,
    OAuth,
    SessionAuth,
)
from maas_collector.rawdata.transfer import download as download_module
from maas_collector.rawdata.transfer.download import (
    Downloaded,
    PermanentTransferError,
    ProductDownloader,
    TransferStopped,
    TransientTransferError,
    product_url,
    with_retries,
)
from maas_collector.rawdata.transfer.rule import ArchiveRule

CONFIG = SimpleNamespace(
    interface_name="ITF_A",
    auth_method="OAuth",
    odata_entity_location="/odata/v1/",
    odata_entities="Products",
    get_config_product_url=lambda: "https://catalogue.example",
)
CONTENT = b"x" * 3000
MD5 = hashlib.md5(CONTENT).hexdigest()
PRODUCT = {
    "product_id": "id-1",
    "product_name": "P1.zip",
    "size": 3000,
    "checksum": MD5,
    "publication_date": None,
    "document_id": "d1",
    "document_index": "i1",
}
RULE = ArchiveRule.default("archive-a")


class FakeResponse:
    def __init__(self, status=200, headers=None, chunks=(CONTENT[:1000], CONTENT[1000:2000], CONTENT[2000:])):
        self.status_code = status
        self.headers = headers or {}
        self.chunks = chunks
        self.closed = False

    def iter_content(self, chunk_size=None):
        yield from self.chunks

    def close(self):
        self.closed = True


class FakeSession:
    """records (url, headers, kwargs) and answers the scripted responses"""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs.get("headers"), kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class FakeAuthentication:
    def __init__(self):
        self.access_token_expiration_date = datetime.datetime.now() + datetime.timedelta(hours=1)
        self.expirations = 0

    def get_headers(self):
        if self.access_token_expiration_date <= datetime.datetime.now():
            self.expirations += 1
            self.access_token_expiration_date = datetime.datetime.now() + datetime.timedelta(hours=1)
        return {"Authorization": f"Bearer t{self.expirations}"}


def make(*responses, rule=RULE, **kwargs):
    session = FakeSession(*responses)
    downloader = ProductDownloader(
        CONFIG, rule, session=session, authentication=FakeAuthentication(), **kwargs
    )
    return downloader, session


def test_default_url_quotes_the_id():
    assert (
        product_url(CONFIG, RULE, "id-1")
        == "https://catalogue.example/odata/v1/Products('id-1')/$value"
    )


def test_template_url_on_another_host_with_an_unquoted_id():
    rule = replace(
        RULE,
        download_url="https://bytes.example/odata/v1/Products({id})/$value",
        quote_id=False,
    )
    assert (
        product_url(CONFIG, rule, "id-1")
        == "https://bytes.example/odata/v1/Products(id-1)/$value"
    )


def test_a_quote_in_an_id_is_escaped():
    assert product_url(CONFIG, RULE, "a'b").endswith("Products('a%27%27b')/$value")


def test_streams_the_product_and_sends_the_token_to_the_configured_host(tmp_path):
    downloader, session = make(FakeResponse())
    assert downloader.download(PRODUCT, str(tmp_path)) == Downloaded(
        str(tmp_path / "P1.zip"), 3000, MD5, MD5
    )
    assert (tmp_path / "P1.zip").read_bytes() == CONTENT
    url, headers, kwargs = session.calls[0]
    assert (url, headers["Authorization"], headers["Accept-Encoding"]) == (
        "https://catalogue.example/odata/v1/Products('id-1')/$value",
        "Bearer t0",
        "identity",
    )
    assert (kwargs["stream"], kwargs["allow_redirects"]) == (True, False)
    assert kwargs["timeout"] == (10.0, 60.0)


def test_checksum_follows_the_algorithm_of_the_rule(tmp_path):
    rule = ArchiveRule.from_entry(
        {
            "interface_name": "ITF_A",
            "archive": {"backup": "archive-a", "fields": {"checksum_algorithm": "SHA-256"}},
        }
    )
    sha = hashlib.sha256(CONTENT).hexdigest()
    downloader, _ = make(FakeResponse(), rule=rule)
    result = downloader.download(PRODUCT | {"checksum": sha.upper()}, str(tmp_path))
    assert (result.md5, result.checksum) == (MD5, sha)


def test_uppercase_catalogue_checksum_is_the_same_md5(tmp_path):
    downloader, _ = make(FakeResponse())
    downloader.download(PRODUCT | {"checksum": f" {MD5.upper()} "}, str(tmp_path))


def test_unknown_size_and_checksum_are_not_checked(tmp_path):
    downloader, _ = make(FakeResponse())
    downloader.download(PRODUCT | {"size": None, "checksum": None}, str(tmp_path))


def test_size_mismatch_is_transient(tmp_path):
    downloader, _ = make(FakeResponse())
    with pytest.raises(TransientTransferError, match="3000 bytes"):
        downloader.download(PRODUCT | {"size": 4000}, str(tmp_path))
    assert os.listdir(tmp_path) == []


def test_checksum_mismatch_is_transient(tmp_path):
    downloader, _ = make(FakeResponse())
    with pytest.raises(TransientTransferError, match="checksum"):
        downloader.download(PRODUCT | {"checksum": "0" * 32}, str(tmp_path))
    assert os.listdir(tmp_path) == []


@pytest.mark.parametrize("status", [400, 404, 410, 422])
def test_client_errors_are_permanent(status, tmp_path):
    downloader, session = make(FakeResponse(status))
    with pytest.raises(PermanentTransferError, match=str(status)):
        downloader.download(PRODUCT, str(tmp_path))
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [408, 429, 500, 503])
def test_server_errors_are_transient(status, tmp_path):
    downloader, _ = make(FakeResponse(status))
    with pytest.raises(TransientTransferError, match=str(status)):
        downloader.download(PRODUCT, str(tmp_path))


def test_connection_error_is_transient(tmp_path):
    downloader, _ = make(requests.ConnectionError("reset"))
    with pytest.raises(TransientTransferError, match="reset"):
        downloader.download(PRODUCT, str(tmp_path))


def test_a_read_error_while_streaming_is_transient(tmp_path):
    class Broken(FakeResponse):
        def iter_content(self, chunk_size=None):
            yield CONTENT[:10]
            raise requests.exceptions.ChunkedEncodingError("cut")

    downloader, _ = make(Broken())
    with pytest.raises(TransientTransferError, match="cut"):
        downloader.download(PRODUCT, str(tmp_path))
    assert os.listdir(tmp_path) == []


def test_401_refreshes_the_token_once(tmp_path):
    downloader, session = make(FakeResponse(401), FakeResponse())
    downloader.download(PRODUCT, str(tmp_path))
    assert [headers["Authorization"] for _url, headers, _kw in session.calls] == [
        "Bearer t0",
        "Bearer t1",
    ]


def test_a_second_401_is_permanent(tmp_path):
    downloader, session = make(FakeResponse(401), FakeResponse(401))
    with pytest.raises(PermanentTransferError, match="401"):
        downloader.download(PRODUCT, str(tmp_path))
    assert len(session.calls) == 2


def test_a_redirect_is_followed_without_the_token(tmp_path):
    downloader, session = make(
        FakeResponse(302, {"Location": "https://storage.example/o?sig=1"}),
        FakeResponse(),
    )
    downloader.download(PRODUCT, str(tmp_path))
    assert session.calls[1][0] == "https://storage.example/o?sig=1"
    assert "Authorization" not in session.calls[1][1]


def test_a_relative_redirect_is_resolved_and_has_no_token_either(tmp_path):
    downloader, session = make(
        FakeResponse(301, {"Location": "/other/path"}), FakeResponse()
    )
    downloader.download(PRODUCT, str(tmp_path))
    assert session.calls[1][0] == "https://catalogue.example/other/path"
    assert "Authorization" not in session.calls[1][1]


def test_401_after_a_redirect_names_the_host_to_configure(tmp_path):
    downloader, _ = make(
        FakeResponse(302, {"Location": "https://bytes.example/x"}), FakeResponse(401)
    )
    with pytest.raises(PermanentTransferError, match=r"bytes\.example.*download\.url"):
        downloader.download(PRODUCT, str(tmp_path))


def test_redirects_are_bounded(tmp_path):
    downloader, _ = make(
        *[FakeResponse(302, {"Location": "https://storage.example/o"}) for _ in range(4)]
    )
    with pytest.raises(PermanentTransferError, match="redirects"):
        downloader.download(PRODUCT, str(tmp_path))


@pytest.mark.parametrize(
    "name", [None, "", ".", "..", "../P1.zip", "a/P1.zip", "a\\P1.zip", "P1\x00.zip"]
)
def test_unsafe_product_name_is_permanent(name, tmp_path):
    downloader, session = make(FakeResponse())
    with pytest.raises(PermanentTransferError):
        downloader.download(PRODUCT | {"product_name": name}, str(tmp_path))
    assert session.calls == [] and os.listdir(tmp_path) == []


@pytest.mark.parametrize("product_id", [None, ""])
def test_missing_product_id_is_permanent(product_id, tmp_path):
    downloader, session = make(FakeResponse())
    with pytest.raises(PermanentTransferError, match="product_id"):
        downloader.download(PRODUCT | {"product_id": product_id}, str(tmp_path))
    assert session.calls == []


def test_stop_between_chunks(tmp_path):
    ticks = []
    downloader, _ = make(
        FakeResponse(),
        should_stop=lambda: len(ticks) >= 1,
        tick=lambda: ticks.append(1),
    )
    with pytest.raises(TransferStopped):
        downloader.download(PRODUCT, str(tmp_path))
    assert len(ticks) == 1 and os.listdir(tmp_path) == []


def test_download_longer_than_max_seconds_is_transient(tmp_path, monkeypatch):
    times = iter([0, 11, 11, 11])
    monkeypatch.setattr(download_module.time, "monotonic", lambda: next(times))
    downloader, _ = make(FakeResponse(), max_seconds=10)
    with pytest.raises(TransientTransferError, match="10"):
        downloader.download(PRODUCT, str(tmp_path))


def scripted(*outcomes):
    pending = list(outcomes)

    def action():
        action.calls += 1
        outcome = pending.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    action.calls = 0
    return action


def test_with_retries_backs_off_on_transient_errors():
    sleeps = []
    assert (
        with_retries(
            scripted(TransientTransferError("a"), TransientTransferError("b"), "ok"),
            3,
            5.0,
            sleeps.append,
        )
        == "ok"
    )
    assert sleeps == [5.0, 10.0]


def test_with_retries_raises_the_last_error_after_max_attempts():
    sleeps = []
    with pytest.raises(TransientTransferError, match="c"):
        with_retries(
            scripted(
                TransientTransferError("a"),
                TransientTransferError("b"),
                TransientTransferError("c"),
            ),
            3,
            5.0,
            sleeps.append,
        )
    assert sleeps == [5.0, 10.0]


def test_with_retries_caps_the_delay():
    sleeps = []
    with pytest.raises(TransientTransferError):
        with_retries(
            scripted(*[TransientTransferError("x")] * 6), 6, 30.0, sleeps.append
        )
    assert sleeps == [30.0, 60.0, 60.0, 60.0, 60.0]


@pytest.mark.parametrize(
    "error", [PermanentTransferError("gone"), TransferStopped("stop")]
)
def test_with_retries_does_not_retry_permanent_errors_nor_a_stop(error):
    sleeps = []
    action = scripted(error, "never")
    with pytest.raises(type(error)):
        with_retries(action, 3, 5.0, sleeps.append)
    assert action.calls == 1 and sleeps == []


class FailingAuthentication(FakeAuthentication):
    """an authentication whose first headers fail with the scripted errors"""

    def __init__(self, *errors):
        super().__init__()
        self.errors = list(errors)

    def get_headers(self):
        error = self.errors.pop(0) if self.errors else None
        if error:
            raise error
        return super().get_headers()


def make_failing(*errors, responses=()):
    session = FakeSession(*responses)
    downloader = ProductDownloader(
        CONFIG, RULE, session=session, authentication=FailingAuthentication(*errors)
    )
    return downloader, session


@pytest.mark.parametrize(
    "error",
    [
        requests.ConnectionError("token server down"),
        requests.Timeout("token server slow"),
        AuthenticationError("Error querying token", 500),
        AuthenticationError("Error querying token", 503),
        AuthenticationError("Error querying token", 429),
    ],
)
def test_a_token_server_failure_is_transient(error, tmp_path):
    downloader, session = make_failing(error)
    with pytest.raises(TransientTransferError):
        downloader.download(PRODUCT, str(tmp_path))
    assert session.calls == []


def test_a_token_server_that_recovers_ends_ok_with_the_retries(tmp_path):
    downloader, _ = make_failing(
        AuthenticationError("Error querying token", 502), responses=[FakeResponse()]
    )
    result = with_retries(
        lambda: downloader.download(PRODUCT, str(tmp_path)), 3, 0.0, lambda _s: None
    )
    assert result.size == 3000


@pytest.mark.parametrize("status", [400, 401, 403])
def test_a_token_refused_by_the_token_server_is_not_retried(status, tmp_path):
    downloader, _ = make_failing(AuthenticationError("Error querying token", status))
    with pytest.raises(AuthenticationError) as error:
        downloader.download(PRODUCT, str(tmp_path))
    assert not isinstance(error.value, TransientTransferError)


def test_a_failure_of_the_new_token_after_a_401_is_transient(tmp_path):
    downloader, _ = make_failing(
        None, requests.ConnectionError("token server down"), responses=[FakeResponse(401)]
    )
    with pytest.raises(TransientTransferError, match="token"):
        downloader.download(PRODUCT, str(tmp_path))


def test_a_login_that_fails_while_the_authentication_is_built_is_transient(
    tmp_path, monkeypatch
):
    def build(*_args):
        raise AuthenticationError("Error login", 503)

    monkeypatch.setattr(download_module, "build_authentication", build)
    session = FakeSession()
    downloader = ProductDownloader(CONFIG, RULE, session=session)
    with pytest.raises(TransientTransferError):
        downloader.download(PRODUCT, str(tmp_path))
    assert session.calls == []


class TokenServer:
    def __init__(self, status):
        self.status = status

    def post(self, *_args, **_kwargs):
        return SimpleNamespace(status_code=self.status, content=b"")


def oauth_config():
    return SimpleNamespace(
        token_field_header="Authorization",
        token="",
        token_url="https://token.example",
        oauth_basic_credential="",
        client_id="",
        client_secret="",
        client_username="",
        client_password="",
        auth_timeout=1,
        scope=None,
        grant_type="client_credentials",
    )


@pytest.mark.parametrize("status", [401, 503])
def test_the_token_error_keeps_the_status_and_stays_a_value_error(status):
    oauth = OAuth(oauth_config(), TokenServer(status))
    with pytest.raises(ValueError, match="Error querying token") as error:
        oauth.get_headers()
    assert error.value.status_code == status


def test_the_login_error_keeps_the_status():
    config = SimpleNamespace(
        token_field_header="",
        token="",
        login_url="https://login.example",
        auth_timeout=1,
        credentials={},
    )
    with pytest.raises(AuthenticationError) as error:
        SessionAuth(config, TokenServer(502))
    assert error.value.status_code == 502

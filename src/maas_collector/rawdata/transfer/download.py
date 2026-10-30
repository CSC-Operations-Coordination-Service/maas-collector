"""Download of a product, streamed to a working directory, and what is checked.

The error classification and the streamed write with its size and checksum checks
follow the download manager's transfer code. The authentication is the collector's
own (``build_authentication``): the token goes to the configured host only, never
along a redirect.
"""

import datetime
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Callable, TypeVar
from urllib.parse import quote, urljoin, urlparse

import requests

from maas_collector.rawdata.collector.http.authentication import build_authentication
from maas_collector.rawdata.collector.httpmixin import HttpMixin
from maas_collector.rawdata.transfer.rule import ArchiveRule

LOGGER = logging.getLogger(__name__)

T = TypeVar("T")

PERMANENT_STATUS = frozenset({400, 401, 403, 404, 405, 410, 422})
"""401 and 403 are permanent after one token refresh"""

TRANSIENT_STATUS = frozenset({408, 429})
"""the 4xx worth another try"""

REDIRECT_STATUS = frozenset({301, 302, 303, 307, 308})

MAX_REDIRECTS = 3

MAX_RETRY_DELAY = 60.0


class TransferError(Exception):
    """A transfer failed: ``retryable`` says whether another try can work"""

    retryable = False


class TransientTransferError(TransferError):
    """Another try may work: timeouts, 5xx, 429, a short read, a bad checksum"""

    retryable = True


class PermanentTransferError(TransferError):
    """Another try changes nothing: the product is gone or the request is wrong"""


class TransferStopped(TransferError):
    """The stage is stopping: not a failure, nothing is reported"""


@dataclass(frozen=True)
class Downloaded:
    """A product on disk"""

    path: str
    size: int
    md5: str  # lower-case hex
    checksum: str  # lower-case hex of the checksum algorithm of the rule


def product_url(config, rule: ArchiveRule, product_id: str) -> str:
    """URL of the bytes of a product

    The template is the one of the rule, or the collector's own form. ``{id}`` is
    replaced by the id, percent-encoded and, with ``quote_id``, wrapped in quotes
    (a quote inside being doubled).
    """
    template = rule.download_url or (
        f"{config.get_config_product_url()}{config.odata_entity_location}"
        f"{config.odata_entities}({{id}})/$value"
    )
    if rule.quote_id:
        identifier = "'" + quote(product_id.replace("'", "''"), safe="") + "'"
    else:
        identifier = quote(product_id, safe="")
    return template.replace("{id}", identifier)


def safe_file_name(name) -> str:
    """A product name that can be a file name, and an object key

    Raises:
        PermanentTransferError: the name is not a bare file name
    """
    if (
        not isinstance(name, str)
        or name in ("", ".", "..")
        or "\x00" in name
        or "/" in name
        or "\\" in name
        or os.path.basename(name) != name
    ):
        raise PermanentTransferError(f"unusable product name: {name!r}")
    return name


def with_retries(
    action: Callable[[], T],
    max_attempts: int,
    delay: float,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run an action, trying again after a transient error

    The delay doubles after each attempt, at most ``MAX_RETRY_DELAY``. A permanent
    error and a stop are never retried; the last transient error is raised after
    ``max_attempts``.
    """
    attempt = 1
    while True:
        try:
            return action()
        except TransientTransferError as error:
            if attempt >= max_attempts:
                raise
            wait = min(delay * 2 ** (attempt - 1), MAX_RETRY_DELAY)
            LOGGER.warning(
                "Attempt %d of %d failed (%s), trying again in %.0fs",
                attempt,
                max_attempts,
                error,
                wait,
            )
            sleep(wait)
            attempt += 1


class ProductDownloader:
    """Downloads the products of one archived entry"""

    # pylint: disable-next=too-many-arguments
    def __init__(
        self,
        config,
        rule: ArchiveRule,
        session=None,
        authentication=None,
        *,
        chunk_size: int = 1 << 20,
        connect_timeout: float = 10.0,
        read_timeout: float = 60.0,
        max_seconds: float = 900.0,
        should_stop: Callable[[], bool] = lambda: False,
        tick: Callable[[], None] = lambda: None,
    ):
        self.config = config
        self.rule = rule
        self._session = session
        self._authentication = authentication
        self.chunk_size = chunk_size
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.max_seconds = max_seconds
        self.should_stop = should_stop
        self.tick = tick

    @property
    def session(self):
        """the http session, built on first use"""
        if self._session is None:
            self._session = HttpMixin.create_http_session(self.config)
        return self._session

    @property
    def authentication(self):
        """the authentication, built on first use and kept for its token"""
        if self._authentication is None:
            self._authentication = build_authentication(
                self.config.auth_method, self.config, self.session
            )
        return self._authentication

    def expire_token(self) -> None:
        """Make the authentication ask for a new token"""
        authentication = self.authentication
        if hasattr(authentication, "access_token_expiration_date"):
            authentication.access_token_expiration_date = datetime.datetime.now()

    def request(self, url: str, headers: dict):
        """One streamed GET, never following a redirect"""
        try:
            return self.session.get(
                url,
                headers=headers,
                stream=True,
                allow_redirects=False,
                timeout=(self.connect_timeout, self.read_timeout),
            )
        except requests.RequestException as error:
            raise TransientTransferError(f"cannot reach {url}: {error}") from error

    def open_stream(self, url: str):
        """The response that streams the product

        The token goes to ``url`` only: a redirect is followed, a few hops at most,
        with no authentication header.

        Raises:
            TransferError: transient or permanent, classified by the status
        """
        headers = {**self.authentication.get_headers(), "Accept-Encoding": "identity"}
        response = self.request(url, headers)

        if response.status_code in (401, 403):
            # the token may have expired: ask for another one, once
            response.close()
            self.expire_token()
            headers = {
                **self.authentication.get_headers(),
                "Accept-Encoding": "identity",
            }
            response = self.request(url, headers)

        redirected = False
        followed = 0
        while response.status_code in REDIRECT_STATUS:
            location = response.headers.get("Location")
            response.close()
            if not location:
                raise PermanentTransferError(
                    f"{url} answered a redirect without a location"
                )
            if followed >= MAX_REDIRECTS:
                raise PermanentTransferError(
                    f"more than {MAX_REDIRECTS} redirects from {url}"
                )
            url = urljoin(url, location)
            followed += 1
            redirected = True
            # never the token, whatever host the location names
            response = self.request(url, {"Accept-Encoding": "identity"})

        status = response.status_code
        if status == 200:
            return response

        response.close()
        host = urlparse(url).netloc
        if redirected and status in (401, 403):
            raise PermanentTransferError(
                f"{host} answered {status} without the token: set the archive"
                " rule's download.url to that host"
            )
        if status in PERMANENT_STATUS or (
            400 <= status < 500 and status not in TRANSIENT_STATUS
        ):
            raise PermanentTransferError(f"{host} answered {status}")
        if status in TRANSIENT_STATUS or status >= 500:
            raise TransientTransferError(f"{host} answered {status}")
        raise PermanentTransferError(f"{host} answered the unexpected status {status}")

    def download(self, product: dict, directory: str) -> Downloaded:
        """Download a product, one attempt, into ``<directory>/<product name>``

        Args:
            product: the product of ``product_of``: name, id, size and checksum
            directory: an existing directory, made by the caller with ``mkdtemp``

        Raises:
            PermanentTransferError: unusable name or id, a 4xx, redirects
            TransientTransferError: network, 5xx, a size or checksum mismatch, too long
            TransferStopped: the stage is stopping
        """
        name = safe_file_name(product.get("product_name"))
        product_id = product.get("product_id")
        if not isinstance(product_id, str) or not product_id:
            raise PermanentTransferError("the document has no product_id")

        path = os.path.join(directory, name)
        url = product_url(self.config, self.rule, product_id)
        started = time.monotonic()

        response = self.open_stream(url)
        try:
            size, md5, checksum = self.write(response, path, name, started)
        except BaseException:
            if os.path.exists(path):
                os.unlink(path)
            raise
        finally:
            response.close()

        expected_size = product.get("size")
        if expected_size and size != expected_size:
            os.unlink(path)
            raise TransientTransferError(
                f"{name}: received {size} bytes, the catalogue announces {expected_size}"
            )
        expected_checksum = product.get("checksum")
        if expected_checksum and str(expected_checksum).strip().lower() != checksum:
            os.unlink(path)
            raise TransientTransferError(
                f"{name}: checksum {checksum} does not match the catalogue's"
                f" {str(expected_checksum).strip().lower()}"
            )
        return Downloaded(path, size, md5, checksum)

    def write(self, response, path: str, name: str, started: float):
        """Stream a response to a file, hashing it as it comes"""
        md5 = hashlib.md5()
        checksum = self.rule.new_checksum()
        size = 0
        try:
            with open(path, "wb") as output:
                for chunk in response.iter_content(chunk_size=self.chunk_size):
                    if not chunk:
                        continue
                    if self.should_stop():
                        raise TransferStopped(f"stopped while downloading {name}")
                    self.tick()
                    if time.monotonic() - started > self.max_seconds:
                        raise TransientTransferError(
                            f"{name}: download longer than {self.max_seconds:g} seconds"
                        )
                    output.write(chunk)
                    md5.update(chunk)
                    checksum.update(chunk)
                    size += len(chunk)
        except requests.RequestException as error:
            raise TransientTransferError(f"cannot download {name}: {error}") from error
        except OSError as error:
            raise TransientTransferError(f"cannot write {name}: {error}") from error
        return size, md5.hexdigest(), checksum.hexdigest()

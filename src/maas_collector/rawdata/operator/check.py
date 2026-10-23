"""
Check lane of the operator worker: compare a product of an odata interface, asked
live, with the document maas stored for it.
"""

import datetime
import json
import logging
import os
import tempfile
from urllib.parse import quote

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.exceptions import NotFoundError

from maas_collector.rawdata.collector.credentialmixin import CredentialMixin
from maas_collector.rawdata.collector.http.authentication import build_authentication
from maas_collector.rawdata.collector.httpmixin import HttpMixin
from maas_collector.rawdata.collector.odata.v4impl import ODataQueryV4Implementation
from maas_collector.rawdata.collector.odatacollector import (
    ODataCollector,
    ODataCollectorConfiguration,
)
from maas_collector.rawdata.configprovider import OpenSearchConfigProvider
from maas_collector.rawdata.configuration import build_model_class, load_json_dict
from maas_collector.rawdata.model import ActionIterator
from maas_collector.rawdata.operator.contract import CheckReply, CheckRequest

LOGGER = logging.getLogger("maas_collector.operator")

# fields that differ at each collect without the product changing
IGNORED_FIELDS = ("reportName", "ingestionTime")

# seconds a check may wait for the interface, token included: the backend gives up
# on a check reply after 30 s
DEFAULT_TIMEOUT = 20.0

CHECKED_CLASS = "ODataCollectorConfiguration"


class CheckNotPossible(ValueError):
    """The requested interface cannot be checked"""


def odata_quote(value: str) -> str:
    """Escape a value for an OData string literal: ' is written ''"""
    return value.replace("'", "''")


def product_query_url(
    config: ODataCollectorConfiguration, request: CheckRequest
) -> str:
    """URL asking the interface for the product of the request

    By name, a filter on the Name of the entities; by id, the entity itself.
    """
    # custom_query_suffix of the interface (like "&$expand=Attributes"), so that the
    # payload has the shape the extractor of the interface expects
    suffix = config.custom_query_suffix or ""

    if request.product_id:
        url = ODataCollector.build_get_an_entity_query(
            config, quote(request.product_id, safe="")
        )
        if suffix:
            url += "?" + suffix.lstrip("&")
        return url

    name_filter = quote(f"Name eq '{odata_quote(request.product_name)}'", safe="'")
    return (
        f"{config.get_config_product_url()}{config.odata_entity_location}"
        f"{config.odata_entities}?$filter={name_filter}{suffix}"
    )


def diff_documents(remote: dict, stored: dict) -> list[dict]:
    """Fields whose values differ, IGNORED_FIELDS left out, sorted by field. A field
    absent from one side has the value None there."""
    fields = (set(remote) | set(stored)) - set(IGNORED_FIELDS)
    return [
        {"field": name, "remote": remote.get(name), "stored": stored.get(name)}
        for name in sorted(fields)
        if remote.get(name) != stored.get(name)
    ]


class ProductChecker:
    """Answer check requests for the odata interfaces of a service"""

    def __init__(
        self,
        service: str,
        config_index: str,
        credential_provider,
        workdir: str,
        timeout: float = DEFAULT_TIMEOUT,
    ):
        """
        Args:
            service (str): collector service whose interfaces are checked
            config_index (str): configuration index, like maas-collector-config
            credential_provider (CredentialProvider | None): credentials of the
                interfaces, None for interfaces without credentials
            workdir (str): folder of the temporary payload files
            timeout (float): seconds the query and the token request may take,
                at most
        """
        self.service = service
        self.config_index = config_index
        self.credential_provider = credential_provider
        self.workdir = workdir
        self.timeout = timeout

    def check(self, request: CheckRequest) -> CheckReply:
        """Ask the interface for the product and compare it with the stored one.

        Never raises: any failure is a reply with status "error" and its message.
        """
        try:
            config = self.interface_config(request.interface_name)
            payload = self.query_remote(config, request)
            if payload is None:
                # maas may still have it, like a product evicted from the interface
                return CheckReply(
                    "ok",
                    stored=self.find_stored(
                        config, request.product_id or request.product_name
                    ),
                )

            action = self.would_store(config, payload)

            remote = action["_source"]
            stored = self.stored_source(config, action["_index"], action["_id"])

            return CheckReply(
                "ok",
                remote=remote,
                stored=stored,
                diff=diff_documents(remote, stored) if stored is not None else [],
            )

        # a check request shall always be answered
        # pylint: disable=broad-exception-caught
        except Exception as error:
            LOGGER.warning(
                "Check of %s on %s failed: %s",
                request.product_name or request.product_id,
                request.interface_name,
                error,
            )
            return CheckReply("error", error=str(error) or error.__class__.__name__)

    def interface_config(self, interface_name: str) -> ODataCollectorConfiguration:
        """Configuration of the interface, with its model resolved and its
        credentials set

        Raises:
            CheckNotPossible: missing, disabled, not odata or not odata v4
            ValueError: no configuration for the service
        """
        sources = OpenSearchConfigProvider(self.config_index, self.service).load()

        # models of the service, by name, kept local: MODEL_DICT is left untouched
        models = {}
        candidates = []

        for source in sources:
            for model_meta in source.content.get("models", []):
                models[model_meta["name"]] = build_model_class(
                    model_meta, register=False
                )

            entries = [
                entry
                for entry in source.content.get("collectors", [])
                if entry.get("interface_name") == interface_name
            ]
            if not entries:
                continue

            for entry in entries:
                if entry.get("class") != CHECKED_CLASS:
                    raise CheckNotPossible(
                        f"{interface_name} of class {entry.get('class')} cannot be"
                        " checked: only odata interfaces"
                    )

            candidates.extend(
                load_json_dict(
                    {"collectors": entries},
                    ODataCollectorConfiguration,
                    source.name,
                    register=False,
                )
            )

        if not candidates:
            raise CheckNotPossible(
                f"{interface_name} not found in service {self.service}"
            )

        enabled = [config for config in candidates if config.enabled]
        if not enabled:
            raise CheckNotPossible(f"{interface_name} is disabled")

        config = enabled[0]

        if isinstance(config.model, str) and config.model:
            if config.model not in models:
                raise CheckNotPossible(
                    f"model {config.model} of {interface_name} not found"
                )
            config.model = models[config.model]

        if config.model is None:
            raise CheckNotPossible(f"{interface_name} has no model")

        self.set_credentials(config)

        # the configuration is built for this check only
        config.auth_timeout = min(config.auth_timeout, self.timeout)

        if config.get_config_protocol_version() != "v4":
            raise CheckNotPossible(
                f"{interface_name} cannot be checked: only odata v4 supported"
            )

        return config

    def set_credentials(self, config: ODataCollectorConfiguration):
        """Set the credential entries of the interface on its configuration

        Raises:
            PermissionError: the vault policy denies an entry of the interface
        """
        if self.credential_provider is None:
            return

        names = {
            name
            for name in (config.interface_credentials, config.interface_name)
            if name
        }

        entries = self.credential_provider.load(names)

        if self.credential_provider.unreadable:
            raise PermissionError(
                "The vault policy does not allow reading the credential entries "
                + ", ".join(sorted(self.credential_provider.unreadable))
            )

        CredentialMixin.set_credential_attributes(config, entries)

    def query_remote(self, config: ODataCollectorConfiguration, request: CheckRequest):
        """Ask the interface for the product

        Returns:
            dict | None: {"value": [entities]} payload, None when not found

        Raises:
            ValueError: the interface answered an error
            requests.RequestException: the interface cannot be reached
        """
        url = product_query_url(config, request)

        with HttpMixin.create_http_session(config) as http_session:
            authentication = build_authentication(
                config.auth_method, config, http_session
            )

            response = http_session.get(
                url,
                headers=authentication.get_headers(),
                timeout=self.timeout,
            )

        if request.product_id and response.status_code == 404:
            # an unknown entity
            return None

        if not 200 <= response.status_code < 300:
            raise ValueError(
                f"{config.interface_name} answered {response.status_code} to {url}"
            )

        payload = response.json()

        if "value" not in payload:
            # a single entity
            payload = {"value": [payload]}

        if not payload["value"]:
            return None

        return payload

    def would_store(self, config: ODataCollectorConfiguration, payload: dict):
        """Run the extraction of the interface on the payload, like a collect does

        Returns:
            dict: first bulk action (_source, _index, _id)

        Raises:
            ValueError: the extraction produced no document
        """
        os.makedirs(self.workdir, exist_ok=True)

        now = datetime.datetime.now(tz=datetime.UTC)

        # page file named like the ones of a collect, the extractor may rely on it
        filename = ODataQueryV4Implementation.FILE_NAME_PATTERN.format(
            interface_name=config.interface_name,
            start_date=now.strftime("%Y%m%dT%H%M%S"),
            end_date=now.strftime("%Y%m%dT%H%M%S"),
            product_per_page=config.product_per_page,
            current_page=0,
        )

        with tempfile.TemporaryDirectory(prefix="check-", dir=self.workdir) as folder:
            path = os.path.join(folder, filename)

            with open(path, "w", encoding="UTF-8") as file_desc:
                json.dump(payload, file_desc)

            # force_update yields the document even when maas has it unchanged
            actions = ActionIterator(
                path,
                config,
                force_update=True,
                report_name=config.get_config_product_url(),
            )
            try:
                action = next(iter(actions), None)
            # whatever the extractor raises
            # pylint: disable=broad-exception-caught
            except Exception as error:
                raise ValueError(f"extraction failed: {error}") from error

        if action is None:
            # a record the model refuses is only logged by the iterator
            if actions.action_iterator_errors:
                raise ValueError(
                    "extraction failed: " + "; ".join(actions.action_iterator_errors)
                )
            raise ValueError("extraction produced no document")

        return action

    @staticmethod
    def normalise(config: ODataCollectorConfiguration, source: dict) -> dict:
        """A stored source as the model serialises it, like the remote document,
        so that a format difference (like dates without milliseconds) is no
        difference. The source unchanged if the model refuses it."""
        try:
            document = config.model(**source)
            document.full_clean()
            return document.to_dict()
        # a stored document is shown even when it no longer fits its model
        # pylint: disable=broad-exception-caught
        except Exception as error:
            LOGGER.warning("Stored document kept as is: %s", error)
            return source

    def stored_source(
        self, config: ODataCollectorConfiguration, index: str, document_id: str
    ):
        """Stored document, None when maas does not have it"""
        try:
            source = es_connections.get_connection().get(index=index, id=document_id)[
                "_source"
            ]
        except NotFoundError:
            return None
        return self.normalise(config, source)

    def find_stored(self, config: ODataCollectorConfiguration, identifier: str):
        """Best effort search of the stored document whose id_field is the
        identifier of the request, for a product the interface does not have

        Returns:
            dict | None: first stored document found, None when none is found or
                the search fails
        """
        if not isinstance(config.id_field, str):
            # composite or computed identifier: no field to search on
            return None

        try:
            hits = (
                config.model.search()
                .filter("term", **{config.id_field: identifier})
                .extra(size=1)
                .execute()
            )
        # the check still answers for the remote side
        # pylint: disable=broad-exception-caught
        except Exception as error:
            LOGGER.warning(
                "Cannot search %s=%s in %s: %s",
                config.id_field,
                identifier,
                config.model.Index.name,
                error,
            )
            return None

        for hit in hits:
            return self.normalise(config, hit.to_dict())
        return None

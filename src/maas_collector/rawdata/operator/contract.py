"""
Message contract shared with the operator backend (names are cross-repo).
"""

import datetime
import json
from dataclasses import dataclass, field

import kombu

EXCHANGE_NAME = "maas-operator-exchange"
# index of the operation records the backend writes and the worker updates
OPERATION_INDEX = "maas-operation"
LANES = ("short", "long")
SHORT_LANE_MAX = datetime.timedelta(hours=24)
# a custom replay request names its interface "<interface>@custom", the real name
# being in the "interface" field of the body: a worker that does not know custom
# requests looks for an interface of that name, finds none and fails the operation
CUSTOM_SUFFIX = "@custom"


def lane_for(start: datetime.datetime, end: datetime.datetime) -> str:
    """Return the lane for a replay window: short up to 24 h included, else long."""
    return "short" if end - start <= SHORT_LANE_MAX else "long"


def replay_routing_key(service: str, lane: str) -> str:
    """Routing key of replay requests"""
    return f"maas.operator.{service}.replay.{lane}"


def replay_queue_name(service: str, lane: str) -> str:
    """Queue name of replay requests"""
    return f"maas-operator-{service}-replay-{lane}"


def replay_custom_routing_key(service: str, lane: str) -> str:
    """Routing key of custom replay requests"""
    return f"maas.operator.{service}.replay-custom.{lane}"


def replay_custom_queue_name(service: str, lane: str) -> str:
    """Queue name of custom replay requests"""
    return f"maas-operator-{service}-replay-custom-{lane}"


def check_routing_key(service: str) -> str:
    """Routing key of check requests"""
    return f"maas.operator.{service}.check"


def check_queue_name(service: str) -> str:
    """Queue name of check requests"""
    return f"maas-operator-{service}-check"


def exchange() -> kombu.Exchange:
    """Operator topic exchange"""
    return kombu.Exchange(EXCHANGE_NAME, type="topic", durable=True)


def _load(body) -> dict:
    if isinstance(body, (bytes, bytearray)):
        try:
            body = bytes(body).decode("utf-8")
        except UnicodeDecodeError as error:
            raise ValueError(f"invalid body encoding: {error}") from error
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError as error:
            raise ValueError(f"invalid JSON body: {error}") from error
    if not isinstance(body, dict):
        raise ValueError("body must be a JSON object")
    return body


def _required(data: dict, name: str) -> str:
    value = data.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing or invalid field: {name}")
    return value


def _parse_date(data: dict, name: str) -> datetime.datetime:
    """Parse ISO 8601; naive values are taken as UTC, others converted to UTC."""
    value = _required(data, name)
    try:
        parsed = datetime.datetime.fromisoformat(value)
    except ValueError as error:
        raise ValueError(f"invalid date in {name}: {value}") from error
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.UTC)
    return parsed.astimezone(datetime.UTC)


def _format_date(value: datetime.datetime) -> str:
    if value.tzinfo is not None:
        value = value.astimezone(datetime.UTC).replace(tzinfo=None)
    return value.isoformat() + "Z"


@dataclass(frozen=True)
class ReplayRequest:
    """Request to replay an interface over a window

    ``interface_name`` is the real name of the interface. A request is custom
    when ``overrides`` or ``archive`` is not ``None`` (``{}`` is set) or
    ``configuration`` is not empty; its body then names the interface
    ``<interface_name>@custom`` and carries the real name in ``interface``.
    """

    operation_id: str
    interface_name: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    requested_by: str = ""
    lane: str = "short"
    overrides: dict | None = None
    configuration: str = ""
    archive: dict | None = None

    @property
    def is_custom(self) -> bool:
        """True when at least one of overrides, configuration and archive is set"""
        return (
            self.overrides is not None
            or bool(self.configuration)
            or self.archive is not None
        )

    def routing_key(self, service: str) -> str:
        """Routing key of the request: the custom one for a custom request"""
        if self.is_custom:
            return replay_custom_routing_key(service, self.lane)
        return replay_routing_key(service, self.lane)

    @classmethod
    def from_body(cls, body: bytes | str | dict) -> "ReplayRequest":
        """Build from a message body

        Raises:
            ValueError: missing field, bad date, start >= end, unknown lane or an
                inconsistent custom request
        """
        data = _load(body)
        operation_id = _required(data, "operation_id")
        interface_name = _required(data, "interface_name")
        start = _parse_date(data, "start_date")
        end = _parse_date(data, "end_date")
        if start >= end:
            raise ValueError("start_date must be before end_date")
        lane = data.get("lane", "short")
        if lane not in LANES:
            raise ValueError(f"unknown lane: {lane}")
        overrides = data.get("overrides")
        archive = data.get("archive")
        configuration = data.get("configuration", "")
        for name, value in (("overrides", overrides), ("archive", archive)):
            if value is not None and not isinstance(value, dict):
                raise ValueError(f"invalid field: {name} must be an object or null")
        if not isinstance(configuration, str):
            raise ValueError("invalid field: configuration must be a string")
        is_set = overrides is not None or archive is not None or bool(configuration)
        if interface_name.endswith(CUSTOM_SUFFIX):
            real = interface_name[: -len(CUSTOM_SUFFIX)]
            if not real or "@" in real or data.get("interface") != real:
                raise ValueError(
                    f"interface must be the interface name without {CUSTOM_SUFFIX}"
                )
            if not is_set:
                raise ValueError(
                    f"{CUSTOM_SUFFIX} without overrides, configuration or archive"
                )
            interface_name = real
        elif is_set:
            raise ValueError(
                f"overrides, configuration and archive need the {CUSTOM_SUFFIX} "
                "interface name"
            )
        return cls(
            operation_id=operation_id,
            interface_name=interface_name,
            start_date=start,
            end_date=end,
            requested_by=data.get("requested_by") or "",
            lane=lane,
            overrides=overrides,
            configuration=configuration,
            archive=archive,
        )

    def to_body(self) -> dict:
        """Message body, dates as ...Z"""
        body = {
            "operation_id": self.operation_id,
            "interface_name": self.interface_name,
            "start_date": _format_date(self.start_date),
            "end_date": _format_date(self.end_date),
            "requested_by": self.requested_by,
            "lane": self.lane,
        }
        if self.is_custom:
            body["interface_name"] = self.interface_name + CUSTOM_SUFFIX
            body["interface"] = self.interface_name
            body["overrides"] = self.overrides
            body["configuration"] = self.configuration
            body["archive"] = self.archive
        return body


@dataclass(frozen=True)
class CheckRequest:
    """Request to compare a remote product with the stored one"""

    operation_id: str
    interface_name: str
    product_name: str = ""
    product_id: str = ""

    @classmethod
    def from_body(cls, body: bytes | str | dict) -> "CheckRequest":
        """Build from a message body

        Raises:
            ValueError: missing field, or not exactly one of product_name/product_id
        """
        data = _load(body)
        operation_id = _required(data, "operation_id")
        interface_name = _required(data, "interface_name")
        name = data.get("product_name") or ""
        product_id = data.get("product_id") or ""
        if bool(name) == bool(product_id):
            raise ValueError("exactly one of product_name and product_id is required")
        return cls(
            operation_id=operation_id,
            interface_name=interface_name,
            product_name=name,
            product_id=product_id,
        )

    def to_body(self) -> dict:
        """Message body"""
        return {
            "operation_id": self.operation_id,
            "interface_name": self.interface_name,
            "product_name": self.product_name,
            "product_id": self.product_id,
        }


@dataclass
class CheckReply:
    """Reply to a check request"""

    status: str
    remote: dict | None = None
    stored: dict | None = None
    diff: list[dict] = field(default_factory=list)
    error: str | None = None

    def to_body(self) -> dict:
        """Reply body"""
        return {
            "status": self.status,
            "remote": self.remote,
            "stored": self.stored,
            "diff": self.diff,
            "error": self.error,
        }

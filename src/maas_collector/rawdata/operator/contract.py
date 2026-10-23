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


def lane_for(start: datetime.datetime, end: datetime.datetime) -> str:
    """Return the lane for a replay window: short up to 24 h included, else long."""
    return "short" if end - start <= SHORT_LANE_MAX else "long"


def replay_routing_key(service: str, lane: str) -> str:
    """Routing key of replay requests"""
    return f"maas.operator.{service}.replay.{lane}"


def replay_queue_name(service: str, lane: str) -> str:
    """Queue name of replay requests"""
    return f"maas-operator-{service}-replay-{lane}"


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
    """Request to replay an interface over a window"""

    operation_id: str
    interface_name: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    requested_by: str = ""
    lane: str = "short"

    @classmethod
    def from_body(cls, body: bytes | str | dict) -> "ReplayRequest":
        """Build from a message body

        Raises:
            ValueError: missing field, bad date, start >= end or unknown lane
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
        return cls(
            operation_id=operation_id,
            interface_name=interface_name,
            start_date=start,
            end_date=end,
            requested_by=data.get("requested_by") or "",
            lane=lane,
        )

    def to_body(self) -> dict:
        """Message body, dates as ...Z"""
        return {
            "operation_id": self.operation_id,
            "interface_name": self.interface_name,
            "start_date": _format_date(self.start_date),
            "end_date": _format_date(self.end_date),
            "requested_by": self.requested_by,
            "lane": self.lane,
        }


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

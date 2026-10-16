"""
Partial updates of the operation records written by the operator backend.
"""

import datetime
import logging
from collections.abc import Callable

from opensearchpy.exceptions import NotFoundError

OPERATION_INDEX = "maas-operation"
RUNNING, DONE, FAILED = "RUNNING", "DONE", "FAILED"

LOGGER = logging.getLogger("maas_collector.operator")


class OperationRecorder:
    """Update operation records; never creates one, never raises"""

    def __init__(
        self,
        client,
        index: str = OPERATION_INDEX,
        clock: Callable[[], datetime.datetime] = lambda: datetime.datetime.now(
            datetime.UTC
        ),
    ):
        self.client = client
        self.index = index
        self.clock = clock

    def update(
        self,
        operation_id: str,
        status: str,
        exit_code: int | None = None,
        details: str | None = None,
    ) -> bool:
        """Set status (and optionally exit_code, details) plus updated_at

        Returns:
            bool: False when the record is missing or the update failed
        """
        doc = {
            "status": status,
            "updated_at": self.clock()
            .astimezone(datetime.UTC)
            .strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        if exit_code is not None:
            doc["exit_code"] = exit_code
        if details is not None:
            doc["details"] = details
        try:
            self.client.update(
                index=self.index,
                id=operation_id,
                body={"doc": doc},
                retry_on_conflict=3,
            )
        except NotFoundError:
            LOGGER.error(
                "Operation record %s not found in %s", operation_id, self.index
            )
            return False
        except Exception:  # pylint: disable=broad-except
            LOGGER.exception("Cannot update operation record %s", operation_id)
            return False
        return True

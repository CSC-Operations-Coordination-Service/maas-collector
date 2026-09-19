"""Storage of backup transfer reports in the raw database.

The backup package describes transfers, this module stores them. It is the only
place that knows both BackupReport and OpenSearch, which keeps maas_collector.backup
free of any database dependency.
"""

import datetime
import logging
import os
import typing

from opensearchpy.connection.connections import connections as es_connections
from opensearchpy.helpers import bulk

from maas_model import MAASRawDocument

from maas_collector.backup.backup import BackupReport
from maas_collector.rawdata.extractor.base import get_hash_func

LOGGER = logging.getLogger("BackupReportWriter")


# One document per transfer. Same file, to the same place, at the same instant is
# the same transfer; a retry gets a new start_time and so a new document. Same md5
# idiom as every other composite identifier of the collector.
get_report_id = get_hash_func(
    "filename", "destination", "destination_path", "interface_name", "start_time"
)


class BackupReportWriter:
    """Turn BackupReport instances into documents of a raw data index.

    Append only: every transfer is a new document and nothing is ever updated, so
    the index is an event log and not a state record.
    """

    def __init__(self, model: typing.Type[MAASRawDocument]):
        self.model = model

        self.logger = LOGGER

        # warn once per writer if the configured model drifted from BackupReport:
        # unknown keys would otherwise be silently indexed as dynamic fields
        unknown = set(BackupReport().to_dict()) - set(model._INITIAL_FIELDS)

        if unknown:
            self.logger.warning(
                "Model %s does not declare backup report field(s) %s:"
                " they will be indexed as dynamic fields",
                model.__name__,
                sorted(unknown),
            )

    def build_action(
        self, report: BackupReport, report_name: str = "", report_folder: str = ""
    ) -> dict:
        """build the bulk action of a single report

        ActionIterator is bypassed, so everything it normally fills has to be
        filled here: the identifier, ingestionTime, reportName and reportFolder.

        Args:
            report (BackupReport): the transfer to store
            report_name (str): name of the ingested file
            report_folder (str): folder of the ingested file

        Returns:
            dict: a bulk action suitable for opensearchpy helpers
        """
        data = report.to_dict()

        document = self.model(**data)

        document.meta.id = get_report_id(data)

        # MAAS document models use camel case naming
        # pylint: disable=C0103

        # a datetime and NOT a zulu string: partition_index_name formats it with
        # "{ingestionTime:%Y}" and str.__format__ raises on "%Y"
        document.ingestionTime = datetime.datetime.now(tz=datetime.UTC)

        document.reportName = report_name or report.source_filename

        document.reportFolder = report_folder or os.path.dirname(report.source_path)

        # pylint: enable=C0103

        # coerce fields, exactly like ActionIterator.process_document does
        document.full_clean()

        # "create" is the branch of to_bulk_action that applies the partitionning
        # rule, and it turns a replayed transfer into a 409 rather than a silent
        # overwrite. A plain save() would target the alias and break as soon as a
        # second yearly partition exists.
        return document.to_bulk_action(op_type="create")

    def write(
        self,
        reports: typing.List[BackupReport],
        report_name: str = "",
        report_folder: str = "",
        **kwargs,
    ) -> tuple:
        """store all the reports of one ingested file in a single bulk call

        Args:
            reports (list[BackupReport]): the transfers to store
            report_name (str): name of the ingested file
            report_folder (str): folder of the ingested file

        Returns:
            tuple: (number of stored reports, list of bulk errors)
        """
        actions = [
            self.build_action(report, report_name, report_folder) for report in reports
        ]

        success, errors = bulk(
            es_connections.get_connection(),
            actions,
            raise_on_error=False,
            raise_on_exception=False,
            **kwargs,
        )

        # a 409 means another process already reported this exact transfer: it is
        # deduplication working, not an error worth an ERROR line
        conflicts = [
            error for error in errors if error.get("create", {}).get("status") == 409
        ]

        if conflicts:
            self.logger.debug(
                "%d backup report(s) already stored by another process", len(conflicts)
            )

        real_errors = [error for error in errors if error not in conflicts]

        if real_errors:
            self.logger.error(
                "%d backup report(s) out of %d could not be stored: %s",
                len(real_errors),
                len(actions),
                real_errors,
            )

        return success, real_errors

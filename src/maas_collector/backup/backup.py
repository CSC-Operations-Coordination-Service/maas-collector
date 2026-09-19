"""Ingested files backup"""

import contextlib
import dataclasses
import datetime
import gzip
import logging
import os
import shutil
import typing

from maas_model import datetime_to_zulu


@dataclasses.dataclass
class BackupReport:
    """One backup transfer: one source file copied to one destination.

    Instances are pure data: the backup package describes what it did and returns
    it, storing the description is the caller's business. See
    maas_collector.rawdata.backup_report.
    """

    # --- what was backed up
    filename: str = ""
    """basename of the file actually transferred, ends with .gz when compressed"""

    source_filename: str = ""
    """basename of the ingested file, before any compression"""

    source_path: str = ""
    """local path of the file actually transferred"""

    size_bytes: int = 0
    """size of the transferred file in bytes, 0 if it could not be stat'ed"""

    compressed: bool = False

    # --- where it went
    backup_type: str = ""
    """the "type" key of the backup block: BackupS3, BackupLocal or BackupSFTP"""

    destination: str = ""
    """destination identity: s3 bucket, local root directory or sftp host"""

    destination_path: str = ""
    """full path of the file on the destination"""

    # --- context
    interface_name: str = ""

    model_name: str = ""

    # --- when and how long
    start_time: datetime.datetime = None

    end_time: datetime.datetime = None

    duration: float = 0.0
    """transfer duration in seconds"""

    # --- outcome
    status: str = ""
    """"OK" or "KO", following the InterfaceProbeData convention"""

    error_type: str = ""
    """exception class name when status is KO, aggregatable in a dashboard"""

    details: str = ""
    """error message when status is KO, truncated to DETAILS_MAX_LENGTH"""

    DETAILS_MAX_LENGTH: typing.ClassVar[int] = 2000
    """an unbounded text field fed by exception messages is a way to blow up a shard"""

    def to_dict(self) -> typing.Dict[str, typing.Any]:
        """Return JSON serializable dictionnary with converted datetime to string

        Mirrors InterfaceProbeData.to_dict.

        Returns:
            Dict[str, Any]: JSON dictionnary
        """
        data_dict = dataclasses.asdict(self)

        # override dates with string representation
        data_dict["start_time"] = datetime_to_zulu(self.start_time)
        data_dict["end_time"] = datetime_to_zulu(self.end_time)

        return data_dict


@dataclasses.dataclass
class CollectorBackupConfiguration:
    """store backup parameters"""

    type: str = None

    directory: str = None

    calendar_tree: bool = False

    enable_gzip: bool = False

    interface_name: str = None

    interface_credentials: str = None

    report_model: str = None
    """name of the model class storing the backup reports of this configuration.

    Absent (None) means backup reporting is off. Declared here and not on a child
    configuration because instanciate_collector_backup splats the whole JSON block
    into this dataclass: an undeclared key raises TypeError.
    """


class CollectorBackup:
    """Main Collector Backup class which contains all
    methods common to all backup implementations.
    This class shall not be instanciated but inherited"""

    CONFIGURATION_CLASS = CollectorBackupConfiguration

    DO_NOT_COMPRESS_EXTENSION = (".xlsx", ".gzip", ".zip")

    TRANSFER_ERRORS: typing.Tuple[typing.Type[BaseException], ...] = (OSError,)
    """exceptions a single transfer is allowed to fail with.

    They are turned into a KO report instead of breaking the ingestion loop. Each
    implementation narrows this to the errors its transport raises, which keeps
    exactly the catch clauses that used to be inlined in each of them.
    """

    def __init__(self, args: CollectorBackupConfiguration):
        self.args = args
        self.logger = logging.getLogger(self.__class__.__name__)
        self.validate_backup_arguments()

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def close(self):
        """close transport and clear directory creation cache"""
        # Abstract Class
        raise NotImplementedError()

    @property
    def destinations(self) -> typing.List[str]:
        """the list of destinations every file is sent to.

        S3 sends to N buckets, the other implementations to a single place: this
        property is what makes that asymmetry explicit.

        Returns:
            list[str]: destination identities
        """
        return [self.args.directory]

    def get_backup_path(
        self, config, path, dirdatetime: datetime.datetime = None
    ) -> tuple:
        """generate remote path"""

        path_elements = [self.args.directory]

        if self.args.calendar_tree:
            # DOI instead of ingestion time ?
            if dirdatetime is None:
                dirdatetime = datetime.datetime.now(tz=datetime.UTC)

            path_elements.extend(
                [
                    f"{dirdatetime.year:04d}",
                    f"{dirdatetime.month:02d}",
                    f"{dirdatetime.day:02d}",
                ]
            )

        if config.interface_name:
            path_elements.append(config.interface_name)
        elif config.model_name:
            # default: model class name. may be rabbit queue mmm ?
            path_elements.append(config.model_name)
        else:
            path_elements.append("Antoine")

        return "/".join(path_elements), os.path.basename(path)

    def new_report(
        self, config, path: str, destination: str, destination_path: str
    ) -> BackupReport:
        """build the report of a transfer that is about to start

        Args:
            config (CollectorConfiguration): ingestion config
            path (str): local path of the file to transfer
            destination (str): destination identity
            destination_path (str): path of the file on the destination

        Returns:
            BackupReport: a report with everything but the outcome filled
        """
        try:
            size_bytes = os.path.getsize(path)
        except OSError:
            size_bytes = 0

        # model_name is the literal string "NoneType" for a config without model
        model_name = config.model_name if config.model_name != "NoneType" else ""

        return BackupReport(
            filename=os.path.basename(path),
            # overriden by backup_file when the file was compressed
            source_filename=os.path.basename(path),
            source_path=path,
            size_bytes=size_bytes,
            backup_type=self.args.type or "",
            destination=destination or "",
            destination_path=destination_path or "",
            # the ingestion config knows the interface, the backup block often
            # does not declare it
            interface_name=config.interface_name or self.args.interface_name or "",
            model_name=model_name,
            start_time=datetime.datetime.now(tz=datetime.UTC),
        )

    @contextlib.contextmanager
    def report_transfer(
        self, config, path: str, destination: str, destination_path: str
    ):
        """time one transfer and turn its failure into a KO report.

        The wrapped block shall perform a single transfer of path to destination.
        Exceptions listed in TRANSFER_ERRORS are caught, logged as critical and
        reported: they never break the ingestion loop, exactly as the inlined
        try/except of each implementation did. Anything else propagates.

        Args:
            config (CollectorConfiguration): ingestion config
            path (str): local path of the file to transfer
            destination (str): destination identity
            destination_path (str): path of the file on the destination

        Yields:
            BackupReport: the report, filled in when the block exits
        """
        report = self.new_report(config, path, destination, destination_path)

        try:
            yield report

            if not report.status:
                # a block that ran without raising is OK. Leaves the door open for
                # an implementation setting a status itself, like run_probe does.
                report.status = "OK"

        except self.TRANSFER_ERRORS as error:
            report.status = "KO"

            report.error_type = type(error).__name__

            report.details = str(error)[: BackupReport.DETAILS_MAX_LENGTH]

            self.logger.critical(
                "Cannot backup file %s of interface %s to %s on %s"
                " due to the following error: %s",
                path,
                report.interface_name,
                destination_path,
                destination,
                error,
            )
            # do not raise as logging critical is the only thing to do to not break
            # the ingestion loop

        finally:
            report.end_time = datetime.datetime.now(tz=datetime.UTC)

            report.duration = (report.end_time - report.start_time).total_seconds()

            self.logger.info(
                "Backup of %s to %s:%s is %s in %ss",
                report.filename,
                report.destination,
                report.destination_path,
                report.status,
                report.duration,
            )

    def backup_file(self, config, path) -> typing.List[BackupReport]:
        """Copy file to backup space

        Args:
            config (CollectorConfiguration): ingestion config
            path (str): local file path on the pod working directory

        Returns:
            list[BackupReport]: one report per destination the file was sent to
        """
        source_path = path

        compression_enabled = self.args.enable_gzip
        for filetype in CollectorBackup.DO_NOT_COMPRESS_EXTENSION:
            if path.endswith(filetype):
                self.logger.debug(
                    "The following file %s will not be compressed before"
                    " being backed-up as its type '%s' in not suitable for compression",
                    path,
                    filetype,
                )
                compression_enabled = False
                break

        # handle compression.
        if compression_enabled:

            gzip_path = f"{path}.gz"

            self.logger.debug("Compressing %s to %s", path, gzip_path)

            with open(path, "rb") as f_in:

                with gzip.open(gzip_path, "wb") as f_out:

                    shutil.copyfileobj(f_in, f_out)

            path = gzip_path

        try:
            reports = self.backup_file_implementation(config, path) or []
        finally:
            # clean up compressed file. In a finally block because an implementation
            # raising outside its TRANSFER_ERRORS used to leak the .gz forever.
            if compression_enabled:
                os.remove(path)

        # the implementation only ever sees the compressed path: restore the
        # identity of the ingested file
        for report in reports:
            report.source_filename = os.path.basename(source_path)
            report.compressed = compression_enabled

        return reports

    def backup_file_implementation(self, config, path) -> typing.List[BackupReport]:
        """This function which shall be redefined by the child class is responsible
           to perform the file backup operation to the remote server

        Args:
            config (CollectorConfiguration): ingestion config
            path (str): local file path on the pod working directory

        Raises:
            NotImplementedError: Raise in case the backup implementation
            function has not been defined in the instanciated child class

        Returns:
            list[BackupReport]: one report per destination written to
        """
        # Abstract Class
        raise NotImplementedError()

    def validate_backup_arguments(self):
        """Function used to make sure that all needed variables
        needed by the backup implementation have been provided
        """
        if not self.args.directory:
            raise ValueError(
                "The following arguments are mandatory for a Local Backup : directory"
            )

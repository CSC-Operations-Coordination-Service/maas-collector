import os
import shutil
from typing import List

from maas_collector.backup.backup import BackupReport, CollectorBackup


class CollectorBackupLocal(CollectorBackup):
    """Local implementation of the Collector Backup

    Args:
        CollectorBackup (CollectorBackup): Main Backup Collector Class
    """

    # IOError is an alias of OSError in python 3, which is what the inlined
    # try/except used to catch here
    TRANSFER_ERRORS = (OSError,)

    def close(self):
        "Nothing to close"

    @property
    def destinations(self) -> List[str]:
        """override: a local backup has a single destination directory"""
        return [self.args.directory]

    def backup_file_implementation(self, config, path) -> List[BackupReport]:
        """Copy path under the configured directory.

        Args:
            config (CollectorConfiguration): ingestion config
            path (str): local file path on the pod working directory

        Returns:
            list[BackupReport]: a single report
        """
        local_file_path = "/".join(self.get_backup_path(config, path))

        with self.report_transfer(
            config, path, self.args.directory, local_file_path
        ) as report:

            parent_folder = os.path.dirname(local_file_path)

            # Create the parent folder if it doesn't exist
            os.makedirs(parent_folder, exist_ok=True)

            self.logger.debug(
                "A backup of %s will be created on %s, its remote path will be %s",
                path,
                self.args.directory,
                local_file_path,
            )
            shutil.copy2(path, local_file_path)

        return [report]

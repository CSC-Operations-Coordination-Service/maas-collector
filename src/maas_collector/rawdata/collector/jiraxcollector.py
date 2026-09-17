"""Extract files from JIRA rest API"""

import base64
import datetime
import json
import os
import pathlib
from dataclasses import dataclass, field
from typing import Any, Dict, List

import dateutil.parser
import jira
import jira.resources
from maas_model import datestr_to_zulu
from maas_collector.rawdata.collector.filecollector import (
    FileCollector,
    FileCollectorConfiguration,
)
from maas_collector.rawdata.collector.journal import (
    CollectingInProgressError,
    CollectorJournal,
    NoRefreshException,
)


# Désactive la génération automatique de __repr__ pour pouvoir utiliser
# celui du parent qui masque les données sensible comme les mot de passe
@dataclass(repr=False)
class JiraExtendedCollectorConfiguration(FileCollectorConfiguration):
    """Configuration for Jira collection"""

    end_point: str = ""

    jql_query: str = ""

    auth_method: str = ""

    username: str = ""

    password: str = field(default="", metadata={"sensitive": True})

    token: str = field(default="", metadata={"sensitive": True})

    proxy_login: str = ""

    proxy_password: str = field(default="", metadata={"sensitive": True})

    ingest_attachements: bool = False

    attachement_prefix: bool = False

    # {raw data field name: jira.resources.Attachment attribute name} stamped on
    # every record extracted from an attachment of this interface. Empty by
    # default, so interfaces that do not opt in are left untouched.
    attachment_extra_fields: Dict[str, str] = field(default_factory=dict)

    # attachment_extra_fields entries whose value is a date and must be
    # normalised to ZULU before being stamped
    attachment_date_fields: List[str] = field(default_factory=list)

    refresh_interval: int = 0

    expand = "changelog"

    timeout = 120

    @property
    def has_proxy_auth(self) -> bool:
        """tell if proxy credentials are present

        Returns:
            bool: true if proxy credentials are present
        """
        return bool(self.proxy_login and self.proxy_password)


class JIRAExtendedCollector(FileCollector):
    """A collector that collect from a JIRA REST api.

    Could be one day refactored to more generic REST api collector.

    Warning: does not support redirect
    """

    CONFIG_CLASS = JiraExtendedCollectorConfiguration

    def ingest(self, path=None, configs=None, force_update=None):
        """Ingest from JIRA. All arguments are ignored so defaults to None"""
        # iterate over all JIRA collector configurations
        for config in self.configs:
            self.logger.info("On going for %s", config.interface_name)
            if not config.end_point:
                # skip configuration with no end_point like configuration for
                # attachement ingestion
                continue

            try:
                # use the journal as context to secure the concurent collect and keep
                # the last update date
                with CollectorJournal(config) as journal:
                    self._healthcheck.tick()

                    self.ingest_tickets(config, journal)

            except CollectingInProgressError:
                # Errors should never pass silently.
                self.logger.info(
                    "On going collection on interface %s: skipping",
                    config.interface_name,
                )
            except NoRefreshException:
                self.logger.info(
                    "Interface %s does not need to be refreshed: skipped",
                    config.interface_name,
                )
            finally:
                # flush messages between interfaces as they don't ingest the same data
                self._flush_message_groups()

            if self.should_stop_loop:
                break

    @classmethod
    def build_client(cls, config: JiraExtendedCollectorConfiguration) -> jira.JIRA:
        """Create a Jira instance

        Args:
            config (JiraExtendedCollectorConfiguration): configuration

        Returns:
            jira.JIRA: initialized Jira client instance
        """
        # arguments for JIRA constructor
        argv: Dict[str, Any] = {}

        match config.auth_method:
            case "basic":
                argv["basic_auth"] = (config.username, config.password)

            case "cookie_based":
                # this is deprecated but used by some subcontractors
                argv["auth"] = (config.username, config.password)

            case "token_based":
                argv["token_auth"] = config.token

            case "jira_cloud":
                # JIRA cloud special case: basic auth with token as password
                # https://jira.readthedocs.io/examples.html#jira-cloud
                argv["basic_auth"] = (config.username, config.token)

            case _:
                raise ValueError(f"Unsupported auth method: '{config.auth_method}'")

        if config.has_proxy_auth:
            # add http "proxy" authorization (EDRS case) that add basic authentication
            # to the Authorization header
            argv["options"] = {
                "headers": {
                    "Authorization": "Basic "
                    + base64.b64encode(
                        b":".join(
                            (
                                config.proxy_login.encode(),
                                config.proxy_password.encode(),
                            )
                        )
                    )
                    .strip()
                    .decode()
                }
            }

        return jira.JIRA(
            config.end_point, timeout=(config.timeout, config.timeout), **argv
        )

    def ingest_tickets(
        self, config: JiraExtendedCollectorConfiguration, journal: CollectorJournal
    ):
        """Ingest tickets

        Args:
            config (JiraCollectorConfiguration): configuration to ingest
        """
        client = self.build_client(config)

        # TODO handle start/stop for replay
        if journal.last_date:
            date_criteria = f'"{journal.last_date.strftime("%Y-%m-%d %H:%M")}"'
        else:
            date_criteria = "-30d"

        jql_str = config.jql_query.format(date_criteria=date_criteria)

        self.logger.info("Querying %s with request: %s", config.interface_name, jql_str)

        filename_prefix = "_".join(
            (
                config.interface_name,
                datetime.datetime.now(tz=datetime.UTC).strftime(
                    "%Y%m%d_%H%M%S%f"
                ),  # UUID could be better
            )
        )

        try:

            # a pseudo full payload to store raw issue json for later ingestion
            # by json extractor (for backward compatibility and replay)
            payload_dict = {"issues": []}

            issues_attachments = []

            last_date = None

            issues = client.enhanced_search_issues(jql_str, maxResults=False)

            # iterate other issues to populate tickets and attachements
            for issue in issues:
                if self.should_stop_loop:
                    break
                self._healthcheck.tick()

                issue_date = dateutil.parser.parse(issue.fields.updated)
                # JQL does not support seconds in its date format
                # 10 years people want it
                # https://jira.atlassian.com/browse/JRASERVER-31250
                # so additionnal check is required with the real issue date

                # if journal.last_date and not issue_date > journal.last_date:
                #     self.logger.debug(
                #         "Skipping %s: too old (%s)", issue, issue.fields.updated
                #     )
                #     continue

                if config.extractor:
                    # conventional ticket ingestion if performed with JSON extractor
                    # for backward compatibility
                    payload_dict["issues"].append(issue.raw)

                if config.ingest_attachements and issue.fields.attachment:
                    self.logger.debug("Attachment found: %s", issue.fields.attachment)
                    # ingest attachements
                    issues_attachments.append((issue, issue.fields.attachment))

                last_date = issue_date

            # ingest tickets
            if payload_dict["issues"]:

                filename = os.path.join(
                    self.args.working_directory,
                    f"{filename_prefix}.json",
                )

                self._healthcheck.tick()

                self.ingest_issues_payload(config, payload_dict, filename)

            # ingest attachments
            for issue, attachments in issues_attachments:
                if config.attachement_prefix:
                    prefix = f"{issue.key}_"

                else:
                    prefix = ""

                if config.attachment_extra_fields:
                    # JIRA allows several attachments to share a file name on the
                    # same issue. They download to the same path and produce the
                    # same document identifiers, but carry different attachment
                    # identities, so they would overwrite each other at every
                    # collect. Only the most recent one is the current list.
                    attachments = self.deduplicate_attachments(attachments)

                for attachment in attachments:
                    self._healthcheck.tick()
                    self.ingest_attachement(attachment, prefix, config)

            # save only at the end of the page cause of compatibility with
            # json extractor
            if last_date:
                journal.last_date = last_date

            journal.tick()

        finally:
            # clean http session
            client.close()

    def ingest_issues_payload(
        self,
        config: JiraExtendedCollectorConfiguration,
        payload_dict: dict,
        filename: str,
    ):
        """ingest a json payload of a search result

        Args:
            config (JiraExtendedCollectorConfiguration): configuration
            payload_dict (dict): data dictionnary
            filename (str): path to save the payload
        """
        with open(filename, "w", encoding="utf-8") as payload_fd:
            json.dump(payload_dict, payload_fd)

        try:
            self.extract_from_file(
                filename,
                config,
                force_update=self.args.force,
                report_name=pathlib.Path(filename).name,
            )
        # catch broad exception to not break the loop
        # pylint: disable=W0703
        except Exception as error:
            self.logger.error("Error ingesting %s: %s", filename, error)
        # pylint: enable=W0703
        finally:
            os.remove(filename)

    @staticmethod
    def deduplicate_attachments(attachments: list) -> list:
        """Keep only the most recently created attachment of each file name

        Args:
            attachments (list): attachments of an issue

        Returns:
            list: at most one attachment per file name
        """
        newest = {}

        for attachment in attachments:
            current = newest.get(attachment.filename)

            if current is None or dateutil.parser.parse(
                attachment.created
            ) > dateutil.parser.parse(current.created):
                newest[attachment.filename] = attachment

        return list(newest.values())

    def build_attachment_extra_fields(
        self,
        attachment: jira.resources.Attachment,
        config: JiraExtendedCollectorConfiguration = None,
    ) -> dict:
        """Build the fields identifying the attachment a record was extracted from

        A JIRA attachment identifier changes at every upload, even when the file
        name and the file content are strictly identical. Stamping it on every
        record makes a re-upload visible downstream: the records still listed by
        the new file get the new identifier, while the records dropped from it keep
        the previous one.

        Args:
            attachment (jira.resources.Attachment): attachment object
            config (JiraExtendedCollectorConfiguration): the issue configuration

        Returns:
            dict: fields to stamp, empty if the interface does not opt in
        """
        if not config or not config.attachment_extra_fields:
            return {}

        extra_fields = {}

        for field_name, attr_name in config.attachment_extra_fields.items():
            try:
                value = getattr(attachment, attr_name)
            except AttributeError:
                self.logger.warning(
                    "Attachment %s has no attribute '%s': field '%s' not stamped",
                    attachment.id,
                    attr_name,
                    field_name,
                )
                continue

            if field_name in config.attachment_date_fields:
                # JIRA dates look like '2024-05-13T09:12:33.000+0200': a basic
                # format offset that the opensearch date_time format rejects
                value = datestr_to_zulu(value)

            extra_fields[field_name] = value

        return extra_fields

    def ingest_attachement(
        self,
        attachment: jira.resources.Attachment,
        prefix: str = "",
        config: JiraExtendedCollectorConfiguration = None,
    ):
        """Ingest a attached file

        Args:
            attachment (jira.resources.Attachment): attachment object
            prefix (str): prefix prepended to the downloaded file name
            config (JiraExtendedCollectorConfiguration): the issue configuration
        """

        configurations = self.get_configurations(attachment.filename)

        if not configurations:
            # does not match
            self.logger.debug(
                "%s does not match any file pattern: skipped", attachment.filename
            )
            return

        download_path = os.path.join(
            self.args.working_directory, prefix + attachment.filename
        )

        self.logger.info(
            "Downloading attachement %s to %s", attachment.id, download_path
        )

        with open(download_path, "wb") as attacement_fd:
            attacement_fd.write(attachment.get())

        extra_fields = self.build_attachment_extra_fields(attachment, config)

        try:
            for configuration in configurations:
                try:
                    self.extract_from_file(
                        download_path,
                        configuration,
                        force_update=self.args.force,
                        report_name=os.path.basename(download_path),
                        extra_fields=extra_fields,
                    )
                # catch broad exception to not break the loop
                # pylint: disable=W0703
                except Exception as error:
                    self.logger.error(
                        "Error ingesting attachement %s: %s", attachment, error
                    )
        finally:
            self.logger.debug("Deleting %s", download_path)
            os.remove(download_path)

    @classmethod
    def probe(cls, config: JiraExtendedCollectorConfiguration, probe_data):
        # test connection by building client
        try:
            client = cls.build_client(config)
            config.status = "OK"
            client.close()

        except (jira.JIRAError, ConnectionError) as error:
            config.status = "KO"
            raise error

    @classmethod
    def attributs_url(cls):
        return super().attributs_url() + ["end_point"]

    @classmethod
    def document(cls, config: JiraExtendedCollectorConfiguration):
        information = super().document(config)

        auth_method = getattr(config, "auth_method", "No auth")
        if config.has_proxy_auth:
            auth_method += f" - with proxy {config.proxy_login}"
        information |= {
            "protocol": "HTTP(S)",
            "auth_method": auth_method,
            "auth_user": getattr(config, "username"),
        }
        return information

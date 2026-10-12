"""

Contains CredentialMixin class that add capability to read credentials from a
credential provider (file or vault), populate interface configuration with credential
info, and refresh them while the collector runs.
"""

import copy
import functools
import logging
import time

from maas_collector.rawdata.collector.credentialprovider import (
    CredentialProvider,
    FileCredentialProvider,
    VaultCredentialProvider,
    build_credential_provider,
)

LOGGER = logging.getLogger("CredentialMixin")


class CredentialMixin:
    """

    Mixin class to provide credentials for collectors who need it. Expects the
    collector arguments in self.args and the configurations in self.configs.
    """

    # credential entries currently set on the configurations, by entry name
    _credential_entries: dict = None

    # time.monotonic() of the last successful read of the credentials
    _credential_load_time: float = None

    # True while the refreshes fail, to log one traceback per outage
    _credential_refresh_failing: bool = False

    @staticmethod
    def load_credential_dict(credential_file: str) -> dict:
        """
        Read a credential file and build a dict with interface names as key and
        the remaining dict as value

        Args:
            credential_file (str): path

        Returns:
            dict: populated dict
        """
        return FileCredentialProvider(credential_file).load(())

    @staticmethod
    def credential_fields(config, credential_dict: dict) -> dict:
        """attributes for a configuration: the shared entry (interface_credentials)
        overridden by the interface own entry (interface_name)

        Args:
            config: configuration
            credential_dict (dict): credential dictionnary

        Returns:
            dict: attribute name -> value
        """
        return {
            **credential_dict.get(config.interface_credentials, {}),
            **credential_dict.get(config.interface_name, {}),
        }

    @staticmethod
    def set_credential_attributes(config, credential_dict: dict):
        """set configuration attributes with data extracted from credential dict

        Args:
            config: configuration
            credential_dict (dict): credential dictionnary
        """

        if (
            config.interface_credentials not in credential_dict
            and config.interface_name not in credential_dict
        ):
            LOGGER.warning(
                "No credential configuration found for interface %s",
                config.interface_name,
            )
            return

        for name, value in CredentialMixin.credential_fields(
            config, credential_dict
        ).items():
            if not hasattr(config, name):
                LOGGER.warning(
                    "%s %s has not attribute %s",
                    config.__class__.__name__,
                    config.interface_name,
                    name,
                )
            setattr(config, name, value)

    @functools.cached_property
    def credential_provider(self) -> CredentialProvider | None:
        """where the credentials are read from, None without credential source"""
        return build_credential_provider(self.args)

    def credential_targets(self) -> list:
        """configurations that receive credentials

        Returns:
            list: configurations
        """
        return self.configs

    def credential_names(self) -> set:
        """names of the credential entries the configurations use"""
        names = set()

        for config in self.credential_targets():
            names.update(
                name
                for name in (config.interface_credentials, config.interface_name)
                if name
            )

        return names

    def load_credentials(self):
        """read the credentials and populate configurations with username, password ...

        Raises:
            ValueError: if the credential source settings are wrong
            FileNotFoundError: if the credential file is not found
            PermissionError: if the vault policy denies some entries
            hvac.exceptions.VaultError: if the vault cannot be read
        """
        if self.credential_provider is None:
            return

        names = self.credential_names()

        entries = self.credential_provider.load(names)

        if self.credential_provider.unreadable:
            raise PermissionError(
                "The vault policy does not allow reading the credential entries "
                + ", ".join(sorted(self.credential_provider.unreadable))
            )

        # unlike the file, the vault only returns the requested entries: none of
        # them is most likely a wrong mount or prefix. Not fatal, as collectors
        # without credentials may share the settings of the others
        if (
            names
            and not entries
            and isinstance(self.credential_provider, VaultCredentialProvider)
        ):
            LOGGER.error(
                "None of the %d credential entries was found in the vault under"
                " %s/%s: check VAULT_KV_MOUNT and VAULT_PATH_PREFIX",
                len(names),
                self.credential_provider.args.kv_mount,
                self.credential_provider.args.path_prefix,
            )

        self._credential_load_time = time.monotonic()
        # copy as configurations may change the values they receive
        self._credential_entries = copy.deepcopy(entries)

        # iterate over all configuration and populate credentials
        for config in self.credential_targets():
            self.set_credential_attributes(config, entries)

    def refresh_credentials(self):
        """read the credentials again once the refresh period has elapsed, and set
        the ones that changed on the configurations.

        A failing read or update is logged and the current credentials are kept: it
        is retried at the next call. An entry the vault policy no longer allows to
        read keeps its current values.
        """
        # nothing to refresh before load_credentials
        if (
            self._credential_load_time is None
            or self.credential_provider is None
            or self.args.credential_refresh_period <= 0
        ):
            return

        if (
            time.monotonic() - self._credential_load_time
            < self.args.credential_refresh_period
        ):
            return

        previous = self._credential_entries

        try:
            names = self.credential_names()

            entries = self.credential_provider.load(names)

            for name in self.credential_provider.unreadable:
                if name in previous:
                    entries[name] = previous[name]

            # the credential file holds the entries of every collector of the stack
            changed = {
                name for name in names if previous.get(name) != entries.get(name)
            }

            if changed:
                self._apply_changed_credentials(previous, entries, changed)
            else:
                LOGGER.debug("Credentials unchanged")

        # a failing credential source shall not stop the collect
        # pylint: disable=W0703
        except Exception as error:
            if self._credential_refresh_failing:
                LOGGER.error(
                    "Cannot refresh the credentials, keeping the current ones: %s",
                    error.__class__.__name__,
                )
            else:
                LOGGER.exception(
                    "Cannot refresh the credentials, keeping the current ones"
                )

            self._credential_refresh_failing = True
            return

        if self._credential_refresh_failing:
            LOGGER.info("Credentials refreshed again")
            self._credential_refresh_failing = False

        self._credential_load_time = time.monotonic()
        self._credential_entries = copy.deepcopy(entries)

    def _apply_changed_credentials(self, previous: dict, entries: dict, changed: set):
        """set the changed entries on the configurations that use them

        Args:
            previous (dict): entries currently set on the configurations
            entries (dict): entries read now
            changed (set): names of the entries that differ
        """
        # entry names only: never log a credential value
        LOGGER.info("Credentials changed for %s", ", ".join(sorted(changed)))

        interface_names = []

        for config in self.credential_targets():
            if not changed & {config.interface_credentials, config.interface_name}:
                continue

            removed = (
                self.credential_fields(config, previous).keys()
                - self.credential_fields(config, entries).keys()
            )

            for name in sorted(removed):
                LOGGER.warning(
                    "%s was removed from the credentials of %s:"
                    " it keeps its value until the collector restarts",
                    name,
                    config.interface_name,
                )

            self.set_credential_attributes(config, entries)

            interface_names.append(config.interface_name)

        self.on_credentials_changed(interface_names)

    def on_credentials_changed(self, interface_names: list):
        """Template method called after a refresh changed the credentials of some
        interfaces

        Args:
            interface_names (list): names of the interfaces
        """

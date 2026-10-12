"""
Credential providers: where the collectors read their credentials from.

A provider returns credential entries by name. An entry holds the configuration
attributes to set on the interfaces whose interface_credentials or interface_name
is that name (see CredentialMixin).
"""

import abc
import dataclasses
import json
import logging
import os
import typing

from maas_model.vault import VaultClient, VaultSettings

LOGGER = logging.getLogger("CredentialProvider")


@dataclasses.dataclass
class VaultArgs(VaultSettings):
    """OpenBao / HashiCorp Vault settings of the collectors"""

    # each credential entry is the secret <kv_mount>/<path_prefix>/<entry name>
    path_prefix: str = "maas-collector"


class CredentialProvider(abc.ABC):
    """Source of credential entries"""

    @abc.abstractmethod
    def load(self, names: typing.Iterable[str]) -> dict:
        """Read the credential entries

        Args:
            names (Iterable[str]): names of the entries the collector needs

        Returns:
            dict: entry name -> attribute dict. Names without entry are left out.
        """


class FileCredentialProvider(CredentialProvider):
    """Credentials from a JSON file: {"interfaces": [{"name": ..., ...}, ...]}"""

    def __init__(self, path: str):
        self.path = path

    def load(self, names: typing.Iterable[str]) -> dict:
        """Read every entry of the file, whatever the requested names

        Raises:
            ValueError: if no file is set
            FileNotFoundError: if the file does not exist
        """
        # consistency checks
        if not self.path:
            raise ValueError("No Credential file specified")

        if not os.path.isfile(self.path):
            raise FileNotFoundError(f"Credential file {self.path} does not exists")

        credential_dict = {}

        with open(self.path, encoding="UTF-8") as credential_fd:
            credential_json = json.load(credential_fd)

            for interface in credential_json["interfaces"]:
                credential_dict[interface["name"]] = interface
                del interface["name"]

        return credential_dict


class VaultCredentialProvider(CredentialProvider):
    """Credentials from an OpenBao or HashiCorp Vault KV version 2 engine.

    Each entry is a secret: <kv_mount>/<path_prefix>/<entry name>, holding the
    same attributes as an entry of the credential file.
    """

    def __init__(self, args: VaultArgs):
        self.args = args

        self.client = VaultClient(args)

    def load(self, names: typing.Iterable[str]) -> dict:
        entries = {}

        for name in sorted(names):
            path = f"{self.args.path_prefix}/{name}" if self.args.path_prefix else name

            secret = self.client.read_secret(path)

            if secret is not None:
                entries[name] = secret

        return entries


def build_credential_provider(args) -> CredentialProvider | None:
    """Create the credential provider of a collector

    Args:
        args (CollectorArgs): collector arguments

    Raises:
        ValueError: on an unknown backend or incomplete vault settings

    Returns:
        CredentialProvider | None: None when the collector has no credential source
    """
    backend = args.credential_backend or "file"

    if backend == "file":
        if not args.credential_file:
            return None
        return FileCredentialProvider(args.credential_file)

    if backend == "vault":
        return VaultCredentialProvider(args.vault or VaultArgs())

    raise ValueError(f"Unknown credential backend {backend}: expected file or vault")

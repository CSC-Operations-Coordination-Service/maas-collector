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

import hvac

LOGGER = logging.getLogger("CredentialProvider")


@dataclasses.dataclass
class VaultArgs:
    """OpenBao / HashiCorp Vault connection settings"""

    url: str = ""

    # mount point of the KV version 2 secret engine
    kv_mount: str = "secret"

    # each credential entry is the secret <kv_mount>/<path_prefix>/<entry name>
    path_prefix: str = "maas-collector"

    # approle or token
    auth_method: str = "approle"

    role_id: str = ""

    # file holding the AppRole secret id, typically a Swarm secret
    secret_id_file: str = ""

    # file holding a token for the token auth method. Without it the token is
    # taken from VAULT_TOKEN
    token_file: str = ""


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


def read_secret_file(path: str) -> str:
    """Read a secret stored alone in a file, without the trailing new line"""
    with open(path, encoding="UTF-8") as secret_fd:
        return secret_fd.read().strip()


class VaultCredentialProvider(CredentialProvider):
    """Credentials from an OpenBao or HashiCorp Vault KV version 2 engine.

    Each entry is a secret: <kv_mount>/<path_prefix>/<entry name>, holding the
    same attributes as an entry of the credential file.
    """

    AUTH_METHODS = ("approle", "token")

    def __init__(self, args: VaultArgs):
        if not args.url:
            raise ValueError("Vault credentials need a vault address: set VAULT_ADDR")

        if args.auth_method not in self.AUTH_METHODS:
            raise ValueError(
                f"Unknown VAULT_AUTH_METHOD {args.auth_method}:"
                f" expected one of {', '.join(self.AUTH_METHODS)}"
            )

        if args.auth_method == "approle":
            if not args.role_id:
                raise ValueError("Vault AppRole authentication needs VAULT_ROLE_ID")

            if not args.secret_id_file:
                raise ValueError(
                    "Vault AppRole authentication needs VAULT_SECRET_ID_FILE"
                )

        self.args = args

        # TLS verification follows VAULT_CACERT / VAULT_CAPATH like the vault CLI
        self.client = hvac.Client(url=args.url)

        self._logged_in = False

    def login(self):
        """Get a new vault token"""
        if self.args.auth_method == "approle":
            LOGGER.info("Logging in to %s with AppRole", self.args.url)
            # read at each login so a new secret id is used without restart
            self.client.auth.approle.login(
                role_id=self.args.role_id,
                secret_id=read_secret_file(self.args.secret_id_file),
            )
        elif self.args.token_file:
            self.client.token = read_secret_file(self.args.token_file)

        self._logged_in = True

    def load(self, names: typing.Iterable[str]) -> dict:
        if not self._logged_in:
            self.login()

        try:
            return self._read_entries(names)

        except hvac.exceptions.Forbidden:
            # most of the time the token expired. Retry once with a new one
            LOGGER.info("Vault refused the token, logging in again")
            self.login()
            return self._read_entries(names)

    def _read_entries(self, names: typing.Iterable[str]) -> dict:
        entries = {}

        for name in sorted(names):
            path = f"{self.args.path_prefix}/{name}" if self.args.path_prefix else name

            try:
                response = self.client.secrets.kv.v2.read_secret_version(
                    path=path,
                    mount_point=self.args.kv_mount,
                    raise_on_deleted_version=True,
                )
            except hvac.exceptions.InvalidPath:
                LOGGER.debug("No vault secret %s/%s", self.args.kv_mount, path)
                continue

            entries[name] = response["data"]["data"]

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

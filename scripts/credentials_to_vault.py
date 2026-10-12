"""Copy the entries of a collector credential file to an OpenBao / HashiCorp Vault
KV version 2 engine, one secret per entry: <kv-mount>/<path-prefix>/<entry name>.

Authenticates with VAULT_TOKEN, a token allowed to write under the prefix. Prints
entry and attribute names only, never values.
"""

import argparse
import os
import sys

import hvac

from maas_collector.rawdata.collector.credentialprovider import FileCredentialProvider


def main(argv: list) -> int:
    """copy the credential file entries to the vault

    Args:
        argv (list): command line arguments

    Returns:
        int: exit code
    """
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument("credential_file", help="collector credential JSON file")

    parser.add_argument(
        "--vault-addr",
        default=os.environ.get("VAULT_ADDR", ""),
        help="vault address (default: VAULT_ADDR)",
    )

    parser.add_argument(
        "--kv-mount",
        default=os.environ.get("VAULT_KV_MOUNT", "secret"),
        help="mount point of the KV version 2 engine"
        " (default: VAULT_KV_MOUNT or %(default)s)",
    )

    parser.add_argument(
        "--path-prefix",
        default=os.environ.get("VAULT_PATH_PREFIX", "maas-collector"),
        help="path of the credential secrets in the engine"
        " (default: VAULT_PATH_PREFIX or %(default)s)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the secrets that would be written, without writing them",
    )

    args = parser.parse_args(argv)

    if not args.vault_addr and not args.dry_run:
        parser.error("set the vault address with --vault-addr or VAULT_ADDR")

    entries = FileCredentialProvider(args.credential_file).load(())

    client = None if args.dry_run else hvac.Client(url=args.vault_addr)

    for name, entry in entries.items():
        path = f"{args.path_prefix}/{name}" if args.path_prefix else name

        if client is not None:
            client.secrets.kv.v2.create_or_update_secret(
                path=path, secret=entry, mount_point=args.kv_mount
            )

        print(
            f"{'would write' if args.dry_run else 'wrote'} {args.kv_mount}/{path}:"
            f" {', '.join(sorted(entry)) or '(no attribute)'}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

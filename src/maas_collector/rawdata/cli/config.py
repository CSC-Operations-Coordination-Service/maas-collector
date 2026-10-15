"""
Administration of the collector configurations stored in OpenSearch.

    python -m maas_collector.rawdata.cli.config <command> ...

The OpenSearch connection comes from the same settings as the collectors: ES_URL,
ES_USERNAME and ES_PASSWORD, or ES_VAULT_PATH with the VAULT_* settings. Writing
needs an OpenSearch user allowed to write to the configuration index.
"""

import argparse
import dataclasses
import difflib
import json
import logging
import os
import sys

from opensearchpy import OpenSearch

from maas_collector.rawdata.cli.lib.args import (
    config_index_parser,
    es_parser,
    get_es_credentials_url,
    vault_parser,
)
from maas_collector.rawdata.configprovider import (
    ConfigSource,
    is_config_content,
    validate_config_sources,
)
from maas_collector.rawdata.configuration import find_configurations
from maas_collector.rawdata.configstore import (
    ConfigConflictError,
    ConfigNotFoundError,
    ConfigStore,
    check_path,
    check_service,
    config_classes_for,
)

LOGGER = logging.getLogger("ConfigAdmin")


@dataclasses.dataclass
class DeployFile:
    """a configuration file of a deploy tree, and the services that load it"""

    content: dict

    services: list = dataclasses.field(default_factory=list)


def build_parser() -> argparse.ArgumentParser:
    """command line parser of the configuration admin CLI"""
    parser = argparse.ArgumentParser(
        prog="python -m maas_collector.rawdata.cli.config",
        description="Manage the collector configurations stored in OpenSearch",
        parents=[es_parser(), vault_parser(), config_index_parser()],
    )

    parser.add_argument(
        "--author",
        default=os.environ.get("USER", ""),
        help="author of the published versions (default: $USER)",
    )

    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        action="store_const",
        const=logging.INFO,
        default=logging.WARNING,
        help="log what is done",
    )

    commands = parser.add_subparsers(dest="command", required=True)

    commands.add_parser("init", help="create the index, unless it exists")

    command = commands.add_parser(
        "import", help="publish the files of a deploy tree <dir>/<service>/**/*.json"
    )
    command.add_argument("deploy_dir", help="deploy tree")
    command.add_argument(
        "--dry-run", action="store_true", help="check and report, write nothing"
    )

    command = commands.add_parser(
        "export", help="write the current versions as <out>/<service>/<path>"
    )
    command.add_argument("out_dir", help="output directory")
    command.add_argument("--service", default="", help="only this service")

    command = commands.add_parser("list", help="list the current versions")
    command.add_argument("--service", default="", help="only this service")
    command.add_argument("--interface", default="", help="only this interface name")

    command = commands.add_parser(
        "get", help="print a version: content on stdout, details on stderr"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("--version", type=int, help="version (default: current)")

    command = commands.add_parser("history", help="list every version of a path")
    command.add_argument("path", help="configuration path")

    command = commands.add_parser("diff", help="unified diff between two versions")
    command.add_argument("path", help="configuration path")
    command.add_argument(
        "--from", dest="from_version", type=int, help="(default: previous version)"
    )
    command.add_argument(
        "--to", dest="to_version", type=int, help="(default: current version)"
    )

    command = commands.add_parser("put", help="publish a new version from a file")
    command.add_argument("path", help="configuration path")
    command.add_argument("file", help="JSON configuration file")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument(
        "--services",
        default="",
        help="comma-separated services loading the file"
        " (default: the services of the current version, required for a new path)",
    )
    command.add_argument(
        "--base-version",
        type=int,
        help="refuse to publish if the current version is not this one",
    )

    command = commands.add_parser(
        "rollback", help="publish the content of an older version again"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("--to", type=int, required=True, help="version to restore")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument("--base-version", type=int, help="see put")

    command = commands.add_parser(
        "delete", help="publish a tombstone: the services stop loading the path"
    )
    command.add_argument("path", help="configuration path")
    command.add_argument("-m", "--comment", required=True, help="why")
    command.add_argument("--base-version", type=int, help="see put")

    return parser


def build_client(namespace: argparse.Namespace) -> OpenSearch:
    """OpenSearch client from the ES_* settings, or from the vault with
    ES_VAULT_PATH

    Raises:
        ValueError: without OpenSearch connection settings
    """
    url = get_es_credentials_url(namespace)

    if not url:
        raise ValueError("no OpenSearch connection: set ES_URL or ES_VAULT_PATH")

    return OpenSearch(
        hosts=[url],
        timeout=namespace.es_timeout,
        max_retries=namespace.es_retries,
        retry_on_timeout=True,
        verify_certs=not namespace.es_ignore_certs_verification,
        ssl_show_warn=not namespace.es_ignore_certs_verification,
    )


def get_author(namespace: argparse.Namespace) -> str:
    """author of a write

    Raises:
        ValueError: without author
    """
    if not namespace.author:
        raise ValueError("no author: set --author or USER")
    return namespace.author


def read_deploy_tree(deploy_dir: str) -> tuple[dict, list, dict]:
    """Read the configuration files of a deploy tree <deploy_dir>/<service>/**/*.json,
    hidden files and directories excluded

    Returns:
        tuple[dict, list, dict]: path -> DeployFile; the skipped files, which are
            not configuration files, like credential files; path -> reason for the
            refused paths
    """
    files = {}
    skipped = []
    refused = {}

    for service in sorted(os.listdir(deploy_dir)):
        service_dir = os.path.join(deploy_dir, service)

        if service.startswith(".") or not os.path.isdir(service_dir):
            continue

        for file_path in sorted(find_configurations(service_dir)):
            path = os.path.relpath(file_path, service_dir).replace(os.sep, "/")

            if path in refused:
                continue

            try:
                with open(file_path, encoding="UTF-8") as conf_fd:
                    content = json.load(conf_fd)
            except ValueError as error:
                refused[path] = f"{service}/{path} is not valid JSON: {error}"
                continue

            if not is_config_content(content):
                skipped.append(f"{service}/{path}")
                continue

            if path in files and files[path].content != content:
                refused[path] = f"the copies of {path} differ between services"
                continue

            files.setdefault(path, DeployFile(content)).services.append(service)

    for path in refused:
        files.pop(path, None)

    return files, skipped, refused


def init_command(store: ConfigStore, _namespace) -> int:
    """create the index"""
    if store.init():
        print(f"created index {store.index}")
    else:
        print(f"index {store.index} exists already")
    return 0


def import_command(store: ConfigStore, namespace) -> int:
    """publish the new paths of a deploy tree, report the others"""
    files, skipped, refused = read_deploy_tree(namespace.deploy_dir)
    status = 0

    for name in skipped:
        print(f"skipped {name}: not a configuration file")

    for path, reason in sorted(refused.items()):
        print(f"refused {path}: {reason}")
        status = 1

    author = get_author(namespace) if not namespace.dry_run else namespace.author

    for path in sorted(files):
        deploy_file = files[path]
        current = store.get(path)

        if current is None:
            try:
                if namespace.dry_run:
                    validate_config_sources(
                        [ConfigSource(path, deploy_file.content)],
                        config_classes_for,
                        resolve_models=False,
                    )
                else:
                    store.publish(
                        path,
                        deploy_file.content,
                        deploy_file.services,
                        author,
                        "initial import",
                        base_version=0,
                    )
            except (ConfigConflictError, ValueError) as error:
                print(f"refused {path}: {error}")
                status = 1
                continue

            verb = "would create" if namespace.dry_run else "created"
            print(f"{verb} {path} v1 ({', '.join(deploy_file.services)})")

        elif (
            not current["deleted"]
            and current["content"] == deploy_file.content
            and current["services"] == sorted(deploy_file.services)
        ):
            print(f"unchanged {path} v{current['version']}")

        else:
            print(
                f"drift {path}: v{current['version']} in the index differs from the"
                " deploy tree, left untouched"
            )

    return status


def export_command(store: ConfigStore, namespace) -> int:
    """write the current versions back as a deploy tree"""
    count = 0
    skipped = 0
    root = os.path.realpath(namespace.out_dir)

    for document in store.latest(service=namespace.service):
        if document["deleted"]:
            continue

        for service in document["services"]:
            if namespace.service and service != namespace.service:
                continue

            # a document written by hand in the index can carry any path or
            # service: never write outside the output directory
            try:
                check_path(document["path"])
                check_service(service)
            except ValueError as error:
                print(f"skipped: {error}", file=sys.stderr)
                skipped += 1
                continue

            target = os.path.join(root, service, *document["path"].split("/"))

            if not os.path.realpath(target).startswith(root + os.sep):
                print(f"skipped: {target} is outside {root}", file=sys.stderr)
                skipped += 1
                continue

            os.makedirs(os.path.dirname(target), exist_ok=True)

            with open(target, "w", encoding="UTF-8") as out_fd:
                json.dump(document["content"], out_fd, indent=4, ensure_ascii=False)
                out_fd.write("\n")

            count += 1

    print(f"exported {count} files to {namespace.out_dir}")
    return 1 if skipped else 0


def list_command(store: ConfigStore, namespace) -> int:
    """print path, version, services, author and date of the current versions"""
    for document in store.latest(
        service=namespace.service, interface=namespace.interface
    ):
        if document["deleted"]:
            continue

        print(
            "\t".join(
                [
                    document["path"],
                    f"v{document['version']}",
                    ",".join(document["services"]),
                    document.get("author") or "",
                    document.get("created_at") or "",
                ]
            )
        )
    return 0


def find_version(store: ConfigStore, path: str, version: int | None) -> dict:
    """a version of a path

    Raises:
        ConfigNotFoundError: when it does not exist
    """
    document = store.get(path, version)

    if document is None:
        name = path if version is None else f"{path}@{version}"
        raise ConfigNotFoundError(f"{name} does not exist")

    return document


def get_command(store: ConfigStore, namespace) -> int:
    """content on stdout, version, services and author on stderr"""
    document = find_version(store, namespace.path, namespace.version)

    print(json.dumps(document["content"], indent=4, ensure_ascii=False))

    deleted = " (deleted)" if document["deleted"] else ""
    print(
        f"{document['path']}@{document['version']}{deleted}"
        f" services={','.join(document['services'])}"
        f" author={document.get('author')} created_at={document.get('created_at')}",
        file=sys.stderr,
    )
    return 0


def history_command(store: ConfigStore, namespace) -> int:
    """one line per version, oldest first"""
    history = store.history(namespace.path)

    if not history:
        raise ConfigNotFoundError(f"{namespace.path} does not exist")

    for document in history:
        flags = [
            flag
            for flag, value in (
                ("latest", document["latest"]),
                ("deleted", document["deleted"]),
            )
            if value
        ]
        based_on = (
            f" (based on v{document['based_on']})" if document.get("based_on") else ""
        )
        print(
            f"v{document['version']}\t{document.get('created_at')}"
            f"\t{document.get('author')}\t{','.join(flags)}"
            f"\t{document.get('comment')}{based_on}"
        )
    return 0


def diff_lines(document: dict) -> list:
    """lines of a version compared by diff: services, tombstone flag and content"""
    shown = {
        "services": document["services"],
        "deleted": document["deleted"],
        "content": document["content"],
    }
    text = json.dumps(shown, indent=4, sort_keys=True, ensure_ascii=False) + "\n"
    return text.splitlines(keepends=True)


def diff_command(store: ConfigStore, namespace) -> int:
    """unified diff between two versions, the current one and its previous one by
    default"""
    to_document = find_version(store, namespace.path, namespace.to_version)

    from_version = namespace.from_version
    if from_version is None:
        from_version = to_document["version"] - 1

    from_document = find_version(store, namespace.path, from_version)

    sys.stdout.writelines(
        difflib.unified_diff(
            diff_lines(from_document),
            diff_lines(to_document),
            fromfile=f"{namespace.path}@{from_version}",
            tofile=f"{namespace.path}@{to_document['version']}",
        )
    )
    return 0


def put_command(store: ConfigStore, namespace) -> int:
    """publish a new version from a file"""
    with open(namespace.file, encoding="UTF-8") as conf_fd:
        content = json.load(conf_fd)

    if namespace.services:
        services = [
            service.strip()
            for service in namespace.services.split(",")
            if service.strip()
        ]
    else:
        current = store.get(namespace.path)
        if current is None:
            raise ValueError(
                f"{namespace.path} is a new path: give its services with --services"
            )
        services = current["services"]

    document = store.publish(
        namespace.path,
        content,
        services,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )

    print(
        f"published {namespace.path}@{document['version']}"
        f" ({', '.join(document['services'])})"
    )
    return 0


def rollback_command(store: ConfigStore, namespace) -> int:
    """publish an older version again"""
    document = store.rollback(
        namespace.path,
        namespace.to,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )
    print(
        f"published {namespace.path}@{document['version']}"
        f" with the content of v{namespace.to}"
    )
    return 0


def delete_command(store: ConfigStore, namespace) -> int:
    """publish a tombstone"""
    document = store.delete(
        namespace.path,
        get_author(namespace),
        namespace.comment,
        base_version=namespace.base_version,
    )
    print(f"deleted {namespace.path}: tombstone v{document['version']}")
    return 0


COMMANDS = {
    "init": init_command,
    "import": import_command,
    "export": export_command,
    "list": list_command,
    "get": get_command,
    "history": history_command,
    "diff": diff_command,
    "put": put_command,
    "rollback": rollback_command,
    "delete": delete_command,
}


def config_main(argv: list) -> int:
    """entry point

    Args:
        argv (list): command line arguments

    Returns:
        int: exit code, 1 on a refused or failed command
    """
    namespace = build_parser().parse_args(argv)

    # stderr: stdout carries the output of get, diff and export
    logging.basicConfig(
        level=namespace.loglevel,
        stream=sys.stderr,
        format="[%(asctime)s] %(levelname)s:%(name)s: %(message)s",
    )

    try:
        store = ConfigStore(build_client(namespace), namespace.config_index)
        return COMMANDS[namespace.command](store, namespace)
    except (ConfigConflictError, ConfigNotFoundError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(config_main(sys.argv[1:]))

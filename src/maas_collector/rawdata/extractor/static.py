"""Validation of an extractor configuration that never evaluates operator code.

A configuration holds Python lambdas (`python` and `rpython` strings) and
converters. They are evaluated where they run, in the collectors. Everything here
only parses them, so that a validation service does not execute what it validates.
"""

import ast
import inspect
import typing

from maas_collector.rawdata import extractor
from maas_collector.rawdata.extractor.base import BaseExtractor

LAMBDA_KEYS = ("python", "rpython")


def check_lambda_source(code: str) -> None:
    """check that code is a lambda without default argument, without evaluating it

    Args:
        code (str): source of the lambda

    Raises:
        ValueError: code is not a string, is not an expression, is not a lambda, or
            has a default argument (a default is evaluated when the lambda is
            defined)
    """
    if not isinstance(code, str):
        raise ValueError(f"a lambda must be a string, got {code!r}")

    try:
        tree = ast.parse(code, mode="eval")
    except (SyntaxError, ValueError) as error:
        raise ValueError(f"not a Python expression: {error}") from error

    if not isinstance(tree.body, ast.Lambda):
        raise ValueError("not a lambda: expected an expression like 'lambda x: x'")

    arguments = tree.body.args
    if arguments.defaults or any(
        default is not None for default in arguments.kw_defaults
    ):
        raise ValueError("a lambda must not have default arguments")


def check_lambdas(node: typing.Any, path: str) -> None:
    """check every value under a `python` or `rpython` key, at any depth

    Raises:
        ValueError: naming the path of the field
    """
    if isinstance(node, dict):
        for key, value in node.items():
            child = f"{path}.{key}"
            if key in LAMBDA_KEYS:
                try:
                    check_lambda_source(value)
                except ValueError as error:
                    raise ValueError(f"{child}: {error}") from error
            else:
                check_lambdas(value, child)
    elif isinstance(node, list):
        for index, item in enumerate(node):
            check_lambdas(item, f"{path}[{index}]")


def check_extractor_static(extractor_meta: dict) -> None:
    """check the extractor of a collector entry without building it

    Args:
        extractor_meta (dict): `class`, `args` and `converter_map` of the entry

    Raises:
        ValueError: the class is not a concrete extractor of the extractor package,
            the arguments do not bind to its constructor, or a lambda is not valid
    """
    name = extractor_meta.get("class")

    class_obj = getattr(extractor, name, None) if isinstance(name, str) else None

    if not (
        inspect.isclass(class_obj)
        and issubclass(class_obj, BaseExtractor)
        and not inspect.isabstract(class_obj)
    ):
        raise ValueError(f"extractor class {name!r} is not a known extractor")

    args = dict(extractor_meta.get("args") or {})

    # as build_extractor does
    if "converter_map" in extractor_meta:
        args["converter_map"] = extractor_meta["converter_map"]

    try:
        inspect.signature(class_obj).bind(**args)
    except TypeError as error:
        raise ValueError(f"extractor {name}: {error}") from error

    check_lambdas(args, "args")

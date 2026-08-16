from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys

from enum import IntEnum
from pathlib import Path
from typing import Any

from mongoeco.conformance.models import ConformanceProfile
from mongoeco.conformance.provider import EngineConformanceProvider
from mongoeco.conformance.runner import run_engine_conformance
from mongoeco.conformance.schema import conformance_report_schema


class ConformanceCliExit(IntEnum):
    SUCCESS = 0
    CONTRACT_FAILED = 1
    LOAD_ERROR = 2
    INTERNAL_ERROR = 3
    TIMEOUT = 4
    SCHEMA_ERROR = 5


class _ConformanceSchemaError(RuntimeError):
    pass


def _load_symbol(specification: str, *, label: str) -> object:
    module_name, separator, attribute_name = specification.partition(":")
    if not separator or not module_name or not attribute_name:
        message = f"{label} must use package.module:attribute syntax"
        raise ValueError(message)
    module = importlib.import_module(module_name)
    try:
        return getattr(module, attribute_name)
    except AttributeError as error:
        message = f"{label} attribute does not exist: {specification}"
        raise ValueError(message) from error


def _parse_profiles(values: list[str] | None) -> tuple[ConformanceProfile, ...]:
    if not values:
        return tuple(ConformanceProfile)
    selected: list[ConformanceProfile] = []
    for raw in values:
        selected.extend(ConformanceProfile(value.strip()) for value in raw.split(","))
    return tuple(dict.fromkeys(selected))


def _validate_report(document: dict[str, object]) -> None:
    try:
        jsonschema = importlib.import_module("jsonschema")
    except ImportError as error:
        message = (
            "JSON Schema validation requires the engine-testing extra or jsonschema"
        )
        raise _ConformanceSchemaError(message) from error
    try:
        jsonschema.Draft202012Validator(conformance_report_schema()).validate(document)
    except jsonschema.exceptions.ValidationError as error:
        raise _ConformanceSchemaError(error.message) from error


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m mongoeco.conformance",
        description="Run MongoEco's public engine conformance profiles.",
    )
    parser.add_argument("provider", help="Engine factory as package.module:attribute")
    parser.add_argument("--name", help="Provider name used in the report")
    parser.add_argument(
        "--profile",
        action="append",
        metavar="PROFILE",
        help=(
            "Profile to run; repeat the option or use commas. Available: "
            + ", ".join(profile.value for profile in ConformanceProfile)
        ),
    )
    parser.add_argument(
        "--cleanup", help="Optional cleanup callable as module:attribute"
    )
    parser.add_argument("--namespace-prefix", default="mongoeco_conformance")
    parser.add_argument("--format", choices=("human", "json"), default="human")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--require-success", action="store_true")
    parser.add_argument("--timeout", type=float, default=60.0)
    return parser


async def _execute(args: argparse.Namespace) -> tuple[dict[str, object], str, bool]:
    factory = _load_symbol(args.provider, label="provider")
    if not callable(factory):
        message = f"provider factory is not callable: {args.provider}"
        raise TypeError(message)
    cleanup: Any = None
    if args.cleanup:
        cleanup = _load_symbol(args.cleanup, label="cleanup")
        if not callable(cleanup):
            message = f"cleanup is not callable: {args.cleanup}"
            raise TypeError(message)
    provider = EngineConformanceProvider(
        args.name or args.provider,
        factory,
        namespace_prefix=args.namespace_prefix,
        cleanup=cleanup,
    )
    profiles = _parse_profiles(args.profile)
    report = await asyncio.wait_for(
        run_engine_conformance(provider, profiles=profiles),
        timeout=args.timeout,
    )
    document = report.to_document()
    _validate_report(document)
    return document, report.human_summary(), report.passed


def _write_result(
    args: argparse.Namespace, document: dict[str, object], human: str
) -> None:
    rendered = (
        json.dumps(document, indent=2, sort_keys=True) + "\n"
        if args.format == "json"
        else human + "\n"
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        return
    sys.stdout.write(rendered)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
        if args.timeout <= 0:
            parser.error("--timeout must be positive")
        document, human, passed = asyncio.run(_execute(args))
        _write_result(args, document, human)
        if args.require_success and not passed:
            return ConformanceCliExit.CONTRACT_FAILED
        return ConformanceCliExit.SUCCESS
    except _ConformanceSchemaError as error:
        sys.stderr.write(f"conformance schema error: {error}\n")
        return ConformanceCliExit.SCHEMA_ERROR
    except (ImportError, TypeError, ValueError) as error:
        sys.stderr.write(f"conformance load error: {type(error).__name__}: {error}\n")
        return ConformanceCliExit.LOAD_ERROR
    except TimeoutError:
        sys.stderr.write("conformance timed out\n")
        return ConformanceCliExit.TIMEOUT
    except Exception as error:
        sys.stderr.write(
            f"conformance internal error: {type(error).__name__}: {error}\n"
        )
        return ConformanceCliExit.INTERNAL_ERROR

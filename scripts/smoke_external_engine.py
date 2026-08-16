from __future__ import annotations

import asyncio
import importlib
import sys

from pathlib import Path

from jsonschema import Draft202012Validator

from mongoeco.conformance import (
    EngineConformanceProvider,
    conformance_report_schema,
    run_engine_conformance,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
ExternalCanaryEngine = importlib.import_module(
    "tests.consumer.engine_canary",
).ExternalCanaryEngine


async def _main() -> None:
    report = await run_engine_conformance(
        EngineConformanceProvider("external-canary", ExternalCanaryEngine),
    )
    Draft202012Validator(conformance_report_schema()).validate(report.to_document())
    report.require_success()
    sys.stdout.write(f"{report.human_summary()}\n")


if __name__ == "__main__":
    asyncio.run(_main())

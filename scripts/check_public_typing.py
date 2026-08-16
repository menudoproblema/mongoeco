from __future__ import annotations

import os
import re
import subprocess
import sys

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
POSITIVE = ROOT / "tests/typing/public_contract_positive.py"
NEGATIVE = ROOT / "tests/typing/public_contract_negative.py"
ERROR_PATTERN = re.compile(
    r"^(?P<path>.*?):(?P<line>\d+): error: .* \[(?P<code>[^]]+)]$"
)
MARKER_PATTERN = re.compile(r"# typing-error: (?P<code>[a-z0-9-]+)$")


def _run_mypy(path: Path) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    if environment.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1":
        source_root = str(ROOT / "src")
        existing = environment.get("MYPYPATH")
        environment["MYPYPATH"] = (
            source_root if not existing else os.pathsep.join((source_root, existing))
        )
    return subprocess.run(  # noqa: S603 - fixed interpreter and mypy arguments
        [
            sys.executable,
            "-m",
            "mypy",
            "--strict",
            "--follow-imports=silent",
            "--no-incremental",
            "--show-error-codes",
            "--no-error-summary",
            str(path),
        ],
        cwd=ROOT,
        capture_output=True,
        env=environment,
        text=True,
        check=False,
    )


def _expected_errors(path: Path) -> set[tuple[int, str]]:
    expected = set()
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        marker = MARKER_PATTERN.search(line)
        if marker:
            expected.add((line_number, marker.group("code")))
    return expected


def _observed_errors(output: str) -> set[tuple[int, str]]:
    observed = set()
    for line in output.splitlines():
        match = ERROR_PATTERN.match(line)
        if match:
            observed.add((int(match.group("line")), match.group("code")))
    return observed


def main() -> int:
    positive = _run_mypy(POSITIVE)
    if positive.returncode != 0:
        sys.stderr.write("positive public typing contract failed\n")
        sys.stderr.write(positive.stdout)
        sys.stderr.write(positive.stderr)
        return 1

    negative = _run_mypy(NEGATIVE)
    expected = _expected_errors(NEGATIVE)
    observed = _observed_errors(negative.stdout)
    if negative.returncode == 0 or observed != expected:
        sys.stderr.write("negative public typing contract diverged\n")
        sys.stderr.write(f"expected={sorted(expected)!r}\n")
        sys.stderr.write(f"observed={sorted(observed)!r}\n")
        sys.stderr.write(negative.stdout)
        sys.stderr.write(negative.stderr)
        return 1
    sys.stdout.write(
        f"public typing contract passed: positive=1, negative-errors={len(expected)}\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

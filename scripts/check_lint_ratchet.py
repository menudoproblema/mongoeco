#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys

from collections import Counter, defaultdict
from pathlib import Path


_HUNK_HEADER = re.compile(r'^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@')
_LINT_ROOTS = ('src', 'tests', 'scripts')


def _run_git(*args: str) -> str:
    git = shutil.which('git')
    if git is None:
        message = 'git executable is required'
        raise RuntimeError(message)
    result = subprocess.run(  # noqa: S603 - explicit git arguments
        [git, *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _changed_lines(base_ref: str) -> dict[Path, set[int]]:
    lines_by_path: dict[Path, set[int]] = defaultdict(set)
    diff = _run_git(
        'diff',
        '--unified=0',
        '--diff-filter=ACMR',
        base_ref,
        '--',
        *_LINT_ROOTS,
    )
    current_path: Path | None = None
    for line in diff.splitlines():
        if line.startswith('+++ b/'):
            current_path = Path(line[6:]).resolve()
            continue
        match = _HUNK_HEADER.match(line)
        if match is None or current_path is None:
            continue
        start = int(match.group(1))
        count = int(match.group(2) or '1')
        lines_by_path[current_path].update(range(start, start + count))

    untracked = _run_git(
        'ls-files',
        '--others',
        '--exclude-standard',
        '--',
        *_LINT_ROOTS,
    )
    for raw_path in untracked.splitlines():
        path = Path(raw_path)
        if path.suffix != '.py':
            continue
        resolved = path.resolve()
        lines_by_path[resolved].update(
            range(1, len(path.read_text(encoding='utf-8').splitlines()) + 1),
        )
    return lines_by_path


def _ruff_diagnostics(paths: list[Path]) -> list[dict[str, object]]:
    if not paths:
        return []
    result = subprocess.run(  # noqa: S603 - current Python executable
        [
            sys.executable,
            '-m',
            'ruff',
            'check',
            '--output-format',
            'json',
            *[str(path) for path in paths],
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode not in {0, 1}:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def _diagnostic_key(diagnostic: dict[str, object]) -> tuple[str, ...]:
    filename = Path(str(diagnostic['filename'])).resolve()
    try:
        path = str(filename.relative_to(Path.cwd().resolve()))
    except ValueError:
        path = str(filename)
    location = diagnostic.get('location')
    row = location.get('row') if isinstance(location, dict) else None
    source = ''
    if isinstance(row, int) and filename.exists():
        source_lines = filename.read_text(encoding='utf-8').splitlines()
        if 0 < row <= len(source_lines):
            source = source_lines[row - 1].strip()
    return (
        path,
        str(diagnostic.get('code', '')),
        str(diagnostic.get('message', '')),
        source,
    )


def _load_baseline(path: Path) -> Counter[tuple[str, ...]]:
    if not path.exists():
        return Counter()
    entries = json.loads(path.read_text(encoding='utf-8'))
    return Counter(
        {
            (
                str(entry['path']),
                str(entry['code']),
                str(entry['message']),
                str(entry['source']),
            ): int(entry['count'])
            for entry in entries
        },
    )


def _write_baseline(
    path: Path,
    diagnostics: list[dict[str, object]],
) -> None:
    counts = Counter(_diagnostic_key(item) for item in diagnostics)
    entries = [
        {
            'path': key[0],
            'code': key[1],
            'message': key[2],
            'source': key[3],
            'count': count,
        }
        for key, count in sorted(counts.items())
    ]
    path.write_text(
        f'{json.dumps(entries, indent=2, ensure_ascii=True)}\n',
        encoding='utf-8',
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description='Reject Ruff violations introduced on changed lines.',
    )
    parser.add_argument(
        '--base-ref',
        default='HEAD^',
        help='Git ref used as the lint baseline (default: HEAD^).',
    )
    parser.add_argument(
        '--baseline',
        type=Path,
        default=Path('scripts/ruff_ratchet_baseline.json'),
        help='Known changed-line diagnostics accepted as migration debt.',
    )
    parser.add_argument(
        '--update-baseline',
        action='store_true',
        help='Replace the baseline with the current changed-line diagnostics.',
    )
    args = parser.parse_args()

    changed_lines = _changed_lines(args.base_ref)
    python_paths = sorted(
        path
        for path in changed_lines
        if path.suffix == '.py' and path.exists()
    )
    changed_diagnostics = []
    for diagnostic in _ruff_diagnostics(python_paths):
        filename = Path(str(diagnostic['filename'])).resolve()
        location = diagnostic.get('location')
        if not isinstance(location, dict):
            continue
        row = location.get('row')
        if isinstance(row, int) and row in changed_lines.get(filename, set()):
            changed_diagnostics.append(diagnostic)

    if args.update_baseline:
        _write_baseline(args.baseline, changed_diagnostics)
        sys.stdout.write(
            f'wrote {len(changed_diagnostics)} diagnostics to '
            f'{args.baseline}\n',
        )
        return 0

    accepted = _load_baseline(args.baseline)
    introduced = []
    for diagnostic in changed_diagnostics:
        key = _diagnostic_key(diagnostic)
        if accepted[key] > 0:
            accepted[key] -= 1
        else:
            introduced.append(diagnostic)

    if not introduced:
        sys.stdout.write(
            f'lint ratchet passed for {len(python_paths)} changed Python '
            'files\n',
        )
        return 0

    for diagnostic in introduced:
        location = diagnostic['location']
        sys.stdout.write(
            f"{diagnostic['filename']}:"
            f"{location['row']}:{location['column']}: "
            f"{diagnostic['code']} {diagnostic['message']}\n",
        )
    sys.stdout.write(
        f'{len(introduced)} Ruff violation(s) introduced on changed lines\n',
    )
    return 1


if __name__ == '__main__':
    raise SystemExit(main())

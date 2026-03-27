import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.differential.runner import available_case_names, build_suite, resolve_case_filter, resolve_target


def main(argv: list[str] | None = None) -> int:
    args = sys.argv if argv is None else argv
    if "--list-cases" in args:
        target = resolve_target(args)
        major, minor = (int(part) for part in target.split(".", 1))
        for case_name in available_case_names((major, minor)):
            print(case_name)
        return 0

    if not os.getenv("MONGOECO_REAL_MONGODB_URI"):
        print("MONGOECO_REAL_MONGODB_URI is not configured", file=sys.stderr)
        return 2

    target = resolve_target(args)
    suite = build_suite(target, resolve_case_filter(args))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())

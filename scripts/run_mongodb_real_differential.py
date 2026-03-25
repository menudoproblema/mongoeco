import os
import sys
import unittest


TARGET_MODULES = {
    '7.0': 'tests.differential.mongodb7_real_parity',
    '8.0': 'tests.differential.mongodb8_real_parity',
}


def _resolve_target(argv: list[str]) -> str:
    if len(argv) >= 2:
        target = argv[1]
    else:
        target = os.getenv('MONGOECO_REAL_MONGODB_TARGET', '7.0')

    if target not in TARGET_MODULES:
        supported = ', '.join(sorted(TARGET_MODULES))
        raise SystemExit(f'unsupported MongoDB target {target!r}; expected one of: {supported}')
    return target


def main(argv: list[str] | None = None) -> int:
    args = sys.argv if argv is None else argv
    if not os.getenv('MONGOECO_REAL_MONGODB_URI'):
        print('MONGOECO_REAL_MONGODB_URI is not configured', file=sys.stderr)
        return 2

    target = _resolve_target(args)
    suite = unittest.defaultTestLoader.loadTestsFromName(TARGET_MODULES[target])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())

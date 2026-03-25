import os
import sys
import unittest


def main() -> int:
    if not os.getenv("MONGOECO_REAL_MONGODB_URI"):
        print("MONGOECO_REAL_MONGODB_URI is not configured", file=sys.stderr)
        return 2

    suite = unittest.defaultTestLoader.loadTestsFromName(
        "tests.differential.mongodb7_real_parity"
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())

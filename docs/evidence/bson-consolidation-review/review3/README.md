# Third review: parser, wire and SQL paths

The [plan](../../../bson-semantics-consolidation-plan.md) owns findings and future
acceptance. This directory adds characterization; it preserves both previous
receipts and all artifacts in the parent and `refinement` directories.

- `observations.json`: eight query forms over six direct and six wire local
  pairs, the same eight native 9.0.2 / FCV 9.0 standalone observations, SDK BSON
  decode results and nine executed SQLite paths. Expected failures are findings,
  not a passing acceptance suite. SQL includes selection and eligibility probes;
  `matcher_calls` measures the actual public query.
- `probe.py.txt`: the exact completed probe using a disposable native server on
  port 49953 and context-managed local proxy servers. To repeat, start a fresh
  owned standalone at `/tmp/mongoeco-bson-review3/db`, bound to 127.0.0.1 on an
  unused port 49953 with an independent log; execute the copied script from the
  stated runtime base and shut down the fixture. The native seed writes only
  to `review3` on that fixture. The SDK client runs in a worker thread while
  each proxy's asynchronous server loop remains active.
- `sources-check.json`: repeated downloads of all 14 recorded r8.0.0 URLs,
  expected/actual hashes, plus URL/hash of expression_parser.cpp. All 14 match;
  this verifies content at those versioned URLs, not a different release binary.
- `cleanup.json`: owned native port closed and proxy contexts exited.
- `receipt.json`: input, chain of evidence, runtime hash checks and doc checks.

This run adds synchronous PyMongo wire characterization but no native 7/8,
async SDK, replica set/watch, package/global tests or corrected runtime acceptance.
The 167 previous green tests and 45 TTL boundary cases are historical evidence;
neither was rerun or relabeled as new verification. Decoder/SQL diagnostics are
not new golden fixtures and do not replace the existing independent runner.

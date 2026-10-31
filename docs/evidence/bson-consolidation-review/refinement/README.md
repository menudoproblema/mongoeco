# Plan refinement characterization

This evidence adds to the immutable characterization in the parent directory.
It does not replace its receipts, captures, source hashes or regression log.
The owner of findings and acceptance is the
[refined plan](../../../bson-semantics-consolidation-plan.md).

`observations.json` records a new native **9.0.2 / FCV 9.0 standalone** on owned
port 49952, all six local engine/dialect pairs, actual matcher argument traces,
public distinct and eq/in results, and 45 deterministic local TTL checks.
`probe.py.txt` is the exact completed probe; `cleanup.json` verifies shutdown.
The `rank` in value descriptors is computed by mongoeco for characterization;
it is not the server's reported BSON rank. Module/class, subtype and byte
content are recorded explicitly, rather than comparing result counts alone.

To reproduce, start an owned, fresh standalone mongod in
`/tmp/mongoeco-bson-refinement/db` on unused port 49952, bound to 127.0.0.1 with
an independent log. Copy `probe.py.txt` to that directory as `probe.py` and run
with the repository venv against the runtime base in `receipt.json`. The native
seed writes only to database `refinement` on that fixture. Shut down the fixture
and verify the port is closed. Temporary SQLite files are isolated per case.

No native 7/8, replica-set/watch, asynchronous/wire or corrected implementation
acceptance is added by this refinement. No new test suite was run or changed.
The earlier 167 green tests remain historical regression checks, not coverage
of H48–H54. The new TTL cases do not prove behavior after mixing previously
rounded auxiliary entries with a future exact cutoff.

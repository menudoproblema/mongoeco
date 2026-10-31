# Characterization evidence, not correction acceptance

Runtime base and source hashes are recorded in `receipt.json`. The owner of
findings and proposed work is [the consolidation plan](../../bson-semantics-consolidation-plan.md).

- `native9-characterization.json`: actual native 9.0.2 / FCV 9.0 one-member
  replica set and local Memory/SQLite observations, including all three local
  dialects. Complex values use repr; this is not a canonical BSON golden.
- `probe.py.txt`, then `extra-probes.py.txt`: completed characterization scripts.
  They use a fixed temporary workspace and port, and write only to the owned
  disposable server and ephemeral local engines. They are not general runners.
- `sources.json`: pinned MongoDB r8.0.0 source URLs and content hashes. No server
  source code is vendored here.
- `tests.txt`: existing focused regression suite. Green is not proof that the
  new findings have been fixed.
- `cleanup.json`: owned port closed; existing external server unchanged.

To reproduce, first create an isolated mongod fixture at
`/tmp/mongoeco-bson-review/db` on **an unused port 49951**, bound to 127.0.0.1,
with replSet `bsonReview` and an independent log. The probe initializes that
replica set; it resets only database `bson_review` on that fixture. Do not run
the scripts against a server owned by another task. Copy the two `.py.txt`
files to `probe.py` and `extra_probes.py` in the temporary workspace, execute
them with the repository venv against the stated base, in that order, then
shutdown the fixture and verify the port is closed. macOS requires foreground
mongod for this installed version; `--fork` is unsupported.

The existing semantic corpus/capturer/replay remains the implementation
acceptance mechanism. New 7/8 native captures, new wire/async coverage and
distribution/global gates belong to the future implementation, not this review.

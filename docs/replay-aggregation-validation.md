# Replay diagnostics and aggregation dependencies

This increment covers A0–A3. Index conflict comparison and Search are a
separate increment.

## A0 — Reference

Product and test base: `69ebd79dd9d5d920e6a37882586577f51b53fe29`. Reuse
the accepted P07 captures for MongoDB 7.0.43, 8.0.32 and 9.0.2, each with its
matching FCV. No capture is changed by the replay work.

The timing workload uses Python 3.14.5, PyMongo 4.19.0, pytest 9.1.1, and
two pytest-xdist 3.8.0 workers. The target is the 54 direct sync
`bucket_granularity` replay nodes on dialect 8.0. The control is six
direct sync Memory nodes on 8.0 across `group_parity`,
`command_let` (cursors), UUID policy (distinct), and BSON mutation
(precedence). The raw node lists and logs are in the local evidence
directory `/tmp/mongoeco-a03`. The combined reference passed all 60
distinct nodes. The aggregation reference passed 119 tests and 1,048
subtests in the existing cursor and semantic context suites.

A2 uses one campaign, at most 240 seconds, with four
`A0,A1,B1,B2,A2` blocks. Each slot runs both fixed lots. The primary metric
is elapsed time per lot; CPU and maximum per-worker RSS are recorded as
secondary measurements. The cache has at most four entries per worker.

## A1 — Replay failures

The BSON comparison still decides equality. A failure reports the first
divergent observation path instead of rendering the complete normalized
tuple. The PyMongo 4.20 mutation exception overlay copies the outer result
and the affected observations; the original capture and its source hash
remain unchanged. The negative focus and all 24 mutation precedence replay
nodes passed with each of PyMongo 4.19.0 and 4.20.0. The capture identity
focus also passed.

## A2 — Reuse of replay captures

The isolation test passed. All 40 subprocesses in the one measurement
campaign passed the same fixed selections, with 20 paired slots and two
lots per slot. Timed subprocesses totalled 122.05 s within the 240 s limit.
[Raw samples](evidence/replay-aggregation/a2-samples.jsonl) include all
wall, CPU, decode and per-worker RSS observations.

For each lot, use the A0/A1 elapsed ratio for calibration and
sqrt(B1 * B2 / (A1 * A2)) for each paired elapsed ratio. One block
for each lot was outside the 5% A/A band. The three calibrated target
blocks all improved: median elapsed ratio 0.93195 and 54 to 2 decodes
per execution. The calibrated control median was 0.98789, with no
ratio above 1.05. Maximum observed per-worker RSS increase, comparing
peaks within each block, was 4.24 MiB across both lots. A2 is accepted.

## A3 — Load prepared aggregation dependencies

The cursor binds only the requests in its prepared pipeline. Stats
loaders take the resolved collection and scale directly. The unused raw
pipeline traversal and its test-only bypasses are removed.

PyMongo 4.20.0: 118 cursor/context tests and 1,050 subtests passed. The
64-bit BSON rules, request-bound session, namespace and scale are
unchanged. The new control observes no stats read for an empty request set
and two reads for three requests for the same collection at scales 4, 4
and 8. The existing source and error cleanup checks passed.

The native replay focus passed 96 tests covering four lookup cases in
three dialects, with both engines and sync, async and wire routes. Eight
selected standalone differential cases passed against each of MongoDB
7.0.43, 8.0.32 and 9.0.2: facet shapes, lookup shapes, union shapes,
collection references in groups, aggregation collation, pipeline
validation, foreign stats, and the followup lookup paths. All 24 used
PyMongo 4.20.0, and none skipped. Fixtures were removed on exit.

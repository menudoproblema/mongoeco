# Index conflicts and Search evaluation

The base is A0–A3's accepted commit `9b7ac8834ad3a89785f2440d343fadfb08c597eb`.
The prior replay measurement and acceptance remain separate.

## B1 — Shared unique-index conflict comparison

Memory and SQLite use the same comparator next to the common multikey
extraction. The candidate lookup, index population and transactions stay
with each engine. The comparator preserves the existing first-conflict
selection and effective collation. SQLite's index-build callback also uses
the shared comparator.

PyMongo 4.20.0 passed 56 targeted engine tests, 992 index and find-and-modify
error tests, and 96 replays on both engines and all four surfaces. Four
native cases passed against each of 7.0.43, 8.0.32 and 9.0.2 with its matching
FCV. The selection covers missing/null inserts, compound multikey arrays,
partial indexes with collation (insert and update), and existing-data
build conflicts. The error probes preserve code, key pattern, key value
and the persisted effect for the witnessed single conflicts.

## B2 — Reuse the distance in Memory near ranking

After downstream filtering, the Memory near branch uses the same distance
for qualification and ordering. The focused engine test observes one
evaluation per remaining document, including a missing path. The public
Search test compares results and metadata to the exhaustive reference,
with missing and out-of-pivot values, a filtered row, ties and a limit.
The Search focus passed 15 tests and four subtests with PyMongo 4.20.0.

Measurement base: `a5e20bdf795dc5b3728c372ed5d69b0709b52c6d`. The timed
candidate changed only the Memory near branch. Python 3.14.5 and PyMongo
4.19.0 ran four A0/A1/B1/B2/A2 blocks. Each slot independently initialized
three 2,048-document indexes, warmed two queries per case and measured
12 queries per case with GC active. Numeric arrays contain eight values
where present. The cases are broad (pivot 2000), selective (pivot 2), and
ties (pivot 0.5). Each result retained its baseline ordered BSON digest.
CPU per query is the primary measure; elapsed wall time is also retained.

[All samples](evidence/index-search/b2-samples.jsonl) totalled 20.13 s
including initialization and warmup within the 240 s budget. Every A/A
control was within 5%. The median paired CPU ratios were 0.50994 (broad),
0.95718 (selective), and 0.86464 (ties). All four broad blocks improved
and neither control exceeded the regression band. This measures the
Memory Search entrypoint for these workloads.

## B3 — Keep compound matching and scoring separate

The bounded probe ran with GC enabled on B2 and the Memory Search entrypoint.
It used 2,048 documents, eight warmed queries per variant, and compared
ordered BSON result hashes. Its five variants ran 40 observed queries
under a 60 s process limit.
[Raw observations](evidence/index-search/b3-probe.json).

- Ranking should clauses during matching increased range checks from 20,480
  to 135,168 on the minimum-should-match rejection case. The result digest
  was unchanged. Matching can stop checking array values once one qualifies;
  ranking counts matching values, including work that would be discarded.
- A diagnostic reuse of distances within one document saved 840 of 9,032
  computations in the compound with near. The single instrumented batch
  recorded about 6% lower CPU. The tie-heavy text case has no near clause
  and cannot benefit from that reuse.
- The tie-heavy case confirmed repeated dispatch (16,384 matches and
  16,384 ranks). The count alone does not establish shared work across
  short-circuit matching and full scoring.

Disposition: defer B3. A general eager evaluator adds work on rejections,
and the probe does not support adding per-document state for a limited
near benefit. No compound production path changes.

## Acceptance in main

B1 and B2 were accepted on `8033a12ff35406953013837f7e25c1fd66a40dad`,
with B3 deferred. The [candidate CI](https://github.com/menudoproblema/mongoeco/actions/runs/44466173652)
passed 19 jobs and covered 48,387 of 48,874 statements (99.00356%).
All 12 selected native cases passed again on the verified wheel without skips.
Its SHA-256 was
`37a3394b2504380d04d2d4b79f295e29d42dee3c27d7481401a5ef69fc71e9a0`.

The [PR #3](https://github.com/menudoproblema/mongoeco/pull/3) was integrated
into `main` on November 19, 2026, followed by the test-only commit `5c41a410`.
That commit adds an unlimited eligibility check alongside the limited
tiebreak case and asserts absent highlights in both. Its ten focused tests
passed against the accepted wheel.

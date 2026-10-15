# Positional update validation

The positional update resolver must distinguish an existing array with no matching
items from an absent array or an existing value of another type. Reaching `$[]`
or `$[identifier]` with an invalid container raises code `2` before the candidate
document is committed. An invalid nested array matters only when its enclosing
array branch is selected. Empty arrays and branches excluded by a filter remain
valid.

The fix also covers the related paths discovered during diagnosis:

- Overlapping concrete targets, including ancestor/descendant targets across
  operators, fail with code `40` before commit. Disjoint matches remain valid.
  Matches are resolved against the original document and never shared across
  documents.
- Numeric path components address a list index or a literal object key according
  to the current container. A numeric object key must not disappear during
  positional expansion.
- `$setOnInsert` validates positional traversal even when its assignments are
  inactive because a document already exists.
- SQLite declines SQL translation for every positional kind (`$`, `$[]`, `$[id]`)
  and uses its existing Python execution path. SQL JSON paths must never receive
  unresolved positional placeholders.
- Traversing an existing null or scalar array element to create a child fails with
  code `28`. Extending a list to create a previously absent slot remains valid.
- Upsert seed containers become detached mutable dictionaries/lists. The filter
  remains immutable, and BSON scalar wrappers retain their types.

Atomicity applies to the failing document. Earlier successful documents in
`update_many` remain changed. Ordered bulk stops at the failed write; unordered
bulk continues independent writes. Bulk exposes code `65` with the original
per-write code in `writeErrors`.

## Regression tests

`tests/fixtures/positional_update_mongodb8.json` contains 114 cases and their native
MongoDB 8.0.32 outcomes. Observations compare success/failure, the main error code, bulk item indexes
and codes, and all final documents. They do not require identical error messages
or an identical exception subclass between the two libraries.

The cross-engine contract tests execute every case through both synchronous and
asynchronous APIs and verify that the case inputs remain unchanged. The live test
checks the captured outcomes against the server again and compares both engines
to those fresh results. A missing URI skips only the live test; connection errors
are failures. The golden was captured on MongoDB 8.0. The live test selects the expected server
from `MONGOECO_REAL_MONGODB_TARGET` (default 8.0), rejects a mismatched server, and
runs in all three MongoDB differential workflow lanes (7.0, 8.0, 9.0).

```bash
python -m pytest tests/unit/core/test_positional_update_validation.py \
  tests/integration/api/test_positional_update_contract.py -q

MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
  python -m pytest tests/differential/test_positional_update_parity.py -q

MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
  python scripts/run_mongodb_real_differential.py 8.0
```

## Initial diagnostic evidence

An isolated local MongoDB **8.0.32**, stable FCV **8.0**, and PyMongo **4.18.2** were
used on 2026-10-15. The instance and its test data were discarded after validation.

- [Run metadata and source/fixture hashes](evidence/positional-updates/mongodb8.json)
- [Initial 95 cases / 190 engine comparisons](evidence/positional-updates/mongodb8-run-1.log)
- [Existing 29-case differential report](evidence/positional-updates/mongodb8-existing-suite.json)
- [Existing differential execution log](evidence/positional-updates/mongodb8-run-2.log)
- [Global validation and scope limits](evidence/positional-updates/validation.json)

These initial records describe the earlier 95-case workset. The expanded 114-case
release candidate and its final hashes are recorded under
[evidence/mongoeco-4.9.1](evidence/mongoeco-4.9.1/). Neither corpus claims exhaustive
MongoDB compatibility.

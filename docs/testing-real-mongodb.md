# Real MongoDB Differential Testing

MongoEco keeps general MongoDB semantics under a recurrent differential suite
against Community Server 7.0 and 8.0. Atlas Search is not part of this suite;
its local subset is governed by `search-v1` normative tests.

The `MongoDB Differential` workflow runs weekly, through `workflow_dispatch`,
or when a pull request receives the `mongodb-differential` label. Every matrix
job starts its own versioned MongoDB service, so CI never skips because an
external URI is absent.

Each run records JSON, JUnit, the complete log and seed. Cases expose a
serializable manifest containing their selector, minimum server version and
seed documents. A failed selector can be replayed locally:

```bash
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
python scripts/run_mongodb_real_differential.py \
  8.0 'failed_case_name' \
  --seed 42 \
  --json-report /tmp/mongoeco-differential.json
```

The runner uses a unique database per engine/case and drops it in `finally`.
The workflow has a bounded timeout and preserves evidence even on failure.
It also records the exact server `buildInfo`. A case filter that resolves to
zero tests exits with a usage error instead of producing a false green.

The aggregation matrix covers `$project`, `$set`, `$addFields`, `$unset`,
`$replaceRoot`, `$replaceWith`, `$unwind`, `$group`, `$facet`, `$lookup`,
`$unionWith` and `$merge`, including nested arrays, missing fields, fan-out and
writeback. `REAL_CAPTURE_PENDING_CASES` identifies executable cases not yet
present in the checked-in real-server replay fixture. Local engine parity does
not remove a case from that set; only a successful real capture may do so.

Tagged releases call this workflow as a required reusable job for both MongoDB
7.0 and 8.0. Publication cannot start unless the differential matrix, the
artifact build and the installed-wheel test jobs have all succeeded.

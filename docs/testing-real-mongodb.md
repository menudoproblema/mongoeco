# Real MongoDB Differential Testing

MongoEco keeps general MongoDB semantics under a recurrent differential suite
against Community Server 7.0, 8.0 and 9.0. Atlas Search is not part of this suite;
its local subset is governed by `search-v1` normative tests.

The `MongoDB Differential` workflow runs weekly, through `workflow_dispatch`,
or when a pull request receives the `mongodb-differential` label. Every matrix
job retains its versioned standalone service and starts an owned replica set
for cases that require it. Both use the same digest-pinned image matrix. CI
verifies that the two partitions are disjoint and cover the requested cases;
a missing external URI never turns a required physical run into a skip.

Each run records JSON, JUnit, the complete log and seed. Cases expose a
serializable manifest containing their selector, minimum server version and
seed documents. A failed selector can be replayed locally:

```bash
MONGOECO_REAL_MONGODB_URI=mongodb://localhost:27017 \
python scripts/run_mongodb_real_differential.py \
  8.0 'failed_case_name' --partition standalone \
  --seed 42 \
  --json-report /tmp/mongoeco-differential.json
```

The runner uses a unique database per engine/case and drops it in `finally`.
The workflow has a bounded timeout and preserves evidence even on failure.
It also records the exact server `buildInfo`. A case filter that resolves to
zero tests exits with a usage error instead of producing a false green.

`--partition all` is the default. It requires a writable replica-set primary
when any selected case needs one. `--partition standalone` selects cases that
do not require a replica set; `--partition replica-set` selects those that do.
Topology requirements live in `SemanticCorpus`/`RealParityCase`, rather than in
name prefixes: TTL and update-event consumers also require a replica set.
Listing, suite construction and JSON reporting share the same selection.

```bash
python scripts/run_mongodb_real_differential.py 8.0 'semantic_watch_*' \
  --partition replica-set --list-cases
python scripts/run_mongodb_real_differential.py 8.0 'semantic_watch_*' \
  --partition replica-set --list-corpora
```

Listing needs no URI. An empty listing is not physical execution; executing an
empty selection fails. In CI, an entirely empty filter fails before either run.
A partition intentionally empty under that filter is recorded as unexecuted.

For a local replica-set run, set `MONGOECO_TEST_IMAGE` to the digest-pinned image
for the target in [the workflow matrix](../.github/workflows/mongodb-differential.yml).
Preload it, then use the lifecycle wrapper with the existing runner:

```bash
docker pull "$MONGOECO_TEST_IMAGE"
python scripts/with_mongodb_replica_set.py \
  --image "$MONGOECO_TEST_IMAGE" --target 8.0 \
  --artifacts /tmp/mongoeco-replica-set \
  -- python scripts/run_mongodb_real_differential.py 8.0 'semantic_watch_*' \
    --partition replica-set --json-report /tmp/mongoeco-replica-set/report.json
```

The wrapper verifies primary, server version and stable FCV; publishes only an
ephemeral loopback port; bounds startup, Docker requests and payload duration;
and uses a 128 MiB oplog and a one-second TTL monitor for these scenarios.
Those fixture settings do not promise product retention or TTL timing. Ownership
uses a UUID name and label. Cleanup removes the container, its anonymous volumes
and the payload process group, including on failure or cancellation. A mutating
Docker request finishes before queued cancellation is delivered. An unresolved
create timeout remains a failed/indeterminate cleanup, never a green result.
No global Docker prune or lifecycle changes to borrowed services are used.

The replica-set step executes the selected live cases, verifies each reached
corpus with the existing native capturer and immutable references, then replays
only that matrix cell's dialect. Release jobs preserve the installed-wheel
import guard and clear pytest's checkout `pythonpath`. JSON, JUnit, fixture
metadata and available server logs are uploaded even when the step fails. A local payload
verification does not claim that a remote GitHub Actions run has occurred.

The aggregation matrix covers `$project`, `$set`, `$addFields`, `$unset`,
`$replaceRoot`, `$replaceWith`, `$unwind`, `$group`, `$facet`, `$lookup`,
`$unionWith` and `$merge`, including nested arrays, missing fields, fan-out and
writeback. `REAL_CAPTURE_PENDING_CASES` identifies executable cases not yet
present in the checked-in real-server replay fixture. Local engine parity does
not remove a case from that set; only a successful real capture may do so.

Tagged releases call this workflow as a required reusable job for MongoDB
7.0, 8.0 and 9.0. Publication cannot start unless the differential matrix, the
artifact build, isolated PyMongo profiles and the installed-wheel test jobs
have all succeeded.

Versioned native corpora also cover server deltas, review improvements and
4.9 semantic guarantees. Every capture stores server/FCV/SDK and corpus identity;
its coverage matrix identifies the effective consumer and comparison scope.
Stored observations without a consumer remain characterization, not parity.

Native capture comparison ignores ordering only in the numeric BSON type
enumeration of `$densify.range.step` TypeMismatch errors. It retains accepted
types and their multiplicity, the received type, field, message format and all
other error fields. Successful data and other diagnostics keep their existing
comparison rules; immutable native fixtures retain the original messages.

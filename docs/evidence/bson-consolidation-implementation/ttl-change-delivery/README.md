# TTL change delivery — D02 functional closure

The native reference is the existing capture/registry/replay mechanism, extended
with one TTL-publication action. MongoDB 7.0.43, 8.0.32 and 9.0.2 use owned,
single-member replica sets with matching stable FCV and ttlMonitorSleepSecs=1.
The five-second polling bound belongs to this controlled fixture; it is not a
production TTL monitor deadline. Native jobs including this case must configure
that fixture (including future D04/D05 jobs). Arbitrary standalone or default
60-second-monitor jobs cannot establish this guarantee. No habitual server or
its parameters were modified. topology.json records actual hello/primary/FCV.

Tracked semantic_ttl_publication_{7,8,9}_0.json were recaptured and verified
with --check. Case results, manifests, corpus and stable runtime metadata match;
replica-set buildInfo operationTime/$clusterTime advance between calls and are
not asserted byte-identical. The original checked files are retained compressed.
They all observe deletes of IDs 1/2, no extra delete and ID3
remaining. Tokens, monitor latency and incidental namespace names are excluded;
this is not a distributed TTL scheduling or transaction-isolation claim.

The previous source 0979fd8 is archived and preloaded before current pytest
loads tests. The final supported-surface corpus fails 12 direct sync/async
checks and passes three metadata checks. The before log records the archived
package path. Initial missing fixtures, wrong create_index option spelling and
attempted wire watch execution were diagnostics, not semantic before gates.

The proxy does not implement $changeStream. SemanticCorpus now declares the
surfaces applicable to a corpus: TTL declares direct sync/async; all previous
corpora retain all four surfaces. No skipped wire cases are counted as native
execution. Separate tests assert the existing unsupported-stage response on
both proxy engines and dialects 7/8/9. Runtime-reference documents this limit.
This does not add wire change streams or close D04's preparation/context work.

Root causes: TTL bypassed change recording, and read/DDL boundaries never
flushed outbox effects. Memory now queues deletion payloads in its existing
atomic publication scope. SQLite appends them in the existing write scope with
fresh maintenance identities, independent of caller publication policy. Request
and maintenance identities are mutually exclusive; replay/dedup/checkpoint
rules still have one owner. No new operation or expression context is invented.

CollectionRuntimeCoordinator owns one delivery scope for read/write/DDL outcomes,
including errors. It registers the existing transaction commit hook and defers
transactional delivery. A first-read notification in the existing ReadSnapshot
handles lazy initialization, empty/error/cancel paths and close. Ordinary and
foreign aggregation reads share one snapshot opening owner. Six additional
lookup/union tests failed before that wiring and pass afterward.

Local tests cover Memory, in-memory SQLite and file SQLite: ordinary reads,
count, distinct, aggregate, index creation, lookup/union, failed second event
recording with document/outbox rollback, retry, committed purge followed by a
read evaluation error, commit/abort, and a waiting watcher while a populated
read cursor remains open. Snapshot tests preserve cleanup and exactly-one
notification on value, empty, error and cancellation. These preserve the local
transaction contract; they do not compare TTL background timing inside native
transactions.

The first read benchmark found unnecessary dispatch lease/heartbeat work on
idle SQLite reads (~34% median overhead for this small workload). The owner
now checks the persisted checkpoint and high-water mark before acquiring the
lease. It does not cache them. A test appends from another SQLite connection
and rejects/retries delivery, so an idle optimization cannot discard new work.
The sequential final benchmark has identical checksums: about +3% Memory and
+2% SQLite for 300 mixed direct async requests over 100 rows, five samples.
This is not D05 workload, package, SDK/Python matrix or whole-suite acceptance.

The receipt distinguishes final gates from intermediate candidates. Historical
receipts/observers/goldens remain unchanged. The final integrated run uses
frozen source and corpus; its result is recorded separately. Cleanup is owned
and verified separately. Performance/fixture integration and the other plan
blocks remain explicit in the implementation ledger.

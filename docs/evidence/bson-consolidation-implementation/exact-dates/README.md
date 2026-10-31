# Exact dates and typed consumers — D02 implementation cut

The existing semantic registry/capturer/replay owns all six actions. Tracked
`semantic_datetime_{7,8,9}_0.json` contain buildInfo, FCV and corpus manifests.
They were captured on owned standalones, then verified with --check, without
rewriting the goldens. Native ports: 49961/49962/49963. No habitual server used.

The source before this cut is archived commit 93b9b34, preloaded before pytest
loads current tests. The exact final six actions fail 144 public replay checks
and pass 3 metadata checks. After correction the focal replay passes 147 checks,
with Memory/SQLite, dialects 7/8/9 and sync/async/wire-sync/wire-async. The runner,
protected observers and historical goldens were not modified.

Coordinates use integer UTC/BSON milliseconds; inverse conversion truncates
fractional input toward zero. Date arithmetic rounds double ties away from zero
and Decimal128 ties to even, as captured on all three native versions. Small
longs must stay long for nested toDate, dateAdd, dateTrunc, dateFromParts and
other consumers. Array/index/range/round/percentile and window/fill/densify
witnesses guard the resulting shared paths. Fill uses the existing BSON sort
owner. Percentile extrema exposed a preexisting return-type error: native
returns double; scalar and group now use the same corrected function. The
supplementary percentile-group-bson.json compares SDK-encoded physical BSON on
three native and six direct local engine/dialect pairs.

Windows additionally cover single-point integral=0, exact derivative operation
order and native error codes for null/string input. Old unit expectations of
plain Python integer outputs, rejected millisecond dateTrunc, integral=None at
one point and silently skipped nulls were corrected against native witnesses.
This is not a claim of complete parity for the entire window subsystem.

SQLite TTL derived coordinates move from auxiliary version 1 to 2, rebuilding
from stored documents on open. Version 2 is retained; future versions fail
without document changes. Tests cover exact expiry -1/0/+1 ms on both engines,
old auxiliaries, index before/after data, reopening, unchanged storage keys and
rollback/retry after injected rebuild failure. Existing TTL publication-scope
regressions also run; they test transactional storage, not watch delivery.

IMPORTANT: ttl-publication-characterization.json is an OPEN finding: the
expired document disappears but no delete event reaches a local watch in either
engine. This predates the arithmetic change and does not accept publication.
D02 remains open for this obligation, coordinated with the native replica-set
fixture of D04 and D01b effects. No native monitor deadline or new distributed
stream guarantee is inferred. Initial attempts using unsupported context-manager
or awaited-close forms were diagnostic errors, not evidence; the stored result
uses AsyncMongoClient and synchronous cursor.close().

Logs with earlier corpus shapes/import errors or fixture changes during a run
are diagnostics only. The final integrated run must use frozen source/corpus;
its result is recorded separately in receipt.json. Package/global matrices,
coverage and performance closure remain D05 obligations.

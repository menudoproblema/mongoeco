# D06a/b — Implementation checkpoint, not acceptance

Base `57b5faf`. The candidate workset is frozen by SHA-256 and archived; the
implementation remains open. Required wire failures are retained as failing
checks, never skipped or accepted. No historical golden or protected observer
was changed. The ten new native action references are still candidate inputs.

Native MongoDB 7.0.43/8.0.32/9.0.2, stable matching FCV, owned one-member replica
sets and PyMongo 4.19.0 capture ten actions and check them freshly per version.
The five historical DBRef actions also pass fresh --check. Native metadata,
payload/check output and cleanup are archived; the final three container/volume
sets are independently absent. Native capture is sync; replay uses direct/wire
sync/async surfaces, not new native async capture.

The isolated baseline has all 283 modules verified against the preceding
accepted product manifest and proves its actual import path. With the final
corpus it fails 200 nodes and passes 43. The frozen candidate passes 235 and
fails eight wire aggregate-response nodes. Every remaining failure was already
red in baseline: 192 nodes turn green, not 192 independent bugs. The earlier
168-failure/27-control run used the initial eight-action corpus; its source and
captures are retained separately and are not evidence for the final ten actions.

Core/API checkpoint: 3,035 pass, one skip, 1,017 subtests. The prior full replay
was diagnostic on an earlier candidate/nine-action corpus: 6,815 pass and 20
fail. It is not a green global gate and cannot accept the final candidate.
The isolated decoded-distinct mutation fails all 24 nodes of that action.

The new response boundary witness explicitly sorts source rows. Earlier SQLite
results used unspecified natural order and did not establish an engine sorting
bug. The async bridge now returns/iterates actual cursors rather than completing
aggregation inside its constructor. This prevents false phase assurance.

Before accepting this cut, wire must retain live aggregate cursors through
getMore with existing command validation/context/failpoints/profiling and session/
principal/namespace guards. Server response encoding must preserve BSON shape
before SDK DBRef decoding; direct public responses decode afterward. Packet byte
limits, materializing suffixes, cancellation and conversion-before-callback
behavior need their own native witnesses. Typed String consumers and the remaining
DBRef/context/key consumers keep D06 owners. No D06 or D05 closure, wheel, SDK-free,
performance, typing/environment matrix or complete final replay is claimed.

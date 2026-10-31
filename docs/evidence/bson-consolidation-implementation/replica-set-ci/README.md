# D04 — Replica-set fixture in the existing differential job

The fixture owns lifecycle only. Case selection, native capture and replay keep
using their existing authorities. The plan defines requirements; this evidence
records a bounded functional CI-fixture cut, not the complete D05 program.

`native-matrix.json` binds each pinned image, actual build/FCV/primary, native
report, fresh corpus capture and immutable reference. Per-version archives retain
JSON/JUnit/mongod logs; payload logs retain --check and per-dialect replay output.
The exact workflow shell payload and its selection step were executed locally.
No remote GitHub Actions run or release publication is claimed.

Standalone fixtures reproduce the existing service's topology and native CLI;
replica-set fixtures use the new owned lifecycle wrapper. Both partitions run
against the unchanged installed wheel. Their union and report manifests are
checked against the same canonical selection; no skipped/empty physical execution
is counted as acceptance. The replay runs only the matrix cell's dialect.

`lifecycle-results.json` and the separate spawn-cancellation archive test the
final helper on owned resources. The controller independently checks Docker
container/volume absence and absence of live payload children after cleanup.
`resource-audit.json` repeats those checks after all accepted runs.

`lifecycle-reuse.json` precisely identifies cancellation-only changes after the
7/8 success wrappers loaded their module; the reconstructed earlier helper is
verified against review13's recorded SHA-256. Success payload/topology/product
inputs did not change. The 9 wrapper and final negative checks use final source.
Both mutation checks and final unit tests cover the changed failure paths.
The standalone harness predates these lifecycle fixes and recreates the existing
standalone service, without calling the new RS startup. Native selection/CLI
and product inputs remain identical; standalone results do not attest final
cancellation behavior. Final negative scenarios provide that guarantee.

Feedback archives are unsuccessful diagnostic attempts, never acceptance. An
initial three-version parallel attempt lost connectivity without evidence of
OOM; later acceptance is sequential. Early cleanup bugs included case-sensitive
Docker absence messages, anonymous volume removal and a startup cancellation
race. Prototype volume ownership and explicit cleanup are recorded separately;
no global prune, borrowed-service restart or unknown-volume removal was used.

`runtime-reuse.json` verifies all 283 product modules against the preceding
accepted runtime manifest. Protected observers/fixtures remain unchanged. Product
semantics, persistence formats, SPI and hot paths do not change in this cut;
their previous evidence keeps its original scope. D06, H53, conversion, optional
D07 disposition, B07 and global D05 acceptance remain pending in their owners.

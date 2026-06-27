# Release 3.5.0

Status: prepared for publication from the committed `3.5.0` release code.

## Headline

`mongoeco 3.5.0` hardens write identity and transactional consistency while
adding an official PyMongo 4.17 compatibility profile.

## Main user-facing changes

### MongoDB-compatible `_id` write semantics

* Classic updates, upserts and pipeline updates now follow MongoDB identity
  rules more closely:
  * `_id` may be touched only when the final value is preserved;
  * valid `_id` creation during upsert remains allowed;
  * root-array `_id` values are rejected on inserts, bulk inserts and upserts.
* Replacement updates now report MongoDB-style immutable `_id` failures when
  they attempt to change identity.
* Bulk writes and admin command write errors preserve index and MongoDB error
  code metadata for `_id` violations.

### Storage identity hardening

* Selected documents are validated against their storage key before mutation,
  deletion, TTL purging, `$merge` target resolution and index TTL backfill.
* Legacy corrupt documents no longer silently mutate, retarget or disappear
  through unrelated write/read maintenance paths.
* The validation is centralized in `mongoeco.core.identity`, reducing drift
  between Memory, SQLite, API helpers and aggregation write stages.

### Atomicity and rollback consistency

* Classic updates are atomic when runtime errors occur during multi-instruction
  update application.
* SQLite write paths now roll back on runtime errors beyond `IntegrityError`,
  including index/search rebuild failures.
* SQLite session transactions use per-operation savepoints so a failing write
  does not leave partial changes that can later be committed by the outer
  transaction.
* Memory and SQLite rollback helpers are centralized to reduce repeated
  snapshot/begin/commit/rollback logic.

### Session ownership consistency

* Session ownership validation is centralized for tracked operations, lazy
  cursors and command execution.
* Profiler, admin commands, aggregation runtime stages, read fallbacks and wire
  `endSessions` now preserve session ownership and failed-abort behavior more
  consistently.

### PyMongo 4.17 profile

* New official `pymongo_profile="4.17"` support.
* `auto-installed` resolves installed PyMongo 4.17 as an exact known profile and
  future 4.x minors as compatible fallbacks to the latest known profile.
* Compat snapshots and the PyMongo surface matrix include PyMongo 4.17.0.
* The PyMongo matrix script can now regenerate the stable summary fixture
  directly with `--summary-output`.

## Validation summary

* `PYTHONPATH=src python -m pytest tests/unit -q`
* `PYTHONPATH=src python -m pytest tests/integration/api/test_async_api.py::AsyncApiIntegrationTests::test_client_propagates_dialect_and_profile_to_database_and_collection -q`
* `PYTHONPATH=src python scripts/run_pymongo_profile_matrix.py --versions 4.9.2 4.11.3 4.13.2 4.17.0 --summary-output tests/fixtures/pymongo_profile_matrix.json`
* `python -m build --sdist --wheel --outdir .tmp/release-3.5.0-dist`
* `python -m twine check .tmp/release-3.5.0-dist/*`
* `python scripts/smoke_installed_wheel.py --wheel .tmp/release-3.5.0-dist/mongoeco-3.5.0-py3-none-any.whl`
* `python scripts/smoke_installed_wheel.py --sdist .tmp/release-3.5.0-dist/mongoeco-3.5.0.tar.gz`
* `git diff --check`

## Limits that remain explicit

* The release prevents new identity/storage corruption; it does not migrate
  already corrupt persisted data.
* PyMongo 4.17 support is modeled for mongoeco's current PyMongo surface matrix;
  unrelated PyMongo APIs outside that matrix remain outside the official local
  compatibility claim.

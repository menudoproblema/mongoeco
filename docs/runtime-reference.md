# Advanced runtime reference

Start with the [testing guide](use-cases/test-runtime.md),
[embedded persistence guide](use-cases/embedded-app.md), or
[search guide](use-cases/local-search-and-retrieval.md). This reference covers
configuration and tooling after choosing a workflow.

## Public APIs and compatibility tooling

MongoEco is a PEP 561 typed package. Wheels and source distributions include
`py.typed`; the supported typing boundary covers the documented client,
engine SPI v2, Search and conformance surfaces. CI checks those contracts with
strict positive and negative consumer fixtures against the installed wheel.

Mongoeco 4.8+ requires `cxp[exchange]>=5.0.0,<6` and exposes its
`database/mongodb` contract through owner-authored exchange documents. Its
`org.mongoeco:mongodb@1.2.0` catalog uses CXP catalog spec_version 2;
snapshots and requirements pin that exact catalog while retaining their v1
document formats.
Reference:

* [docs/cxp-exchange.md](cxp-exchange.md)

The public compatibility export and the top-level `cxp` blocks surfaced by
`find(...).explain()` / `aggregate(...).explain()` are now projected from that
canonical CXP capability model.

The public CXP story now includes canonical first-level operation bindings as
well:

* `read` -> `find`, `find_one`, `count_documents`, `estimated_document_count`,
  `distinct`
* `write` -> `insert_one`, `insert_many`, `update_one`, `update_many`,
  `replace_one`, `delete_one`, `delete_many`, `bulk_write`
* `aggregation` -> `aggregate`
* `change_streams` -> `watch`
* `transactions` -> `start_session`, `with_transaction`

For `search` and `vector_search`, `mongoeco` still binds the public operation
`aggregate` and uses metadata to describe the supported stage-level subset.
That metadata also carries operation-level hints such as result shape, scope,
session support and core accepted inputs for `read`, `write` and
`aggregation`.

For reusable profile gates, the practical split is now:

* `mongodb-text-search` when you need textual `$search` without requiring
  `vector_search`;
* `mongodb-search` when you need both textual and vector search;
* `mongodb-platform` when you need the broader platform surface, including
  canonical metadata for collation, persistence and topology discovery.

The exchange documents publish these named profiles with exact catalog pins.
The `cxp` block in `explain()` carries their evaluated verdicts for the declared
library snapshot; it does not infer a minimal profile from a query path.
Consumers can load each profile from `mongoeco.cxp.exchange` and evaluate it
with a context that explicitly states accepted provenance. The compat report
embeds the same documents through `export_exchange_catalog()`.
Operational telemetry shape requirements live in Mongoeco's driver resources
and are validated separately from component compatibility.

The root `mongoeco` package is intentionally narrower than the full runtime
internals. It centers on:

* clients and session entry points
* common BSON-facing types and operation models
* URI/configuration helpers needed by the main runtime surface

Compatibility tooling and the canonical MongoDB CXP contract surface live in
their own packages:

* `mongoeco.compat`
* `mongoeco.cxp.exchange`
* `mongoeco.engines` for the public storage-engine SPI v2
* `mongoeco.conformance` for versioned engine contract validation

Lower-level driver/runtime details remain available from subpackages such as
`mongoeco.driver`.

`mongoeco.compat` follows the same idea: the top-level package keeps the
catalog exports, profile resolution helpers and option-support surface, while
more tactical constants and raw catalog data stay in explicit submodules.
It also exposes `deprecation_entries()` and `public_api_manifest()` so release
tooling can inspect planned removals and the semantic 4.x surface without
scraping documentation.
The catalog lifecycle is documented in
[docs/deprecations.md](deprecations.md).

Import guidance by layer:

* use `mongoeco` for clients, sessions, BSON-facing types and URI/config helpers
* use `mongoeco.compat` for dialect/profile resolution and compat/tooling exports
* use `mongoeco.cxp.exchange` for pinned MongoDB compatibility documents
* use `mongoeco.engines` and `mongoeco.conformance` for third-party storage
  engines and their public contract tests
* use `mongoeco.driver` and `mongoeco.wire` only when you intentionally need
  lower-level runtime surfaces

Custom storage engines should implement the versioned SPI v2 contract. The
public capability, outcome, operation-context and snapshot types are exported
from `mongoeco.engines`; see the
[SPI v2 contract](architecture/engine-spi-v2.md) and the
[MongoEco 4.7 migration guide](migrating-to-4.7.md). Starting with 4.7,
SPI v2 is the only stable engine SPI and engine capabilities must be declared
explicitly.

## Public Surface Stability (4.8+)

Mongoeco 4.8.0 introduced the exchange-only CXP surface. Its import roots are
explicit:

* stable import roots are `mongoeco`, `mongoeco.compat`, `mongoeco.cxp.exchange`,
  `mongoeco.engines` and `mongoeco.conformance`
* each root surface is curated through its `__all__`
* lower-level runtime symbols stay in explicit subpackages

The root package no longer exposes transport aliases. Import transport classes
from `mongoeco.driver` directly.

`mongoeco` does not ship a live CXP provider wrapper for its clients. Instead,
it exposes the canonical catalog and projects the active capability path
through `compat` and `explain()`. External systems can wrap `mongoeco` if they
want to negotiate profiles or instantiate resources through CXP.

`mongoeco.cxp.exchange` is the canonical compatibility source. `mongoeco.compat`
exports those exact documents in its reporting view.
See the [CXP removal migration guide](cxp-c4-migration.md) for import and
reporting changes.

The direct path for CXP-facing tooling is:

```python
from mongoeco import MongoClient
from mongoeco.compat import export_exchange_catalog
from mongoeco.engines.memory import MemoryEngine


print(export_exchange_catalog()["catalog_sha256"])

with MongoClient(MemoryEngine()) as client:
    collection = client.get_database("demo").get_collection("items")
    collection.insert_one({"_id": 1, "score": 8})
    explain = collection.aggregate([{"$match": {"score": {"$gte": 8}}}]).explain()
    print(explain["cxp"])
```

The base package now includes `pyuca` as a runtime dependency so Unicode
collations keep a deterministic UCA-backed behavior even when `PyICU` is not
installed.

Advanced ICU collation backend (optional):

```bash
python -m pip install PyICU
```

Collation backend policy:

* `PyICU` stays optional by contract: it is never required for the supported
  baseline subset
* `PyICU` if available: preferred backend, including advanced collation knobs
  such as `backwards`, `alternate`, `maxVariable` and `normalization`
* `pyuca` fallback: Unicode collation for the supported basic subset
  (`locale=en`, `strength`, `caseLevel`, `numericOrdering`)
* if advanced knobs are requested without `PyICU`, `mongoeco` raises an error
  instead of silently ignoring them

Optional fast JSON backend:

```bash
python -m pip install "mongoeco[json-fast]"
```

`mongoeco` uses the standard library `json` module by default, even if
`orjson` is installed. You can choose the backend at process start with
`MONGOECO_JSON_BACKEND`:

* `stdlib`: always use the standard library JSON backend
* `orjson`: require `orjson` and use it
* `auto`: use `orjson` when available, otherwise fall back to `stdlib`

Example:

```bash
MONGOECO_JSON_BACKEND=orjson python your_app.py
```

Runtime behavior highlights:

* the `simple` collation keeps using the BSON/Python baseline comparator and
  rejects Unicode tailoring knobs such as `caseLevel` or `numericOrdering`
* the currently supported locale surface is `simple` and `en`
* the currently supported strengths are `1`, `2` and `3`
* `numericOrdering` and `caseLevel` are supported for `locale=en`
* `PyICU` and `pyuca` are intentionally close, but may still differ on
  advanced tailoring details outside the currently supported locale surface
* local change streams retain a bounded in-memory history; the retention size
  can be tuned with `change_stream_history_size` on async/sync clients and on
  direct async database/collection constructors
* local change streams can also persist that retained history to a journal file
  with `change_stream_journal_path`, allowing `resume_after` / `start_after`
  to survive client recreation inside the same local environment
* the journal can also be hardened with `change_stream_journal_fsync=True`
  and bounded by size with `change_stream_journal_max_bytes`
* when journaling is enabled, `mongoeco` keeps an incremental event log and
  compacts it back into a retained snapshot as the local history rolls forward;
  each log entry carries an integrity checksum and truncated tail writes are
  ignored on reload
* clients, databases and direct collections expose `change_stream_state()` so
  local retained history, journal files and compaction progress can be
  inspected at runtime
* clients, databases and direct collections also expose
  `change_stream_backend_info()`, which makes the contract explicit:
  change streams are local, optionally persistent via journal, resumable inside
  that local environment, and not distributed across nodes
* the local driver now starts non-direct single-seed topologies as
  provisional `UNKNOWN` and relies on `hello` discovery to converge towards
  `standalone`, `replicaSet` or `sharded` topology shapes
* retryable reads and writes now apply to real wire connection failures too:
  connect/read/write socket errors are normalized to `ConnectionFailure`
* replica-set discovery also tracks per-server health states (`healthy`,
  `recovering`, `degraded`, `unreachable`) and uses them to prefer healthier
  candidates when ordering eligible servers
* clients expose `sdam_capabilities()` so the supported SDAM subset is
  inspectable at runtime instead of being implicit in the implementation
* `mongoeco.collation_backend_info()` reports the active Unicode backend,
  while `mongoeco.collation_capabilities_info()` reports the supported locale
  surface and which advanced knobs require `PyICU`

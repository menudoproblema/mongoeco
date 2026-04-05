# Local Search And Retrieval

## When To Use It

Use local `$search` and `$vectorSearch` when you want:

* Atlas-like query shapes for demos, local development or embedded apps;
* explicit local matching and explain metadata;
* a search/vector workflow without running a separate service;
* search operators that are good enough to demonstrate product behavior;
* explicit local field mappings for both textual and scalar search fields.

## Choosing Between `text`, `phrase` and `phrase.slop`

Use `text` when:

* token presence matters more than order;
* you want a broader local full-text style filter.

Use `phrase` when:

* token order matters;
* the words should stay adjacent.

Use `phrase.slop` when:

* token order still matters;
* you want to allow a small number of extra tokens between phrase terms.

Example:

```python
{
    "$search": {
        "index": "content_search",
        "phrase": {
            "query": "Ada designed local search patterns",
            "path": "body",
            "slop": 2,
        },
    }
}
```

Expected result:

* exact phrase hits still match;
* documents with up to two extra tokens between phrase terms can also match;
* `SQLiteEngine` may use FTS5 as a candidate prefilter and then apply exact
  local validation in Python when `slop > 0`.

## Field Mappings You Can Declare Locally

Local search index mappings now accept:

* textual fields: `string`, `autocomplete`, `token`
* scalar fields for exact/range-style matching: `number`, `date`, `boolean`,
  `objectId`, `uuid`

That keeps the product story honest:

* textual operators stay tied to textual mappings;
* scalar operators such as `equals`, `in`, `range` and `near` can target
  explicitly declared local scalar fields.

## Minimal Recipe

```python
from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


with MongoClient(SQLiteEngine("search.db")) as client:
    collection = client.demo.docs
    hits = collection.aggregate(
        [
            {
                "$search": {
                    "index": "content_search",
                    "compound": {
                        "must": [
                            {"phrase": {"query": "Ada designed local search patterns", "path": "body", "slop": 2}}
                        ],
                        "filter": [{"in": {"path": "kind", "value": ["note"]}}],
                    },
                }
            }
        ]
    ).to_list()
    print(hits)
```

## Examples

* [examples/search_and_vector_local.py](/Users/uve/Proyectos/mongoeco2/examples/search_and_vector_local.py)
* [examples/vector_search_diagnostics.py](/Users/uve/Proyectos/mongoeco2/examples/vector_search_diagnostics.py)

These examples show:

* exact `phrase` versus `phrase.slop`;
* `near` over numeric fields with visible ranking metadata;
* `compound` with `phrase`, `equals`, `in`, `range`, `near`, `exists` and `regex`;
* public vector diagnostics such as `similarity`, `numCandidates`,
  `vectorSearchScore`, residual filters and fallback reasons.

## Limits To Keep In Mind

* The local subset is deliberate and documented; it does not claim Atlas Search
  parity.
* `vectorSearch` stays local-first and must still be the first pipeline stage.
* `explain()` is part of the contract: use it to see candidate prefilters,
  residual filters and fallback behavior.

"""Native index creation probes; no local implementation supplies expectations."""

from pymongo import IndexModel

from tests.differential.cases import RealParityCase
from tests.differential.review_improvement_cases import _index_case
from tests.differential.version_delta_cases import capture_outcome


def _invalid_batch(collection):
    collection.create_index("n", name="existing")
    before = list(collection.list_indexes())
    call = capture_outcome(
        lambda: collection.create_indexes(
            [
                IndexModel("p", name="first"),
                IndexModel("n", name="existing", unique=True),
            ]
        )
    )
    return {"before": before, "call": call, "after": list(collection.list_indexes())}


INDEX_GUARANTEE_CASES = (
    *(
        _index_case(f"index_id_name_{label}", [([("_id", 1)], {"name": value})])
        for label, value in (("custom", "custom"), ("integer", 123), ("empty", ""))
    ),
    RealParityCase("index_invalid_batch_conflict", [], _invalid_batch),
)

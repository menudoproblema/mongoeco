from tests.differential.error_phase_cases import (
    case,
    expr,
    projections,
    queries,
    updates,
)


CASES = []
for context in ["no_match", "empty_collection"]:
    seed = [] if context == "empty_collection" else None
    for name in ["unknown", "in_type", "and_empty", "expr_divzero", "expr_variable"]:
        q = {"$and": [{"_id": "absent"}, queries[name]]}
        for method in [
            "find_one",
            "count_documents",
            "update_one",
            "delete_one",
            "find_one_and_delete",
        ]:
            args = [q] + (
                [{"$set": {"touched": True}}] if method == "update_one" else []
            )
            CASES.append(
                case(context + "_" + method + "_query_" + name, method, args, seed=seed)
            )
    for name in ["mixed", "path_collision"]:
        for method in ["find_one", "find_one_and_delete"]:
            CASES.append(  # noqa: PERF401 - explicit absence control inventory
                case(
                    context + "_" + method + "_projection_" + name,
                    method,
                    [{"_id": "absent"}],
                    {"projection": projections[name]},
                    seed=seed,
                )
            )
    for name in ["sqrt_negative", "ln_zero", "eq_arity", "regex_invalid"]:
        change = [{"$set": {"value": expr[name]}}]
        for method in ["aggregate", "update_one"]:
            args = (
                [[{"$match": {"_id": "absent"}}, *change]]
                if method == "aggregate"
                else [{"_id": "absent"}, change]
            )
            CASES.append(
                case(context + "_" + method + "_" + name, method, args, seed=seed)
            )
    for name in ["push_each", "pipeline_stage_type", "filter_expr"]:
        change, opts = updates[name]
        CASES.append(
            case(
                context + "_update_" + name,
                "update_one",
                [{"_id": "absent"}, change],
                opts,
                seed=seed,
            )
        )
# Control: PyMongo rejects an empty pipeline at its client validation boundary.
for method in ["update_one", "find_one_and_update"]:
    CASES.append(  # noqa: PERF401 - explicit absence control inventory
        case("empty_pipeline_" + method, method, [{"_id": "a"}, []])
    )
_ABSENCE_CASE_COUNT = 82
assert len(CASES) == _ABSENCE_CASE_COUNT, len(CASES)

EXTRA_CASES = CASES

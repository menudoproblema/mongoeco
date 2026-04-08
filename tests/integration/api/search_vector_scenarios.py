from __future__ import annotations

import unittest


def assert_compound_should_near_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["compound"]["shouldOperators"], ["exists", "near"])
    if engine_name == "sqlite":
        case.assertEqual(details["compoundPrefilter"]["requiredCandidateableShould"], 1)
        case.assertEqual(details["compoundPrefilter"]["candidateableShouldOperators"], ["exists"])
    case.assertEqual(
        details["ranking"],
        {
            "usesShouldRanking": True,
            "nearAware": True,
            "nearAwareShouldCount": 1,
            "shouldRankingMode": "matched-should-plus-clause-score",
            "minimumShouldMatchApplied": True,
        },
    )


def assert_compound_candidateable_should_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    if engine_name != "sqlite":
        return
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["compoundPrefilter"]["candidateableShouldCount"], 3)
    case.assertEqual(
        details["compoundPrefilter"]["clauseClasses"]["should"],
        ["candidateable-exact", "candidateable-exact", "candidateable-exact"],
    )
    case.assertEqual(
        details["compoundPrefilter"]["candidateableShouldOperators"],
        ["exists", "wildcard", "autocomplete"],
    )
    case.assertEqual(details["rankingSource"], "fts-materialized-entries")


def assert_compound_candidateable_should_limited_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    pushdown = explanation["pushdown"]
    details = explanation["engine_plan"]["details"]
    case.assertEqual(pushdown["searchResultLimitHint"], 1)
    case.assertEqual(pushdown["searchTopKStrategy"], "direct-window")
    case.assertEqual(details["topKLimitHint"], 1)
    if engine_name != "sqlite":
        return
    case.assertTrue(details["topKPrefilter"]["applied"])
    case.assertEqual(details["topKPrefilter"]["strategy"], "exact-should-score-tier")
    case.assertGreaterEqual(details["topKPrefilter"]["cutoffTier"]["matchedShould"], 2)
    case.assertEqual(
        details["topKPrefilter"]["partialRanking"]["strategy"],
        "matched-should-prefilter+exact-should-score-tier",
    )
    case.assertGreaterEqual(
        details["topKPrefilter"]["candidateCountBeforePartialRanking"],
        details["topKPrefilter"]["candidateCountAfterPartialRanking"],
    )
    case.assertLess(details["candidateCount"], details["candidateCountBeforeTopK"])
    case.assertEqual(details["rankingSource"], "fts-materialized-entries")


def assert_compound_candidateable_should_matched_limited_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    pushdown = explanation["pushdown"]
    details = explanation["engine_plan"]["details"]
    case.assertEqual(pushdown["searchResultLimitHint"], 1)
    case.assertEqual(pushdown["searchTopKStrategy"], "prefix-iterative")
    case.assertEqual(pushdown["searchTopKGrowthStrategy"], "adaptive-retention")
    case.assertEqual(pushdown["searchDownstreamFilterPrefilter"], True)
    case.assertEqual(details["topKLimitHint"], 1)
    case.assertEqual(details["downstreamFilterPrefilter"], {"_id": 3})
    if engine_name == "sqlite":
        case.assertEqual(details["compoundPrefilter"]["downstreamFilter"]["candidateable"], False)


def assert_compound_candidateable_should_title_prefilter_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    if engine_name != "sqlite":
        return
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["compoundPrefilter"]["downstreamFilter"]["candidateable"], True)
    case.assertEqual(details["compoundPrefilter"]["downstreamFilter"]["supportedPaths"], ["title"])
    case.assertTrue(details["compoundPrefilter"]["should"][0]["downstreamRefinement"]["applied"])
    case.assertEqual(details["compoundPrefilter"]["should"][0]["downstreamRefinement"]["path"], "title")


def assert_filtered_vector_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["hybridRetrieval"]["filterMode"], "candidate-prefilter")
    case.assertEqual(details["hybridRetrieval"]["queryFilterMode"], "candidate-prefilter")
    case.assertIsNone(details["hybridRetrieval"]["downstreamFilterMode"])
    case.assertIsNotNone(details["pruningSummary"])
    case.assertEqual(
        details["pruningSummary"]["prefilterCandidateCount"],
        details["candidatePlan"]["prefilterCandidateCount"],
    )
    case.assertEqual(
        details["pruningSummary"]["candidatesEvaluated"],
        details["candidatePlan"]["evaluatedCandidates"],
    )
    case.assertEqual(
        details["pruningSummary"]["postCandidateFilteredCount"],
        details["documentsFiltered"],
    )
    case.assertEqual(
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["prefilterCandidateCount"],
    )
    case.assertEqual(
        details["hybridRetrieval"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["prefilterCandidateCount"],
    )
    case.assertIsNone(details["hybridRetrieval"]["downstreamFilterPrefilter"])
    case.assertEqual(
        details["candidatePlan"]["prefilterSources"],
        [
            {
                "source": "queryFilter",
                "candidateable": True,
                "exact": True,
                "supportedPaths": ["kind"],
                "supportedOperators": ["eq"],
            }
        ],
    )
    case.assertLessEqual(
        details["candidatePlan"]["prefilterCandidateCount"],
        details["documentsScanned"],
    )
    if details.get("documentsScannedAfterPrefilter") is not None:
        case.assertEqual(
            details["candidatePlan"]["prefilterCandidateCount"],
            details["documentsScannedAfterPrefilter"],
        )
    if engine_name == "sqlite":
        case.assertEqual(details["candidateExpansionStrategy"], "adaptive-retention")
        case.assertEqual(details["filterMode"], "candidate-prefilter")
        case.assertEqual(details["vectorFilterPrefilter"]["backend"], "vector-filter-index")
        case.assertEqual(details["vectorFilterPrefilter"]["supportedPaths"], ["kind"])
        return
    case.assertEqual(details["filterMode"], "candidate-prefilter")
    case.assertEqual(details["vectorFilterPrefilter"]["backend"], "memory-vector-filter-index")
    case.assertLess(details["documentsScannedAfterPrefilter"], details["documentsScanned"])


def assert_vector_downstream_filter_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertIsNone(details["filterMode"])
    case.assertEqual(details["downstreamFilterMode"], "candidate-prefilter")
    case.assertEqual(
        details["hybridRetrieval"]["queryFilter"],
        None,
    )
    case.assertEqual(
        details["hybridRetrieval"]["downstreamFilter"],
        {"score": {"$gte": 15}},
    )
    case.assertEqual(
        details["hybridRetrieval"]["queryFilterMode"],
        None,
    )
    case.assertEqual(
        details["hybridRetrieval"]["downstreamFilterMode"],
        "candidate-prefilter",
    )
    case.assertEqual(
        details["candidatePlan"]["queryPrefilterCandidateCount"],
        details["documentsScanned"],
    )
    case.assertIsNotNone(details["pruningSummary"])
    case.assertEqual(
        details["pruningSummary"]["prefilterCandidateCount"],
        details["candidatePlan"]["prefilterCandidateCount"],
    )
    case.assertEqual(
        details["pruningSummary"]["candidatesEvaluated"],
        details["candidatePlan"]["evaluatedCandidates"],
    )
    case.assertEqual(
        details["pruningSummary"]["postCandidateFilteredCount"],
        0,
    )
    case.assertEqual(
        details["candidatePlan"]["prefilterSources"],
        [
            {
                "source": "downstreamFilter",
                "candidateable": True,
                "exact": True,
                "supportedPaths": ["score"],
                "supportedOperators": ["range"],
            }
        ],
    )
    case.assertEqual(
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["prefilterCandidateCount"],
    )
    case.assertEqual(
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["downstreamPrefilterCandidateCount"],
    )
    case.assertEqual(details["documentsFiltered"], 0)
    case.assertEqual(details["hybridRetrieval"]["documentsFilteredPostCandidate"], 0)
    case.assertTrue(details["downstreamFilterCandidatePrefilter"]["exact"])
    case.assertEqual(
        details["downstreamFilterCandidatePrefilter"]["supportedPaths"],
        ["score"],
    )
    case.assertEqual(
        details["downstreamFilterCandidatePrefilter"]["supportedOperators"],
        ["range"],
    )
    if engine_name == "sqlite":
        case.assertEqual(
            details["downstreamFilterCandidatePrefilter"]["backend"],
            "vector-filter-index",
        )
        return
    case.assertEqual(
        details["downstreamFilterCandidatePrefilter"]["backend"],
        "memory-vector-filter-index",
    )


def assert_vector_query_and_downstream_filter_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["filterMode"], "candidate-prefilter")
    case.assertEqual(details["downstreamFilterMode"], "candidate-prefilter")
    case.assertEqual(
        details["hybridRetrieval"]["queryFilterMode"],
        "candidate-prefilter",
    )
    case.assertEqual(
        details["hybridRetrieval"]["downstreamFilterMode"],
        "candidate-prefilter",
    )
    case.assertEqual(
        details["hybridRetrieval"]["queryFilter"],
        {"kind": "note"},
    )
    case.assertEqual(
        details["hybridRetrieval"]["downstreamFilter"],
        {"score": {"$gte": 15}},
    )
    case.assertEqual(
        details["candidatePlan"]["prefilterSources"],
        [
            {
                "source": "queryFilter",
                "candidateable": True,
                "exact": True,
                "supportedPaths": ["kind"],
                "supportedOperators": ["eq"],
            },
            {
                "source": "downstreamFilter",
                "candidateable": True,
                "exact": True,
                "supportedPaths": ["score"],
                "supportedOperators": ["range"],
            },
        ],
    )
    case.assertLessEqual(
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["queryPrefilterCandidateCount"],
    )
    case.assertLessEqual(
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
        details["candidatePlan"]["downstreamPrefilterCandidateCount"],
    )
    case.assertEqual(details["documentsFiltered"], 0)
    case.assertEqual(details["hybridRetrieval"]["documentsFilteredPostCandidate"], 0)
    case.assertEqual(
        details["pruningSummary"]["prefilterCandidateCount"],
        details["candidatePlan"]["combinedPrefilterCandidateCount"],
    )
    if engine_name == "sqlite":
        case.assertEqual(details["vectorFilterPrefilter"]["backend"], "vector-filter-index")
        case.assertEqual(
            details["downstreamFilterCandidatePrefilter"]["backend"],
            "vector-filter-index",
        )
        return
    case.assertEqual(details["vectorFilterPrefilter"]["backend"], "memory-vector-filter-index")
    case.assertEqual(
        details["downstreamFilterCandidatePrefilter"]["backend"],
        "memory-vector-filter-index",
    )


def assert_in_equals_and_range_explanations(
    case: unittest.TestCase,
    in_explanation: dict[str, object],
    equals_explanation: dict[str, object],
    range_explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    in_details = in_explanation["engine_plan"]["details"]
    case.assertEqual(in_details["queryOperator"], "in")
    case.assertEqual(in_details["path"], "kind")
    case.assertEqual(in_details["value"], ["note", "reference"])
    case.assertEqual(in_details["backend"], "python")
    case.assertEqual(in_details["compound"], None)
    case.assertEqual(
        in_details["pathSummary"],
        {
            "all": ["kind"],
            "pathCount": 1,
            "multiPath": False,
            "usesEmbeddedPaths": False,
            "embeddedPaths": [],
            "parentPaths": ["kind"],
            "leafPaths": [],
            "sections": ["in"],
            "resolvedLeafPaths": [],
            "unresolvedPaths": ["kind"],
        },
    )
    equals_details = equals_explanation["engine_plan"]["details"]
    case.assertEqual(equals_details["queryOperator"], "equals")
    case.assertEqual(equals_details["path"], "kind")
    case.assertEqual(equals_details["value"], "note")
    case.assertEqual(equals_details["backend"], "python")
    case.assertEqual(equals_details["compound"], None)
    case.assertEqual(
        equals_details["pathSummary"],
        {
            "all": ["kind"],
            "pathCount": 1,
            "multiPath": False,
            "usesEmbeddedPaths": False,
            "embeddedPaths": [],
            "parentPaths": ["kind"],
            "leafPaths": [],
            "sections": ["equals"],
            "resolvedLeafPaths": [],
            "unresolvedPaths": ["kind"],
        },
    )
    range_details = range_explanation["engine_plan"]["details"]
    case.assertEqual(range_details["queryOperator"], "range")
    case.assertEqual(range_details["path"], "score")
    case.assertEqual(
        range_details["range"],
        {"gt": None, "gte": 9.0, "lt": None, "lte": 11.0, "boundKind": "number"},
    )
    case.assertEqual(range_details["backend"], "python")
    case.assertEqual(
        range_details["pathSummary"],
        {
            "all": ["score"],
            "pathCount": 1,
            "multiPath": False,
            "usesEmbeddedPaths": False,
            "embeddedPaths": [],
            "parentPaths": ["score"],
            "leafPaths": [],
            "sections": ["range"],
            "resolvedLeafPaths": ["score"],
            "unresolvedPaths": [],
        },
    )
    if engine_name == "sqlite":
        case.assertFalse(bool(range_details.get("fts5_match")))


def assert_regex_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
    expected_flags: str = "",
    expected_allow_analyzed_field: bool = False,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["queryOperator"], "regex")
    case.assertEqual(details["query"], "Ada.*algorithm")
    case.assertEqual(details["paths"], ["body"])
    case.assertEqual(details["querySemantics"]["flags"], expected_flags)
    case.assertEqual(
        details["querySemantics"]["allowAnalyzedField"],
        expected_allow_analyzed_field,
    )
    case.assertEqual(details["backend"], "python")
    if engine_name == "sqlite":
        case.assertFalse(bool(details.get("fts5_match")))


def assert_search_advanced_option_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(
        details["stageOptions"],
        {
            "count": {"type": "total"},
            "highlight": {
                "paths": ["title", "body"],
                "maxChars": 40,
                "maxNumPassages": 1,
                "resultField": "searchHighlights",
            },
            "facet": {
                "type": "string",
                "path": "kind",
                "numBuckets": 5,
                "previewOnly": True,
            },
        },
    )
    case.assertEqual(
        details["countPreview"],
        {
            "type": "total",
            "value": 2,
            "exact": True,
        },
    )
    case.assertEqual(
        details["facetPreview"],
        {
            "type": "string",
            "path": "kind",
            "numBuckets": 5,
            "buckets": [{"value": "note", "count": 2}],
        },
    )
    case.assertEqual(details["highlightPreview"]["resultField"], "searchHighlights")
    case.assertEqual(details["highlightPreview"]["requestedPaths"], ["title", "body"])
    case.assertEqual(details["highlightPreview"]["maxNumPassages"], 1)
    case.assertGreaterEqual(details["highlightPreview"]["fragmentCount"], 1)
    case.assertTrue(details["highlightPreview"]["sample"])


def assert_phrase_in_range_compound_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["queryOperator"], "compound")
    case.assertEqual(details["compound"]["mustOperators"], ["phrase"])
    case.assertEqual(details["compound"]["filterOperators"], ["in", "range"])
    case.assertEqual(details["compound"]["shouldOperators"], ["exists", "regex"])


def assert_phrase_slop_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
    expected_slop: int,
    expected_match: str | None,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["queryOperator"], "phrase")
    case.assertEqual(details["slop"], expected_slop)
    if engine_name == "sqlite":
        case.assertEqual(details["fts5_match"], expected_match)
        case.assertEqual(details["candidateExact"], expected_slop == 0)
        case.assertEqual(details["postCandidateValidationRequired"], expected_slop > 0)


def assert_vector_similarity_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    expected_similarity: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["queryOperator"], "vectorSearch")
    case.assertEqual(details["similarity"], expected_similarity)
    case.assertEqual(details["scoreBreakdown"]["similarity"], expected_similarity)
    case.assertEqual(details["scoreBreakdown"]["scoreField"], "vectorSearchScore")
    case.assertEqual(details["candidatePlan"]["requestedCandidates"], details["candidatesRequested"])
    case.assertEqual(details["candidatePlan"]["evaluatedCandidates"], details["candidatesEvaluated"])
    case.assertEqual(
        details["pathSummary"]["resolvedLeafPaths"],
        [details["path"]],
    )
    candidates_requested = details.get("candidatesRequested")
    candidates_evaluated = details.get("candidatesEvaluated")
    if candidates_requested is not None and candidates_evaluated is not None:
        case.assertGreaterEqual(candidates_requested, candidates_evaluated)


def assert_vector_min_score_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    expected_min_score: float,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["minScore"], expected_min_score)
    case.assertEqual(details["documentsFilteredByMinScore"], 1)
    case.assertEqual(details["scoreBreakdown"]["documentsFilteredByMinScore"], 1)
    case.assertEqual(details["scoreBreakdown"]["minScore"], expected_min_score)


def assert_vector_score_projection_results(
    case: unittest.TestCase,
    hits: list[dict[str, object]],
    *,
    expected_ids: list[object],
) -> None:
    case.assertEqual([document["_id"] for document in hits], expected_ids)
    case.assertIn("vectorScore", hits[0])
    case.assertIn("vectorScore", hits[1])
    case.assertGreater(float(hits[0]["vectorScore"]), float(hits[1]["vectorScore"]))
    case.assertNotIn("__mongoeco_vectorSearchScore__", hits[0])
    case.assertNotIn("__mongoeco_vectorSearchScore__", hits[1])


def assert_ranged_vector_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    if engine_name != "sqlite":
        return
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["filterMode"], "candidate-prefilter")
    case.assertEqual(details["vectorFilterPrefilter"]["supportedPaths"], ["score"])
    case.assertEqual(details["vectorFilterPrefilter"]["supportedOperators"], ["range"])
    case.assertTrue(details["vectorFilterPrefilter"]["exact"])
    case.assertEqual(details["hybridRetrieval"]["filterMode"], "candidate-prefilter")


def assert_boolean_vector_residual_explanation(
    case: unittest.TestCase,
    explanation: dict[str, object],
    *,
    engine_name: str,
) -> None:
    details = explanation["engine_plan"]["details"]
    case.assertEqual(details["filterMode"], "candidate-prefilter+post-candidate")
    case.assertTrue(details["vectorFilterPrefilter"]["candidateable"])
    case.assertFalse(details["vectorFilterPrefilter"]["exact"])
    case.assertEqual(details["vectorFilterPrefilter"]["booleanShape"], "$and")
    case.assertEqual(details["vectorFilterPrefilter"]["supportedPaths"], ["kind", "score"])
    case.assertEqual(details["vectorFilterPrefilter"]["supportedOperators"], ["$in", "range"])
    case.assertTrue(details["vectorFilterResidual"]["required"])
    case.assertEqual(details["vectorFilterResidual"]["reason"], "unsupported-clauses")
    case.assertEqual(details["vectorFilterResidual"]["unsupportedClauseCount"], 1)
    case.assertEqual(
        details["hybridRetrieval"]["documentsFilteredPostCandidate"],
        details["documentsFiltered"],
    )
    if engine_name == "sqlite":
        case.assertEqual(details["vectorFilterPrefilter"]["backend"], "vector-filter-index")
    else:
        case.assertEqual(details["vectorFilterPrefilter"]["backend"], "memory-vector-filter-index")

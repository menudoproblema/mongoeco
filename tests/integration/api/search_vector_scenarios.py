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
    case.assertEqual(details["ranking"], {"usesShouldRanking": True, "nearAware": True})


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
    if engine_name == "sqlite":
        case.assertEqual(details["candidateExpansionStrategy"], "adaptive-retention")
        case.assertEqual(details["filterMode"], "candidate-prefilter")
        case.assertEqual(details["vectorFilterPrefilter"]["backend"], "vector-filter-index")
        case.assertEqual(details["vectorFilterPrefilter"]["supportedPaths"], ["kind"])
        return
    case.assertEqual(details["filterMode"], "candidate-prefilter")
    case.assertEqual(details["vectorFilterPrefilter"]["backend"], "memory-vector-filter-index")
    case.assertLess(details["documentsScannedAfterPrefilter"], details["documentsScanned"])


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

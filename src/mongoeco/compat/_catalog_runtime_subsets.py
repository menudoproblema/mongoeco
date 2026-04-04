LOCAL_RUNTIME_SUBSET_CATALOG: dict[str, dict[str, object]] = {
    "vectorSearch": {
        "backend": "usearch",
        "mode": "local-ann-with-exact-baseline",
        "similarities": ["cosine", "dotProduct", "euclidean"],
        "filterMode": "post-candidate-with-adaptive-candidate-expansion",
        "fallback": "exact",
        "note": (
            "SQLiteEngine uses a local usearch ANN backend when the vector index is "
            "materialized; MemoryEngine remains the exact semantic baseline."
        ),
    },
    "geospatial": {
        "semantics": "planar-local",
        "storedGeometries": [
            "Point",
            "LineString",
            "Polygon",
            "MultiPoint",
            "MultiLineString",
            "MultiPolygon",
            "GeometryCollection",
            "legacy [x, y]",
        ],
        "queryOperators": ["$geoWithin", "$geoIntersects", "$near", "$nearSphere"],
        "aggregationStages": ["$geoNear"],
        "note": (
            "The embedded runtime uses planar local geometry operations. "
            "$nearSphere and 2dsphere remain Mongo-like names over local planar distance."
        ),
    },
    "search": {
        "operators": [
            "text",
            "phrase",
            "autocomplete",
            "wildcard",
            "exists",
            "in",
            "equals",
            "range",
            "near",
            "compound",
        ],
        "sqliteBackends": ["fts5", "fts5-glob", "fts5-path", "fts5-prefilter", "python"],
        "note": "The local $search surface remains an explicit Atlas-like subset.",
    },
    "classicText": {
        "supportsTextScore": True,
        "supportsMetaProjection": True,
        "supportsSortByTextScore": True,
        "note": (
            "The classic $text subset remains local, token-based and intentionally "
            "lighter than MongoDB server full-text behavior."
        ),
    },
}

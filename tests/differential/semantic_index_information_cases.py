"""Optional uniqueness flags in the public index-information view."""

SEED = [{"_id": 1}]


def index_information_flags(collection):
    collection.create_index("default", name="default")
    collection.create_index("false", name="false", unique=False)
    collection.create_index("true", name="true", unique=True)
    information = collection.index_information()
    return {
        name: {
            "key": information[name]["key"],
            "hasUnique": "unique" in information[name],
            "unique": information[name].get("unique"),
        }
        for name in ("default", "false", "true")
    }


SEMANTIC_INDEX_INFORMATION_CASES = (
    ("semantic_index_information_flags", index_information_flags),
)

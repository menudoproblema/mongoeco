"""Administrative rename grammar, errors and stored effects vs native MongoDB."""

from bson import Code


SEED = [{"_id": "initial"}]


def _client(collection):
    client = getattr(collection, "_client", None)
    return collection.database.client if client is None else client


def command_spec(label, database, source, target):
    spec = {"renameCollection": f"{database}.{source}", "to": f"{database}.{target}"}
    if label in {"overwrite", "missing_source", "same", "nonadmin"}:
        spec["dropTarget"] = True
    if label == "same":
        spec["to"] = spec["renameCollection"]
    if label.startswith("drop_"):
        spec["dropTarget"] = {"drop_number": 1, "drop_null": None, "drop_text": "yes"}[
            label
        ]
    if label in {"missing_to", "missing_to_bool"}:
        spec.pop("to")
    if label in {"missing_to_bool", "namespace_bool"}:
        spec["dropTarget"] = 1
    if label in {"bad_namespace", "namespace_bool"}:
        spec["renameCollection"] = "unqualified"
    if label == "source_code":
        spec["renameCollection"] = Code(spec["renameCollection"])
    if label in {"unknown", "typed_unknown"}:
        spec["unknown"] = 1
    if label == "typed_unknown":
        spec["renameCollection"] = 3
    if label == "stay_temp":
        spec["stayTemp"] = True
    return spec


def rename_commands(collection):
    db, client = collection.database, _client(collection)
    output = {}
    labels = (
        "default",
        "overwrite",
        "collision",
        "missing_source",
        "same",
        "nonadmin",
        "drop_number",
        "drop_null",
        "drop_text",
        "missing_to",
        "source_code",
        "bad_namespace",
        "unknown",
        "missing_to_bool",
        "namespace_bool",
        "typed_unknown",
        "stay_temp",
    )
    for index, label in enumerate(labels):
        source, target = f"source_{index}", f"target_{index}"
        if label != "missing_source":
            subject = db.create_collection(
                source,
                validator={"v": {"$type": "number"}},
                changeStreamPreAndPostImages={"enabled": True},
            )
            subject.insert_one({"_id": "source", "v": 1})
            subject.create_index("v", name="source_index")
        if label not in {"default", "stay_temp"}:
            db[target].insert_one({"_id": "target", "old": "retained"})
            db[target].create_index("old", name="target_index")
        try:
            owner = db if label == "nonadmin" else client["admin"]
            owner.command(command_spec(label, db.name, source, target))
            observed = {"ok": True}
        except Exception as error:
            observed = {
                "error": type(error).__name__,
                "code": getattr(error, "code", None),
            }
        observed["source"] = list(db[source].find({}))
        observed["target"] = list(db[target].find({}))
        if observed.get("ok"):
            options = db[target].options()
            # Catalog option member order is incidental; BSON validator order is not.
            observed["options"] = {key: options[key] for key in sorted(options)}
            observed["indexes"] = sorted(
                row["name"] for row in db[target].list_indexes()
            )
        output[label] = observed
    return output


SEMANTIC_RENAME_COMMAND_CASES = (("semantic_rename_commands", rename_commands),)

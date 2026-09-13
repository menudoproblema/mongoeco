"""Insertion metadata belongs to the same copy/rollback view as its rows."""

import datetime
import random

from copy import copy, deepcopy

import pytest

from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.engines._memory_collection import (
    MemoryCollection,
    MemoryIndexMap,
    _iter_membership_keys,
    _MembershipNode,
    _merge_membership,
    _remove_membership,
)
from mongoeco.engines.mvcc import MemoryMvccState


def assert_order(collection):
    assert collection.ordered_keys(reversed(list(collection))) == list(collection)
    assert set(collection._ordinals) == set(collection)
    assert set(collection._locations) == set(collection)
    assert len(set(collection._ordinals.values())) == len(collection)
    assert sum(chunk.live_count for chunk in collection._chunks.values()) == len(
        collection
    )
    for chunk_id, chunk in collection._chunks.items():
        assert (len(chunk.items) - chunk.live_count) * 2 < len(chunk.items)
        for offset, item in enumerate(chunk.items):
            if isinstance(item, tuple):
                key, value = item
                assert collection._locations[key] == (chunk_id, offset)
                assert collection[key] is value
    assert collection.ordered_keys(["absent"]) == []


def test_bson_datetime_index_keys_normalize_timezone_and_precision():
    aware = datetime.datetime(
        2026,
        1,
        1,
        1,
        0,
        0,
        999,
        tzinfo=datetime.timezone(datetime.timedelta(hours=1)),
    )
    stored = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC).replace(tzinfo=None)

    assert bson_equality_key(aware) == bson_equality_key(stored)


@pytest.mark.parametrize("copier", [copy, deepcopy, lambda value: value.copy()])
@pytest.mark.parametrize("mutated_copy", [False, True])
def test_copies_keep_independent_membership_and_order(copier, mutated_copy):
    original = MemoryCollection({"z": [1], "a": [2], "m": [3]})
    snapshot = copier(original)
    changed, unchanged = (snapshot, original) if mutated_copy else (original, snapshot)
    changed["a"] = [4]
    del changed["z"]
    changed["z"] = [5]
    changed["new"] = [6]
    assert list(changed) == ["a", "m", "z", "new"]
    assert list(unchanged) == ["z", "a", "m"]
    assert unchanged["a"] == [2]
    assert_order(changed)
    assert_order(unchanged)


def test_shallow_copy_shares_persistent_roots_until_mutation():
    original = MemoryCollection({"z": [1], "a": [2]})
    snapshot = original.copy()
    assert original._values is snapshot._values
    assert original._ordinals is snapshot._ordinals
    assert original._locations is snapshot._locations
    assert original._chunks is snapshot._chunks
    original["z"] = [3]
    assert original._values is not snapshot._values
    assert original._ordinals is snapshot._ordinals
    assert original._locations is snapshot._locations
    assert original._chunks is not snapshot._chunks
    assert snapshot["z"] == [1]
    original["new"] = [4]
    assert original._ordinals is not snapshot._ordinals
    assert original._locations is not snapshot._locations
    assert original._chunks is not snapshot._chunks
    assert_order(original)
    assert_order(snapshot)


def test_ordered_keys_can_select_only_the_natural_order_prefix():
    collection = MemoryCollection({"z": 1, "a": 2, "m": 3, "b": 4, "late": 5})

    assert collection.ordered_keys(
        {"late", "b", "a", "z"},
        limit=2,
    ) == ["z", "a"]
    assert collection.ordered_keys({"late"}, limit=0) == []


def test_index_map_copy_shares_root_and_isolates_membership_changes():
    original = MemoryIndexMap({("blue",): {1, 2}})
    snapshot = original.copy()
    assert original._buckets is snapshot._buckets
    original.add(("blue",), 3)
    original.add(("green",), 4)
    original.discard(("blue",), 1)
    assert original._buckets is not snapshot._buckets
    assert original == {("blue",): frozenset({2, 3}), ("green",): frozenset({4})}
    assert snapshot == {("blue",): frozenset({1, 2})}


def test_index_map_preserves_natural_membership_order_in_persistent_roots():
    index = MemoryIndexMap()
    index.add(("blue",), "late", ordinal=20)
    index.add(("blue",), "first", ordinal=2)
    index.add(("blue",), "middle", ordinal=9)
    snapshot = index.copy()

    assert index.ordered_storage_keys(("blue",)) == (
        "first",
        "middle",
        "late",
    )
    assert index.ordered_storage_keys(("blue",), limit=2) == (
        "first",
        "middle",
    )

    index.discard(("blue",), "first")
    index.add(("blue",), "first", ordinal=30)
    assert index.ordered_storage_keys(("blue",)) == (
        "middle",
        "late",
        "first",
    )
    assert snapshot.ordered_storage_keys(("blue",)) == (
        "first",
        "middle",
        "late",
    )


def test_index_map_replacement_duplicate_and_defensive_tree_paths():
    index = MemoryIndexMap({("blue",): {"old"}})
    index[("blue",)] = ("first", "second")
    index.add(("blue",), "first")

    assert index[("blue",)] == frozenset({"first", "second"})
    assert tuple(_iter_membership_keys(None, 0)) == ()
    assert _remove_membership(None, 1) is None

    left = _MembershipNode(1, "left", 0)
    right = _MembershipNode(2, "right", 1)
    assert _merge_membership(left, right).storage_key == "left"


def test_index_map_detects_a_missing_order_root_for_a_nonempty_bucket():
    index = MemoryIndexMap()
    index.add(("blue",), "first", ordinal=1)
    index.add(("blue",), "second", ordinal=2)
    index._bucket_order_roots = index._bucket_order_roots.remove(("blue",))

    with pytest.raises(RuntimeError, match="lost a non-empty bucket"):
        index.discard(("blue",), "first")


def test_mvcc_capture_shares_persistent_document_and_index_roots():
    collection = MemoryCollection({1: {"kind": "blue"}, 2: {"kind": "green"}})
    index_map = MemoryIndexMap({("blue",): {1}, ("green",): {2}})
    snapshot = MemoryMvccState.capture(
        snapshot_version=7,
        storage={"db": {"items": collection}},
        indexes={"db": {"items": []}},
        index_data={"db": {"items": {"kind_1": index_map}}},
        search_indexes={},
        collections={"db": {"items"}},
        collection_options={},
    )
    captured_collection = snapshot.storage["db"]["items"]
    captured_index = snapshot.index_data["db"]["items"]["kind_1"]
    assert captured_collection is not collection
    assert captured_collection._values is collection._values
    assert captured_collection._ordinals is collection._ordinals
    assert captured_collection._locations is collection._locations
    assert captured_collection._chunks is collection._chunks
    assert captured_index is not index_map
    assert captured_index._buckets is index_map._buckets

    new_key = 3
    captured_collection[new_key] = {"kind": "blue"}
    captured_index.add(("blue",), new_key)
    assert new_key not in collection
    assert index_map[("blue",)] == frozenset({1})


def test_deepcopy_preserves_aliases_cycles_and_owns_payloads():
    value = []
    original = MemoryCollection({"z": value, "a": value})
    original["self"] = original
    snapshot = deepcopy(original)
    assert snapshot["self"] is snapshot
    assert snapshot["z"] is snapshot["a"]
    assert snapshot["z"] is not value
    assert snapshot._ordinals is not original._ordinals
    assert_order(snapshot)


def test_dict_mutators_keep_order_metadata_coherent():
    collection = MemoryCollection.fromkeys(["z", "a"], 1)
    assert isinstance(collection, MemoryCollection)
    assert collection.setdefault("z", 2) == 1
    assert collection.setdefault("new") is None
    collection.update({"z": 3}, m=4)
    replacement = 5
    collection |= {"z": replacement, "last": 6}
    assert_order(collection)
    assert collection.pop("a") == 1
    default = object()
    assert collection.pop("absent", default) is default
    assert collection.popitem() == ("last", 6)
    assert_order(collection)
    for action in (
        lambda: collection.pop("absent"),
        lambda: collection.__delitem__("absent"),
    ):
        with pytest.raises(KeyError):
            action()
        assert_order(collection)
    with pytest.raises(TypeError):
        collection.pop("z", 1, 2)
    assert collection["z"] == replacement
    snapshot = collection.copy()
    collection.clear()
    collection["z"] = 9
    assert_order(collection)
    assert_order(snapshot)
    assert list(collection) == ["z"]
    assert snapshot["z"] == replacement
    assert repr(snapshot) == repr(dict(snapshot.items()))
    assert snapshot.__eq__(object()) is False
    assert list(snapshot.keys()) == list(snapshot)
    assert list(snapshot.values()) == [snapshot[key] for key in snapshot]
    assert snapshot.get("absent", "fallback") == "fallback"
    state = snapshot._snapshot_state()
    snapshot["temporary"] = 7
    snapshot._restore_state(state)
    assert "temporary" not in snapshot
    collection.clear()
    with pytest.raises(KeyError):
        collection.popitem()
    assert_order(collection)


def test_none_key_does_not_collide_with_order_sentinel():
    collection = MemoryCollection({None: 1, "last": 2})
    assert list(collection.items()) == [(None, 1), ("last", 2)]
    snapshot = collection.copy()
    del collection[None]
    collection[None] = 3
    assert list(collection.items()) == [("last", 2), (None, 3)]
    assert list(snapshot.items()) == [(None, 1), ("last", 2)]
    assert collection == {"last": 2, None: 3}


def test_order_chunks_compact_locally_without_changing_snapshot():
    original = MemoryCollection((key, object()) for key in range(384))
    snapshot = original.copy()
    for key in range(0, 384, 2):
        del original[key]
    for key in range(384, 576):
        original[key] = object()
    assert list(original) == [*range(1, 384, 2), *range(384, 576)]
    assert list(snapshot) == list(range(384))
    assert_order(original)
    assert_order(snapshot)


def test_removing_a_middle_chunk_repairs_both_links():
    collection = MemoryCollection((key, key) for key in range(257))

    for key in range(128, 256):
        del collection[key]

    assert list(collection) == [*range(128), 256]
    assert (
        collection._chunks[collection._head_chunk].following == collection._tail_chunk
    )
    assert collection._chunks[collection._tail_chunk].previous == collection._head_chunk
    assert_order(collection)


def test_index_map_exposes_complete_mutable_mapping_contract():
    index = MemoryIndexMap([(("blue",), [1, 2])])
    assert repr(index) == repr(dict(index.items()))
    assert index.__eq__(object()) is False
    assert list(index.keys()) == [("blue",)]
    assert list(index.values()) == [frozenset({1, 2})]
    assert index.get(("missing",), "fallback") == "fallback"

    assert index.setdefault(("blue",), {3}) == frozenset({1, 2})
    assert index.setdefault(("green",), {3}) == frozenset({3})
    index.update([(("red",), [4])])
    index.update(yellow=[5])
    assert index[("red",)] == frozenset({4})
    assert index["yellow"] == frozenset({5})

    shallow = copy(index)
    deep = deepcopy(index)
    assert shallow == index == deep
    assert shallow._buckets is index._buckets
    assert deep._buckets is not index._buckets

    index.discard(("missing",), 1)
    index.discard(("blue",), 99)
    index.discard(("blue",), 1)
    assert index[("blue",)] == frozenset({2})
    index.discard(("blue",), 2)
    assert ("blue",) not in index

    assert index.pop(("missing",), "fallback") == "fallback"
    assert index.pop(("green",)) == frozenset({3})
    with pytest.raises(KeyError):
        index.pop(("missing",))
    with pytest.raises(TypeError):
        index.pop(("missing",), 1, 2)
    del index[("red",)]
    index.clear()
    assert index == {}


def test_index_map_orders_expirations_and_shares_snapshot_roots():
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    initial_count = 3
    index = MemoryIndexMap()
    index.set_expiration("late", now + datetime.timedelta(seconds=20))
    index.set_expiration("early", now - datetime.timedelta(seconds=1))
    index.set_expiration("middle", now + datetime.timedelta(seconds=10))
    snapshot = index.copy()

    assert index.expired_storage_keys(now) == ("early",)
    assert snapshot._expiry_root is index._expiry_root
    assert snapshot._expiry_by_storage is index._expiry_by_storage
    assert snapshot.expiration_count == initial_count

    index.set_expiration("late", now - datetime.timedelta(seconds=2))
    index.set_expiration("early", None)
    assert index.expired_storage_keys(now) == ("late",)
    assert snapshot.expired_storage_keys(now) == ("early",)
    assert index.expiration_count == initial_count - 1
    assert snapshot.expiration_count == initial_count

    state = index._snapshot_state()
    index.set_expiration("new", now - datetime.timedelta(seconds=3))
    index._restore_state(state)
    assert index.expired_storage_keys(now) == ("late",)
    index.clear()
    assert index.expiration_count == 0
    assert index.expired_storage_keys(now) == ()


@pytest.mark.parametrize("seed", range(5))
def test_index_map_expiration_histories_match_reference(seed):
    rng = random.Random(seed)  # noqa: S311 -- deterministic structure histories
    epoch = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    index = MemoryIndexMap()
    reference = {}
    for _step in range(500):
        storage_key = rng.randrange(50)
        if rng.randrange(4) == 0:
            expires_at = None
            reference.pop(storage_key, None)
        else:
            expires_at = epoch + datetime.timedelta(seconds=rng.randrange(100))
            reference[storage_key] = expires_at
        index.set_expiration(storage_key, expires_at)
        now = epoch + datetime.timedelta(seconds=rng.randrange(100))
        expected = {key for key, expiration in reference.items() if expiration <= now}
        assert set(index.expired_storage_keys(now)) == expected
        assert index.expiration_count == len(reference)


@pytest.mark.parametrize("seed", range(10))
def test_mutation_histories_match_dict_and_preserve_snapshots(seed):
    rng = random.Random(seed)  # noqa: S311 -- deterministic mutation histories
    collection = MemoryCollection()
    expected = {}
    snapshots = []
    for step in range(200):
        key = str(rng.randrange(20))
        action = rng.choice(["copy", "pop", "setdefault", "popitem", "set"])
        if action == "copy":
            snapshots.append((collection.copy(), expected.copy()))
        elif action == "pop":
            assert collection.pop(key, None) == expected.pop(key, None)
        elif action == "setdefault":
            assert collection.setdefault(key, step) == expected.setdefault(key, step)
        elif action == "popitem" and collection:
            assert collection.popitem() == expected.popitem()
        else:
            collection[key] = expected[key] = step
        assert list(collection.items()) == list(expected.items())
        assert_order(collection)
        for snapshot, old in snapshots:
            assert list(snapshot.items()) == list(old.items())
            assert_order(snapshot)

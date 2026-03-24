import unittest
from mongoeco.core.filtering import QueryEngine

class TestQueryEngine(unittest.TestCase):
    def test_basic_equality(self):
        doc = {"name": "Val", "age": 30}
        self.assertTrue(QueryEngine.match(doc, {"name": "Val"}))
        self.assertFalse(QueryEngine.match(doc, {"name": "Other"}))

    def test_dot_notation(self):
        doc = {"profile": {"name": "Val", "details": {"city": "Madrid"}}}
        self.assertTrue(QueryEngine.match(doc, {"profile.name": "Val"}))
        self.assertTrue(QueryEngine.match(doc, {"profile.details.city": "Madrid"}))
        self.assertFalse(QueryEngine.match(doc, {"profile.name": "Other"}))

    def test_comparison_operators(self):
        doc = {"age": 30}
        self.assertTrue(QueryEngine.match(doc, {"age": {"$gt": 20}}))
        self.assertTrue(QueryEngine.match(doc, {"age": {"$gte": 30}}))
        self.assertTrue(QueryEngine.match(doc, {"age": {"$lt": 40}}))
        self.assertTrue(QueryEngine.match(doc, {"age": {"$lte": 30}}))
        self.assertTrue(QueryEngine.match(doc, {"age": {"$ne": 25}}))
        
        self.assertFalse(QueryEngine.match(doc, {"age": {"$gt": 35}}))
        self.assertFalse(QueryEngine.match(doc, {"age": {"$lt": 25}}))

    def test_in_operators(self):
        doc = {"tag": "A"}
        self.assertTrue(QueryEngine.match(doc, {"tag": {"$in": ["A", "B"]}}))
        self.assertFalse(QueryEngine.match(doc, {"tag": {"$in": ["C", "D"]}}))
        self.assertTrue(QueryEngine.match(doc, {"tag": {"$nin": ["C", "D"]}}))

    def test_exists_operator(self):
        doc = {"a": 1}
        self.assertTrue(QueryEngine.match(doc, {"a": {"$exists": True}}))
        self.assertTrue(QueryEngine.match(doc, {"b": {"$exists": False}}))
        self.assertFalse(QueryEngine.match(doc, {"a": {"$exists": False}}))

    def test_type_brackets_comparison(self):
        # MongoDB compara por tipos: null < numbers < strings < objects < arrays ...
        doc = {"v": "10"}
        # "10" (string) es mayor que 5 (int) en el orden de tipos de BSON
        self.assertTrue(QueryEngine.match(doc, {"v": {"$gt": 5}}))
        self.assertTrue(QueryEngine.match(doc, {"v": {"$gt": None}}))

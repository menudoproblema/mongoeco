import unittest

from mongoeco.driver.uri import (
    MongoAuthOptions,
    MongoClientOptions,
    MongoTlsOptions,
    MongoUri,
    MongoUriSeed,
    _finalize_client_options,
    _parse_auth_mechanism_properties,
    parse_mongo_uri,
)


class MongoUriFocusedTests(unittest.TestCase):
    def test_seed_and_uri_validate_basic_invariants(self):
        with self.assertRaises(ValueError):
            MongoUriSeed("", 27017)
        with self.assertRaises(ValueError):
            MongoUriSeed("db1", 0)
        with self.assertRaises(ValueError):
            MongoUri(original="mongodb:///", scheme="mongodb", seeds=())

    def test_parse_mongo_uri_rejects_empty_scheme_netloc_and_ipv6(self):
        with self.assertRaises(TypeError):
            parse_mongo_uri("")
        with self.assertRaises(ValueError):
            parse_mongo_uri("http://localhost/")
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb:///db")
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb://[::1]/")

    def test_parse_mongo_uri_decodes_credentials_without_and_with_password(self):
        username_only = parse_mongo_uri("mongodb://ada%40team@localhost/")
        with_password = parse_mongo_uri("mongodb://ada%40team:s%40cret@localhost/app")

        self.assertEqual(username_only.username, "ada@team")
        self.assertIsNone(username_only.password)
        self.assertEqual(with_password.username, "ada@team")
        self.assertEqual(with_password.password, "s@cret")
        self.assertEqual(with_password.default_database, "app")

    def test_parse_mongo_uri_rejects_invalid_option_shapes(self):
        with self.assertRaisesRegex(ValueError, "retryReads must be a boolean option"):
            parse_mongo_uri("mongodb://localhost/?retryReads=maybe")
        with self.assertRaisesRegex(ValueError, "maxPoolSize must be >= 0"):
            parse_mongo_uri("mongodb://localhost/?maxPoolSize=-1")
        with self.assertRaisesRegex(ValueError, "readPreferenceTags items must be key:value pairs"):
            parse_mongo_uri("mongodb://localhost/?readPreferenceTags=region")
        with self.assertRaisesRegex(ValueError, "readPreferenceTags keys must be non-empty"):
            parse_mongo_uri("mongodb://localhost/?readPreferenceTags=:eu")
        with self.assertRaisesRegex(ValueError, "w must be a non-empty string or integer"):
            parse_mongo_uri("mongodb://localhost/?w=")
        with self.assertRaisesRegex(ValueError, "must not specify ports"):
            parse_mongo_uri("mongodb+srv://cluster.example.net:27017/")

    def test_parse_auth_mechanism_properties_parses_and_validates_entries(self):
        self.assertEqual(
            _parse_auth_mechanism_properties("SERVICE_NAME:mongodb,,CANONICALIZE_HOST_NAME:true"),
            {"SERVICE_NAME": "mongodb", "CANONICALIZE_HOST_NAME": "true"},
        )
        with self.assertRaisesRegex(ValueError, "key:value pairs"):
            _parse_auth_mechanism_properties("SERVICE_NAME")
        with self.assertRaisesRegex(ValueError, "keys must be non-empty"):
            _parse_auth_mechanism_properties(":mongodb")

    def test_finalize_client_options_enables_srv_tls_and_rejects_invalid_combinations(self):
        options = MongoClientOptions(raw_options={}, tls=MongoTlsOptions(enabled=False))
        finalized = _finalize_client_options(
            "mongodb+srv",
            seeds=(MongoUriSeed("cluster.example.net"),),
            username="ada",
            password=None,
            default_database=None,
            options=options,
        )
        self.assertTrue(finalized.tls.enabled)

        with self.assertRaisesRegex(ValueError, "loadBalanced and replicaSet are mutually exclusive"):
            _finalize_client_options(
                "mongodb",
                seeds=(MongoUriSeed("db1", 27017),),
                username="ada",
                password=None,
                default_database=None,
                options=MongoClientOptions(load_balanced=True, replica_set="rs0"),
            )
        with self.assertRaisesRegex(ValueError, "loadBalanced requires exactly one seed"):
            _finalize_client_options(
                "mongodb",
                seeds=(MongoUriSeed("db1", 27017), MongoUriSeed("db2", 27017)),
                username="ada",
                password=None,
                default_database=None,
                options=MongoClientOptions(load_balanced=True),
            )
        with self.assertRaisesRegex(ValueError, "password requires username"):
            _finalize_client_options(
                "mongodb",
                seeds=(MongoUriSeed("db1", 27017),),
                username=None,
                password="secret",
                default_database=None,
                options=MongoClientOptions(),
            )
        with self.assertRaisesRegex(ValueError, "MONGODB-X509 requires a username"):
            _finalize_client_options(
                "mongodb",
                seeds=(MongoUriSeed("db1", 27017),),
                username=None,
                password=None,
                default_database=None,
                options=MongoClientOptions(auth=MongoAuthOptions(mechanism="MONGODB-X509")),
            )

import unittest
from unittest.mock import patch

from mongoeco.errors import OperationFailure
from mongoeco.types import Binary
from mongoeco.wire.auth import WireAuthenticationService, WireAuthUser, _ScramConversation
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.scram import build_scram_client_final, build_scram_client_start


class WireAuthenticationServiceTests(unittest.TestCase):
    def test_authentication_service_helpers_cover_database_payload_and_guard_paths(self):
        service = WireAuthenticationService((WireAuthUser("ada", "pencil"),))
        connection = WireConnectionContext(connection_id=1, peer_host="127.0.0.1", peer_port=27017)

        self.assertTrue(service.enabled)
        self.assertEqual(service.authenticate({"authenticate": 1, "mechanism": "SCRAM-SHA-256", "user": "ada", "pwd": "pencil", "db": "admin"}, connection=connection), {"ok": 1.0})
        service.require_authenticated(connection, "find")
        self.assertEqual(service.logout(connection), {"ok": 1.0})
        with self.assertRaisesRegex(OperationFailure, "Authentication required for find"):
            service.require_authenticated(
                WireConnectionContext(connection_id=2, peer_host="127.0.0.1", peer_port=27017),
                "find",
            )

        self.assertEqual(service._resolve_database({"db": "admin"}), "admin")
        self.assertEqual(service._resolve_database({"$db": "admin"}), "admin")
        self.assertEqual(service._resolve_database({}), "admin")
        with self.assertRaisesRegex(OperationFailure, "authenticate requires db"):
            service._resolve_database({"db": 1})

        payload = Binary(b"abc", subtype=0)
        self.assertEqual(service._payload_bytes(payload, "saslStart"), b"abc")
        self.assertEqual(service._payload_bytes(b"abc", "saslStart"), b"abc")
        with self.assertRaisesRegex(OperationFailure, "requires binary payload"):
            service._payload_bytes("abc", "saslStart")
        with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
            service._find_user(username="ada", database="other", mechanism="SCRAM-SHA-256")

    def test_sasl_start_and_continue_cover_error_and_success_paths(self):
        service = WireAuthenticationService(
            (
                WireAuthUser("ada", "pencil", mechanisms=("SCRAM-SHA-256",)),
                WireAuthUser("x509", "secret", mechanisms=("MONGODB-X509",)),
                WireAuthUser("nopass", None, mechanisms=("SCRAM-SHA-256",)),
            )
        )
        connection = WireConnectionContext(connection_id=1, peer_host="127.0.0.1", peer_port=27017)

        with patch("mongoeco.wire.auth.BsonBinary", Binary):
            with self.assertRaisesRegex(OperationFailure, "saslStart requires mechanism"):
                service.sasl_start({"payload": Binary(b"x", subtype=0), "db": "admin"}, connection=connection)

            client_start = build_scram_client_start(username="ada", nonce="clientnonce")
            start = service.sasl_start(
                {
                    "mechanism": "SCRAM-SHA-256",
                    "payload": Binary(client_start.payload, subtype=0),
                    "db": "admin",
                },
                connection=connection,
            )
            self.assertFalse(start["done"])

            conversation_id = start["conversationId"]
            server_first = bytes(start["payload"]).decode("utf-8")
            client_final = build_scram_client_final(
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare=client_start.first_bare,
                server_first_message=server_first,
                combined_nonce=server_first.split(",")[0].split("=", 1)[1],
            )
            finish = service.sasl_continue(
                {
                    "conversationId": conversation_id,
                    "payload": Binary(client_final.payload, subtype=0),
                },
                connection=connection,
            )
            self.assertTrue(finish["done"])

            with self.assertRaisesRegex(OperationFailure, "conversationId"):
                service.sasl_continue({"payload": Binary(client_final.payload, subtype=0)}, connection=connection)
            with self.assertRaisesRegex(OperationFailure, "Unknown SASL conversation"):
                service.sasl_continue({"conversationId": 999, "payload": Binary(client_final.payload, subtype=0)}, connection=connection)

            connection.auth_conversations[77] = _ScramConversation(
                conversation_id=77,
                username="x509",
                database="admin",
                mechanism="MONGODB-X509",
                client_first_bare="n=x509,r=x509nonce",
                server_first_message="r=x509nonce,s=c2FsdA==,i=4096",
                roles=(),
            )
            with self.assertRaisesRegex(OperationFailure, "does not support MONGODB-X509"):
                service.sasl_continue(
                    {
                        "conversationId": 77,
                        "payload": Binary(client_final.payload, subtype=0),
                    },
                    connection=connection,
                )

            nopass_start = build_scram_client_start(username="nopass", nonce="nopassnonce")
            nopass_result = service.sasl_start(
                {
                    "mechanism": "SCRAM-SHA-256",
                    "payload": Binary(nopass_start.payload, subtype=0),
                    "db": "admin",
                },
                connection=connection,
            )
            with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
                service.sasl_continue(
                    {
                        "conversationId": nopass_result["conversationId"],
                        "payload": Binary(client_final.payload, subtype=0),
                    },
                    connection=connection,
                )

        with patch("mongoeco.wire.auth.BsonBinary", type("_MissingBsonBinary", (bytes,), {})):
            with self.assertRaisesRegex(OperationFailure, "optional 'pymongo'/'bson' dependency"):
                service.sasl_start({"mechanism": "SCRAM-SHA-256", "payload": b"x", "db": "admin"}, connection=connection)

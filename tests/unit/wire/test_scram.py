import base64
import unittest

from mongoeco.errors import OperationFailure
from mongoeco.wire import scram


class ScramHelperTests(unittest.TestCase):
    def test_scram_round_trip_and_error_branches(self):
        start = scram.build_scram_client_start(username="ad,a=1", nonce="clientnonce")
        self.assertEqual(start.first_bare, "n=ad=2Ca=3D1,r=clientnonce")
        self.assertEqual(
            scram.parse_scram_client_start(start.payload, mechanism="SCRAM-SHA-256"),
            ("ad,a=1", "clientnonce", start.first_bare),
        )
        self.assertEqual(scram._normalize_password("p\u00aa"), "pa")
        self.assertEqual(scram._scram_unescape_username("ad=2Ca=3D1"), "ad,a=1")
        self.assertTrue(scram.generate_scram_nonce())

        server_first = scram.build_scram_server_first(
            client_nonce="clientnonce",
            mechanism="SCRAM-SHA-256",
            salt=b"salt",
            iterations=4096,
            server_nonce="servernonce",
        )
        parsed_server = scram.parse_scram_server_first(
            server_first.message,
            mechanism="SCRAM-SHA-256",
            client_nonce="clientnonce",
        )
        self.assertEqual(parsed_server.nonce, "clientnonceservernonce")

        client_final = scram.build_scram_client_final(
            password="pencil",
            mechanism="SCRAM-SHA-256",
            client_first_bare=start.first_bare,
            server_first_message=server_first.message,
            combined_nonce=server_first.nonce,
        )
        server_signature = scram.verify_scram_client_final(
            client_final.payload,
            password="pencil",
            mechanism="SCRAM-SHA-256",
            client_first_bare=start.first_bare,
            server_first_message=server_first.message,
        )
        scram.verify_scram_server_final(
            f"v={server_signature}".encode("utf-8"),
            expected_server_signature=client_final.expected_server_signature,
        )

    def test_scram_helpers_reject_invalid_messages_and_parameters(self):
        with self.assertRaisesRegex(OperationFailure, "Unsupported SCRAM mechanism"):
            scram._digest_name_for_mechanism("SCRAM-SHA-999")
        with self.assertRaisesRegex(OperationFailure, "valid UTF-8"):
            scram._decode_scram_payload(b"\xff")
        with self.assertRaisesRegex(OperationFailure, "invalid attribute"):
            scram._parse_scram_attributes("broken")

        with self.assertRaisesRegex(OperationFailure, "gs2 header"):
            scram.parse_scram_client_start(b"p,,n=user,r=x", mechanism="SCRAM-SHA-256")
        with self.assertRaisesRegex(OperationFailure, "requires username"):
            scram.parse_scram_client_start(b"n,,r=x", mechanism="SCRAM-SHA-256")
        with self.assertRaisesRegex(OperationFailure, "requires nonce"):
            scram.parse_scram_client_start(b"n,,n=user", mechanism="SCRAM-SHA-256")
        with self.assertRaisesRegex(OperationFailure, "iterations must be positive"):
            scram.build_scram_server_first(
                client_nonce="client",
                mechanism="SCRAM-SHA-256",
                salt=b"salt",
                iterations=0,
            )
        with self.assertRaisesRegex(OperationFailure, "must extend the client nonce"):
            scram.parse_scram_server_first("r=other,s=c2FsdA==,i=4096", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "requires salt"):
            scram.parse_scram_server_first("r=clientnonce,i=4096", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "requires iterations"):
            scram.parse_scram_server_first("r=clientnonce,s=c2FsdA==", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "valid base64"):
            scram.parse_scram_server_first("r=clientnonce,s=***,i=4096", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "must be an integer"):
            scram.parse_scram_server_first("r=clientnonce,s=c2FsdA==,i=bad", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "must be positive"):
            scram.parse_scram_server_first("r=clientnonce,s=c2FsdA==,i=0", mechanism="SCRAM-SHA-256", client_nonce="client")
        with self.assertRaisesRegex(OperationFailure, "requires nonce"):
            scram.build_scram_client_final(
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare="n=user",
                server_first_message="r=clientnonce,s=c2FsdA==,i=4096",
                combined_nonce="clientnonce",
            )

        start = scram.build_scram_client_start(username="ada", nonce="clientnonce")
        server_first = scram.build_scram_server_first(
            client_nonce="clientnonce",
            mechanism="SCRAM-SHA-256",
            salt=b"salt",
            iterations=4096,
            server_nonce="servernonce",
        )
        final = scram.build_scram_client_final(
            password="pencil",
            mechanism="SCRAM-SHA-256",
            client_first_bare=start.first_bare,
            server_first_message=server_first.message,
            combined_nonce=server_first.nonce,
        )
        with self.assertRaisesRegex(OperationFailure, "channel binding data"):
            scram.verify_scram_client_final(
                final.payload.replace(b"c=biws", b"c=bad"),
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare=start.first_bare,
                server_first_message=server_first.message,
            )
        with self.assertRaisesRegex(OperationFailure, "requires proof"):
            scram.verify_scram_client_final(
                b"c=biws,r=clientnonceservernonce",
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare=start.first_bare,
                server_first_message=server_first.message,
            )
        with self.assertRaisesRegex(OperationFailure, "valid base64"):
            scram.verify_scram_client_final(
                b"c=biws,r=clientnonceservernonce,p=***",
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare=start.first_bare,
                server_first_message=server_first.message,
            )
        valid_proof = base64.b64decode(final.payload.decode("utf-8").split("p=", 1)[1].encode("ascii"))
        tampered_proof = bytes([valid_proof[0] ^ 1, *valid_proof[1:]])
        tampered_payload = (
            final.payload.decode("utf-8").split("p=", 1)[0]
            + "p="
            + base64.b64encode(tampered_proof).decode("ascii")
        ).encode("utf-8")
        with self.assertRaisesRegex(OperationFailure, "Authentication failed"):
            scram.verify_scram_client_final(
                tampered_payload,
                password="pencil",
                mechanism="SCRAM-SHA-256",
                client_first_bare=start.first_bare,
                server_first_message=server_first.message,
            )
        with self.assertRaisesRegex(OperationFailure, "authentication failed: auth-failed"):
            scram.verify_scram_server_final(b"e=auth-failed", expected_server_signature="sig")
        with self.assertRaisesRegex(OperationFailure, "requires verifier"):
            scram.verify_scram_server_final(b"r=nonce", expected_server_signature="sig")
        with self.assertRaisesRegex(OperationFailure, "did not match"):
            scram.verify_scram_server_final(b"v=bad", expected_server_signature="sig")

import asyncio
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

import mongoeco.driver as driver_module
import mongoeco.driver._topology_reducer as reducer_module
import mongoeco.driver.execution as execution_module
import mongoeco.driver.uri as uri_module
from mongoeco import MongoClient, ReadPreference, ReadPreferenceMode, parse_mongo_uri
from mongoeco.driver import (
    ConnectionPool,
    ConnectionRegistry,
    DriverRuntime,
    ServerDescription,
    ServerState,
    ServerType,
    TopologyDescription,
    TopologyType,
    build_auth_policy,
    build_local_topology_description,
    build_selection_policy,
    build_tls_policy,
    build_connection_pool_options,
    resolve_srv_dns,
)


class DriverCoverageTests(unittest.TestCase):
    def test_driver_module_getattr_rejects_unknown_export(self):
        with self.assertRaises(AttributeError):
            driver_module.__getattr__("missing")

    def test_uri_and_srv_resolution_helpers_cover_empty_seeds_and_sparse_txt_records(self):
        with self.assertRaisesRegex(ValueError, "at least one host"):
            uri_module._parse_seeds("", scheme="mongodb")

        uri = parse_mongo_uri("mongodb://localhost/?readPreferenceTags=&readPreferenceTags=region:eu")
        self.assertEqual(uri.options.read_preference_tags, ({}, {"region": "eu"}))

        srv_uri = parse_mongo_uri("mongodb+srv://cluster.example.net/?srvMaxHosts=1")

        class _SrvAnswer:
            def __init__(self, target, port):
                self.target = target
                self.port = port

        class _TxtAnswer:
            def __init__(self, strings):
                self.strings = strings

        def _resolver(name: str, record_type: str):
            del name
            if record_type == "SRV":
                return (_SrvAnswer(None, 27017), _SrvAnswer("db1.example.net.", 27018))
            return (_TxtAnswer(()), _TxtAnswer((b"broken", b"replicaSet=rs0")))

        resolution = resolve_srv_dns(srv_uri, resolver=_resolver)

        assert resolution is not None
        self.assertEqual([seed.address for seed in resolution.resolved_seeds], ["db1.example.net:27018"])
        self.assertEqual(resolution.txt_options, {"replicaSet": "rs0"})

        def _resolver_txt_failure(name: str, record_type: str):
            del name
            if record_type == "SRV":
                return (_SrvAnswer("db1.example.net.", 27017),)
            raise RuntimeError("txt lookup failed")

        txt_failure_resolution = resolve_srv_dns(srv_uri, resolver=_resolver_txt_failure)
        assert txt_failure_resolution is not None
        self.assertIsNone(txt_failure_resolution.txt_options)

    def test_selection_security_and_topology_helpers_cover_remaining_branches(self):
        nearest_policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018/?readPreference=nearest"),
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )
        unknown_topology = TopologyDescription(
            topology_type=TopologyType.UNKNOWN,
            servers=(
                ServerDescription("db2:27018", round_trip_time_ms=10.0),
                ServerDescription("db1:27017", round_trip_time_ms=5.0),
            ),
        )
        self.assertEqual(
            [server.address for server in nearest_policy.select_servers(unknown_topology)],
            ["db1:27017", "db2:27018"],
        )

        primary_preferred = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0&readPreference=primaryPreferred"),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY_PREFERRED),
        )
        secondaries_only = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY),),
            set_name="rs0",
        )
        self.assertEqual(
            [server.address for server in primary_preferred.select_servers(secondaries_only)],
            ["db2:27018"],
        )

        writable = nearest_policy.select_servers(
            TopologyDescription(
                topology_type=TopologyType.SINGLE,
                servers=(ServerDescription("db1:27017", server_type=ServerType.STANDALONE),),
            ),
            for_writes=True,
        )
        self.assertEqual([server.address for server in writable], ["db1:27017"])
        tagged_policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017/?readPreference=secondary&readPreferenceTags=region:eu"),
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY, tag_sets=({"region": "eu"},)),
        )
        self.assertEqual(
            tagged_policy._apply_tag_sets((ServerDescription("db1:27017", tags={"region": "us"}),)),
            (),
        )

        with self.assertRaisesRegex(ValueError, "must not include a password"):
            build_auth_policy(parse_mongo_uri("mongodb://user:secret@localhost/?authMechanism=MONGODB-X509&tls=true"))
        with self.assertRaisesRegex(ValueError, "tlsCertificateKeyFile requires TLS to be enabled"):
            build_tls_policy(parse_mongo_uri("mongodb://localhost/?tlsCertificateKeyFile=client.pem"))
        with self.assertRaisesRegex(ValueError, "tlsCAFile requires TLS to be enabled"):
            build_tls_policy(parse_mongo_uri("mongodb://localhost/?tlsCAFile=ca.pem"))
        with self.assertRaisesRegex(ValueError, "MONGODB-X509 requires TLS to be enabled"):
            build_tls_policy(parse_mongo_uri("mongodb://client@localhost/?authMechanism=MONGODB-X509"))

        errored = ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY, error="boom")
        self.assertFalse(errored.is_writable)
        topology = TopologyDescription(topology_type=TopologyType.SINGLE, servers=(errored,))
        self.assertIs(topology.get_server("db1:27017"), errored)
        self.assertIsNone(topology.get_server("db2:27018"))

    def test_topology_reducer_helpers_cover_remaining_branches(self):
        self.assertIsNone(
            reducer_module.derive_set_name(
                (
                    ServerDescription("db1:27017", set_name="rs0"),
                    ServerDescription("db2:27018", set_name="rs1"),
                ),
                topology_type=TopologyType.REPLICA_SET,
            )
        )
        self.assertTrue(
            reducer_module.is_stale_topology_update(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY, set_version=5),
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY, set_version=3),
            )
        )
        self.assertIsNone(reducer_module.topology_version_signature({"processId": 1, "counter": 2}))
        self.assertIsNone(reducer_module.topology_version_signature({"processId": "p1", "counter": True}))

    def test_connection_pool_async_checkout_covers_immediate_deadline_exhaustion(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1&waitQueueTimeoutMS=1")
        topology = build_local_topology_description(uri)
        pool = ConnectionPool(
            ConnectionRegistry(uri).pool_key_for_server(topology.servers[0]),
            build_connection_pool_options(uri),
        )
        checked_out = pool.checkout(topology.servers[0])

        async def _checkout_exhausted() -> None:
            try:
                with patch("mongoeco.driver.connections.time.monotonic", side_effect=[0.0, 1.0]):
                    with self.assertRaisesRegex(RuntimeError, "connection pool exhausted"):
                        await pool.checkout_async(topology.servers[0])
            finally:
                pool.checkin(checked_out.connection_id)

        asyncio.run(_checkout_exhausted())

    def test_driver_runtime_start_topology_monitoring_is_idempotent_for_running_task(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        self.assertEqual(runtime.timeout_policy.server_selection_timeout_ms, 30000)
        runtime._topology_monitor_task = SimpleNamespace(done=lambda: False)

        async def _start_monitoring() -> None:
            with patch("mongoeco.driver.runtime.asyncio.create_task", side_effect=AssertionError("unexpected create_task")):
                await runtime.start_topology_monitoring()

        asyncio.run(_start_monitoring())

    def test_execute_request_pipeline_fallback_branch_is_defensive_only(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("admin", "ping", {"ping": 1}, read_only=True)

        async def _exercise_fallback() -> None:
            result = await execution_module.execute_request_pipeline(
                plan=plan,
                prepare_execution=AsyncMock(),
                complete_execution=AsyncMock(),
                transport=SimpleNamespace(send=AsyncMock()),
            )
            self.assertFalse(result.outcome.ok)
            self.assertEqual(result.outcome.error, "request execution failed")
            self.assertEqual(result.trace.attempts, ())

        with patch.object(execution_module, "range", return_value=(), create=True):
            asyncio.run(_exercise_fallback())

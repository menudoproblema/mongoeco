import asyncio
import unittest

from mongoeco import (
    AsyncMongoClient,
    MongoClient,
    ReadPreference,
    ReadPreferenceMode,
    parse_mongo_uri,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import ConnectionFailure, OperationFailure
from mongoeco.driver import (
    AuthPolicy,
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
    CommandRequest,
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    ConnectionRegistry,
    DriverRuntime,
    LocalCommandTransport,
    PreparedRequestExecution,
    RequestAttempt,
    RequestExecutionPlan,
    RequestExecutionResult,
    RequestExecutionTrace,
    ServerSelectedEvent,
    ServerDescription,
    ServerType,
    SrvResolution,
    TlsPolicy,
    TopologyDescription,
    TopologyType,
    WireProtocolCommandTransport,
    build_auth_policy,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_tls_policy,
    build_write_concern_from_uri,
    classify_request_exception,
    build_local_topology_description,
    materialize_srv_uri,
    resolve_srv_dns,
    resolve_srv_seeds,
    build_selection_policy,
    build_timeout_policy,
    refresh_topology,
)
from mongoeco.wire import AsyncMongoEcoProxyServer, WireAuthUser


class MongoUriTests(unittest.TestCase):
    def test_parse_mongo_uri_defaults_to_localhost(self):
        uri = parse_mongo_uri(None)

        self.assertEqual(uri.scheme, "mongodb")
        self.assertEqual(uri.seeds[0].address, "localhost:27017")
        self.assertEqual(uri.normalized, "mongodb://localhost:27017/")

    def test_parse_mongo_uri_parses_options_and_credentials(self):
        uri = parse_mongo_uri(
            "mongodb://ada:secret@db1:27018,db2:27019/observe"
            "?replicaSet=rs0&retryReads=false&retryWrites=true"
            "&serverSelectionTimeoutMS=1500&connectTimeoutMS=800"
            "&maxPoolSize=20&minPoolSize=5&readPreference=secondaryPreferred"
            "&readPreferenceTags=region:eu,role:analytics"
            "&maxStalenessSeconds=120&w=majority&journal=true&wtimeoutMS=6000"
            "&readConcernLevel=majority&authMechanism=SCRAM-SHA-256"
            "&tls=true&tlsAllowInvalidCertificates=true"
        )

        self.assertEqual(uri.username, "ada")
        self.assertEqual(uri.password, "secret")
        self.assertEqual(uri.default_database, "observe")
        self.assertEqual([seed.address for seed in uri.seeds], ["db1:27018", "db2:27019"])
        self.assertEqual(uri.options.replica_set, "rs0")
        self.assertEqual(uri.options.auth.source, None)
        self.assertEqual(uri.options.auth.mechanism, "SCRAM-SHA-256")
        self.assertTrue(uri.options.tls.enabled)
        self.assertTrue(uri.options.tls.allow_invalid_certificates)
        self.assertFalse(uri.options.retry_reads)
        self.assertTrue(uri.options.retry_writes)
        self.assertEqual(uri.options.server_selection_timeout_ms, 1500)
        self.assertEqual(uri.options.connect_timeout_ms, 800)
        self.assertEqual(uri.options.max_pool_size, 20)
        self.assertEqual(uri.options.min_pool_size, 5)
        self.assertEqual(uri.options.read_preference, "secondaryPreferred")
        self.assertEqual(uri.options.read_preference_tags, ({"region": "eu", "role": "analytics"},))
        self.assertEqual(uri.options.max_staleness_seconds, 120)
        self.assertEqual(uri.options.write_concern_w, "majority")
        self.assertTrue(uri.options.write_concern_journal)
        self.assertEqual(uri.options.write_concern_wtimeout_ms, 6000)
        self.assertEqual(uri.options.read_concern_level, "majority")

    def test_parse_mongo_uri_rejects_invalid_pool_bounds(self):
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb://localhost/?maxPoolSize=1&minPoolSize=2")

    def test_parse_mongo_uri_rejects_conflicting_direct_connection_and_load_balanced(self):
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb://localhost/?directConnection=true&loadBalanced=true")

    def test_parse_mongodb_srv_defaults_tls_and_rejects_multiple_hosts(self):
        uri = parse_mongo_uri("mongodb+srv://cluster.example.net/?srvServiceName=custom")

        self.assertEqual(uri.scheme, "mongodb+srv")
        self.assertTrue(uri.options.tls.enabled)
        self.assertEqual(uri.options.srv_service_name, "custom")

        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb+srv://a.example.net,b.example.net/")

    def test_parse_uri_rejects_password_without_username(self):
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb://:secret@localhost/")

    def test_parse_uri_rejects_srv_with_direct_connection(self):
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb+srv://cluster.example.net/?directConnection=true")

    def test_build_concerns_and_preference_from_uri(self):
        uri = parse_mongo_uri(
            "mongodb://localhost/?w=majority&journal=true&wtimeoutMS=5000"
            "&readConcernLevel=snapshot&readPreference=secondary"
            "&readPreferenceTags=region:eu&maxStalenessSeconds=90"
        )

        write_concern = build_write_concern_from_uri(uri, MongoClient().write_concern)
        read_concern = build_read_concern_from_uri(uri, MongoClient().read_concern)
        read_preference = build_read_preference_from_uri(
            uri,
            ReadPreference(ReadPreferenceMode.PRIMARY),
        )

        self.assertEqual(write_concern.w, "majority")
        self.assertTrue(write_concern.j)
        self.assertEqual(write_concern.wtimeout, 5000)
        self.assertEqual(read_concern.level, "snapshot")
        self.assertEqual(read_preference.mode, ReadPreferenceMode.SECONDARY)
        self.assertEqual(read_preference.tag_sets, ({"region": "eu"},))
        self.assertEqual(read_preference.max_staleness_seconds, 90)

    def test_srv_resolution_materializes_effective_uri(self):
        uri = parse_mongo_uri("mongodb+srv://cluster.example.net/?srvMaxHosts=2")

        resolution = resolve_srv_seeds(
            uri,
            srv_records=(
                ("db1.example.net", 27017),
                ("db2.example.net", 27018),
                ("db3.example.net", 27019),
            ),
        )
        effective_uri = materialize_srv_uri(uri, resolution=resolution)

        self.assertIsInstance(resolution, SrvResolution)
        self.assertEqual([seed.address for seed in resolution.resolved_seeds], ["db1.example.net:27017", "db2.example.net:27018"])
        self.assertEqual([seed.address for seed in effective_uri.seeds], ["db1.example.net:27017", "db2.example.net:27018"])

    def test_resolve_srv_dns_uses_resolver_and_applies_txt_options(self):
        class _SrvAnswer:
            def __init__(self, target: str, port: int):
                self.target = target
                self.port = port

        class _TxtAnswer:
            def __init__(self, *strings: bytes):
                self.strings = strings

        def _resolver(name: str, record_type: str):
            if record_type == "SRV":
                self.assertEqual(name, "_mongodb._tcp.cluster.example.net")
                return (
                    _SrvAnswer("db1.example.net.", 27017),
                    _SrvAnswer("db2.example.net.", 27018),
                )
            if record_type == "TXT":
                self.assertEqual(name, "cluster.example.net")
                return (_TxtAnswer(b"replicaSet=rs0&readPreference=secondary"),)
            raise AssertionError((name, record_type))

        uri = parse_mongo_uri("mongodb+srv://cluster.example.net/?srvMaxHosts=1")

        resolution = resolve_srv_dns(uri, resolver=_resolver)
        effective_uri = materialize_srv_uri(uri, resolution=resolution)

        self.assertIsNotNone(resolution)
        self.assertEqual([seed.address for seed in resolution.resolved_seeds], ["db1.example.net:27017"])
        self.assertEqual(resolution.txt_options, {"replicaSet": "rs0", "readPreference": "secondary"})
        self.assertEqual(effective_uri.options.replica_set, "rs0")
        self.assertEqual(effective_uri.options.read_preference, "secondary")

    def test_resolve_srv_dns_falls_back_to_synthetic_seed_when_lookup_fails(self):
        uri = parse_mongo_uri("mongodb+srv://cluster.example.net/")

        def _resolver(name: str, record_type: str):
            raise RuntimeError(f"lookup failed for {name} {record_type}")

        resolution = resolve_srv_dns(uri, resolver=_resolver)

        self.assertIsNotNone(resolution)
        self.assertEqual([seed.address for seed in resolution.resolved_seeds], ["cluster.example.net:27017"])

    def test_build_auth_and_tls_policies_validate_mechanism_constraints(self):
        x509_uri = parse_mongo_uri(
            "mongodb://client@localhost/?authMechanism=MONGODB-X509&tls=true"
        )

        auth_policy = build_auth_policy(x509_uri)
        tls_policy = build_tls_policy(x509_uri)

        self.assertIsInstance(auth_policy, AuthPolicy)
        self.assertTrue(auth_policy.external)
        self.assertEqual(auth_policy.source, "$external")
        self.assertIsInstance(tls_policy, TlsPolicy)
        self.assertTrue(tls_policy.enabled)

        with self.assertRaises(ValueError):
            build_auth_policy(parse_mongo_uri("mongodb://localhost/?authMechanism=SCRAM-SHA-256"))

        with self.assertRaises(ValueError):
            build_tls_policy(parse_mongo_uri("mongodb://localhost/?authMechanism=MONGODB-X509"))


class TopologyAndPolicyTests(unittest.TestCase):
    def test_build_local_topology_description_reflects_replica_set(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.REPLICA_SET)
        self.assertEqual(topology.servers[0].server_type, ServerType.RS_PRIMARY)
        self.assertEqual(topology.servers[1].server_type, ServerType.RS_SECONDARY)
        self.assertEqual(topology.set_name, "rs0")

    def test_build_local_topology_description_marks_load_balanced_as_sharded(self):
        uri = parse_mongo_uri("mongodb://lb1/?loadBalanced=true")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.SHARDED)
        self.assertEqual(topology.servers[0].server_type, ServerType.MONGOS)

    def test_selection_policy_prefers_direct_connection_seed(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?directConnection=true")
        topology = build_local_topology_description(uri)
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db1:27017"])

    def test_selection_policy_for_secondary_prefers_secondaries_in_replica_set(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0")
        topology = build_local_topology_description(uri)
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db2:27018"])

    def test_selection_policy_filters_replica_set_secondaries_by_tags(self):
        uri = parse_mongo_uri(
            "mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0"
            "&readPreference=secondary&readPreferenceTags=region:eu"
        )
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY, tags={"region": "us"}),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, tags={"region": "eu"}),
                ServerDescription("db3:27019", server_type=ServerType.RS_SECONDARY, tags={"region": "us"}),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=build_read_preference_from_uri(uri, ReadPreference(ReadPreferenceMode.PRIMARY)),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db2:27018"])

    def test_selection_policy_applies_max_staleness_to_secondaries(self):
        uri = parse_mongo_uri(
            "mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0"
            "&readPreference=secondaryPreferred&maxStalenessSeconds=90"
        )
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, staleness_seconds=45),
                ServerDescription("db3:27019", server_type=ServerType.RS_SECONDARY, staleness_seconds=180),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=build_read_preference_from_uri(uri, ReadPreference(ReadPreferenceMode.PRIMARY)),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db2:27018"])

    def test_selection_policy_ignores_hidden_and_arbiter_members(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0&readPreference=secondaryPreferred")
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, hidden=True),
                ServerDescription("db3:27019", server_type=ServerType.RS_SECONDARY, arbiter_only=True),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=build_read_preference_from_uri(uri, ReadPreference(ReadPreferenceMode.PRIMARY)),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db1:27017"])

    def test_selection_policy_nearest_orders_by_round_trip_time(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0")
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY, round_trip_time_ms=20.0),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, round_trip_time_ms=5.0),
                ServerDescription("db3:27019", server_type=ServerType.RS_SECONDARY, round_trip_time_ms=10.0),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )

        self.assertEqual(
            [server.address for server in policy.select_servers(topology)],
            ["db2:27018", "db3:27019", "db1:27017"],
        )

    def test_secondary_preferred_falls_back_to_primary_when_staleness_filters_all_secondaries(self):
        uri = parse_mongo_uri(
            "mongodb://db1:27017,db2:27018/?replicaSet=rs0"
            "&readPreference=secondaryPreferred&maxStalenessSeconds=10"
        )
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, staleness_seconds=30),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=build_read_preference_from_uri(uri, ReadPreference(ReadPreferenceMode.PRIMARY)),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db1:27017"])

    def test_timeout_policy_is_derived_from_uri(self):
        uri = parse_mongo_uri(
            "mongodb://localhost/?serverSelectionTimeoutMS=1200&connectTimeoutMS=700&socketTimeoutMS=5000"
        )
        policy = build_timeout_policy(uri)

        self.assertEqual(policy.server_selection_timeout_ms, 1200)
        self.assertEqual(policy.connect_timeout_ms, 700)
        self.assertEqual(policy.socket_timeout_ms, 5000)


class ConnectionArchitectureTests(unittest.TestCase):
    def test_connection_registry_creates_and_reuses_server_pool(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=3&authSource=admin")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        first = registry.checkout(topology.servers[0])
        snapshots = registry.snapshots()

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].total_size, 1)
        self.assertEqual(snapshots[0].checked_out, 1)
        self.assertEqual(first.pool_key.auth_source, "admin")

        registry.checkin(first)
        reused = registry.checkout(topology.servers[0])

        self.assertEqual(first.connection_id, reused.connection_id)
        registry.checkin(reused)

    def test_connection_registry_can_clear_all_pools(self):
        uri = parse_mongo_uri("mongodb://db1:27017/")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        lease = registry.checkout(topology.servers[0])
        registry.checkin(lease)
        registry.clear()

        self.assertEqual(registry.snapshots(), ())

    def test_connection_registry_prunes_idle_connections(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxIdleTimeMS=1")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        first = registry.checkout(topology.servers[0])
        registry.checkin(first)
        connection = registry.get_connection(first)
        assert connection is not None
        connection.last_used_at_monotonic -= 10

        second = registry.checkout(topology.servers[0])

        self.assertNotEqual(first.connection_id, second.connection_id)
        registry.checkin(second)

    def test_connection_registry_discard_removes_connection(self):
        uri = parse_mongo_uri("mongodb://db1:27017/")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        lease = registry.checkout(topology.servers[0])
        registry.discard(lease)

        self.assertIsNone(registry.get_connection(lease))


class ClientDriverArchitectureTests(unittest.TestCase):
    def test_async_client_exposes_driver_state_and_request_planning(self):
        client = AsyncMongoClient(
            uri="mongodb://db1:27017/?appName=test-suite&retryReads=false",
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual(client.client_uri.options.app_name, "test-suite")
        self.assertEqual(client.topology_description.topology_type, TopologyType.SINGLE)
        self.assertFalse(client.retry_policy.retry_reads)
        self.assertEqual(client.selection_policy.mode, ReadPreferenceMode.SECONDARY)

        plan = client.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        self.assertIsInstance(plan, RequestExecutionPlan)
        self.assertIsInstance(plan.request, CommandRequest)
        self.assertEqual(plan.request.database, "observe")
        self.assertEqual(plan.request.command_name, "find")
        self.assertEqual(plan.primary_server.address, "db1:27017")

    def test_driver_runtime_prepares_and_completes_request_execution(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?maxPoolSize=4",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request(
            "observe",
            "find",
            {"find": "events"},
            read_only=True,
        )

        execution = runtime.prepare_request_execution(plan)

        self.assertIsInstance(execution, PreparedRequestExecution)
        self.assertEqual(execution.selected_server.address, "db1:27017")
        self.assertEqual(runtime.connection_snapshots[0].checked_out, 1)

        runtime.complete_request_execution(execution)

        self.assertEqual(runtime.connection_snapshots[0].checked_out, 0)

    def test_driver_runtime_exposes_srv_auth_and_tls_state(self):
        runtime = DriverRuntime(
            uri="mongodb+srv://client@cluster.example.net/admin?authMechanism=PLAIN&srvMaxHosts=1",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
            srv_records=(("db1.example.net", 27017), ("db2.example.net", 27018)),
        )

        self.assertEqual(runtime.uri.scheme, "mongodb+srv")
        self.assertEqual(runtime.effective_uri.seeds[0].address, "db1.example.net:27017")
        self.assertTrue(runtime.tls_policy.enabled)
        self.assertEqual(runtime.auth_policy.source, "$external")
        self.assertEqual(runtime.srv_resolution.max_hosts, 1)

        plan = runtime.plan_command_request("admin", "ping", {"ping": 1}, read_only=True)

        self.assertTrue(plan.tls_policy.enabled)
        self.assertEqual(plan.auth_policy.mechanism, "PLAIN")

    def test_sync_client_with_options_preserves_uri_architecture_state(self):
        client = MongoClient(
            uri="mongodb://db1:27017/?retryWrites=false",
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )

        tuned = client.with_options()

        self.assertEqual(tuned.client_uri.original, client.client_uri.original)
        self.assertFalse(tuned.retry_policy.retry_writes)
        self.assertEqual(tuned.selection_policy.mode, ReadPreferenceMode.NEAREST)

    def test_sync_client_exposes_driver_runtime(self):
        client = MongoClient(uri="mongodb://db1:27017/")

        snapshots = client.driver_runtime.connection_snapshots

        self.assertEqual(snapshots, ())

    def test_client_derives_concerns_from_uri_when_present(self):
        client = AsyncMongoClient(
            uri="mongodb://db1:27017/?w=majority&journal=true&readConcernLevel=majority&readPreference=secondaryPreferred"
        )

        self.assertEqual(client.write_concern.w, "majority")
        self.assertTrue(client.write_concern.j)
        self.assertEqual(client.read_concern.level, "majority")
        self.assertEqual(client.read_preference.mode, ReadPreferenceMode.SECONDARY_PREFERRED)

    def test_client_exposes_auth_tls_and_effective_uri(self):
        client = AsyncMongoClient(
            uri="mongodb+srv://client@cluster.example.net/admin?authMechanism=PLAIN",
        )

        self.assertEqual(client.auth_policy.source, "$external")
        self.assertTrue(client.tls_policy.enabled)
        self.assertEqual(client.effective_client_uri.seeds[0].address, "cluster.example.net:27017")

    def test_async_client_execute_driver_command_uses_local_transport_and_records_monitoring(self):
        client = AsyncMongoClient(uri="mongodb://db1:27017/")

        result = asyncio.run(
            client.execute_driver_command(
                "admin",
                "ping",
                {"ping": 1},
                read_only=True,
            )
        )

        self.assertTrue(result.outcome.ok)
        self.assertEqual(result.outcome.response["ok"], 1.0)
        events = client.driver_monitor.history
        self.assertIsInstance(events[0], ServerSelectedEvent)
        self.assertIsInstance(events[1], ConnectionCheckedOutEvent)
        self.assertIsInstance(events[2], CommandStartedEvent)
        self.assertIsInstance(events[3], CommandSucceededEvent)
        self.assertIsInstance(events[4], ConnectionCheckedInEvent)

    def test_sync_client_execute_driver_command_wraps_async_runtime(self):
        client = MongoClient(uri="mongodb://db1:27017/")

        result = client.execute_driver_command(
            "admin",
            "ping",
            {"ping": 1},
            read_only=True,
        )

        self.assertTrue(result.outcome.ok)
        self.assertEqual(result.outcome.response["ok"], 1.0)

    def test_network_transport_executes_against_wire_proxy_and_reuses_pool(self):
        async def _run() -> tuple[RequestExecutionResult, RequestExecutionResult, tuple]:
            async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
                client = AsyncMongoClient(uri=proxy.address.uri)
                await client._engine.connect()
                try:
                    result_one = await client.execute_network_command(
                        "admin",
                        "ping",
                        {"ping": 1},
                        read_only=True,
                    )
                    result_two = await client.execute_network_command(
                        "admin",
                        "ping",
                        {"ping": 1},
                        read_only=True,
                    )
                    return result_one, result_two, client.driver_runtime.connection_snapshots
                finally:
                    await client.close()

        result_one, result_two, snapshots = asyncio.run(_run())

        self.assertTrue(result_one.outcome.ok)
        self.assertTrue(result_two.outcome.ok)
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].total_size, 1)

    def test_refresh_topology_uses_real_wire_hello(self):
        async def _run() -> TopologyDescription:
            async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
                client = AsyncMongoClient(uri=proxy.address.uri)
                await client._engine.connect()
                try:
                    return await client.refresh_topology()
                finally:
                    await client.close()

        topology = asyncio.run(_run())

        self.assertEqual(topology.topology_type, TopologyType.SINGLE)
        self.assertEqual(len(topology.servers), 1)
        self.assertEqual(topology.servers[0].server_type, ServerType.STANDALONE)
        self.assertIsNone(topology.servers[0].error)

    def test_refresh_topology_marks_hidden_and_arbiter_members_non_readable(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
        )

        class HelloTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                address = execution.selected_server.address
                if address == "db1:27017":
                    return {"ok": 1.0, "setName": "rs0", "isWritablePrimary": True}
                if address == "db2:27018":
                    return {"ok": 1.0, "setName": "rs0", "secondary": True, "hidden": True}
                return {"ok": 1.0, "setName": "rs0", "secondary": True, "arbiterOnly": True}

        topology = asyncio.run(runtime.refresh_topology(transport=HelloTransport()))
        policy = runtime.selection_policy

        self.assertEqual(topology.topology_type, TopologyType.REPLICA_SET)
        self.assertFalse(topology.servers[1].is_readable)
        self.assertFalse(topology.servers[2].is_readable)
        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db1:27017"])

    def test_async_client_can_start_and_stop_topology_monitoring(self):
        async def _run() -> TopologyDescription:
            async with AsyncMongoEcoProxyServer(engine=MemoryEngine()) as proxy:
                client = AsyncMongoClient(uri=f"{proxy.address.uri}&heartbeatFrequencyMS=10")
                await client._engine.connect()
                try:
                    await client.start_topology_monitoring()
                    await asyncio.sleep(0.03)
                    await client.stop_topology_monitoring()
                    return client.topology_description
                finally:
                    await client.close()

        topology = asyncio.run(_run())

        self.assertEqual(topology.topology_type, TopologyType.SINGLE)

    def test_network_transport_authenticates_against_wire_proxy(self):
        async def _run() -> tuple[dict[str, object], dict[str, object]]:
            async with AsyncMongoEcoProxyServer(
                engine=MemoryEngine(),
                auth_users=(
                    WireAuthUser(
                        username="ada",
                        password="secret",
                        source="admin",
                        mechanisms=("SCRAM-SHA-256",),
                        roles=({"role": "root", "db": "admin"},),
                    ),
                ),
            ) as proxy:
                uri = (
                    f"mongodb://ada:secret@{proxy.address.host}:{proxy.address.port}/admin"
                    "?directConnection=true&authMechanism=SCRAM-SHA-256&authSource=admin"
                )
                client = AsyncMongoClient(uri=uri)
                await client._engine.connect()
                try:
                    ping = await client.execute_network_command(
                        "admin",
                        "ping",
                        {"ping": 1},
                        read_only=True,
                    )
                    status = await client.execute_network_command(
                        "admin",
                        "connectionStatus",
                        {"connectionStatus": 1},
                        read_only=True,
                    )
                    return ping.outcome.response, status.outcome.response
                finally:
                    await client.close()

        ping, status = asyncio.run(_run())

        self.assertEqual(ping["ok"], 1.0)
        self.assertEqual(
            status["authInfo"]["authenticatedUsers"],
            [{"user": "ada", "db": "admin", "mechanism": "SCRAM-SHA-256"}],
        )
        self.assertEqual(
            status["authInfo"]["authenticatedUserRoles"],
            [{"role": "root", "db": "admin"}],
        )

    def test_network_transport_requires_auth_when_proxy_enables_it(self):
        async def _run() -> RequestExecutionResult:
            async with AsyncMongoEcoProxyServer(
                engine=MemoryEngine(),
                auth_users=(WireAuthUser(username="ada", password="secret", source="admin"),),
            ) as proxy:
                client = AsyncMongoClient(uri=proxy.address.uri)
                await client._engine.connect()
                try:
                    return await client.execute_network_command(
                        "admin",
                        "ping",
                        {"ping": 1},
                        read_only=True,
                    )
                finally:
                    await client.close()

        result = asyncio.run(_run())

        self.assertFalse(result.outcome.ok)
        self.assertIn("Authentication required", result.outcome.error)


class RequestExecutionPipelineTests(unittest.TestCase):
    def test_execute_request_retries_retryable_read_errors(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0&retryReads=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)
        seen: list[str] = []

        class RetryReadTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                seen.append(execution.selected_server.address)
                if len(seen) == 1:
                    raise OperationFailure(
                        "retryable read failure",
                        error_labels=("RetryableReadError",),
                    )
                return {"ok": 1.0, "cursor": {"id": 0}}

        result = asyncio.run(runtime.execute_request(plan, RetryReadTransport()))

        self.assertIsInstance(result, RequestExecutionResult)
        self.assertTrue(result.outcome.ok)
        self.assertEqual(seen, ["db2:27018", "db3:27019"])
        self.assertEqual(len(result.trace.attempts), 2)

    def test_execute_request_retries_connection_failures_for_writes(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018/?replicaSet=rs0&retryWrites=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "insert", {"insert": "events"}, read_only=False)
        seen: list[str] = []

        class RetryWriteTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                seen.append(execution.selected_server.address)
                if len(seen) == 1:
                    raise ConnectionFailure("temporary network failure")
                return {"ok": 1.0, "n": 1}

        result = asyncio.run(runtime.execute_request(plan, RetryWriteTransport()))

        self.assertTrue(result.outcome.ok)
        self.assertEqual(seen, ["db1:27017", "db1:27017"])

    def test_execute_request_honors_socket_timeout(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?socketTimeoutMS=5",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        class SlowTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                del execution
                await asyncio.sleep(0.05)
                return {"ok": 1.0}

        result = asyncio.run(runtime.execute_request(plan, SlowTransport()))

        self.assertFalse(result.outcome.ok)
        self.assertIn("socket timeout", result.outcome.error or "")
        self.assertFalse(result.outcome.retryable)

    def test_classify_request_exception_marks_retryable_labels(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?retryReads=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        outcome = classify_request_exception(
            OperationFailure("read failed", error_labels=("RetryableReadError",)),
            plan=plan,
        )

        self.assertFalse(outcome.ok)
        self.assertTrue(outcome.retryable)

    def test_execute_request_trace_captures_attempt_metadata(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?retryReads=false",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        class FailingTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                raise ConnectionFailure(f"failed on {execution.selected_server.address}")

        result = asyncio.run(runtime.execute_request(plan, FailingTransport()))

        self.assertFalse(result.outcome.ok)
        self.assertIsInstance(result.trace, RequestExecutionTrace)
        self.assertEqual(len(result.trace.attempts), 1)
        self.assertIsInstance(result.trace.attempts[0], RequestAttempt)

    def test_execute_request_monitoring_tracks_retry_lifecycle(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018/?replicaSet=rs0&retryReads=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        class RetryTransport:
            def __init__(self):
                self._calls = 0

            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                self._calls += 1
                if self._calls == 1:
                    raise OperationFailure(
                        "retryable read failure",
                        error_labels=("RetryableReadError",),
                    )
                return {"ok": 1.0, "cursor": {"id": 0}}

        result = asyncio.run(runtime.execute_request(plan, RetryTransport()))

        self.assertTrue(result.outcome.ok)
        event_types = [type(event) for event in runtime.monitor.history]
        self.assertEqual(
            event_types,
            [
                ServerSelectedEvent,
                ConnectionCheckedOutEvent,
                CommandStartedEvent,
                CommandFailedEvent,
                ConnectionCheckedInEvent,
                ServerSelectedEvent,
                ConnectionCheckedOutEvent,
                CommandStartedEvent,
                CommandSucceededEvent,
                ConnectionCheckedInEvent,
            ],
        )
        self.assertTrue(runtime.monitor.history[3].retryable)

    def test_local_command_transport_executes_database_commands_with_session(self):
        client = AsyncMongoClient(uri="mongodb://db1:27017/")
        session = client.start_session()
        plan = client.plan_command_request(
            "admin",
            "ping",
            {"ping": 1},
            session=session,
            read_only=True,
        )
        execution = client.prepare_command_request_execution(
            "admin",
            "ping",
            {"ping": 1},
            session=session,
            read_only=True,
        )

        try:
            response = asyncio.run(LocalCommandTransport(client).send(execution))
        finally:
            client.complete_command_request_execution(execution)

        self.assertEqual(plan.request.session_id, session.session_id)
        self.assertEqual(response["ok"], 1.0)

    def test_wire_protocol_transport_is_constructible_from_runtime(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )

        transport = runtime.create_network_transport()

        self.assertIsInstance(transport, WireProtocolCommandTransport)

    def test_execute_request_returns_selection_timeout_when_no_candidates(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?directConnection=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        runtime._topology = TopologyDescription(topology_type=TopologyType.UNKNOWN, servers=())  # type: ignore[attr-defined]
        plan = runtime.plan_command_request("admin", "ping", {"ping": 1}, read_only=True)

        class NoopTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                del execution
                return {"ok": 1.0}

        result = asyncio.run(runtime.execute_request(plan, NoopTransport()))

        self.assertFalse(result.outcome.ok)
        self.assertIn("no eligible servers", result.outcome.error or "")

    def test_execute_request_discards_broken_connection_on_failure(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?retryReads=false",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        class BrokenTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                raise ConnectionFailure(f"socket closed on {execution.selected_server.address}")

        result = asyncio.run(runtime.execute_request(plan, BrokenTransport()))

        self.assertFalse(result.outcome.ok)
        self.assertEqual(runtime.connection_snapshots[0].total_size, 0)

    def test_refresh_topology_derives_replica_set_membership_from_hello(self):
        topology = TopologyDescription(
            topology_type=TopologyType.UNKNOWN,
            servers=(
                ServerDescription("db1:27017"),
                ServerDescription("db2:27018"),
            ),
        )

        def prepare(plan, *, attempt_number):
            del attempt_number
            return PreparedRequestExecution(
                plan=plan,
                selected_server=plan.candidate_servers[0],
                connection=type(
                    "Lease",
                    (),
                    {
                        "pool_key": None,
                        "connection_id": plan.candidate_servers[0].address,
                        "server": plan.candidate_servers[0],
                    },
                )(),
            )

        def complete(execution):
            del execution

        class ReplicaSetHelloTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                if execution.selected_server.address == "db1:27017":
                    return {
                        "ok": 1.0,
                        "setName": "rs0",
                        "isWritablePrimary": True,
                        "minWireVersion": 0,
                        "maxWireVersion": 20,
                        "logicalSessionTimeoutMinutes": 30,
                    }
                return {
                    "ok": 1.0,
                    "setName": "rs0",
                    "secondary": True,
                    "minWireVersion": 0,
                    "maxWireVersion": 20,
                    "logicalSessionTimeoutMinutes": 30,
                    "tags": {"region": "eu"},
                }

        refreshed = asyncio.run(
            refresh_topology(
                current_topology=topology,
                prepare_execution=prepare,
                complete_execution=complete,
                transport=ReplicaSetHelloTransport(),
            )
        )

        self.assertEqual(refreshed.topology_type, TopologyType.REPLICA_SET)
        self.assertEqual(refreshed.servers[0].server_type, ServerType.RS_PRIMARY)
        self.assertEqual(refreshed.servers[1].server_type, ServerType.RS_SECONDARY)
        self.assertEqual(refreshed.servers[1].tags, {"region": "eu"})

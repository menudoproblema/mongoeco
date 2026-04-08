import asyncio
import ast
from pathlib import Path
import unittest

from mongoeco import (
    AsyncMongoClient,
    MongoClient,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    WriteConcern,
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
    ServerSelectionFailedEvent,
    ServerDescription,
    ServerState,
    ServerType,
    SrvResolution,
    TlsPolicy,
    TopologyDescription,
    TopologyType,
    WireProtocolCommandTransport,
    build_auth_policy,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_retry_policy,
    build_tls_policy,
    build_concern_policy,
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
from mongoeco.driver.connections import ConnectionPool, build_connection_pool_options
from mongoeco.driver.execution import _is_retryable_exception
from mongoeco.driver._runtime_attempts import RuntimeAttemptLifecycle
from mongoeco.driver._runtime_plan_resolution import resolve_runtime_execution_plan
from mongoeco.driver.topology_monitor import build_probe_plan
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
    def test_driver_runtime_module_stays_split_between_planning_resolution_and_attempts(self):
        module_path = Path(__file__).resolve().parents[2] / "src" / "mongoeco" / "driver" / "runtime.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }

        self.assertIn("mongoeco.driver._runtime_planning", imported_modules)
        self.assertIn("mongoeco.driver._runtime_plan_resolution", imported_modules)
        self.assertIn("mongoeco.driver._runtime_attempts", imported_modules)

    def test_build_local_topology_description_reflects_replica_set(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.REPLICA_SET)
        self.assertEqual(topology.servers[0].server_type, ServerType.UNKNOWN)
        self.assertEqual(topology.servers[1].server_type, ServerType.UNKNOWN)
        self.assertEqual(topology.set_name, "rs0")

    def test_build_local_topology_description_marks_load_balanced_as_sharded(self):
        uri = parse_mongo_uri("mongodb://lb1/?loadBalanced=true")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.SHARDED)
        self.assertEqual(topology.servers[0].server_type, ServerType.MONGOS)

    def test_build_local_topology_description_keeps_single_seed_unknown_without_direct_connection(self):
        uri = parse_mongo_uri("mongodb://db1:27017/")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.UNKNOWN)
        self.assertEqual(topology.servers[0].server_type, ServerType.UNKNOWN)

    def test_build_local_topology_description_prefers_replica_set_over_single_seed_shape(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?replicaSet=rs0")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.REPLICA_SET)
        self.assertEqual(topology.servers[0].server_type, ServerType.UNKNOWN)

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
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db2:27018"])

    def test_selection_policy_uses_all_replica_set_seeds_before_discovery(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0")
        topology = build_local_topology_description(uri)
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual(
            [server.address for server in policy.select_servers(topology)],
            ["db1:27017", "db2:27018", "db3:27019"],
        )

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
            "&readPreference=secondaryPreferred&maxStalenessSeconds=90"
        )
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),
                ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY, staleness_seconds=120),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            uri,
            read_preference=build_read_preference_from_uri(uri, ReadPreference(ReadPreferenceMode.PRIMARY)),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db1:27017"])

    def test_selection_policy_replica_set_primary_mode_ignores_standalone_and_mongos_members(self):
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription("db1:27017", server_type=ServerType.STANDALONE),
                ServerDescription("db2:27018", server_type=ServerType.MONGOS),
                ServerDescription("db3:27019", server_type=ServerType.RS_PRIMARY),
            ),
            set_name="rs0",
        )
        policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018,db3:27019/?replicaSet=rs0"),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )

        self.assertEqual([server.address for server in policy.select_servers(topology)], ["db3:27019"])

    def test_timeout_policy_is_derived_from_uri(self):
        uri = parse_mongo_uri(
            "mongodb://localhost/?serverSelectionTimeoutMS=1200&connectTimeoutMS=700&socketTimeoutMS=5000&waitQueueTimeoutMS=900"
        )
        policy = build_timeout_policy(uri)

        self.assertEqual(policy.server_selection_timeout_ms, 1200)
        self.assertEqual(policy.connect_timeout_ms, 700)
        self.assertEqual(policy.socket_timeout_ms, 5000)
        self.assertEqual(policy.wait_queue_timeout_ms, 900)

    def test_policy_and_discovery_helpers_cover_small_remaining_branches(self):
        import mongoeco.driver.discovery as discovery_module
        import mongoeco.driver.monitoring as monitoring_module
        import mongoeco.driver.policies as policies_module

        non_srv_uri = parse_mongo_uri("mongodb://localhost/")
        self.assertIsNone(resolve_srv_seeds(non_srv_uri))
        self.assertIsNone(resolve_srv_dns(non_srv_uri, resolver=lambda *_args: ()))
        with self.assertRaisesRegex(ValueError, "at least one seed"):
            resolve_srv_seeds(parse_mongo_uri("mongodb+srv://cluster.example.net/"), srv_records=())

        resolution = resolve_srv_seeds(
            parse_mongo_uri("mongodb+srv://cluster.example.net/?srvMaxHosts=1"),
            srv_records=(discovery_module.MongoUriSeed("db1.example.net", None),),
        )
        self.assertEqual(resolution.resolved_seeds[0].address, "db1.example.net:27017")
        self.assertIs(materialize_srv_uri(non_srv_uri, resolution=None), non_srv_uri)

        policy = policies_module.SelectionPolicy(mode=ReadPreferenceMode.NEAREST)
        self.assertTrue(policies_module._server_matches_tag_set(ServerDescription("db1:27017"), {}))
        self.assertEqual(policies_module._server_state_sort_weight(ServerState.UNKNOWN), 3)
        self.assertEqual(policies_module._server_state_sort_weight(ServerState.UNREACHABLE), 4)
        servers = (
            ServerDescription("db2:27018", state=ServerState.DEGRADED, round_trip_time_ms=None),
            ServerDescription("db1:27017", state=ServerState.HEALTHY, round_trip_time_ms=5.0),
        )
        self.assertEqual([server.address for server in policy._order_nearest(servers)], ["db1:27017", "db2:27018"])

        monitor = monitoring_module.DriverMonitor()
        seen = []
        listener = seen.append
        event = ServerSelectedEvent(
            database="db",
            command_name="find",
            server_address="db1:27017",
            attempt_number=1,
            read_only=True,
        )
        monitor.add_listener(listener)
        monitor.emit(event)
        monitor.remove_listener(lambda _event: None)
        monitor.clear_listeners()
        self.assertEqual(monitor.history, (event,))
        self.assertEqual(seen, [event])
        monitor.clear_history()
        self.assertEqual(monitor.history, ())

    def test_driver_helper_builders_cover_small_constructor_paths(self):
        uri = parse_mongo_uri("mongodb://localhost/?retryReads=false&retryWrites=false")
        retry = build_retry_policy(uri)
        concern = build_concern_policy(
            write_concern=WriteConcern(1),
            read_concern=ReadConcern("local"),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        self.assertFalse(retry.retry_reads)
        self.assertFalse(retry.retry_writes)
        self.assertEqual(concern.write_concern, WriteConcern(1))

    def test_selection_policy_covers_unknown_and_non_replica_set_edge_paths(self):
        policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018/?readPreference=primaryPreferred"),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY_PREFERRED),
        )
        unknown_topology = TopologyDescription(
            topology_type=TopologyType.UNKNOWN,
            servers=(ServerDescription("db2:27018"), ServerDescription("db1:27017")),
        )
        self.assertEqual(
            [server.address for server in policy.select_servers(unknown_topology)],
            ["db1:27017", "db2:27018"],
        )

        non_repl_policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017/?readPreference=nearest"),
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )
        sharded = TopologyDescription(
            topology_type=TopologyType.SHARDED,
            servers=(
                ServerDescription("db2:27018", server_type=ServerType.MONGOS, round_trip_time_ms=12),
                ServerDescription("db1:27017", server_type=ServerType.MONGOS, round_trip_time_ms=5),
            ),
        )
        self.assertEqual(
            [server.address for server in non_repl_policy.select_servers(sharded)],
            ["db1:27017", "db2:27018"],
        )

        secondary_preferred = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0&readPreference=secondaryPreferred"),
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
        )
        replica_set = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(ServerDescription("db1:27017", server_type=ServerType.RS_PRIMARY),),
            set_name="rs0",
        )
        self.assertEqual(
            [server.address for server in secondary_preferred.select_servers(replica_set)],
            ["db1:27017"],
        )


class ConnectionArchitectureTests(unittest.TestCase):
    def test_connection_pool_helpers_cover_exhaustion_options_and_missing_pool_paths(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1")
        topology = build_local_topology_description(uri)
        pool = ConnectionPool(
            ConnectionRegistry(uri).pool_key_for_server(topology.servers[0]),
            build_connection_pool_options(uri),
        )

        self.assertEqual(pool.options.max_pool_size, 1)

        first = pool.checkout(topology.servers[0])
        with self.assertRaisesRegex(RuntimeError, "connection pool exhausted"):
            pool.checkout(topology.servers[0])

        pool.checkin("missing")
        first.mark_closed()
        pool.checkin(first.connection_id)
        pool.discard("missing")

        registry = ConnectionRegistry(uri)
        self.assertIsNone(registry.get_connection(first))

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

    def test_connection_registry_async_checkout_waits_for_released_connection(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1&waitQueueTimeoutMS=100")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        async def _run() -> tuple[str, str]:
            first = await registry.checkout_async(topology.servers[0])

            async def _release() -> None:
                await asyncio.sleep(0.01)
                await registry.checkin_async(first)

            release_task = asyncio.create_task(_release())
            try:
                second = await registry.checkout_async(topology.servers[0])
            finally:
                await release_task
            return first.connection_id, second.connection_id

        first_id, second_id = asyncio.run(_run())

        self.assertEqual(first_id, second_id)

    def test_connection_registry_async_checkout_times_out_when_pool_stays_full(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1&waitQueueTimeoutMS=1")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        async def _run() -> None:
            first = await registry.checkout_async(topology.servers[0])
            try:
                with self.assertRaises(RuntimeError):
                    await registry.checkout_async(topology.servers[0])
            finally:
                await registry.checkin_async(first)

        asyncio.run(_run())

    def test_connection_registry_async_checkout_honors_fifo_wait_order(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1&waitQueueTimeoutMS=100")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        async def _run() -> list[str]:
            first = await registry.checkout_async(topology.servers[0])
            order: list[str] = []

            async def _waiter(label: str) -> None:
                lease = await registry.checkout_async(topology.servers[0])
                order.append(label)
                await asyncio.sleep(0)
                await registry.checkin_async(lease)

            waiter_a = asyncio.create_task(_waiter("a"))
            await asyncio.sleep(0)
            waiter_b = asyncio.create_task(_waiter("b"))
            await asyncio.sleep(0.01)
            await registry.checkin_async(first)
            await asyncio.gather(waiter_a, waiter_b)
            return order

        order = asyncio.run(_run())

        self.assertEqual(order, ["a", "b"])

    def test_connection_registry_async_checkout_without_timeout_waits_for_checkin(self):
        uri = parse_mongo_uri("mongodb://db1:27017/?maxPoolSize=1")
        topology = build_local_topology_description(uri)
        registry = ConnectionRegistry(uri)

        async def _run() -> tuple[str, str]:
            first = await registry.checkout_async(topology.servers[0])

            async def _release() -> None:
                await asyncio.sleep(0.01)
                await registry.checkin_async(first)

            release_task = asyncio.create_task(_release())
            try:
                second = await registry.checkout_async(topology.servers[0])
            finally:
                await release_task
            return first.connection_id, second.connection_id

        first_id, second_id = asyncio.run(_run())
        self.assertEqual(first_id, second_id)


class ClientDriverArchitectureTests(unittest.TestCase):
    def test_async_client_exposes_driver_state_and_request_planning(self):
        client = AsyncMongoClient(
            uri="mongodb://db1:27017/?appName=test-suite&retryReads=false",
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual(client.client_uri.options.app_name, "test-suite")
        self.assertEqual(client.topology_description.topology_type, TopologyType.UNKNOWN)
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

        async def _run() -> None:
            execution = await runtime.prepare_request_execution(plan)

            self.assertIsInstance(execution, PreparedRequestExecution)
            self.assertEqual(execution.selected_server.address, "db1:27017")
            self.assertEqual(runtime.connection_snapshots[0].checked_out, 1)

            await runtime.complete_request_execution(execution)

            self.assertEqual(runtime.connection_snapshots[0].checked_out, 0)

        asyncio.run(_run())

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
        request_ids = {event.request_id for event in events}
        self.assertEqual(len(request_ids), 1)
        self.assertTrue(next(iter(request_ids)))

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
    def test_request_execution_trace_and_exception_classification_cover_generic_paths(self):
        self.assertIsNone(RequestExecutionTrace().final_outcome)

        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?retryWrites=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("admin", "insert", {"insert": "events"}, read_only=False)

        generic = classify_request_exception(RuntimeError("boom"), plan=plan)
        self.assertEqual(generic.error, "RuntimeError: boom")
        self.assertFalse(
            _is_retryable_exception(
                OperationFailure("write failed", error_labels=()),
                plan=plan,
            )
        )

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
        self.assertEqual(seen, ["db1:27017", "db2:27018"])
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
        self.assertEqual(seen, ["db1:27017", "db2:27018"])

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
        first_request_ids = {event.request_id for event in runtime.monitor.history[:5]}
        second_request_ids = {event.request_id for event in runtime.monitor.history[5:]}
        self.assertEqual(len(first_request_ids), 1)
        self.assertEqual(len(second_request_ids), 1)
        self.assertNotEqual(first_request_ids, second_request_ids)

    def test_local_command_transport_executes_database_commands_with_session(self):
        async def _run() -> tuple[str, dict[str, object]]:
            client = AsyncMongoClient(uri="mongodb://db1:27017/")
            session = client.start_session()
            plan = client.plan_command_request(
                "admin",
                "ping",
                {"ping": 1},
                session=session,
                read_only=True,
            )
            execution = await client.prepare_command_request_execution(
                "admin",
                "ping",
                {"ping": 1},
                session=session,
                read_only=True,
            )
            try:
                response = await LocalCommandTransport(client).send(execution)
            finally:
                await client.complete_command_request_execution(execution)
            return plan.request.session_id or "", response

        session_id, response = asyncio.run(_run())

        self.assertTrue(session_id)
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
        self.assertEqual(len(runtime.monitor.history), 1)
        selection_failed = runtime.monitor.history[0]
        self.assertIsInstance(selection_failed, ServerSelectionFailedEvent)
        self.assertEqual(selection_failed.topology_type, "unknown")
        self.assertEqual(selection_failed.known_server_count, 0)
        self.assertEqual(selection_failed.timeout_ms, 30000)

    def test_unknown_topology_still_uses_provisional_seed_selection(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )

        plan = runtime.plan_command_request("admin", "ping", {"ping": 1}, read_only=True)

        self.assertEqual([server.address for server in plan.candidate_servers], ["db1:27017"])

    def test_execute_request_reselects_candidates_from_current_topology(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018/?replicaSet=rs0&retryReads=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)
        runtime._topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription(
                    address="db2:27018",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.HEALTHY,
                    set_name="rs0",
                ),
            ),
            set_name="rs0",
        )  # type: ignore[attr-defined]
        seen: list[str] = []

        class EchoTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                seen.append(execution.selected_server.address)
                return {"ok": 1.0, "cursor": {"id": 0}}

        result = asyncio.run(runtime.execute_request(plan, EchoTransport()))

        self.assertTrue(result.outcome.ok)
        self.assertEqual(seen, ["db2:27018"])

    def test_execute_request_uses_current_topology_when_stale_plan_still_has_candidates(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?directConnection=true",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("admin", "ping", {"ping": 1}, read_only=True)
        runtime._topology = TopologyDescription(topology_type=TopologyType.UNKNOWN, servers=())  # type: ignore[attr-defined]

        class NoopTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                del execution
                return {"ok": 1.0}

        result = asyncio.run(runtime.execute_request(plan, NoopTransport()))

        self.assertFalse(result.outcome.ok)
        self.assertIn("no eligible servers", result.outcome.error or "")

    def test_runtime_plan_resolution_updates_dynamic_plans_against_current_topology(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017,db2:27018/?replicaSet=rs0",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)
        refreshed_topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription(
                    address="db2:27018",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.HEALTHY,
                    set_name="rs0",
                ),
            ),
            set_name="rs0",
        )

        resolved = resolve_runtime_execution_plan(
            current_topology=refreshed_topology,
            plan=plan,
        )

        self.assertIs(resolved.topology, refreshed_topology)
        self.assertEqual([server.address for server in resolved.candidate_servers], ["db2:27018"])

    def test_runtime_plan_resolution_keeps_probe_plans_stable(self):
        server = ServerDescription(address="db1:27017")
        probe_plan = build_probe_plan(server)
        changed_topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription(
                    address="db2:27018",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.HEALTHY,
                    set_name="rs0",
                ),
            ),
            set_name="rs0",
        )

        resolved = resolve_runtime_execution_plan(
            current_topology=changed_topology,
            plan=probe_plan,
        )

        self.assertIs(resolved, probe_plan)
        self.assertEqual([candidate.address for candidate in resolved.candidate_servers], ["db1:27017"])

    def test_driver_runtime_delegates_attempt_lifecycle_to_helper(self):
        runtime = DriverRuntime(
            uri="mongodb://db1:27017/?retryReads=false",
            write_concern=MongoClient().write_concern,
            read_concern=MongoClient().read_concern,
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        )
        plan = runtime.plan_command_request("observe", "find", {"find": "events"}, read_only=True)

        class FakeAttempts:
            def __init__(self):
                self.calls = []

            async def prepare(self, current_plan, *, attempt_number):
                self.calls.append(("prepare", current_plan, attempt_number))
                return "prepared"

            async def complete(self, execution):
                self.calls.append(("complete", execution))

            async def discard(self, execution):
                self.calls.append(("discard", execution))

            async def execute(self, current_plan, *, transport):
                self.calls.append(("execute", current_plan, transport))
                return "result"

        fake = FakeAttempts()
        runtime._attempts = fake

        prepared = asyncio.run(runtime.prepare_request_execution_attempt(plan, attempt_number=2))
        asyncio.run(runtime.complete_request_execution("prepared"))
        asyncio.run(runtime.discard_request_execution("broken"))
        result = asyncio.run(runtime.execute_request(plan, transport=object()))

        self.assertEqual(prepared, "prepared")
        self.assertEqual(result, "result")
        self.assertEqual(fake.calls[0], ("prepare", plan, 2))
        self.assertEqual(fake.calls[1], ("complete", "prepared"))
        self.assertEqual(fake.calls[2], ("discard", "broken"))
        self.assertEqual(fake.calls[3][0], "execute")
        self.assertIs(fake.calls[3][1], plan)

    def test_runtime_attempt_lifecycle_preserves_probe_plans_during_prepare(self):
        class StubConnections:
            async def checkout_async(self, server):
                return type("Lease", (), {"connection_id": server.address, "server": server, "pool_key": None})()

        class StubMonitor:
            def __init__(self):
                self.events = []

            def emit(self, event):
                self.events.append(type(event).__name__)

        server = ServerDescription(address="db1:27017")
        probe_plan = build_probe_plan(server)
        lifecycle = RuntimeAttemptLifecycle(
            connections=StubConnections(),
            monitor=StubMonitor(),
            resolve_plan=lambda plan: resolve_runtime_execution_plan(
                current_topology=TopologyDescription(
                    topology_type=TopologyType.REPLICA_SET,
                    servers=(ServerDescription("db2:27018", server_type=ServerType.RS_SECONDARY),),
                ),
                plan=plan,
            ),
        )

        prepared = asyncio.run(lifecycle.prepare(probe_plan, attempt_number=1))

        self.assertIs(prepared.plan, probe_plan)
        self.assertEqual(prepared.selected_server.address, "db1:27017")

    def test_selection_policy_prefers_healthier_servers_when_ordering_nearest(self):
        policy = build_selection_policy(
            parse_mongo_uri("mongodb://db1:27017,db2:27018,db3:27019/"),
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription(
                    "db1:27017",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.DEGRADED,
                    round_trip_time_ms=1.0,
                ),
                ServerDescription(
                    "db2:27018",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.HEALTHY,
                    round_trip_time_ms=2.0,
                ),
                ServerDescription(
                    "db3:27019",
                    server_type=ServerType.RS_SECONDARY,
                    state=ServerState.RECOVERING,
                    round_trip_time_ms=1.5,
                ),
            ),
        )

        selected = policy.select_servers(topology)

        self.assertEqual([server.address for server in selected], ["db2:27018", "db3:27019", "db1:27017"])

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

        async def prepare(plan, *, attempt_number):
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

        async def complete(execution):
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

    def test_refresh_topology_discovers_additional_replica_set_members_from_hello(self):
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(ServerDescription("db1:27017", set_name="rs0"),),
            set_name="rs0",
        )

        async def prepare(plan, *, attempt_number):
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

        async def complete(execution):
            del execution

        class DiscoveryTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                if execution.selected_server.address == "db1:27017":
                    return {
                        "ok": 1.0,
                        "setName": "rs0",
                        "isWritablePrimary": True,
                        "hosts": ["db1:27017", "db2:27018"],
                    }
                return {
                    "ok": 1.0,
                    "setName": "rs0",
                    "secondary": True,
                    "hosts": ["db1:27017", "db2:27018"],
                    "tags": {"region": "eu"},
                }

        refreshed = asyncio.run(
            refresh_topology(
                current_topology=topology,
                prepare_execution=prepare,
                complete_execution=complete,
                transport=DiscoveryTransport(),
            )
        )

        self.assertEqual(refreshed.topology_type, TopologyType.REPLICA_SET)
        self.assertEqual([server.address for server in refreshed.servers], ["db1:27017", "db2:27018"])
        self.assertEqual(refreshed.servers[1].server_type, ServerType.RS_SECONDARY)

    def test_refresh_topology_discovers_primary_and_arbiter_addresses_from_secondary_hello(self):
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(ServerDescription("db2:27018", set_name="rs0"),),
            set_name="rs0",
        )

        async def prepare(plan, *, attempt_number):
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

        async def complete(execution):
            del execution

        class DiscoveryTransport:
            async def send(self, execution: PreparedRequestExecution) -> dict[str, object]:
                if execution.selected_server.address == "db2:27018":
                    return {
                        "ok": 1.0,
                        "setName": "rs0",
                        "secondary": True,
                        "primary": "db1:27017",
                        "arbiters": ["db3:27019"],
                        "me": "db2:27018",
                    }
                if execution.selected_server.address == "db1:27017":
                    return {"ok": 1.0, "setName": "rs0", "isWritablePrimary": True}
                return {"ok": 1.0, "setName": "rs0", "arbiterOnly": True}

        refreshed = asyncio.run(
            refresh_topology(
                current_topology=topology,
                prepare_execution=prepare,
                complete_execution=complete,
                transport=DiscoveryTransport(),
            )
        )

        self.assertEqual(
            [server.address for server in refreshed.servers],
            ["db2:27018", "db1:27017", "db3:27019"],
        )
        self.assertEqual(refreshed.servers[2].server_type, ServerType.RS_ARBITER)

import unittest

from mongoeco import (
    AsyncMongoClient,
    MongoClient,
    ReadPreference,
    ReadPreferenceMode,
    parse_mongo_uri,
)
from mongoeco.driver import (
    CommandRequest,
    ConnectionRegistry,
    ConnectionState,
    DriverRuntime,
    PreparedRequestExecution,
    RequestExecutionPlan,
    ServerType,
    TopologyType,
    build_local_topology_description,
    build_selection_policy,
    build_timeout_policy,
)


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
        )

        self.assertEqual(uri.username, "ada")
        self.assertEqual(uri.password, "secret")
        self.assertEqual(uri.default_database, "observe")
        self.assertEqual([seed.address for seed in uri.seeds], ["db1:27018", "db2:27019"])
        self.assertEqual(uri.options.replica_set, "rs0")
        self.assertFalse(uri.options.retry_reads)
        self.assertTrue(uri.options.retry_writes)
        self.assertEqual(uri.options.server_selection_timeout_ms, 1500)
        self.assertEqual(uri.options.connect_timeout_ms, 800)
        self.assertEqual(uri.options.max_pool_size, 20)
        self.assertEqual(uri.options.min_pool_size, 5)
        self.assertEqual(uri.options.read_preference, "secondaryPreferred")

    def test_parse_mongo_uri_rejects_invalid_pool_bounds(self):
        with self.assertRaises(ValueError):
            parse_mongo_uri("mongodb://localhost/?maxPoolSize=1&minPoolSize=2")


class TopologyAndPolicyTests(unittest.TestCase):
    def test_build_local_topology_description_reflects_replica_set(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?replicaSet=rs0")
        topology = build_local_topology_description(uri)

        self.assertEqual(topology.topology_type, TopologyType.REPLICA_SET)
        self.assertTrue(all(server.server_type is ServerType.RS_PRIMARY for server in topology.servers))
        self.assertEqual(topology.set_name, "rs0")

    def test_selection_policy_prefers_direct_connection_seed(self):
        uri = parse_mongo_uri("mongodb://db1:27017,db2:27018/?directConnection=true")
        topology = build_local_topology_description(uri)
        policy = build_selection_policy(
            uri,
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
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


class ClientDriverArchitectureTests(unittest.TestCase):
    def test_async_client_exposes_driver_state_and_request_planning(self):
        client = AsyncMongoClient(
            uri="mongodb://db1:27017/?appName=test-suite&retryReads=false",
            read_preference=ReadPreference(ReadPreferenceMode.SECONDARY),
        )

        self.assertEqual(client.client_uri.options.app_name, "test-suite")
        self.assertEqual(client.topology_description.topology_type, TopologyType.SINGLE)
        self.assertFalse(client.retry_policy.retry_reads)
        self.assertEqual(client.selection_policy.mode, "secondary")

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

    def test_sync_client_with_options_preserves_uri_architecture_state(self):
        client = MongoClient(
            uri="mongodb://db1:27017/?retryWrites=false",
            read_preference=ReadPreference(ReadPreferenceMode.NEAREST),
        )

        tuned = client.with_options()

        self.assertEqual(tuned.client_uri.original, client.client_uri.original)
        self.assertFalse(tuned.retry_policy.retry_writes)
        self.assertEqual(tuned.selection_policy.mode, "nearest")

    def test_sync_client_exposes_driver_runtime(self):
        client = MongoClient(uri="mongodb://db1:27017/")

        snapshots = client.driver_runtime.connection_snapshots

        self.assertEqual(snapshots, ())

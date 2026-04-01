import unittest

from mongoeco.driver.topology import ServerDescription, ServerType, TopologyDescription, TopologyType
from mongoeco.driver.topology_monitor import (
    _derive_topology_type,
    _server_description_from_hello,
    _topology_is_compatible,
    build_probe_plan,
    refresh_topology,
)


class TopologyMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_topology_marks_failed_probe_as_incompatible(self):
        case = self
        server = ServerDescription(address="db1:27017", server_type=ServerType.STANDALONE)
        topology = TopologyDescription(topology_type=TopologyType.SINGLE, servers=(server,))
        prepared: list[object] = []
        completed: list[object] = []

        async def prepare_execution(plan, attempt_number):
            self.assertEqual(plan.request.command_name, "hello")
            self.assertEqual(attempt_number, 1)
            token = object()
            prepared.append(token)
            return token

        async def complete_execution(execution):
            completed.append(execution)

        class _FailingTransport:
            async def send(self, execution):
                case.assertIs(execution, prepared[0])
                raise RuntimeError("boom")

        refreshed = await refresh_topology(
            current_topology=topology,
            prepare_execution=prepare_execution,
            complete_execution=complete_execution,
            transport=_FailingTransport(),
        )

        self.assertEqual(len(refreshed.servers), 1)
        self.assertEqual(refreshed.servers[0].server_type, ServerType.UNKNOWN)
        self.assertEqual(refreshed.servers[0].error, "RuntimeError: boom")
        self.assertFalse(refreshed.compatible)
        self.assertEqual(refreshed.topology_type, TopologyType.SINGLE)
        self.assertEqual(completed, prepared)

    def test_build_probe_plan_uses_expected_defaults(self):
        server = ServerDescription(address="db1:27017", server_type=ServerType.STANDALONE)

        plan = build_probe_plan(server)

        self.assertEqual(plan.request.database, "admin")
        self.assertEqual(plan.request.command_name, "hello")
        self.assertEqual(plan.request.payload, {"hello": 1})
        self.assertTrue(plan.request.read_only)
        self.assertEqual(plan.topology.topology_type, TopologyType.SINGLE)
        self.assertEqual(plan.candidate_servers, (server,))
        self.assertFalse(plan.retry_policy.retry_reads)
        self.assertFalse(plan.retry_policy.retry_writes)
        self.assertFalse(plan.tls_policy.enabled)
        self.assertTrue(plan.tls_policy.verify_certificates)

    def test_server_description_from_hello_covers_edge_branches(self):
        mongos = _server_description_from_hello(
            "router:27017",
            {"msg": "isdbgrid", "tags": [("x", "y")]},
            round_trip_time_ms=3.5,
        )
        arbiter = _server_description_from_hello(
            "db1:27017",
            {"setName": "rs0", "arbiterOnly": True, "me": "db1:27017"},
            round_trip_time_ms=0.8,
        )
        unknown_rs_member = _server_description_from_hello(
            "db2:27017",
            {"setName": "rs0"},
            round_trip_time_ms=1.0,
        )
        unknown_server = _server_description_from_hello(
            "db3:27017",
            {},
            round_trip_time_ms=2.0,
        )

        self.assertEqual(mongos.server_type, ServerType.MONGOS)
        self.assertEqual(arbiter.server_type, ServerType.RS_ARBITER)
        self.assertEqual(arbiter.me, "db1:27017")
        self.assertEqual(mongos.tags, {})
        self.assertIsNone(mongos.wire_version_range)
        self.assertIsNone(mongos.logical_session_timeout_minutes)
        self.assertIsNone(mongos.topology_version)
        self.assertIsNone(mongos.set_version)
        self.assertEqual(unknown_rs_member.server_type, ServerType.UNKNOWN)
        self.assertEqual(unknown_rs_member.set_name, "rs0")
        self.assertEqual(unknown_server.server_type, ServerType.UNKNOWN)

    def test_derive_topology_type_supports_sharded_and_fallback(self):
        sharded = _derive_topology_type(
            (ServerDescription(address="router:27017", server_type=ServerType.MONGOS),),
            fallback=TopologyType.UNKNOWN,
        )
        fallback = _derive_topology_type(
            (ServerDescription(address="db1:27017", server_type=ServerType.UNKNOWN),),
            fallback=TopologyType.REPLICA_SET,
        )

        self.assertEqual(sharded, TopologyType.SHARDED)
        self.assertEqual(fallback, TopologyType.REPLICA_SET)

    def test_derive_topology_type_marks_mixed_families_unknown(self):
        topology_type = _derive_topology_type(
            (
                ServerDescription(address="router:27017", server_type=ServerType.MONGOS),
                ServerDescription(address="db1:27017", server_type=ServerType.STANDALONE),
            ),
            fallback=TopologyType.SINGLE,
        )

        self.assertEqual(topology_type, TopologyType.UNKNOWN)

    def test_topology_compatibility_rejects_mixed_families_and_conflicting_set_names(self):
        self.assertFalse(
            _topology_is_compatible(
                (
                    ServerDescription(address="router:27017", server_type=ServerType.MONGOS),
                    ServerDescription(address="db1:27017", server_type=ServerType.STANDALONE),
                ),
                topology_type=TopologyType.UNKNOWN,
            )
        )
        self.assertFalse(
            _topology_is_compatible(
                (
                    ServerDescription(address="db1:27017", server_type=ServerType.RS_PRIMARY, set_name="rs0"),
                    ServerDescription(address="db2:27018", server_type=ServerType.RS_SECONDARY, set_name="rs1"),
                ),
                topology_type=TopologyType.REPLICA_SET,
            )
        )

    async def test_refresh_topology_marks_mixed_server_families_incompatible(self):
        topology = TopologyDescription(
            topology_type=TopologyType.UNKNOWN,
            servers=(
                ServerDescription(address="router:27017", server_type=ServerType.UNKNOWN),
                ServerDescription(address="db1:27017", server_type=ServerType.UNKNOWN),
            ),
        )
        prepared: list[object] = []

        async def prepare_execution(plan, attempt_number):
            token = object()
            prepared.append(token)
            return token

        async def complete_execution(_execution):
            return None

        class _MixedTransport:
            async def send(self, execution):
                if execution is prepared[0]:
                    return {"msg": "isdbgrid"}
                return {"isWritablePrimary": True}

        refreshed = await refresh_topology(
            current_topology=topology,
            prepare_execution=prepare_execution,
            complete_execution=complete_execution,
            transport=_MixedTransport(),
        )

        self.assertEqual(refreshed.topology_type, TopologyType.UNKNOWN)
        self.assertFalse(refreshed.compatible)

    async def test_refresh_topology_marks_replica_set_name_mismatch_incompatible(self):
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(ServerDescription(address="db1:27017", set_name="rs0"),),
            set_name="rs0",
        )

        async def prepare_execution(plan, attempt_number):
            del attempt_number
            return object()

        async def complete_execution(_execution):
            return None

        class _MismatchTransport:
            async def send(self, _execution):
                return {"setName": "rs1", "isWritablePrimary": True}

        refreshed = await refresh_topology(
            current_topology=topology,
            prepare_execution=prepare_execution,
            complete_execution=complete_execution,
            transport=_MismatchTransport(),
        )

        self.assertFalse(refreshed.compatible)
        self.assertIn("replica set name mismatch", refreshed.servers[0].error or "")

    async def test_refresh_topology_keeps_newer_topology_version_when_probe_is_stale(self):
        topology = TopologyDescription(
            topology_type=TopologyType.REPLICA_SET,
            servers=(
                ServerDescription(
                    address="db1:27017",
                    server_type=ServerType.RS_PRIMARY,
                    set_name="rs0",
                    topology_version={"processId": "p1", "counter": 5},
                    set_version=5,
                ),
            ),
            set_name="rs0",
        )

        async def prepare_execution(plan, attempt_number):
            del plan, attempt_number
            return object()

        async def complete_execution(_execution):
            return None

        class _StaleTransport:
            async def send(self, _execution):
                return {
                    "setName": "rs0",
                    "isWritablePrimary": True,
                    "topologyVersion": {"processId": "p1", "counter": 3},
                    "setVersion": 3,
                }

        refreshed = await refresh_topology(
            current_topology=topology,
            prepare_execution=prepare_execution,
            complete_execution=complete_execution,
            transport=_StaleTransport(),
        )

        self.assertEqual(refreshed.servers[0].topology_version, {"processId": "p1", "counter": 5})
        self.assertEqual(refreshed.servers[0].set_version, 5)

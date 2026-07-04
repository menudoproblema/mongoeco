from importlib import import_module

_URI_EXPORTS = (
    "MongoClientOptions",
    "MongoAuthOptions",
    "MongoTlsOptions",
    "MongoUri",
    "MongoUriSeed",
    "parse_mongo_uri",
    "build_write_concern_from_uri",
    "build_read_concern_from_uri",
    "build_read_preference_from_uri",
)

_DISCOVERY_EXPORTS = (
    "SrvResolution",
    "resolve_srv_dns",
    "resolve_srv_seeds",
    "materialize_srv_uri",
)

_CONNECTION_EXPORTS = (
    "PoolKey",
    "ConnectionState",
    "ConnectionPoolOptions",
    "DriverConnection",
    "ConnectionLease",
    "ConnectionPoolSnapshot",
    "ConnectionPool",
    "ConnectionRegistry",
    "build_connection_pool_options",
)

_EXECUTION_EXPORTS = (
    "AsyncCommandTransport",
    "RequestAttempt",
    "RequestExecutionTrace",
    "RequestExecutionResult",
    "classify_request_exception",
    "execute_request_pipeline",
)

_FAILPOINT_EXPORTS = ("DriverFailpointController",)

_MONITORING_EXPORTS = (
    "DriverEvent",
    "DriverMonitor",
    "ServerSelectedEvent",
    "ServerSelectionFailedEvent",
    "TopologyRefreshedEvent",
    "ConnectionCheckedOutEvent",
    "ConnectionCheckedInEvent",
    "CommandStartedEvent",
    "CommandSucceededEvent",
    "CommandFailedEvent",
)

_TOPOLOGY_EXPORTS = (
    "ServerDescription",
    "ServerState",
    "ServerType",
    "SdamCapabilitiesInfo",
    "TopologyDescription",
    "TopologyType",
    "build_local_topology_description",
    "sdam_capabilities_info",
)

_TOPOLOGY_MONITOR_EXPORTS = ("refresh_topology",)

_POLICY_EXPORTS = (
    "TimeoutPolicy",
    "RetryPolicy",
    "SelectionPolicy",
    "ConcernPolicy",
    "build_timeout_policy",
    "build_retry_policy",
    "build_selection_policy",
    "build_concern_policy",
)

_SECURITY_EXPORTS = (
    "AuthPolicy",
    "TlsPolicy",
    "build_auth_policy",
    "build_tls_policy",
)

_REQUEST_EXPORTS = (
    "CommandRequest",
    "PreparedRequestExecution",
    "RequestExecutionPlan",
    "RequestOutcome",
)

_RUNTIME_EXPORTS = ("DriverRuntime",)

_TRANSPORT_EXPORTS = (
    "CallbackCommandTransport",
    "LocalCommandTransport",
    "WireProtocolCommandTransport",
)

__all__ = [
    "MongoClientOptions",
    "MongoAuthOptions",
    "MongoTlsOptions",
    "MongoUri",
    "MongoUriSeed",
    "parse_mongo_uri",
    "SrvResolution",
    "resolve_srv_dns",
    "resolve_srv_seeds",
    "materialize_srv_uri",
    "build_write_concern_from_uri",
    "build_read_concern_from_uri",
    "build_read_preference_from_uri",
    "PoolKey",
    "ConnectionState",
    "ConnectionPoolOptions",
    "DriverConnection",
    "ConnectionLease",
    "ConnectionPoolSnapshot",
    "ConnectionPool",
    "ConnectionRegistry",
    "build_connection_pool_options",
    "AsyncCommandTransport",
    "RequestAttempt",
    "RequestExecutionTrace",
    "RequestExecutionResult",
    "classify_request_exception",
    "execute_request_pipeline",
    "DriverFailpointController",
    "DriverEvent",
    "DriverMonitor",
    "ServerSelectedEvent",
    "ServerSelectionFailedEvent",
    "TopologyRefreshedEvent",
    "ConnectionCheckedOutEvent",
    "ConnectionCheckedInEvent",
    "CommandStartedEvent",
    "CommandSucceededEvent",
    "CommandFailedEvent",
    "ServerDescription",
    "ServerState",
    "ServerType",
    "SdamCapabilitiesInfo",
    "TopologyDescription",
    "TopologyType",
    "build_local_topology_description",
    "sdam_capabilities_info",
    "refresh_topology",
    "TimeoutPolicy",
    "RetryPolicy",
    "SelectionPolicy",
    "ConcernPolicy",
    "AuthPolicy",
    "TlsPolicy",
    "build_timeout_policy",
    "build_retry_policy",
    "build_selection_policy",
    "build_concern_policy",
    "build_auth_policy",
    "build_tls_policy",
    "CommandRequest",
    "PreparedRequestExecution",
    "RequestExecutionPlan",
    "RequestOutcome",
    "DriverRuntime",
    "CallbackCommandTransport",
    "LocalCommandTransport",
    "WireProtocolCommandTransport",
]

_EXPORT_MODULES = {
    **dict.fromkeys(_URI_EXPORTS, "mongoeco.driver.uri"),
    **dict.fromkeys(_DISCOVERY_EXPORTS, "mongoeco.driver.discovery"),
    **dict.fromkeys(_CONNECTION_EXPORTS, "mongoeco.driver.connections"),
    **dict.fromkeys(_EXECUTION_EXPORTS, "mongoeco.driver.execution"),
    **dict.fromkeys(_FAILPOINT_EXPORTS, "mongoeco.driver.failpoints"),
    **dict.fromkeys(_MONITORING_EXPORTS, "mongoeco.driver.monitoring"),
    **dict.fromkeys(_TOPOLOGY_EXPORTS, "mongoeco.driver.topology"),
    **dict.fromkeys(_TOPOLOGY_MONITOR_EXPORTS, "mongoeco.driver.topology_monitor"),
    **dict.fromkeys(_POLICY_EXPORTS, "mongoeco.driver.policies"),
    **dict.fromkeys(_SECURITY_EXPORTS, "mongoeco.driver.security"),
    **dict.fromkeys(_REQUEST_EXPORTS, "mongoeco.driver.requests"),
    **dict.fromkeys(_RUNTIME_EXPORTS, "mongoeco.driver.runtime"),
    **dict.fromkeys(_TRANSPORT_EXPORTS, "mongoeco.driver.transports"),
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})

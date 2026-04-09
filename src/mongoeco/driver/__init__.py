from mongoeco.driver.discovery import SrvResolution, materialize_srv_uri, resolve_srv_dns, resolve_srv_seeds
from mongoeco.driver.execution import (
    AsyncCommandTransport,
    RequestAttempt,
    RequestExecutionResult,
    RequestExecutionTrace,
    classify_request_exception,
    execute_request_pipeline,
)
from mongoeco.driver.failpoints import DriverFailpointController
from mongoeco.driver.monitoring import (
    CommandFailedEvent,
    CommandStartedEvent,
    CommandSucceededEvent,
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    DriverEvent,
    DriverMonitor,
    ServerSelectedEvent,
    ServerSelectionFailedEvent,
    TopologyRefreshedEvent,
)
from mongoeco.driver.connections import (
    ConnectionLease,
    ConnectionPool,
    ConnectionPoolOptions,
    ConnectionPoolSnapshot,
    ConnectionRegistry,
    ConnectionState,
    DriverConnection,
    PoolKey,
    build_connection_pool_options,
)
from mongoeco.driver.policies import (
    ConcernPolicy,
    RetryPolicy,
    SelectionPolicy,
    TimeoutPolicy,
    build_concern_policy,
    build_retry_policy,
    build_selection_policy,
    build_timeout_policy,
)
from mongoeco.driver.requests import CommandRequest, PreparedRequestExecution, RequestExecutionPlan, RequestOutcome
from mongoeco.driver.runtime import DriverRuntime
from mongoeco.driver.security import AuthPolicy, TlsPolicy, build_auth_policy, build_tls_policy
from mongoeco.driver.topology import (
    SdamCapabilitiesInfo,
    ServerDescription,
    ServerState,
    ServerType,
    TopologyDescription,
    TopologyType,
    build_local_topology_description,
    sdam_capabilities_info,
)
from mongoeco.driver.topology_monitor import refresh_topology
from mongoeco.driver.uri import (
    MongoAuthOptions,
    MongoClientOptions,
    MongoTlsOptions,
    MongoUri,
    MongoUriSeed,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_write_concern_from_uri,
    parse_mongo_uri,
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


def __getattr__(name: str):
    if name in {"CallbackCommandTransport", "LocalCommandTransport", "WireProtocolCommandTransport"}:
        from mongoeco.driver.transports import (
            CallbackCommandTransport,
            LocalCommandTransport,
            WireProtocolCommandTransport,
        )

        mapping = {
            "CallbackCommandTransport": CallbackCommandTransport,
            "LocalCommandTransport": LocalCommandTransport,
            "WireProtocolCommandTransport": WireProtocolCommandTransport,
        }
        value = mapping[name]
        globals()[name] = value
        return value
    raise AttributeError(name)

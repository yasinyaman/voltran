"""Core module - Hexagonal architecture ports, domain and adapters."""

# Domain models
from voltran.core.domain import (
    Capability,
    Cluster,
    Endpoint,
    Federation,
    HealthStatus,
    ModuleDescriptor,
    ModuleQuery,
    NodeRole,
    PortDirection,
    VoltranNode,
)

# Domain services
from voltran.core.domain.services import (
    AuthorizationService,
    AuthorizationRulesLoader,
    DiscoveryService,
    FusionService,
    LoggingService,
    MonitoringService,
    ScopedLogger,
)

# Ports
from voltran.core.ports import (
    IClusterPort,
    IFederationPort,
    IMessagingPort,
    IModuleRegistryPort,
)

# Adapters
from voltran.core.adapters import (
    GrpcMessagingAdapter,
    InMemoryAuthorizationAdapter,
    InMemoryLoggingAdapter,
    InMemoryMonitoringAdapter,
    NatsMessagingAdapter,
    RestMessagingAdapter,
)

__all__ = [
    # Domain Models
    "Capability",
    "Cluster",
    "Endpoint",
    "Federation",
    "HealthStatus",
    "ModuleDescriptor",
    "ModuleQuery",
    "NodeRole",
    "PortDirection",
    "VoltranNode",
    # Domain Services
    "AuthorizationService",
    "AuthorizationRulesLoader",
    "DiscoveryService",
    "FusionService",
    "LoggingService",
    "MonitoringService",
    "ScopedLogger",
    # Ports
    "IClusterPort",
    "IFederationPort",
    "IMessagingPort",
    "IModuleRegistryPort",
    # Adapters
    "GrpcMessagingAdapter",
    "InMemoryAuthorizationAdapter",
    "InMemoryLoggingAdapter",
    "InMemoryMonitoringAdapter",
    "NatsMessagingAdapter",
    "RestMessagingAdapter",
]

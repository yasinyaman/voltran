"""
Voltran - Hexagonal Modular Federation System

Her modul tek basina veya kumeler halinde birlesip tek modulmus gibi calisabilir.
Birden fazla Voltran node'u federation olusturarak dagitik sistem oluşturabilir.
"""

__version__ = "0.1.0"

from voltran.core.domain.models import (
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
from voltran.core.domain.models import PermissionDeniedError
from voltran.core.domain.services.authorization import AuthorizationService
from voltran.core.domain.services.logging_service import LoggingService, ScopedLogger
from voltran.core.domain.services.monitoring import MonitoringService
from voltran.core.ports.outbound.authorization import (
    Permission,
    PermissionEffect,
    Policy,
    ResourceType,
    Role,
)
from voltran.core.ports.outbound.logging import LogEntry, LogLevel, LogQuery, LogStats
from voltran.core.ports.outbound.monitoring import (
    Alert,
    AlertRule,
    AlertSeverity,
    MetricType,
    MetricValue,
)
from voltran.sdk.authorization import AuthorizationGuard, get_auth_guard
from voltran.sdk.decorators import (
    inbound_port,
    outbound_port,
    require_permission,
    require_role,
    voltran_module,
)
from voltran.sdk.rest import (
    RestService,
    get,
    post,
    put,
    patch,
    delete,
)
from voltran.server import Voltran

__all__ = [
    # Main
    "Voltran",
    # Models
    "ModuleDescriptor",
    "VoltranNode",
    "Cluster",
    "Federation",
    "Capability",
    "Endpoint",
    "ModuleQuery",
    "HealthStatus",
    "PortDirection",
    "NodeRole",
    # Decorators
    "voltran_module",
    "inbound_port",
    "outbound_port",
    "require_permission",
    "require_role",
    # REST Decorators
    "RestService",
    "get",
    "post",
    "put",
    "patch",
    "delete",
    # Monitoring
    "MonitoringService",
    "MetricValue",
    "MetricType",
    "AlertRule",
    "Alert",
    "AlertSeverity",
    # Logging
    "LoggingService",
    "ScopedLogger",
    "LogEntry",
    "LogLevel",
    "LogQuery",
    "LogStats",
    # Authorization
    "AuthorizationService",
    "Permission",
    "Role",
    "Policy",
    "PermissionEffect",
    "ResourceType",
    "PermissionDeniedError",
    "AuthorizationGuard",
    "get_auth_guard",
]


"""Outbound ports - interfaces for external system connections."""

from voltran.core.ports.outbound.authorization import (
    AuthorizationContext,
    AuthorizationResult,
    IAuthorizationPort,
    Permission,
    PermissionEffect,
    Policy,
    ResourceType,
    Role,
)
from voltran.core.ports.outbound.logging import (
    ILoggingPort,
    LogEntry,
    LogHandler,
    LogLevel,
    LogQuery,
    LogStats,
)
from voltran.core.ports.outbound.messaging import IMessagingPort
from voltran.core.ports.outbound.monitoring import (
    Alert,
    AlertHandler,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    HistogramBucket,
    HistogramValue,
    IMonitoringPort,
    MetricType,
    MetricValue,
)
from voltran.core.ports.outbound.rest_client import (
    IRestClientPort,
    RestClientConfig,
    HttpRequest,
    HttpResponse,
    HttpMethod,
    RequestInterceptor,
    ResponseInterceptor,
    AuthInterceptor,
    LoggingRequestInterceptor,
    LoggingResponseInterceptor,
    RetryInterceptor,
    RestClientError,
)

__all__ = [
    # Messaging
    "IMessagingPort",
    # REST Client
    "IRestClientPort",
    "RestClientConfig",
    "HttpRequest",
    "HttpResponse",
    "HttpMethod",
    "RequestInterceptor",
    "ResponseInterceptor",
    "AuthInterceptor",
    "LoggingRequestInterceptor",
    "LoggingResponseInterceptor",
    "RetryInterceptor",
    "RestClientError",
    # Monitoring
    "IMonitoringPort",
    "MetricValue",
    "MetricType",
    "HistogramValue",
    "HistogramBucket",
    "AlertRule",
    "Alert",
    "AlertSeverity",
    "AlertStatus",
    "AlertHandler",
    # Logging
    "ILoggingPort",
    "LogEntry",
    "LogLevel",
    "LogQuery",
    "LogStats",
    "LogHandler",
    # Authorization
    "IAuthorizationPort",
    "Permission",
    "Role",
    "Policy",
    "AuthorizationContext",
    "AuthorizationResult",
    "PermissionEffect",
    "ResourceType",
]


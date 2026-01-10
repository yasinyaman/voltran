"""Adapters - concrete implementations of ports."""

from voltran.core.adapters.authorization_adapter import InMemoryAuthorizationAdapter
from voltran.core.adapters.grpc_adapter import GrpcMessagingAdapter
from voltran.core.adapters.logging_adapter import (
    FileLoggingAdapter,
    InMemoryLoggingAdapter,
)
from voltran.core.adapters.monitoring_adapter import InMemoryMonitoringAdapter
from voltran.core.adapters.nats_adapter import NatsMessagingAdapter
from voltran.core.adapters.rest_adapter import RestMessagingAdapter

__all__ = [
    # Messaging
    "GrpcMessagingAdapter",
    "NatsMessagingAdapter",
    "RestMessagingAdapter",
    # Monitoring
    "InMemoryMonitoringAdapter",
    # Logging
    "InMemoryLoggingAdapter",
    "FileLoggingAdapter",
    # Authorization
    "InMemoryAuthorizationAdapter",
]

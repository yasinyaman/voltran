"""Monitoring domain service."""

import asyncio
import time
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

import structlog

from voltran.core.domain.models import HealthStatus, ModuleDescriptor
from voltran.core.ports.outbound.monitoring import (
    Alert,
    AlertRule,
    AlertSeverity,
    HistogramValue,
    IMonitoringPort,
    MetricValue,
)

logger = structlog.get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class MonitoringService:
    """
    Domain service for system monitoring.
    
    Provides high-level monitoring operations including:
    - Standard Voltran metrics
    - Request/response timing
    - Health aggregation
    - Alert management with notification
    
    Usage:
        monitoring = MonitoringService(adapter)
        await monitoring.start()
        
        # Record metrics
        await monitoring.record_request("my_endpoint", duration_ms=45.2)
        await monitoring.record_module_registered("my-module")
        
        # Get dashboard data
        dashboard = await monitoring.get_dashboard()
    """

    # Standard metric names
    METRIC_REQUESTS_TOTAL = "voltran_requests_total"
    METRIC_REQUEST_DURATION = "voltran_request_duration_ms"
    METRIC_MODULES_REGISTERED = "voltran_modules_registered"
    METRIC_MODULES_HEALTHY = "voltran_modules_healthy"
    METRIC_FEDERATION_PEERS = "voltran_federation_peers"
    METRIC_MESSAGES_SENT = "voltran_messages_sent_total"
    METRIC_MESSAGES_RECEIVED = "voltran_messages_received_total"
    METRIC_ERRORS_TOTAL = "voltran_errors_total"
    METRIC_UPTIME_SECONDS = "voltran_uptime_seconds"

    def __init__(
        self,
        monitoring_port: IMonitoringPort,
        voltran_id: str = "",
        node_name: str = "",
    ):
        """
        Initialize monitoring service.
        
        Args:
            monitoring_port: Monitoring port implementation
            voltran_id: Voltran node ID
            node_name: Node name for labeling
        """
        self._monitoring = monitoring_port
        self._voltran_id = voltran_id
        self._node_name = node_name
        self._started_at: Optional[datetime] = None
        self._uptime_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start the monitoring service."""
        if self._running:
            return

        self._started_at = datetime.now()
        self._running = True

        # Start the underlying adapter
        await self._monitoring.start()

        # Initialize standard gauges
        await self._monitoring.gauge(
            self.METRIC_MODULES_REGISTERED,
            0,
            {"voltran_id": self._voltran_id, "node": self._node_name},
        )
        await self._monitoring.gauge(
            self.METRIC_MODULES_HEALTHY,
            0,
            {"voltran_id": self._voltran_id, "node": self._node_name},
        )
        await self._monitoring.gauge(
            self.METRIC_FEDERATION_PEERS,
            0,
            {"voltran_id": self._voltran_id, "node": self._node_name},
        )

        # Start uptime tracker
        self._uptime_task = asyncio.create_task(self._update_uptime())

        # Setup default alert rules
        await self._setup_default_alerts()

        logger.info(
            "monitoring_service_started",
            voltran_id=self._voltran_id,
            node_name=self._node_name,
        )

    async def stop(self) -> None:
        """Stop the monitoring service."""
        self._running = False

        if self._uptime_task:
            self._uptime_task.cancel()
            try:
                await self._uptime_task
            except asyncio.CancelledError:
                pass

        await self._monitoring.stop()
        logger.info("monitoring_service_stopped")

    # === Request Metrics ===

    async def record_request(
        self,
        endpoint: str,
        method: str = "POST",
        status: int = 200,
        duration_ms: Optional[float] = None,
    ) -> None:
        """
        Record a request metric.
        
        Args:
            endpoint: Endpoint/contract being called
            method: HTTP method or RPC method type
            status: Response status code
            duration_ms: Request duration in milliseconds
        """
        labels = {
            "voltran_id": self._voltran_id,
            "endpoint": endpoint,
            "method": method,
            "status": str(status),
        }

        # Increment request counter
        await self._monitoring.increment(self.METRIC_REQUESTS_TOTAL, 1, labels)

        # Record timing if provided
        if duration_ms is not None:
            await self._monitoring.timing(
                self.METRIC_REQUEST_DURATION, duration_ms, labels
            )

        # Track errors
        if status >= 400:
            error_labels = {
                "voltran_id": self._voltran_id,
                "endpoint": endpoint,
                "status": str(status),
            }
            await self._monitoring.increment(self.METRIC_ERRORS_TOTAL, 1, error_labels)

    @asynccontextmanager
    async def track_request(self, endpoint: str, method: str = "POST"):
        """
        Context manager for tracking request duration.
        
        Usage:
            async with monitoring.track_request("my.endpoint.v1"):
                result = await do_something()
        """
        start_time = time.perf_counter()
        status = 200
        try:
            yield
        except Exception:
            status = 500
            raise
        finally:
            duration_ms = (time.perf_counter() - start_time) * 1000
            await self.record_request(endpoint, method, status, duration_ms)

    def track_endpoint(self, endpoint: str, method: str = "POST") -> Callable[[F], F]:
        """
        Decorator for tracking async function calls.
        
        Usage:
            @monitoring.track_endpoint("my.endpoint.v1")
            async def my_handler(request):
                return response
        """

        def decorator(func: F) -> F:
            @wraps(func)
            async def wrapper(*args, **kwargs):
                async with self.track_request(endpoint, method):
                    return await func(*args, **kwargs)

            return wrapper  # type: ignore

        return decorator

    # === Module Metrics ===

    async def record_module_registered(self, module_name: str) -> None:
        """Record a module registration."""
        await self._monitoring.increment(
            self.METRIC_MODULES_REGISTERED,
            1,
            {"voltran_id": self._voltran_id, "module": module_name},
        )

    async def update_module_counts(
        self,
        total_modules: int,
        healthy_modules: int,
    ) -> None:
        """
        Update module count gauges.
        
        Args:
            total_modules: Total registered modules
            healthy_modules: Healthy module count
        """
        labels = {"voltran_id": self._voltran_id, "node": self._node_name}

        await self._monitoring.gauge(
            self.METRIC_MODULES_REGISTERED, total_modules, labels
        )
        await self._monitoring.gauge(
            self.METRIC_MODULES_HEALTHY, healthy_modules, labels
        )

    async def update_federation_peers(self, peer_count: int) -> None:
        """Update federation peer count gauge."""
        await self._monitoring.gauge(
            self.METRIC_FEDERATION_PEERS,
            peer_count,
            {"voltran_id": self._voltran_id, "node": self._node_name},
        )

    # === Message Metrics ===

    async def record_message_sent(self, topic: str) -> None:
        """Record a message sent."""
        await self._monitoring.increment(
            self.METRIC_MESSAGES_SENT,
            1,
            {"voltran_id": self._voltran_id, "topic": topic},
        )

    async def record_message_received(self, topic: str) -> None:
        """Record a message received."""
        await self._monitoring.increment(
            self.METRIC_MESSAGES_RECEIVED,
            1,
            {"voltran_id": self._voltran_id, "topic": topic},
        )

    # === Error Metrics ===

    async def record_error(
        self,
        error_type: str,
        component: str = "unknown",
    ) -> None:
        """
        Record an error occurrence.
        
        Args:
            error_type: Type of error (exception name)
            component: Component where error occurred
        """
        await self._monitoring.increment(
            self.METRIC_ERRORS_TOTAL,
            1,
            {
                "voltran_id": self._voltran_id,
                "error_type": error_type,
                "component": component,
            },
        )

    # === Custom Metrics ===

    async def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        merged_labels = {"voltran_id": self._voltran_id}
        if labels:
            merged_labels.update(labels)
        await self._monitoring.increment(name, value, merged_labels)

    async def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric."""
        merged_labels = {"voltran_id": self._voltran_id}
        if labels:
            merged_labels.update(labels)
        await self._monitoring.gauge(name, value, merged_labels)

    async def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a histogram observation."""
        merged_labels = {"voltran_id": self._voltran_id}
        if labels:
            merged_labels.update(labels)
        await self._monitoring.histogram(name, value, merged_labels)

    # === Querying ===

    async def get_metric(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[MetricValue]:
        """Get a metric value."""
        return await self._monitoring.get_metric(name, labels)

    async def get_all_metrics(self, prefix: Optional[str] = None) -> list[MetricValue]:
        """Get all metrics."""
        return await self._monitoring.get_all_metrics(prefix or "voltran_")

    async def get_histogram(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[HistogramValue]:
        """Get histogram metric."""
        return await self._monitoring.get_histogram(name, labels)

    # === Dashboard ===

    async def get_dashboard(self) -> dict[str, Any]:
        """
        Get dashboard-ready metrics summary.
        
        Returns:
            Dictionary with all key metrics for display
        """
        uptime_seconds = 0
        if self._started_at:
            uptime_seconds = (datetime.now() - self._started_at).total_seconds()

        # Get request histogram
        request_histogram = await self.get_histogram(self.METRIC_REQUEST_DURATION)
        avg_latency = 0.0
        if request_histogram and request_histogram.count > 0:
            avg_latency = request_histogram.sum / request_histogram.count

        # Get all metrics
        all_metrics = await self.get_all_metrics("voltran_")

        # Calculate totals
        total_requests = sum(
            m.value for m in all_metrics if m.name == self.METRIC_REQUESTS_TOTAL
        )
        total_errors = sum(
            m.value for m in all_metrics if m.name == self.METRIC_ERRORS_TOTAL
        )
        error_rate = (total_errors / total_requests * 100) if total_requests > 0 else 0

        # Get active alerts
        active_alerts = await self._monitoring.get_active_alerts()

        return {
            "voltran_id": self._voltran_id,
            "node_name": self._node_name,
            "uptime_seconds": uptime_seconds,
            "uptime_human": self._format_uptime(uptime_seconds),
            "requests": {
                "total": total_requests,
                "avg_latency_ms": round(avg_latency, 2),
                "error_rate_percent": round(error_rate, 2),
            },
            "errors": {
                "total": total_errors,
            },
            "alerts": {
                "active_count": len(active_alerts),
                "critical_count": len(
                    [a for a in active_alerts if a.severity == AlertSeverity.CRITICAL]
                ),
            },
            "timestamp": datetime.now().isoformat(),
        }

    def _format_uptime(self, seconds: float) -> str:
        """Format uptime in human-readable form."""
        td = timedelta(seconds=seconds)
        days = td.days
        hours, remainder = divmod(td.seconds, 3600)
        minutes, secs = divmod(remainder, 60)

        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        parts.append(f"{secs}s")

        return " ".join(parts)

    # === Alerts ===

    async def add_alert_rule(
        self,
        name: str,
        metric_name: str,
        condition: str,
        threshold: float,
        severity: AlertSeverity = AlertSeverity.WARNING,
        description: str = "",
    ) -> str:
        """
        Add an alert rule.
        
        Args:
            name: Alert name
            metric_name: Metric to monitor
            condition: Condition (>, <, >=, <=, ==, !=)
            threshold: Threshold value
            severity: Alert severity
            description: Alert description
            
        Returns:
            Rule ID
        """
        rule = AlertRule(
            name=name,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            severity=severity,
            description=description,
        )
        return await self._monitoring.add_alert_rule(rule)

    async def get_active_alerts(self) -> list[Alert]:
        """Get all active alerts."""
        return await self._monitoring.get_active_alerts()

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        return await self._monitoring.acknowledge_alert(alert_id)

    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        return await self._monitoring.resolve_alert(alert_id)

    def on_alert(self, handler: Callable[[Alert], None]) -> None:
        """Register an alert handler."""
        self._monitoring.on_alert(handler)

    async def _setup_default_alerts(self) -> None:
        """Setup default alert rules for Voltran."""
        # High error rate alert
        await self.add_alert_rule(
            name="High Error Rate",
            metric_name=self.METRIC_ERRORS_TOTAL,
            condition=">",
            threshold=100,
            severity=AlertSeverity.ERROR,
            description="Error count exceeded threshold",
        )

    # === Export ===

    async def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        return await self._monitoring.export_prometheus()

    async def export_json(self) -> dict[str, Any]:
        """Export metrics as JSON."""
        return await self._monitoring.export_json()

    # === Internal ===

    async def _update_uptime(self) -> None:
        """Background task to update uptime gauge."""
        while self._running:
            if self._started_at:
                uptime = (datetime.now() - self._started_at).total_seconds()
                await self._monitoring.gauge(
                    self.METRIC_UPTIME_SECONDS,
                    uptime,
                    {"voltran_id": self._voltran_id, "node": self._node_name},
                )
            await asyncio.sleep(10)


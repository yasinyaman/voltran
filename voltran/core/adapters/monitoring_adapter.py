"""In-memory monitoring adapter implementation."""

import asyncio
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Optional

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


# Default histogram buckets (latency in ms)
DEFAULT_BUCKETS = [5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000]


class InMemoryMonitoringAdapter(IMonitoringPort):
    """
    In-memory implementation of monitoring port.
    
    Stores all metrics in memory - suitable for development
    and single-node deployments. For production with multiple
    nodes, use a distributed adapter (Redis, Prometheus, etc.)
    """

    def __init__(
        self,
        voltran_id: str = "",
        service_name: str = "voltran",
        default_buckets: Optional[list[float]] = None,
        alert_check_interval: float = 10.0,
    ):
        """
        Initialize monitoring adapter.
        
        Args:
            voltran_id: Voltran node ID
            service_name: Service name for labeling
            default_buckets: Default histogram buckets
            alert_check_interval: Seconds between alert checks
        """
        self._voltran_id = voltran_id
        self._service_name = service_name
        self._default_buckets = default_buckets or DEFAULT_BUCKETS
        self._alert_check_interval = alert_check_interval
        
        # Metric storage
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, MetricValue] = {}
        self._histograms: dict[str, dict] = {}  # name -> {buckets, sum, count}
        
        # Labels for metrics
        self._metric_labels: dict[str, dict[str, str]] = {}
        
        # Alert system
        self._alert_rules: dict[str, AlertRule] = {}
        self._active_alerts: dict[str, Alert] = {}
        self._alert_handlers: list[AlertHandler] = []
        self._last_alert_times: dict[str, datetime] = {}  # rule_id -> last trigger time
        
        # Background task
        self._alert_task: Optional[asyncio.Task] = None
        self._running = False

    # === Metric Recording ===

    async def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        self._counters[key] += value
        self._metric_labels[key] = labels or {}

    async def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Set a gauge metric value."""
        key = self._make_key(name, labels)
        self._gauges[key] = MetricValue(
            name=name,
            value=value,
            metric_type=MetricType.GAUGE,
            labels=labels or {},
            timestamp=datetime.now(),
        )

    async def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
        buckets: Optional[list[float]] = None,
    ) -> None:
        """Record a histogram observation."""
        key = self._make_key(name, labels)
        bucket_bounds = buckets or self._default_buckets
        
        if key not in self._histograms:
            self._histograms[key] = {
                "name": name,
                "labels": labels or {},
                "buckets": {b: 0 for b in bucket_bounds},
                "sum": 0.0,
                "count": 0,
            }
        
        hist = self._histograms[key]
        hist["sum"] += value
        hist["count"] += 1
        
        # Update buckets
        for bound in bucket_bounds:
            if value <= bound:
                hist["buckets"][bound] += 1

    async def timing(
        self,
        name: str,
        duration_ms: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a timing/latency observation."""
        await self.histogram(name, duration_ms, labels)

    # === Metric Querying ===

    async def get_metric(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[MetricValue]:
        """Get current value of a metric."""
        key = self._make_key(name, labels)
        
        # Check gauges first
        if key in self._gauges:
            return self._gauges[key]
        
        # Check counters
        if key in self._counters:
            return MetricValue(
                name=name,
                value=self._counters[key],
                metric_type=MetricType.COUNTER,
                labels=labels or {},
                timestamp=datetime.now(),
            )
        
        return None

    async def get_all_metrics(
        self,
        prefix: Optional[str] = None,
    ) -> list[MetricValue]:
        """Get all metrics, optionally filtered by prefix."""
        metrics = []
        
        # Add counters
        for key, value in self._counters.items():
            name = key.split("{")[0]
            if prefix and not name.startswith(prefix):
                continue
            metrics.append(MetricValue(
                name=name,
                value=value,
                metric_type=MetricType.COUNTER,
                labels=self._metric_labels.get(key, {}),
            ))
        
        # Add gauges
        for key, metric in self._gauges.items():
            if prefix and not metric.name.startswith(prefix):
                continue
            metrics.append(metric)
        
        return metrics

    async def get_histogram(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[HistogramValue]:
        """Get histogram metric with buckets."""
        key = self._make_key(name, labels)
        
        if key not in self._histograms:
            return None
        
        hist = self._histograms[key]
        buckets = [
            HistogramBucket(le=bound, count=count)
            for bound, count in sorted(hist["buckets"].items())
        ]
        
        return HistogramValue(
            name=name,
            buckets=buckets,
            sum=hist["sum"],
            count=hist["count"],
            labels=hist["labels"],
        )

    # === Alert Management ===

    async def add_alert_rule(self, rule: AlertRule) -> str:
        """Add an alert rule."""
        self._alert_rules[rule.id] = rule
        return rule.id

    async def remove_alert_rule(self, rule_id: str) -> bool:
        """Remove an alert rule."""
        if rule_id in self._alert_rules:
            del self._alert_rules[rule_id]
            return True
        return False

    async def get_alert_rules(self) -> list[AlertRule]:
        """Get all alert rules."""
        return list(self._alert_rules.values())

    async def get_active_alerts(self) -> list[Alert]:
        """Get all active (unresolved) alerts."""
        return [
            alert for alert in self._active_alerts.values()
            if alert.status == AlertStatus.ACTIVE
        ]

    async def acknowledge_alert(self, alert_id: str) -> bool:
        """Acknowledge an alert."""
        if alert_id in self._active_alerts:
            self._active_alerts[alert_id].status = AlertStatus.ACKNOWLEDGED
            self._active_alerts[alert_id].acknowledged_at = datetime.now()
            return True
        return False

    async def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert."""
        if alert_id in self._active_alerts:
            self._active_alerts[alert_id].status = AlertStatus.RESOLVED
            self._active_alerts[alert_id].resolved_at = datetime.now()
            return True
        return False

    def on_alert(self, handler: AlertHandler) -> None:
        """Register a handler for new alerts."""
        self._alert_handlers.append(handler)

    async def _check_alerts(self) -> None:
        """Check all alert rules against current metrics."""
        for rule in self._alert_rules.values():
            if not rule.enabled:
                continue
            
            # Check cooldown
            last_time = self._last_alert_times.get(rule.id)
            if last_time:
                cooldown = timedelta(seconds=rule.cooldown_seconds)
                if datetime.now() - last_time < cooldown:
                    continue
            
            # Get metric value
            metric = await self.get_metric(rule.metric_name, rule.labels)
            if not metric:
                continue
            
            # Check condition
            triggered = self._evaluate_condition(
                metric.value, rule.condition, rule.threshold
            )
            
            if triggered:
                await self._fire_alert(rule, metric.value)

    def _evaluate_condition(
        self,
        value: float,
        condition: str,
        threshold: float,
    ) -> bool:
        """Evaluate alert condition."""
        ops = {
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            "==": lambda a, b: a == b,
            "!=": lambda a, b: a != b,
        }
        return ops.get(condition, lambda a, b: False)(value, threshold)

    async def _fire_alert(self, rule: AlertRule, value: float) -> None:
        """Fire an alert."""
        alert = Alert(
            rule_id=rule.id,
            rule_name=rule.name,
            metric_name=rule.metric_name,
            metric_value=value,
            threshold=rule.threshold,
            severity=rule.severity,
            message=f"{rule.name}: {rule.metric_name} is {value} (threshold: {rule.condition} {rule.threshold})",
            labels=rule.labels,
        )
        
        self._active_alerts[alert.id] = alert
        self._last_alert_times[rule.id] = datetime.now()
        
        # Notify handlers
        for handler in self._alert_handlers:
            try:
                handler(alert)
            except Exception:
                pass  # Don't let handler errors stop alerting

    # === Export ===

    async def export_prometheus(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        
        # Export counters
        for key, value in self._counters.items():
            name = key.split("{")[0]
            labels = self._metric_labels.get(key, {})
            labels_str = self._format_labels(labels)
            lines.append(f"# TYPE {name} counter")
            lines.append(f"{name}{labels_str} {value}")
        
        # Export gauges
        for key, metric in self._gauges.items():
            labels_str = self._format_labels(metric.labels)
            lines.append(f"# TYPE {metric.name} gauge")
            lines.append(f"{metric.name}{labels_str} {metric.value}")
        
        # Export histograms
        for key, hist in self._histograms.items():
            name = hist["name"]
            labels = hist["labels"]
            labels_str = self._format_labels(labels)
            
            lines.append(f"# TYPE {name} histogram")
            
            cumulative = 0
            for bound, count in sorted(hist["buckets"].items()):
                cumulative += count
                bucket_labels = {**labels, "le": str(bound)}
                lines.append(f"{name}_bucket{self._format_labels(bucket_labels)} {cumulative}")
            
            # +Inf bucket
            inf_labels = {**labels, "le": "+Inf"}
            lines.append(f"{name}_bucket{self._format_labels(inf_labels)} {hist['count']}")
            
            lines.append(f"{name}_sum{labels_str} {hist['sum']}")
            lines.append(f"{name}_count{labels_str} {hist['count']}")
        
        return "\n".join(lines)

    async def export_json(self) -> dict[str, Any]:
        """Export metrics as JSON."""
        metrics = await self.get_all_metrics()
        histograms = []
        
        for key, hist in self._histograms.items():
            histograms.append({
                "name": hist["name"],
                "labels": hist["labels"],
                "buckets": [
                    {"le": b, "count": c}
                    for b, c in sorted(hist["buckets"].items())
                ],
                "sum": hist["sum"],
                "count": hist["count"],
            })
        
        return {
            "voltran_id": self._voltran_id,
            "service_name": self._service_name,
            "timestamp": datetime.now().isoformat(),
            "counters": [
                {"name": m.name, "value": m.value, "labels": m.labels}
                for m in metrics if m.metric_type == MetricType.COUNTER
            ],
            "gauges": [
                {"name": m.name, "value": m.value, "labels": m.labels}
                for m in metrics if m.metric_type == MetricType.GAUGE
            ],
            "histograms": histograms,
            "alerts": {
                "rules": len(self._alert_rules),
                "active": len(await self.get_active_alerts()),
            },
        }

    # === Lifecycle ===

    async def start(self) -> None:
        """Start the monitoring system."""
        if self._running:
            return
        
        self._running = True
        self._alert_task = asyncio.create_task(self._alert_loop())

    async def stop(self) -> None:
        """Stop the monitoring system."""
        self._running = False
        
        if self._alert_task:
            self._alert_task.cancel()
            try:
                await self._alert_task
            except asyncio.CancelledError:
                pass

    async def reset(self) -> None:
        """Reset all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._metric_labels.clear()
        self._active_alerts.clear()
        self._last_alert_times.clear()

    async def _alert_loop(self) -> None:
        """Background loop for checking alerts."""
        while self._running:
            try:
                await self._check_alerts()
            except Exception:
                pass  # Log this in production
            
            await asyncio.sleep(self._alert_check_interval)

    # === Helpers ===

    def _make_key(self, name: str, labels: Optional[dict[str, str]] = None) -> str:
        """Create unique key for metric name + labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def _format_labels(self, labels: dict[str, str]) -> str:
        """Format labels for Prometheus export."""
        if not labels:
            return ""
        parts = [f'{k}="{v}"' for k, v in sorted(labels.items())]
        return "{" + ",".join(parts) + "}"


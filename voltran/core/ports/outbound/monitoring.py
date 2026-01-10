"""Monitoring outbound port interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4


class MetricType(str, Enum):
    """Type of metric."""
    
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(str, Enum):
    """Alert severity levels."""
    
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    """Alert status."""
    
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


@dataclass
class MetricValue:
    """Single metric measurement."""
    
    name: str
    value: float
    metric_type: MetricType
    labels: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    unit: str = ""
    
    @property
    def key(self) -> str:
        """Get unique key for this metric (name + labels)."""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(self.labels.items()))
        return f"{self.name}{{{label_str}}}" if label_str else self.name


@dataclass
class HistogramBucket:
    """Histogram bucket for latency/distribution metrics."""
    
    le: float  # less than or equal
    count: int


@dataclass
class HistogramValue:
    """Histogram metric with buckets."""
    
    name: str
    buckets: list[HistogramBucket]
    sum: float
    count: int
    labels: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AlertRule:
    """Alert rule definition."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    metric_name: str = ""
    condition: str = ">"  # >, <, >=, <=, ==, !=
    threshold: float = 0.0
    severity: AlertSeverity = AlertSeverity.WARNING
    labels: dict[str, str] = field(default_factory=dict)
    description: str = ""
    enabled: bool = True
    cooldown_seconds: int = 300  # Don't re-alert for 5 minutes


@dataclass
class Alert:
    """Active alert instance."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    rule_id: str = ""
    rule_name: str = ""
    metric_name: str = ""
    metric_value: float = 0.0
    threshold: float = 0.0
    severity: AlertSeverity = AlertSeverity.WARNING
    status: AlertStatus = AlertStatus.ACTIVE
    message: str = ""
    labels: dict[str, str] = field(default_factory=dict)
    triggered_at: datetime = field(default_factory=datetime.now)
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None


AlertHandler = Callable[[Alert], None]


class IMonitoringPort(ABC):
    """
    Outbound port for system monitoring and metrics.
    
    This port defines the interface for:
    - Recording metrics (counters, gauges, histograms)
    - Querying metrics
    - Alert management
    - Health aggregation
    """

    # === Metric Recording ===

    @abstractmethod
    async def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name
            value: Amount to increment (default 1)
            labels: Optional labels for metric
        """
        pass

    @abstractmethod
    async def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric value.
        
        Args:
            name: Metric name
            value: Current value
            labels: Optional labels for metric
        """
        pass

    @abstractmethod
    async def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
        buckets: Optional[list[float]] = None,
    ) -> None:
        """
        Record a histogram observation.
        
        Args:
            name: Metric name
            value: Observed value
            labels: Optional labels for metric
            buckets: Custom bucket boundaries (optional)
        """
        pass

    @abstractmethod
    async def timing(
        self,
        name: str,
        duration_ms: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Record a timing/latency observation.
        
        Args:
            name: Metric name
            duration_ms: Duration in milliseconds
            labels: Optional labels for metric
        """
        pass

    # === Metric Querying ===

    @abstractmethod
    async def get_metric(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[MetricValue]:
        """
        Get current value of a metric.
        
        Args:
            name: Metric name
            labels: Labels to filter by
            
        Returns:
            MetricValue if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_all_metrics(
        self,
        prefix: Optional[str] = None,
    ) -> list[MetricValue]:
        """
        Get all metrics, optionally filtered by prefix.
        
        Args:
            prefix: Optional metric name prefix filter
            
        Returns:
            List of all matching metrics
        """
        pass

    @abstractmethod
    async def get_histogram(
        self,
        name: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Optional[HistogramValue]:
        """
        Get histogram metric with buckets.
        
        Args:
            name: Metric name
            labels: Labels to filter by
            
        Returns:
            HistogramValue if found, None otherwise
        """
        pass

    # === Alert Management ===

    @abstractmethod
    async def add_alert_rule(self, rule: AlertRule) -> str:
        """
        Add an alert rule.
        
        Args:
            rule: Alert rule definition
            
        Returns:
            Rule ID
        """
        pass

    @abstractmethod
    async def remove_alert_rule(self, rule_id: str) -> bool:
        """
        Remove an alert rule.
        
        Args:
            rule_id: ID of rule to remove
            
        Returns:
            True if removed, False if not found
        """
        pass

    @abstractmethod
    async def get_alert_rules(self) -> list[AlertRule]:
        """
        Get all alert rules.
        
        Returns:
            List of alert rules
        """
        pass

    @abstractmethod
    async def get_active_alerts(self) -> list[Alert]:
        """
        Get all active (unresolved) alerts.
        
        Returns:
            List of active alerts
        """
        pass

    @abstractmethod
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """
        Acknowledge an alert.
        
        Args:
            alert_id: ID of alert to acknowledge
            
        Returns:
            True if acknowledged, False if not found
        """
        pass

    @abstractmethod
    async def resolve_alert(self, alert_id: str) -> bool:
        """
        Resolve an alert.
        
        Args:
            alert_id: ID of alert to resolve
            
        Returns:
            True if resolved, False if not found
        """
        pass

    @abstractmethod
    def on_alert(self, handler: AlertHandler) -> None:
        """
        Register a handler for new alerts.
        
        Args:
            handler: Function to call when alert fires
        """
        pass

    # === Export ===

    @abstractmethod
    async def export_prometheus(self) -> str:
        """
        Export metrics in Prometheus format.
        
        Returns:
            Prometheus exposition format string
        """
        pass

    @abstractmethod
    async def export_json(self) -> dict[str, Any]:
        """
        Export metrics as JSON.
        
        Returns:
            Dictionary of all metrics
        """
        pass

    # === Lifecycle ===

    @abstractmethod
    async def start(self) -> None:
        """Start the monitoring system."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the monitoring system."""
        pass

    @abstractmethod
    async def reset(self) -> None:
        """Reset all metrics (useful for testing)."""
        pass


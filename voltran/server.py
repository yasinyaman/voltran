"""Voltran server - main entry point for running a Voltran node."""

import asyncio
import signal
from typing import Any, Optional

import structlog

from voltran.core.adapters.grpc_adapter import GrpcMessagingAdapter
from voltran.core.adapters.logging_adapter import (
    FileLoggingAdapter,
    InMemoryLoggingAdapter,
)
from voltran.core.adapters.monitoring_adapter import InMemoryMonitoringAdapter
from voltran.core.adapters.nats_adapter import NatsMessagingAdapter
from voltran.core.adapters.rest_adapter import RestMessagingAdapter
from voltran.core.domain.models import (
    Endpoint,
    HealthStatus,
    ModuleDescriptor,
    NodeRole,
    VoltranNode,
)
from voltran.core.adapters.authorization_adapter import InMemoryAuthorizationAdapter
from voltran.core.domain.services.authorization import AuthorizationService
from voltran.core.domain.services.logging_service import LoggingService, ScopedLogger
from voltran.core.domain.services.monitoring import MonitoringService
from voltran.core.ports.outbound.authorization import IAuthorizationPort
from voltran.core.ports.outbound.logging import ILoggingPort, LogLevel, LogQuery
from voltran.core.ports.outbound.messaging import IMessagingPort
from voltran.core.ports.outbound.monitoring import AlertSeverity, IMonitoringPort
from voltran.discovery.auto_discovery import AutoDiscoveryService, DiscoveredService
from voltran.discovery.local import LocalDiscoveryService
from voltran.federation.mesh import FederationMesh
from voltran.sdk.decorators import is_voltran_module
from voltran.sdk.module import BaseModule, ModuleContext, create_module_descriptor

logger = structlog.get_logger(__name__)


class Voltran:
    """
    Main Voltran server class.
    
    This is the primary interface for running a Voltran node.
    It manages modules, discovery, and federation.
    
    Usage:
        voltran = Voltran(name="my-node", port=50051)
        voltran.register_module(MyModule())
        await voltran.start()
        
        # Join federation
        await voltran.join_federation("leader-node-id")
    """

    def __init__(
        self,
        name: str = "voltran-node",
        host: str = "0.0.0.0",
        port: int = 50051,
        nats_servers: Optional[list[str]] = None,
        use_nats: bool = True,
        use_rest: bool = False,
        rest_port: int = 8080,
        auto_discovery: bool = False,
        service_type: str = "_voltran._tcp.local.",
        enable_monitoring: bool = True,
        enable_logging: bool = True,
        log_dir: Optional[str] = None,
        log_to_file: bool = False,
        min_log_level: LogLevel = LogLevel.INFO,
        enable_authorization: bool = True,
    ):
        """
        Initialize Voltran node.
        
        Args:
            name: Node name
            host: Host to bind to
            port: Port for gRPC
            nats_servers: NATS server URLs for federation
            use_nats: Whether to use NATS for messaging
            use_rest: Whether to use REST API for messaging
            rest_port: Port for REST API server
            auto_discovery: Enable Bonjour/mDNS auto-discovery
            service_type: mDNS service type for auto-discovery
            enable_monitoring: Enable built-in monitoring
            enable_logging: Enable built-in logging
            log_dir: Directory for log files (if log_to_file=True)
            log_to_file: Write logs to file instead of memory only
            min_log_level: Minimum log level to record
            enable_authorization: Enable built-in authorization
        """
        # Create node
        self._node = VoltranNode(
            name=name,
            role=NodeRole.STANDALONE,
            health=HealthStatus.UNKNOWN,
            discovery_endpoint=Endpoint(host=host, port=port),
        )
        
        self._host = host
        self._port = port
        self._rest_port = rest_port
        self._auto_discovery_enabled = auto_discovery
        self._service_type = service_type
        
        # Create messaging adapter
        if use_rest:
            self._messaging: IMessagingPort = RestMessagingAdapter(
                host=host,
                port=rest_port,
                voltran_id=self._node.id,
            )
        elif use_nats and nats_servers:
            self._messaging = NatsMessagingAdapter(
                servers=nats_servers,
                voltran_id=self._node.id,
            )
        else:
            self._messaging = GrpcMessagingAdapter(
                host=host,
                port=port,
                voltran_id=self._node.id,
            )
        
        # === Monitoring Setup ===
        self._monitoring_enabled = enable_monitoring
        self._monitoring_adapter: Optional[IMonitoringPort] = None
        self._monitoring: Optional[MonitoringService] = None
        
        if enable_monitoring:
            self._monitoring_adapter = InMemoryMonitoringAdapter(
                voltran_id=self._node.id,
                service_name=name,
            )
            self._monitoring = MonitoringService(
                monitoring_port=self._monitoring_adapter,
                voltran_id=self._node.id,
                node_name=name,
            )
        
        # === Logging Setup ===
        self._logging_enabled = enable_logging
        self._logging_adapter: Optional[ILoggingPort] = None
        self._logging: Optional[LoggingService] = None
        
        if enable_logging:
            if log_to_file:
                self._logging_adapter = FileLoggingAdapter(
                    voltran_id=self._node.id,
                    service_name=name,
                    log_dir=log_dir or "logs",
                    min_level=min_log_level,
                )
            else:
                self._logging_adapter = InMemoryLoggingAdapter(
                    voltran_id=self._node.id,
                    service_name=name,
                    min_level=min_log_level,
                )
            self._logging = LoggingService(
                logging_port=self._logging_adapter,
                voltran_id=self._node.id,
                node_name=name,
            )
        
        # === Authorization Setup ===
        self._authorization_enabled = enable_authorization
        self._authorization_adapter: Optional[IAuthorizationPort] = None
        self._authorization: Optional[AuthorizationService] = None
        
        if enable_authorization:
            self._authorization_adapter = InMemoryAuthorizationAdapter(
                voltran_id=self._node.id,
            )
            self._authorization = AuthorizationService(
                authorization_port=self._authorization_adapter,
                voltran_id=self._node.id,
            )
        
        # Auto-discovery service (Bonjour/mDNS)
        self._auto_discovery: Optional[AutoDiscoveryService] = None
        if auto_discovery:
            self._auto_discovery = AutoDiscoveryService(
                voltran_id=self._node.id,
                name=name,
                port=port if not use_rest else rest_port,
                host=host if host != "0.0.0.0" else None,
                service_type=service_type,
                properties={
                    "modules": "0",
                    "protocol": "rest" if use_rest else ("nats" if use_nats else "grpc"),
                },
            )
        
        # Create discovery service
        self._discovery = LocalDiscoveryService(
            voltran_id=self._node.id,
            messaging=self._messaging,
        )
        
        # Create federation mesh
        self._federation = FederationMesh(
            local_node=self._node,
            local_discovery=self._discovery,
            messaging=self._messaging,
        )
        
        # Module instances
        self._modules: dict[str, Any] = {}
        
        # Running state
        self._running = False
        self._shutdown_event = asyncio.Event()

    @property
    def id(self) -> str:
        """Get node ID."""
        return self._node.id

    @property
    def name(self) -> str:
        """Get node name."""
        return self._node.name

    @property
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running

    @property
    def is_federated(self) -> bool:
        """Check if node is in a federation."""
        return self._federation.is_federated

    @property
    def is_leader(self) -> bool:
        """Check if node is federation leader."""
        return self._federation.is_leader

    @property
    def monitoring(self) -> Optional[MonitoringService]:
        """Get monitoring service (if enabled)."""
        return self._monitoring

    @property
    def logging(self) -> Optional[LoggingService]:
        """Get logging service (if enabled)."""
        return self._logging

    @property
    def authorization(self) -> Optional[AuthorizationService]:
        """Get authorization service (if enabled)."""
        return self._authorization

    def get_logger(self, name: str, module_id: Optional[str] = None) -> Optional[ScopedLogger]:
        """
        Get a scoped logger for a module or component.
        
        Args:
            name: Logger name
            module_id: Optional module ID
            
        Returns:
            ScopedLogger if logging is enabled, None otherwise
        """
        if self._logging:
            return self._logging.get_logger(name, module_id)
        return None

    def register_module(self, module: Any) -> str:
        """
        Register a module with this Voltran node.
        
        Args:
            module: Module instance (decorated with @voltran_module)
            
        Returns:
            Module ID
        """
        # Validate module
        if not is_voltran_module(type(module)):
            raise ValueError(
                f"Class {type(module).__name__} is not decorated with @voltran_module"
            )
        
        # Create descriptor
        descriptor = create_module_descriptor(
            module,
            endpoints=[Endpoint(host=self._host, port=self._port)],
        )
        
        # Set module ID
        if hasattr(module, "_id"):
            module._id = descriptor.id
        
        # Inject context
        context = ModuleContext(
            voltran_id=self._node.id,
            discovery=self._discovery,
            messaging=self._messaging,
            authorization=self._authorization,
        )
        
        if hasattr(module, "set_context"):
            module.set_context(context)
        else:
            module._voltran_context = context
        
        # Store module
        self._modules[descriptor.id] = {
            "instance": module,
            "descriptor": descriptor,
        }

        logger.info(
            "module_registered",
            module_id=descriptor.id,
            module_name=descriptor.name,
        )

        return descriptor.id

    def unregister_module(self, module_id: str) -> bool:
        """
        Unregister a module.
        
        Args:
            module_id: ID of module to unregister
            
        Returns:
            True if unregistered
        """
        if module_id in self._modules:
            del self._modules[module_id]
            logger.info("module_unregistered", module_id=module_id)
            return True
        return False

    async def start(self) -> None:
        """Start the Voltran node."""
        if self._running:
            return

        logger.info(
            "voltran_starting",
            node_id=self._node.id,
            node_name=self._node.name,
            host=self._host,
            port=self._port,
            auto_discovery=self._auto_discovery_enabled,
            monitoring=self._monitoring_enabled,
            logging=self._logging_enabled,
        )

        self._running = True
        self._node.health = HealthStatus.HEALTHY
        
        # Start monitoring service
        if self._monitoring:
            await self._monitoring.start()
            logger.info("monitoring_started", voltran_id=self._node.id)
        
        # Start logging service
        if self._logging:
            await self._logging.start()
        
        # Start authorization service
        if self._authorization:
            await self._authorization.start()
        
        # Start federation mesh (includes discovery and messaging)
        await self._federation.start()
        
        # Register modules with discovery
        for module_data in self._modules.values():
            descriptor = module_data["descriptor"]
            instance = module_data["instance"]
            
            # Start module if it has start method
            if hasattr(instance, "start"):
                await instance.start()
            
            # Register with discovery
            await self._discovery.register(descriptor)
            
            # Add to node's local modules
            self._node.local_modules[descriptor.id] = descriptor
            
            # Record module registration metric
            if self._monitoring:
                await self._monitoring.record_module_registered(descriptor.name)

        # Update module counts in monitoring
        if self._monitoring:
            healthy_count = sum(
                1 for m in self._modules.values()
                if m["descriptor"].health == HealthStatus.HEALTHY
            )
            await self._monitoring.update_module_counts(
                total_modules=len(self._modules),
                healthy_modules=healthy_count,
            )

        # Start auto-discovery (Bonjour/mDNS)
        if self._auto_discovery:
            # Update module count in properties
            self._auto_discovery._properties["modules"] = str(len(self._modules))
            await self._auto_discovery.start()
            
            # Register callback for discovered services
            self._auto_discovery.on_service_added(self._on_peer_discovered)

        logger.info(
            "voltran_started",
            node_id=self._node.id,
            module_count=len(self._modules),
        )

    async def stop(self) -> None:
        """Stop the Voltran node."""
        if not self._running:
            return

        logger.info("voltran_stopping", node_id=self._node.id)

        # Stop auto-discovery
        if self._auto_discovery:
            await self._auto_discovery.stop()

        # Stop modules
        for module_data in self._modules.values():
            instance = module_data["instance"]
            descriptor = module_data["descriptor"]
            
            # Unregister from discovery
            await self._discovery.unregister(descriptor.id)
            
            # Stop module if it has stop method
            if hasattr(instance, "stop"):
                await instance.stop()

        # Stop federation mesh
        await self._federation.stop()
        
        # Stop logging service
        if self._logging:
            await self._logging.stop()
        
        # Stop authorization service
        if self._authorization:
            await self._authorization.stop()
        
        # Stop monitoring service
        if self._monitoring:
            await self._monitoring.stop()
        
        self._running = False
        self._node.health = HealthStatus.UNKNOWN
        self._shutdown_event.set()

        logger.info("voltran_stopped", node_id=self._node.id)

    async def run_forever(self) -> None:
        """Run the server until shutdown signal."""
        # Setup signal handlers
        loop = asyncio.get_event_loop()
        
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                lambda: asyncio.create_task(self._handle_shutdown()),
            )
        
        # Start server
        await self.start()
        
        # Wait for shutdown
        await self._shutdown_event.wait()

    async def _handle_shutdown(self) -> None:
        """Handle shutdown signal."""
        logger.info("shutdown_signal_received")
        await self.stop()

    def _on_peer_discovered(self, service: DiscoveredService) -> None:
        """Callback when a peer is discovered via mDNS."""
        logger.info(
            "peer_auto_discovered",
            name=service.name,
            voltran_id=service.voltran_id[:8] + "...",
            host=service.host,
            port=service.port,
        )
        
        # If using REST adapter, register the peer
        if hasattr(self._messaging, "register_peer"):
            protocol = service.properties.get("protocol", "grpc")
            if protocol == "rest":
                base_url = f"http://{service.host}:{service.port}/api/v1"
                self._messaging.register_peer(service.voltran_id, base_url)

    # === Auto-Discovery Methods ===

    async def discover_local_peers(self, timeout: float = 3.0) -> list[DiscoveredService]:
        """
        Discover Voltran nodes on the local network using mDNS/Bonjour.
        
        Args:
            timeout: How long to wait for discovery
            
        Returns:
            List of discovered services
        """
        if not self._auto_discovery:
            logger.warning("auto_discovery_not_enabled")
            return []
        
        return await self._auto_discovery.discover_once(timeout)

    def get_discovered_peers(self) -> dict[str, DiscoveredService]:
        """
        Get all discovered peers (from auto-discovery cache).
        
        Returns:
            Dictionary of voltran_id -> DiscoveredService
        """
        if not self._auto_discovery:
            return {}
        return self._auto_discovery.discovered_services

    async def auto_join_federation(self, timeout: float = 5.0) -> bool:
        """
        Automatically discover and join an existing federation.
        
        Searches for federation leaders on the local network and joins
        the first one found.
        
        Args:
            timeout: Discovery timeout
            
        Returns:
            True if joined a federation, False otherwise
        """
        if not self._auto_discovery:
            logger.warning("auto_discovery_not_enabled")
            return False
        
        # Discover peers
        peers = await self.discover_local_peers(timeout)
        
        # Look for federation leaders
        for peer in peers:
            if peer.properties.get("role") == "leader":
                logger.info(
                    "auto_joining_federation",
                    leader=peer.name,
                    voltran_id=peer.voltran_id,
                )
                return await self.join_federation(peer.voltran_id)
        
        logger.info("no_federation_leaders_found")
        return False

    # === Federation Methods ===

    async def create_federation(self, name: str) -> None:
        """
        Create a new federation with this node as leader.
        
        Args:
            name: Federation name
        """
        await self._federation.create_federation(name)

    async def join_federation(self, leader_id: str) -> bool:
        """
        Join an existing federation.
        
        Args:
            leader_id: ID of federation leader node
            
        Returns:
            True if joined successfully
        """
        return await self._federation.join_federation(leader_id)

    async def leave_federation(self) -> bool:
        """
        Leave the current federation.
        
        Returns:
            True if left successfully
        """
        return await self._federation.leave_federation()

    async def get_federation_peers(self) -> list[VoltranNode]:
        """
        Get all peers in the federation.
        
        Returns:
            List of peer nodes
        """
        return await self._federation.discover_peers()

    async def sync_registry(self) -> dict[str, ModuleDescriptor]:
        """
        Sync module registry across federation.
        
        Returns:
            Complete global registry
        """
        return await self._federation.sync_registry()

    # === Cluster Methods ===

    async def create_cluster(self, name: str) -> str:
        """
        Create a new module cluster.
        
        Args:
            name: Cluster name
            
        Returns:
            Cluster ID
        """
        cluster = await self._discovery.create_cluster(name)
        self._node.local_clusters[cluster.id] = cluster
        return cluster.id

    async def add_to_cluster(self, cluster_id: str, module_id: str) -> bool:
        """
        Add a module to a cluster.
        
        Args:
            cluster_id: Cluster ID
            module_id: Module ID
            
        Returns:
            True if added
        """
        return await self._discovery.add_module(cluster_id, module_id)

    async def fuse_cluster(self, cluster_id: str, virtual_name: str) -> ModuleDescriptor:
        """
        Fuse a cluster into a virtual module.
        
        Args:
            cluster_id: Cluster ID
            virtual_name: Name for virtual module
            
        Returns:
            Virtual module descriptor
        """
        return await self._discovery.fuse_cluster(cluster_id, virtual_name)

    # === Discovery Methods ===

    async def find_module(self, contract_id: str) -> Optional[ModuleDescriptor]:
        """
        Find a module by contract ID.
        
        Args:
            contract_id: Contract ID to search for
            
        Returns:
            Module descriptor if found
        """
        from voltran.core.domain.models import ModuleQuery
        
        query = ModuleQuery(contract_id=contract_id)
        modules = await self._discovery.find(query)
        
        if modules:
            return modules[0]
        return None

    async def find_all_modules(self, contract_id: str) -> list[ModuleDescriptor]:
        """
        Find all modules by contract ID (including federation).
        
        Args:
            contract_id: Contract ID to search for
            
        Returns:
            List of matching modules
        """
        if self.is_federated:
            return await self._federation.find_in_federation(contract_id)
        
        from voltran.core.domain.models import ModuleQuery
        query = ModuleQuery(contract_id=contract_id)
        return await self._discovery.find(query)

    async def get_module(self, module_id: str) -> Optional[ModuleDescriptor]:
        """
        Get a module by ID.
        
        Args:
            module_id: Module ID
            
        Returns:
            Module descriptor if found
        """
        return await self._discovery.get(module_id)

    async def list_modules(self) -> list[ModuleDescriptor]:
        """
        List all local modules.
        
        Returns:
            List of module descriptors
        """
        return await self._discovery.get_all()

    # === Health ===

    async def health_check(self) -> dict[str, Any]:
        """
        Get health status of this node.
        
        Returns:
            Health status dictionary
        """
        modules_health = []
        
        for module_data in self._modules.values():
            instance = module_data["instance"]
            descriptor = module_data["descriptor"]
            
            health = HealthStatus.UNKNOWN
            if hasattr(instance, "health_check"):
                try:
                    result = await instance.health_check()
                    if isinstance(result, HealthStatus):
                        health = result
                    elif hasattr(instance, "health") and isinstance(instance.health, HealthStatus):
                        health = instance.health
                    elif isinstance(descriptor.health, HealthStatus):
                        health = descriptor.health
                except Exception:
                    if hasattr(instance, "health") and isinstance(instance.health, HealthStatus):
                        health = instance.health
                    elif isinstance(descriptor.health, HealthStatus):
                        health = descriptor.health
            elif hasattr(instance, "health") and isinstance(instance.health, HealthStatus):
                health = instance.health
            elif isinstance(descriptor.health, HealthStatus):
                health = descriptor.health
            
            modules_health.append({
                "id": descriptor.id,
                "name": descriptor.name,
                "health": health.value,
            })
        
        result = {
            "node_id": self._node.id,
            "node_name": self._node.name,
            "health": self._node.health.value,
            "running": self._running,
            "federated": self.is_federated,
            "is_leader": self.is_leader,
            "module_count": len(self._modules),
            "modules": modules_health,
        }
        
        # Add monitoring info if available
        if self._monitoring:
            result["monitoring"] = await self._monitoring.get_dashboard()
        
        return result

    # === Monitoring API ===

    async def get_metrics(self, prefix: Optional[str] = None) -> dict[str, Any]:
        """
        Get all metrics.
        
        Args:
            prefix: Optional metric name prefix filter
            
        Returns:
            Metrics data
        """
        if not self._monitoring:
            return {"error": "Monitoring not enabled"}
        
        return await self._monitoring.export_json()

    async def get_metrics_prometheus(self) -> str:
        """
        Get metrics in Prometheus format.
        
        Returns:
            Prometheus exposition format string
        """
        if not self._monitoring:
            return "# Monitoring not enabled"
        
        return await self._monitoring.export_prometheus()

    async def get_monitoring_dashboard(self) -> dict[str, Any]:
        """
        Get monitoring dashboard data.
        
        Returns:
            Dashboard metrics summary
        """
        if not self._monitoring:
            return {"error": "Monitoring not enabled"}
        
        return await self._monitoring.get_dashboard()

    async def get_active_alerts(self) -> list[dict[str, Any]]:
        """
        Get all active alerts.
        
        Returns:
            List of active alerts
        """
        if not self._monitoring:
            return []
        
        alerts = await self._monitoring.get_active_alerts()
        return [
            {
                "id": alert.id,
                "rule_name": alert.rule_name,
                "metric_name": alert.metric_name,
                "metric_value": alert.metric_value,
                "threshold": alert.threshold,
                "severity": alert.severity.value,
                "status": alert.status.value,
                "message": alert.message,
                "triggered_at": alert.triggered_at.isoformat(),
            }
            for alert in alerts
        ]

    async def add_alert_rule(
        self,
        name: str,
        metric_name: str,
        condition: str,
        threshold: float,
        severity: str = "warning",
        description: str = "",
    ) -> Optional[str]:
        """
        Add a new alert rule.
        
        Args:
            name: Alert name
            metric_name: Metric to monitor
            condition: Condition (>, <, >=, <=, ==, !=)
            threshold: Threshold value
            severity: Alert severity (info, warning, error, critical)
            description: Alert description
            
        Returns:
            Rule ID if added, None if monitoring disabled
        """
        if not self._monitoring:
            return None
        
        severity_map = {
            "info": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "error": AlertSeverity.ERROR,
            "critical": AlertSeverity.CRITICAL,
        }
        
        return await self._monitoring.add_alert_rule(
            name=name,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            severity=severity_map.get(severity.lower(), AlertSeverity.WARNING),
            description=description,
        )

    # === Logging API ===

    async def get_logs(
        self,
        limit: int = 100,
        min_level: Optional[str] = None,
        logger_name: Optional[str] = None,
        message_contains: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Get recent logs.
        
        Args:
            limit: Maximum number of entries
            min_level: Minimum log level filter
            logger_name: Filter by logger name
            message_contains: Filter by message content
            
        Returns:
            List of log entries
        """
        if not self._logging:
            return []
        
        level_map = {
            "trace": LogLevel.TRACE,
            "debug": LogLevel.DEBUG,
            "info": LogLevel.INFO,
            "warning": LogLevel.WARNING,
            "error": LogLevel.ERROR,
            "critical": LogLevel.CRITICAL,
        }
        
        query = LogQuery(
            min_level=level_map.get(min_level.lower()) if min_level else None,
            logger_name=logger_name,
            message_contains=message_contains,
            limit=limit,
        )
        
        logs = await self._logging.query(query)
        return [log.to_dict() for log in logs]

    async def get_log_stats(self) -> dict[str, Any]:
        """
        Get log statistics.
        
        Returns:
            Log statistics
        """
        if not self._logging:
            return {"error": "Logging not enabled"}
        
        return await self._logging.get_dashboard()

    async def get_logs_by_trace(self, trace_id: str) -> list[dict[str, Any]]:
        """
        Get all logs for a trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of log entries for the trace
        """
        if not self._logging:
            return []
        
        logs = await self._logging.get_by_trace(trace_id)
        return [log.to_dict() for log in logs]

    async def export_logs(
        self,
        format: str = "json",
        limit: int = 1000,
    ) -> Any:
        """
        Export logs in specified format.
        
        Args:
            format: Export format (json or jsonl)
            limit: Maximum number of entries
            
        Returns:
            Logs in requested format
        """
        if not self._logging:
            return [] if format == "json" else ""
        
        query = LogQuery(limit=limit)
        
        if format == "jsonl":
            return await self._logging.export_jsonl(query)
        else:
            return await self._logging.export_json(query)

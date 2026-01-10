"""Local discovery service - manages modules within a single Voltran node."""

import asyncio
from datetime import datetime, timedelta
from typing import Any, Optional

import structlog

from voltran.core.domain.models import (
    Cluster,
    HealthStatus,
    ModuleDescriptor,
    ModuleQuery,
)
from voltran.core.ports.inbound.cluster import IClusterPort
from voltran.core.ports.inbound.module_registry import IModuleRegistryPort
from voltran.core.ports.outbound.messaging import IMessagingPort
from voltran.core.domain.services.fusion import FusionService

logger = structlog.get_logger(__name__)


class LocalDiscoveryService(IModuleRegistryPort, IClusterPort):
    """
    Local discovery service for a single Voltran node.
    
    Manages module registration, health checks, and cluster operations.
    This is the primary registry for modules running on a single node.
    """

    def __init__(
        self,
        voltran_id: str,
        messaging: Optional[IMessagingPort] = None,
        heartbeat_timeout: int = 60,
        health_check_interval: int = 10,
    ):
        """
        Initialize local discovery.
        
        Args:
            voltran_id: ID of this Voltran node
            messaging: Optional messaging port for federation events
            heartbeat_timeout: Seconds before module marked unhealthy
            health_check_interval: Seconds between health checks
        """
        self._voltran_id = voltran_id
        self._messaging = messaging
        self._heartbeat_timeout = heartbeat_timeout
        self._health_check_interval = health_check_interval
        
        # Module registry
        self._registry: dict[str, ModuleDescriptor] = {}
        
        # Cluster registry
        self._clusters: dict[str, Cluster] = {}
        
        # Fusion service
        self._fusion_service = FusionService(self)
        
        # Background tasks
        self._health_check_task: Optional[asyncio.Task[None]] = None
        self._running = False

    @property
    def voltran_id(self) -> str:
        """Get Voltran node ID."""
        return self._voltran_id

    async def start(self) -> None:
        """Start the discovery service."""
        if self._running:
            return

        self._running = True
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        
        # Setup messaging handlers if available
        if self._messaging:
            await self._setup_message_handlers()

        logger.info(
            "local_discovery_started",
            voltran_id=self._voltran_id,
        )

    async def stop(self) -> None:
        """Stop the discovery service."""
        self._running = False
        
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        logger.info(
            "local_discovery_stopped",
            voltran_id=self._voltran_id,
        )

    # === Module Registry Implementation ===

    async def register(self, module: ModuleDescriptor) -> str:
        """Register a module."""
        module.voltran_id = self._voltran_id
        module.last_heartbeat = datetime.now()
        module.health = HealthStatus.HEALTHY
        
        self._registry[module.id] = module
        
        # Notify federation
        if self._messaging:
            await self._messaging.publish(
                "voltran.federation.module.registered",
                {
                    "voltran_id": self._voltran_id,
                    "module_id": module.id,
                    "module_name": module.name,
                    "capabilities": [
                        {"name": c.name, "contract_id": c.contract_id, "direction": c.direction.value}
                        for c in module.capabilities
                    ],
                },
            )

        logger.info(
            "module_registered",
            module_id=module.id,
            module_name=module.name,
            capabilities_count=len(module.capabilities),
        )

        return module.id

    async def unregister(self, module_id: str) -> bool:
        """Unregister a module."""
        if module_id not in self._registry:
            return False

        module = self._registry.pop(module_id)
        
        # Remove from any clusters
        for cluster in self._clusters.values():
            if module_id in cluster.module_ids:
                cluster.module_ids.remove(module_id)

        # Notify federation
        if self._messaging:
            await self._messaging.publish(
                "voltran.federation.module.unregistered",
                {
                    "voltran_id": self._voltran_id,
                    "module_id": module_id,
                },
            )

        logger.info(
            "module_unregistered",
            module_id=module_id,
            module_name=module.name,
        )

        return True

    async def get(self, module_id: str) -> Optional[ModuleDescriptor]:
        """Get a module by ID."""
        return self._registry.get(module_id)

    async def find(self, query: ModuleQuery) -> list[ModuleDescriptor]:
        """Find modules matching query."""
        results: list[ModuleDescriptor] = []
        
        for module in self._registry.values():
            if self._matches_query(module, query):
                results.append(module)
        
        return results

    async def heartbeat(self, module_id: str) -> bool:
        """Update module heartbeat."""
        if module_id not in self._registry:
            return False

        module = self._registry[module_id]
        module.last_heartbeat = datetime.now()
        
        if module.health == HealthStatus.UNHEALTHY:
            module.health = HealthStatus.HEALTHY
            logger.info(
                "module_recovered",
                module_id=module_id,
            )

        return True

    async def get_all(self) -> list[ModuleDescriptor]:
        """Get all registered modules."""
        return list(self._registry.values())

    async def count(self) -> int:
        """Get count of registered modules."""
        return len(self._registry)

    # === Cluster Management Implementation ===

    async def create_cluster(self, name: str) -> Cluster:
        """Create a new cluster."""
        cluster = Cluster(
            name=name,
            voltran_id=self._voltran_id,
        )
        self._clusters[cluster.id] = cluster

        logger.info(
            "cluster_created",
            cluster_id=cluster.id,
            cluster_name=name,
        )

        return cluster

    async def delete_cluster(self, cluster_id: str) -> bool:
        """Delete a cluster."""
        if cluster_id not in self._clusters:
            return False

        cluster = self._clusters.pop(cluster_id)
        
        logger.info(
            "cluster_deleted",
            cluster_id=cluster_id,
            cluster_name=cluster.name,
        )

        return True

    async def get_cluster(self, cluster_id: str) -> Optional[Cluster]:
        """Get a cluster by ID."""
        return self._clusters.get(cluster_id)

    async def add_module(self, cluster_id: str, module_id: str) -> bool:
        """Add a module to a cluster."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            return False

        if module_id not in self._registry:
            return False

        if module_id not in cluster.module_ids:
            cluster.module_ids.append(module_id)
            self._registry[module_id].cluster_id = cluster_id

        logger.info(
            "module_added_to_cluster",
            cluster_id=cluster_id,
            module_id=module_id,
        )

        return True

    async def remove_module(self, cluster_id: str, module_id: str) -> bool:
        """Remove a module from a cluster."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            return False

        if module_id in cluster.module_ids:
            cluster.module_ids.remove(module_id)
            if module_id in self._registry:
                self._registry[module_id].cluster_id = None
            return True

        return False

    async def get_cluster_modules(self, cluster_id: str) -> list[ModuleDescriptor]:
        """Get all modules in a cluster."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            return []

        modules: list[ModuleDescriptor] = []
        for module_id in cluster.module_ids:
            module = self._registry.get(module_id)
            if module:
                modules.append(module)

        return modules

    async def fuse_cluster(self, cluster_id: str, virtual_name: str) -> ModuleDescriptor:
        """Fuse a cluster into a virtual module."""
        cluster = self._clusters.get(cluster_id)
        if not cluster:
            raise ValueError(f"Cluster not found: {cluster_id}")

        modules = await self.get_cluster_modules(cluster_id)
        if not modules:
            raise ValueError(f"Cluster has no modules: {cluster_id}")

        virtual_module = await self._fusion_service.fuse_cluster(cluster, virtual_name)
        
        cluster.virtual_module = virtual_module
        cluster.is_fused = True
        
        # Register virtual module
        await self.register(virtual_module)

        logger.info(
            "cluster_fused",
            cluster_id=cluster_id,
            virtual_module_id=virtual_module.id,
            module_count=len(modules),
        )

        return virtual_module

    async def unfuse_cluster(self, cluster_id: str) -> bool:
        """Unfuse a cluster."""
        cluster = self._clusters.get(cluster_id)
        if not cluster or not cluster.is_fused:
            return False

        if cluster.virtual_module:
            await self.unregister(cluster.virtual_module.id)

        cluster.virtual_module = None
        cluster.is_fused = False

        logger.info(
            "cluster_unfused",
            cluster_id=cluster_id,
        )

        return True

    async def list_clusters(self) -> list[Cluster]:
        """List all clusters."""
        return list(self._clusters.values())

    # === Private Methods ===

    async def _health_check_loop(self) -> None:
        """Periodic health check for modules."""
        while self._running:
            try:
                await asyncio.sleep(self._health_check_interval)
                
                now = datetime.now()
                timeout = timedelta(seconds=self._heartbeat_timeout)
                
                for module in self._registry.values():
                    if now - module.last_heartbeat > timeout:
                        if module.health != HealthStatus.UNHEALTHY:
                            module.health = HealthStatus.UNHEALTHY
                            logger.warning(
                                "module_unhealthy",
                                module_id=module.id,
                                module_name=module.name,
                                last_heartbeat=module.last_heartbeat.isoformat(),
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("health_check_error", error=str(e))

    async def _setup_message_handlers(self) -> None:
        """Setup message handlers for federation events."""
        if not self._messaging:
            return

        # Handle discovery queries from federation
        await self._messaging.subscribe(
            f"voltran.{self._voltran_id}.discovery.query",
            self._handle_discovery_query,
        )

    async def _handle_discovery_query(self, message: Any) -> None:
        """Handle discovery query from federation."""
        from voltran.core.ports.outbound.messaging import Message
        
        if not isinstance(message, Message):
            return

        query_data = message.payload.get("query", {})
        query = ModuleQuery(**query_data)
        
        results = await self.find(query)
        
        if self._messaging:
            await self._messaging.respond(
                message,
                {
                    "modules": [
                        {
                            "id": m.id,
                            "name": m.name,
                            "version": m.version,
                            "health": m.health.value,
                            "capabilities": [
                                {"name": c.name, "contract_id": c.contract_id}
                                for c in m.capabilities
                            ],
                        }
                        for m in results
                    ],
                },
            )

    def _matches_query(self, module: ModuleDescriptor, query: ModuleQuery) -> bool:
        """Check if module matches query criteria."""
        if query.contract_id:
            has_contract = any(
                c.contract_id == query.contract_id for c in module.capabilities
            )
            if not has_contract:
                return False

        if query.name_pattern and query.name_pattern.lower() not in module.name.lower():
            return False

        if query.cluster_id and module.cluster_id != query.cluster_id:
            return False

        if query.voltran_id and module.voltran_id != query.voltran_id:
            return False

        if query.health_status and module.health != query.health_status:
            return False

        return True


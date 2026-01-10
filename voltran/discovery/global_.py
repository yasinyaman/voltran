"""Global discovery service - manages modules across federation."""

import asyncio
from datetime import datetime
from typing import Optional

import structlog

from voltran.core.domain.models import (
    Federation,
    HealthStatus,
    ModuleDescriptor,
    ModuleQuery,
    VoltranNode,
)
from voltran.core.ports.outbound.messaging import IMessagingPort, Message
from voltran.discovery.local import LocalDiscoveryService

logger = structlog.get_logger(__name__)


class GlobalDiscoveryService:
    """
    Global discovery service for Voltran federation.
    
    Aggregates module registries from all Voltran nodes in the federation
    and provides federated discovery capabilities.
    """

    def __init__(
        self,
        local_discovery: LocalDiscoveryService,
        messaging: IMessagingPort,
        sync_interval: int = 30,
    ):
        """
        Initialize global discovery.
        
        Args:
            local_discovery: Local discovery service for this node
            messaging: Messaging port for federation communication
            sync_interval: Seconds between registry syncs
        """
        self._local = local_discovery
        self._messaging = messaging
        self._sync_interval = sync_interval
        
        # Global registry (module_id -> descriptor)
        self._global_registry: dict[str, ModuleDescriptor] = {}
        
        # Known Voltran nodes (node_id -> node)
        self._known_nodes: dict[str, VoltranNode] = {}
        
        # Background tasks
        self._sync_task: Optional[asyncio.Task[None]] = None
        self._running = False

    @property
    def voltran_id(self) -> str:
        """Get local Voltran node ID."""
        return self._local.voltran_id

    async def start(self) -> None:
        """Start global discovery service."""
        if self._running:
            return

        self._running = True
        
        # Setup message handlers
        await self._setup_message_handlers()
        
        # Start sync loop
        self._sync_task = asyncio.create_task(self._sync_loop())

        logger.info(
            "global_discovery_started",
            voltran_id=self.voltran_id,
        )

    async def stop(self) -> None:
        """Stop global discovery service."""
        self._running = False
        
        if self._sync_task:
            self._sync_task.cancel()
            try:
                await self._sync_task
            except asyncio.CancelledError:
                pass

        logger.info("global_discovery_stopped")

    async def sync_registry(self) -> dict[str, ModuleDescriptor]:
        """
        Synchronize global registry with all known nodes.
        
        Returns:
            Complete global registry
        """
        # Start with local modules
        local_modules = await self._local.get_all()
        for module in local_modules:
            self._global_registry[module.id] = module

        # Request modules from all known nodes
        for node_id in list(self._known_nodes.keys()):
            try:
                response = await self._messaging.request(
                    target_voltran_id=node_id,
                    payload={"action": "get_modules"},
                    timeout=10.0,
                )
                
                modules_data = response.get("modules", [])
                for module_data in modules_data:
                    module = self._parse_module(module_data)
                    if module:
                        self._global_registry[module.id] = module

            except TimeoutError:
                logger.warning(
                    "node_sync_timeout",
                    node_id=node_id,
                )
            except Exception as e:
                logger.error(
                    "node_sync_error",
                    node_id=node_id,
                    error=str(e),
                )

        logger.info(
            "registry_synced",
            total_modules=len(self._global_registry),
            known_nodes=len(self._known_nodes),
        )

        return self._global_registry

    async def find_in_federation(
        self,
        query: ModuleQuery,
    ) -> list[ModuleDescriptor]:
        """
        Find modules across the entire federation.
        
        Args:
            query: Query parameters
            
        Returns:
            List of matching modules from all nodes
        """
        results: list[ModuleDescriptor] = []
        
        for module in self._global_registry.values():
            if self._matches_query(module, query):
                results.append(module)
        
        return results

    async def find_by_contract(
        self,
        contract_id: str,
        healthy_only: bool = True,
    ) -> list[ModuleDescriptor]:
        """
        Find modules by contract ID across federation.
        
        Args:
            contract_id: Contract ID to search for
            healthy_only: Only return healthy modules
            
        Returns:
            List of matching modules
        """
        query = ModuleQuery(
            contract_id=contract_id,
            health_status=HealthStatus.HEALTHY if healthy_only else None,
        )
        return await self.find_in_federation(query)

    async def register_node(self, node: VoltranNode) -> None:
        """
        Register a Voltran node in the federation.
        
        Args:
            node: VoltranNode to register
        """
        self._known_nodes[node.id] = node
        
        # Add node's modules to global registry
        for module in node.local_modules.values():
            self._global_registry[module.id] = module

        logger.info(
            "node_registered",
            node_id=node.id,
            node_name=node.name,
            module_count=node.module_count(),
        )

    async def unregister_node(self, node_id: str) -> bool:
        """
        Unregister a Voltran node from federation.
        
        Args:
            node_id: ID of node to unregister
            
        Returns:
            True if node was unregistered
        """
        if node_id not in self._known_nodes:
            return False

        node = self._known_nodes.pop(node_id)
        
        # Remove node's modules from global registry
        for module_id in list(self._global_registry.keys()):
            if self._global_registry[module_id].voltran_id == node_id:
                del self._global_registry[module_id]

        logger.info(
            "node_unregistered",
            node_id=node_id,
            node_name=node.name,
        )

        return True

    async def get_known_nodes(self) -> list[VoltranNode]:
        """Get all known nodes in federation."""
        return list(self._known_nodes.values())

    async def get_global_registry(self) -> dict[str, ModuleDescriptor]:
        """Get the complete global registry."""
        return self._global_registry.copy()

    # === Private Methods ===

    async def _sync_loop(self) -> None:
        """Periodic sync with all nodes."""
        while self._running:
            try:
                await asyncio.sleep(self._sync_interval)
                await self.sync_registry()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("sync_loop_error", error=str(e))

    async def _setup_message_handlers(self) -> None:
        """Setup message handlers for federation events."""
        # Handle module registration events
        await self._messaging.subscribe(
            "voltran.federation.module.registered",
            self._handle_module_registered,
        )
        
        # Handle module unregistration events
        await self._messaging.subscribe(
            "voltran.federation.module.unregistered",
            self._handle_module_unregistered,
        )
        
        # Handle node announcements
        await self._messaging.subscribe(
            "voltran.federation.node.announce",
            self._handle_node_announce,
        )
        
        # Handle node leaving
        await self._messaging.subscribe(
            "voltran.federation.node.leaving",
            self._handle_node_leaving,
        )

        # Handle get_modules requests
        await self._messaging.subscribe(
            f"voltran.{self.voltran_id}.request",
            self._handle_request,
        )

    async def _handle_module_registered(self, message: Message) -> None:
        """Handle module registration event."""
        payload = message.payload
        
        module = ModuleDescriptor(
            id=payload.get("module_id", ""),
            name=payload.get("module_name", ""),
            voltran_id=payload.get("voltran_id", ""),
            health=HealthStatus.HEALTHY,
        )
        
        self._global_registry[module.id] = module

        logger.debug(
            "global_module_registered",
            module_id=module.id,
            voltran_id=module.voltran_id,
        )

    async def _handle_module_unregistered(self, message: Message) -> None:
        """Handle module unregistration event."""
        module_id = message.payload.get("module_id", "")
        
        if module_id in self._global_registry:
            del self._global_registry[module_id]

        logger.debug(
            "global_module_unregistered",
            module_id=module_id,
        )

    async def _handle_node_announce(self, message: Message) -> None:
        """Handle node announcement."""
        payload = message.payload
        
        node = VoltranNode(
            id=payload.get("node_id", ""),
            name=payload.get("node_name", ""),
        )
        
        await self.register_node(node)

    async def _handle_node_leaving(self, message: Message) -> None:
        """Handle node leaving event."""
        node_id = message.payload.get("node_id", "")
        await self.unregister_node(node_id)

    async def _handle_request(self, message: Message) -> None:
        """Handle incoming requests."""
        action = message.payload.get("action", "")
        
        if action == "get_modules":
            modules = await self._local.get_all()
            await self._messaging.respond(
                message,
                {
                    "modules": [
                        {
                            "id": m.id,
                            "name": m.name,
                            "version": m.version,
                            "voltran_id": m.voltran_id,
                            "health": m.health.value,
                        }
                        for m in modules
                    ],
                },
            )

    def _matches_query(self, module: ModuleDescriptor, query: ModuleQuery) -> bool:
        """Check if module matches query."""
        if query.contract_id:
            has_contract = any(
                c.contract_id == query.contract_id for c in module.capabilities
            )
            if not has_contract:
                return False

        if query.name_pattern and query.name_pattern.lower() not in module.name.lower():
            return False

        if query.voltran_id and module.voltran_id != query.voltran_id:
            return False

        if query.health_status and module.health != query.health_status:
            return False

        if not query.include_federated and module.voltran_id != self.voltran_id:
            return False

        return True

    def _parse_module(self, data: dict) -> Optional[ModuleDescriptor]:
        """Parse module from dictionary."""
        try:
            return ModuleDescriptor(
                id=data.get("id", ""),
                name=data.get("name", ""),
                version=data.get("version", "0.0.1"),
                voltran_id=data.get("voltran_id", ""),
                health=HealthStatus(data.get("health", "unknown")),
            )
        except Exception:
            return None


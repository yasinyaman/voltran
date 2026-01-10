"""Discovery domain service - business logic for module discovery."""

from typing import Optional

import structlog

from voltran.core.domain.models import (
    HealthStatus,
    ModuleDescriptor,
    ModuleQuery,
    PortDirection,
)
from voltran.core.ports.inbound.module_registry import IModuleRegistryPort
from voltran.core.ports.outbound.messaging import IMessagingPort

logger = structlog.get_logger(__name__)


class DiscoveryService:
    """
    Domain service for module discovery.
    
    Handles business logic for finding and matching modules
    based on capabilities and health status.
    """

    def __init__(
        self,
        registry: IModuleRegistryPort,
        messaging: Optional[IMessagingPort] = None,
    ):
        """
        Initialize discovery service.
        
        Args:
            registry: Module registry port
            messaging: Optional messaging port for federated discovery
        """
        self._registry = registry
        self._messaging = messaging

    async def discover_by_capability(
        self,
        contract_id: str,
        direction: PortDirection = PortDirection.INBOUND,
        healthy_only: bool = True,
    ) -> list[ModuleDescriptor]:
        """
        Discover modules by capability contract.
        
        Args:
            contract_id: Contract ID to search for
            direction: Port direction (inbound/outbound)
            healthy_only: Only return healthy modules
            
        Returns:
            List of matching modules
        """
        query = ModuleQuery(contract_id=contract_id)
        modules = await self._registry.find(query)

        # Filter by direction
        matching = [
            m for m in modules if m.has_capability(contract_id, direction)
        ]

        # Filter by health
        if healthy_only:
            matching = [m for m in matching if m.health == HealthStatus.HEALTHY]

        logger.info(
            "capability_discovery",
            contract_id=contract_id,
            direction=direction.value,
            found_count=len(matching),
        )

        return matching

    async def find_provider(
        self,
        contract_id: str,
    ) -> Optional[ModuleDescriptor]:
        """
        Find a single healthy provider for a contract.
        
        This is useful when you need exactly one module to handle a request.
        Returns the first healthy module found.
        
        Args:
            contract_id: Contract ID to search for
            
        Returns:
            Module descriptor if found, None otherwise
        """
        providers = await self.discover_by_capability(
            contract_id=contract_id,
            direction=PortDirection.INBOUND,
            healthy_only=True,
        )

        if providers:
            return providers[0]
        return None

    async def find_consumers(
        self,
        contract_id: str,
    ) -> list[ModuleDescriptor]:
        """
        Find all modules that consume (require) a contract.
        
        Args:
            contract_id: Contract ID to search for
            
        Returns:
            List of modules with outbound port for this contract
        """
        return await self.discover_by_capability(
            contract_id=contract_id,
            direction=PortDirection.OUTBOUND,
            healthy_only=False,  # Include all, even unhealthy
        )

    async def get_dependency_graph(self) -> dict[str, list[str]]:
        """
        Build a dependency graph of all modules.
        
        Returns:
            Dictionary mapping module IDs to list of dependent module IDs
        """
        all_modules = await self._registry.get_all()
        graph: dict[str, list[str]] = {}

        for module in all_modules:
            graph[module.id] = []

            # For each outbound port, find matching inbound ports
            for outbound in module.get_outbound_ports():
                providers = await self.discover_by_capability(
                    contract_id=outbound.contract_id,
                    direction=PortDirection.INBOUND,
                    healthy_only=False,
                )
                for provider in providers:
                    if provider.id != module.id:
                        graph[module.id].append(provider.id)

        return graph

    async def check_circular_dependencies(self) -> list[list[str]]:
        """
        Check for circular dependencies in module graph.
        
        Returns:
            List of cycles (each cycle is a list of module IDs)
        """
        graph = await self.get_dependency_graph()
        cycles: list[list[str]] = []
        visited: set[str] = set()
        rec_stack: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> bool:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if dfs(neighbor):
                        return True
                elif neighbor in rec_stack:
                    # Found a cycle
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])
                    return True

            path.pop()
            rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                dfs(node)

        return cycles

    async def get_health_summary(self) -> dict[str, int]:
        """
        Get summary of module health statuses.
        
        Returns:
            Dictionary with count of modules per health status
        """
        all_modules = await self._registry.get_all()
        summary: dict[str, int] = {
            HealthStatus.HEALTHY.value: 0,
            HealthStatus.DEGRADED.value: 0,
            HealthStatus.UNHEALTHY.value: 0,
            HealthStatus.UNKNOWN.value: 0,
        }

        for module in all_modules:
            summary[module.health.value] += 1

        return summary


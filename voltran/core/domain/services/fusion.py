"""Fusion domain service - business logic for merging modules and clusters."""

from typing import Optional
from uuid import uuid4

import structlog

from voltran.core.domain.models import (
    Capability,
    CircularDependencyError,
    Cluster,
    Endpoint,
    HealthStatus,
    ModuleDescriptor,
    PortDirection,
    VoltranNode,
)
from voltran.core.ports.inbound.module_registry import IModuleRegistryPort

logger = structlog.get_logger(__name__)


class PortMapping:
    """Mapping between consumer outbound port and provider inbound port."""

    def __init__(
        self,
        consumer_module_id: str,
        consumer_port_name: str,
        provider_module_id: str,
        provider_port_name: str,
        contract_id: str,
    ):
        self.consumer_module_id = consumer_module_id
        self.consumer_port_name = consumer_port_name
        self.provider_module_id = provider_module_id
        self.provider_port_name = provider_port_name
        self.contract_id = contract_id


class FusionService:
    """
    Domain service for fusing modules and clusters.
    
    Handles the logic of combining multiple modules or Voltran nodes
    into single virtual entities.
    """

    def __init__(self, registry: IModuleRegistryPort):
        """
        Initialize fusion service.
        
        Args:
            registry: Module registry port for lookups
        """
        self._registry = registry

    async def fuse_modules(
        self,
        modules: list[ModuleDescriptor],
        virtual_name: str,
        virtual_version: str = "1.0.0",
    ) -> tuple[ModuleDescriptor, list[PortMapping]]:
        """
        Fuse multiple modules into a single virtual module.
        
        The virtual module will:
        - Have all external-facing capabilities from the modules
        - Hide internal port connections
        - Present a unified health status
        
        Args:
            modules: List of modules to fuse
            virtual_name: Name for the virtual module
            virtual_version: Version for the virtual module
            
        Returns:
            Tuple of (virtual module descriptor, internal port mappings)
            
        Raises:
            CircularDependencyError: If circular dependencies detected
        """
        if not modules:
            raise ValueError("Cannot fuse empty module list")

        # Check for circular dependencies
        cycles = self._detect_cycles(modules)
        if cycles:
            raise CircularDependencyError(cycles[0])

        # Resolve internal port mappings
        port_mappings = self._resolve_port_mappings(modules)

        # Collect external capabilities (ports not connected internally)
        internal_contracts = {pm.contract_id for pm in port_mappings}
        external_capabilities: list[Capability] = []

        for module in modules:
            for cap in module.capabilities:
                if cap.contract_id not in internal_contracts:
                    external_capabilities.append(cap)

        # Collect all endpoints
        all_endpoints: list[Endpoint] = []
        for module in modules:
            all_endpoints.extend(module.endpoints)

        # Calculate aggregate health
        health = self._calculate_aggregate_health(modules)

        # Create virtual module
        virtual_module = ModuleDescriptor(
            id=f"virtual-{uuid4()}",
            name=virtual_name,
            version=virtual_version,
            capabilities=external_capabilities,
            endpoints=all_endpoints,
            health=health,
            metadata={
                "is_virtual": True,
                "fused_modules": [m.id for m in modules],
                "internal_mappings_count": len(port_mappings),
            },
        )

        logger.info(
            "modules_fused",
            virtual_module_id=virtual_module.id,
            virtual_name=virtual_name,
            module_count=len(modules),
            external_capabilities=len(external_capabilities),
            internal_mappings=len(port_mappings),
        )

        return virtual_module, port_mappings

    async def fuse_cluster(
        self,
        cluster: Cluster,
        virtual_name: str,
    ) -> ModuleDescriptor:
        """
        Fuse a cluster into a virtual module.
        
        Args:
            cluster: Cluster to fuse
            virtual_name: Name for virtual module
            
        Returns:
            Virtual module descriptor
        """
        # Fetch all modules in cluster
        modules: list[ModuleDescriptor] = []
        for module_id in cluster.module_ids:
            module = await self._registry.get(module_id)
            if module:
                modules.append(module)

        virtual_module, _ = await self.fuse_modules(modules, virtual_name)
        return virtual_module

    async def fuse_voltrans(
        self,
        voltrans: list[VoltranNode],
        virtual_name: str,
    ) -> VoltranNode:
        """
        Fuse multiple Voltran nodes into a virtual Voltran.
        
        Args:
            voltrans: List of Voltran nodes to fuse
            virtual_name: Name for virtual Voltran
            
        Returns:
            Virtual VoltranNode
        """
        if not voltrans:
            raise ValueError("Cannot fuse empty Voltran list")

        # Collect all modules from all Voltrans
        all_modules: list[ModuleDescriptor] = []
        all_clusters: dict[str, Cluster] = {}

        for voltran in voltrans:
            all_modules.extend(voltran.local_modules.values())
            all_clusters.update(voltran.local_clusters)

        # Create virtual Voltran
        virtual_voltran = VoltranNode(
            id=f"virtual-voltran-{uuid4()}",
            name=virtual_name,
            health=self._calculate_voltran_health(voltrans),
            local_modules={m.id: m for m in all_modules},
            local_clusters=all_clusters,
            metadata={
                "is_virtual": True,
                "fused_voltrans": [v.id for v in voltrans],
            },
        )

        logger.info(
            "voltrans_fused",
            virtual_voltran_id=virtual_voltran.id,
            virtual_name=virtual_name,
            voltran_count=len(voltrans),
            total_modules=len(all_modules),
        )

        return virtual_voltran

    def _resolve_port_mappings(
        self, modules: list[ModuleDescriptor]
    ) -> list[PortMapping]:
        """
        Resolve port connections between modules.
        
        For each outbound port, find matching inbound ports in the same group.
        """
        mappings: list[PortMapping] = []
        module_map = {m.id: m for m in modules}

        for consumer in modules:
            for out_cap in consumer.get_outbound_ports():
                # Find provider in same group
                for provider in modules:
                    if provider.id == consumer.id:
                        continue

                    for in_cap in provider.get_inbound_ports():
                        if in_cap.contract_id == out_cap.contract_id:
                            mappings.append(
                                PortMapping(
                                    consumer_module_id=consumer.id,
                                    consumer_port_name=out_cap.name,
                                    provider_module_id=provider.id,
                                    provider_port_name=in_cap.name,
                                    contract_id=out_cap.contract_id,
                                )
                            )
                            break

        return mappings

    def _detect_cycles(
        self, modules: list[ModuleDescriptor]
    ) -> list[list[str]]:
        """Detect circular dependencies in module graph."""
        # Build dependency graph
        graph: dict[str, list[str]] = {m.id: [] for m in modules}
        module_map = {m.id: m for m in modules}

        for consumer in modules:
            for out_cap in consumer.get_outbound_ports():
                for provider in modules:
                    if provider.id == consumer.id:
                        continue
                    if provider.has_capability(out_cap.contract_id, PortDirection.INBOUND):
                        graph[consumer.id].append(provider.id)

        # DFS cycle detection
        cycles: list[list[str]] = []
        visited: set[str] = set()
        rec_stack: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> None:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    dfs(neighbor)
                elif neighbor in rec_stack:
                    cycle_start = path.index(neighbor)
                    cycles.append(path[cycle_start:] + [neighbor])

            path.pop()
            rec_stack.remove(node)

        for node in graph:
            if node not in visited:
                dfs(node)

        return cycles

    def _calculate_aggregate_health(
        self, modules: list[ModuleDescriptor]
    ) -> HealthStatus:
        """Calculate aggregate health from multiple modules."""
        if not modules:
            return HealthStatus.UNKNOWN

        health_counts = {status: 0 for status in HealthStatus}
        for module in modules:
            health_counts[module.health] += 1

        # If any unhealthy, overall is unhealthy
        if health_counts[HealthStatus.UNHEALTHY] > 0:
            return HealthStatus.UNHEALTHY

        # If any degraded, overall is degraded
        if health_counts[HealthStatus.DEGRADED] > 0:
            return HealthStatus.DEGRADED

        # If all healthy, overall is healthy
        if health_counts[HealthStatus.HEALTHY] == len(modules):
            return HealthStatus.HEALTHY

        return HealthStatus.UNKNOWN

    def _calculate_voltran_health(
        self, voltrans: list[VoltranNode]
    ) -> HealthStatus:
        """Calculate aggregate health from multiple Voltrans."""
        if not voltrans:
            return HealthStatus.UNKNOWN

        # Collect all module healths
        all_modules: list[ModuleDescriptor] = []
        for voltran in voltrans:
            all_modules.extend(voltran.local_modules.values())

        return self._calculate_aggregate_health(all_modules)


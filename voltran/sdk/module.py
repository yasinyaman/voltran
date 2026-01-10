"""Base module class and context for Voltran modules."""

from abc import ABC
from typing import Any, Optional
from uuid import uuid4

import structlog

from voltran.core.domain.models import (
    Capability,
    Endpoint,
    HealthStatus,
    ModuleDescriptor,
    PortDirection,
)
from voltran.sdk.decorators import get_module_metadata

logger = structlog.get_logger(__name__)


class ModuleContext:
    """
    Context provided to modules for discovery and communication.
    
    This context is injected into modules to allow them to:
    - Find other modules by contract
    - Call methods on other modules
    - Access discovery information
    """

    def __init__(
        self,
        voltran_id: str,
        discovery: Any,  # LocalDiscoveryService
        messaging: Optional[Any] = None,  # IMessagingPort
        authorization: Optional[Any] = None,  # AuthorizationService
    ):
        """
        Initialize module context.
        
        Args:
            voltran_id: ID of parent Voltran node
            discovery: Discovery service for finding modules
            messaging: Optional messaging for remote calls
            authorization: Optional authorization service
        """
        self._voltran_id = voltran_id
        self._discovery = discovery
        self._messaging = messaging
        self._authorization = authorization
        
        # Cache of resolved providers
        self._provider_cache: dict[str, ModuleDescriptor] = {}
    
    @property
    def authorization(self) -> Optional[Any]:
        """Get authorization service."""
        return self._authorization

    async def find_provider(self, contract_id: str) -> Optional[ModuleDescriptor]:
        """
        Find a provider for a contract.
        
        Args:
            contract_id: Contract ID to find provider for
            
        Returns:
            Module descriptor if found, None otherwise
        """
        # Check cache
        if contract_id in self._provider_cache:
            cached = self._provider_cache[contract_id]
            if cached.health == HealthStatus.HEALTHY:
                return cached
        
        # Query discovery
        from voltran.core.domain.models import ModuleQuery
        
        query = ModuleQuery(contract_id=contract_id)
        modules = await self._discovery.find(query)
        
        # Filter for healthy modules with inbound port
        for module in modules:
            if module.health == HealthStatus.HEALTHY:
                if module.has_capability(contract_id, PortDirection.INBOUND):
                    self._provider_cache[contract_id] = module
                    return module
        
        return None

    async def call_provider(
        self,
        provider: ModuleDescriptor,
        method_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Call a method on a provider module.
        
        Args:
            provider: Provider module descriptor
            method_name: Name of method to call
            *args: Positional arguments
            **kwargs: Keyword arguments
            
        Returns:
            Result from provider
        """
        # If local module, call directly
        if provider.voltran_id == self._voltran_id:
            # Get module instance from discovery
            # This would need module instance registry
            logger.debug(
                "local_call",
                provider_id=provider.id,
                method=method_name,
            )
            # For now, return mock
            return {"status": "local_call", "method": method_name}
        
        # Remote call via messaging
        if self._messaging:
            logger.debug(
                "remote_call",
                provider_id=provider.id,
                provider_voltran=provider.voltran_id,
                method=method_name,
            )
            
            response = await self._messaging.request(
                target_voltran_id=provider.voltran_id or "",
                payload={
                    "action": "call",
                    "module_id": provider.id,
                    "method": method_name,
                    "args": args,
                    "kwargs": kwargs,
                },
            )
            return response
        
        raise RuntimeError(
            f"Cannot call remote provider {provider.id} - no messaging configured"
        )

    def clear_cache(self) -> None:
        """Clear provider cache."""
        self._provider_cache.clear()


class BaseModule(ABC):
    """
    Base class for Voltran modules.
    
    Provides common functionality for modules including:
    - Context injection
    - Lifecycle management
    - Health reporting
    
    Usage:
        @voltran_module(name="my-service", version="1.0.0")
        class MyModule(BaseModule):
            @inbound_port(contract="my.service.v1")
            async def handle_request(self, request):
                return {"status": "ok"}
    """

    def __init__(self) -> None:
        """Initialize base module."""
        self._id = str(uuid4())
        self._context: Optional[ModuleContext] = None
        self._health = HealthStatus.UNKNOWN
        self._started = False

    @property
    def id(self) -> str:
        """Get module ID."""
        return self._id

    @property
    def health(self) -> HealthStatus:
        """Get module health status."""
        return self._health

    @property
    def is_started(self) -> bool:
        """Check if module is started."""
        return self._started

    def set_context(self, context: ModuleContext) -> None:
        """
        Set module context.
        
        Args:
            context: Module context for discovery
        """
        self._context = context
        self._voltran_context = context  # For decorator access

    async def start(self) -> None:
        """
        Start the module.
        
        Override to add startup logic.
        """
        self._started = True
        self._health = HealthStatus.HEALTHY
        logger.info("module_started", module_id=self._id)

    async def stop(self) -> None:
        """
        Stop the module.
        
        Override to add shutdown logic.
        """
        self._started = False
        self._health = HealthStatus.UNKNOWN
        logger.info("module_stopped", module_id=self._id)

    async def health_check(self) -> HealthStatus:
        """
        Perform health check.
        
        Override to add custom health check logic.
        
        Returns:
            Current health status
        """
        if not self._started:
            return HealthStatus.UNHEALTHY
        return self._health

    def to_descriptor(self) -> ModuleDescriptor:
        """
        Convert module to descriptor.
        
        Returns:
            ModuleDescriptor for this module
        """
        metadata = get_module_metadata(type(self))
        
        name = ""
        version = "0.0.1"
        
        if metadata:
            name = metadata.get("name", "")
            version = metadata.get("version", "0.0.1")
        
        # Get capabilities
        capabilities: list[Capability] = []
        if hasattr(self, "_get_capabilities"):
            capabilities = self._get_capabilities()
        
        return ModuleDescriptor(
            id=self._id,
            name=name,
            version=version,
            capabilities=capabilities,
            health=self._health,
        )


def create_module_descriptor(
    instance: Any,
    endpoints: Optional[list[Endpoint]] = None,
) -> ModuleDescriptor:
    """
    Create a module descriptor from a module instance.
    
    Args:
        instance: Module instance (decorated with @voltran_module)
        endpoints: Optional list of endpoints
        
    Returns:
        ModuleDescriptor for the module
    """
    cls = type(instance)
    metadata = get_module_metadata(cls)
    
    if not metadata:
        raise ValueError(f"Class {cls.__name__} is not a Voltran module")
    
    # Build capabilities
    capabilities: list[Capability] = []
    
    for port in metadata.get("inbound_ports", []):
        capabilities.append(
            Capability(
                name=port["name"],
                contract_id=port["contract"],
                direction=PortDirection.INBOUND,
                protocol=port.get("protocol", "grpc"),
            )
        )
    
    for port in metadata.get("outbound_ports", []):
        capabilities.append(
            Capability(
                name=port["name"],
                contract_id=port["contract"],
                direction=PortDirection.OUTBOUND,
                protocol=port.get("protocol", "grpc"),
            )
        )
    
    # Get or generate ID
    module_id = getattr(instance, "_id", None) or str(uuid4())
    
    return ModuleDescriptor(
        id=module_id,
        name=metadata["name"],
        version=metadata["version"],
        capabilities=capabilities,
        endpoints=endpoints or [],
        health=HealthStatus.UNKNOWN,
        metadata={
            "description": metadata.get("description", ""),
        },
    )


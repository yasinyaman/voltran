"""Module registry inbound port interface."""

from abc import ABC, abstractmethod
from typing import Optional

from voltran.core.domain.models import ModuleDescriptor, ModuleQuery


class IModuleRegistryPort(ABC):
    """
    Inbound port for module registration and discovery.
    
    This port defines the interface for:
    - Registering modules with the local discovery
    - Unregistering modules
    - Finding modules by various criteria
    - Health check via heartbeat
    """

    @abstractmethod
    async def register(self, module: ModuleDescriptor) -> str:
        """
        Register a module with the registry.
        
        Args:
            module: Module descriptor to register
            
        Returns:
            Module ID after registration
        """
        pass

    @abstractmethod
    async def unregister(self, module_id: str) -> bool:
        """
        Unregister a module from the registry.
        
        Args:
            module_id: ID of module to unregister
            
        Returns:
            True if module was unregistered, False if not found
        """
        pass

    @abstractmethod
    async def get(self, module_id: str) -> Optional[ModuleDescriptor]:
        """
        Get a module by its ID.
        
        Args:
            module_id: ID of module to retrieve
            
        Returns:
            Module descriptor if found, None otherwise
        """
        pass

    @abstractmethod
    async def find(self, query: ModuleQuery) -> list[ModuleDescriptor]:
        """
        Find modules matching the query criteria.
        
        Args:
            query: Query parameters for filtering modules
            
        Returns:
            List of matching module descriptors
        """
        pass

    @abstractmethod
    async def heartbeat(self, module_id: str) -> bool:
        """
        Update module heartbeat timestamp.
        
        Args:
            module_id: ID of module sending heartbeat
            
        Returns:
            True if heartbeat recorded, False if module not found
        """
        pass

    @abstractmethod
    async def get_all(self) -> list[ModuleDescriptor]:
        """
        Get all registered modules.
        
        Returns:
            List of all module descriptors
        """
        pass

    @abstractmethod
    async def count(self) -> int:
        """
        Get count of registered modules.
        
        Returns:
            Number of registered modules
        """
        pass


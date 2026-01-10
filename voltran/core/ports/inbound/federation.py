"""Federation management inbound port interface."""

from abc import ABC, abstractmethod
from typing import Optional

from voltran.core.domain.models import Federation, ModuleDescriptor, VoltranNode


class IFederationPort(ABC):
    """
    Inbound port for Voltran federation management.
    
    This port defines the interface for:
    - Creating new federations
    - Joining/leaving federations
    - Discovering peers
    - Syncing module registries across federation
    """

    @abstractmethod
    async def create_federation(self, name: str) -> Federation:
        """
        Create a new federation with this node as leader.
        
        Args:
            name: Name of the federation
            
        Returns:
            Created federation instance
        """
        pass

    @abstractmethod
    async def join_federation(self, federation_endpoint: str) -> bool:
        """
        Join an existing federation.
        
        Args:
            federation_endpoint: Endpoint of federation to join (e.g., "nats://host:4222")
            
        Returns:
            True if successfully joined, False otherwise
        """
        pass

    @abstractmethod
    async def leave_federation(self) -> bool:
        """
        Leave the current federation.
        
        Returns:
            True if successfully left, False if not in federation
        """
        pass

    @abstractmethod
    async def discover_peers(self) -> list[VoltranNode]:
        """
        Discover all peer Voltran nodes in the federation.
        
        Returns:
            List of discovered peer nodes
        """
        pass

    @abstractmethod
    async def sync_registry(self) -> dict[str, ModuleDescriptor]:
        """
        Synchronize module registry across the federation.
        
        Returns:
            Complete global registry after sync
        """
        pass

    @abstractmethod
    async def get_federation_state(self) -> Optional[Federation]:
        """
        Get current federation state.
        
        Returns:
            Current federation if member, None if standalone
        """
        pass

    @abstractmethod
    async def find_in_federation(self, contract_id: str) -> list[ModuleDescriptor]:
        """
        Find modules in federation by contract ID.
        
        Args:
            contract_id: Contract ID to search for
            
        Returns:
            List of modules providing this contract
        """
        pass

    @abstractmethod
    async def fuse_voltrans(
        self, voltran_ids: list[str], virtual_name: str
    ) -> VoltranNode:
        """
        Fuse multiple Voltran nodes into a single virtual node.
        
        Args:
            voltran_ids: IDs of Voltran nodes to fuse
            virtual_name: Name for the virtual Voltran
            
        Returns:
            Virtual VoltranNode representing the fusion
        """
        pass


"""Cluster management inbound port interface."""

from abc import ABC, abstractmethod
from typing import Optional

from voltran.core.domain.models import Cluster, ModuleDescriptor


class IClusterPort(ABC):
    """
    Inbound port for module cluster management.
    
    This port defines the interface for:
    - Creating module clusters
    - Adding/removing modules from clusters
    - Fusing clusters to act as single modules
    """

    @abstractmethod
    async def create_cluster(self, name: str) -> Cluster:
        """
        Create a new empty cluster.
        
        Args:
            name: Name of the cluster
            
        Returns:
            Created cluster instance
        """
        pass

    @abstractmethod
    async def delete_cluster(self, cluster_id: str) -> bool:
        """
        Delete a cluster.
        
        Args:
            cluster_id: ID of cluster to delete
            
        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def get_cluster(self, cluster_id: str) -> Optional[Cluster]:
        """
        Get a cluster by ID.
        
        Args:
            cluster_id: ID of cluster to retrieve
            
        Returns:
            Cluster if found, None otherwise
        """
        pass

    @abstractmethod
    async def add_module(self, cluster_id: str, module_id: str) -> bool:
        """
        Add a module to a cluster.
        
        Args:
            cluster_id: ID of cluster
            module_id: ID of module to add
            
        Returns:
            True if added, False if cluster or module not found
        """
        pass

    @abstractmethod
    async def remove_module(self, cluster_id: str, module_id: str) -> bool:
        """
        Remove a module from a cluster.
        
        Args:
            cluster_id: ID of cluster
            module_id: ID of module to remove
            
        Returns:
            True if removed, False if not found
        """
        pass

    @abstractmethod
    async def get_cluster_modules(self, cluster_id: str) -> list[ModuleDescriptor]:
        """
        Get all modules in a cluster.
        
        Args:
            cluster_id: ID of cluster
            
        Returns:
            List of modules in the cluster
        """
        pass

    @abstractmethod
    async def fuse_cluster(self, cluster_id: str, virtual_name: str) -> ModuleDescriptor:
        """
        Fuse a cluster to act as a single virtual module.
        
        This combines all module capabilities into one virtual module
        that can be discovered and used as if it were a single module.
        
        Args:
            cluster_id: ID of cluster to fuse
            virtual_name: Name for the virtual module
            
        Returns:
            Virtual ModuleDescriptor representing the fused cluster
        """
        pass

    @abstractmethod
    async def unfuse_cluster(self, cluster_id: str) -> bool:
        """
        Unfuse a cluster, reverting to individual modules.
        
        Args:
            cluster_id: ID of cluster to unfuse
            
        Returns:
            True if unfused, False if not fused or not found
        """
        pass

    @abstractmethod
    async def list_clusters(self) -> list[Cluster]:
        """
        List all clusters.
        
        Returns:
            List of all clusters
        """
        pass


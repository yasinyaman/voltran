"""Domain models for Voltran system."""

from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class HealthStatus(str, Enum):
    """Module or node health status."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class PortDirection(str, Enum):
    """Port direction - inbound or outbound."""

    INBOUND = "inbound"
    OUTBOUND = "outbound"


class NodeRole(str, Enum):
    """Voltran node role in federation."""

    STANDALONE = "standalone"
    FEDERATION_MEMBER = "member"
    FEDERATION_LEADER = "leader"


class Capability(BaseModel):
    """Module capability - a port that module provides or requires."""

    name: str
    contract_id: str
    direction: PortDirection
    protocol: str = "grpc"
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"frozen": True}


class Endpoint(BaseModel):
    """Network endpoint for module communication."""

    host: str
    port: int
    protocol: str = "grpc"
    path: Optional[str] = None

    model_config = {"frozen": True}

    def to_uri(self) -> str:
        """Convert to URI string."""
        base = f"{self.protocol}://{self.host}:{self.port}"
        if self.path:
            return f"{base}{self.path}"
        return base


class ModuleDescriptor(BaseModel):
    """Complete module description with capabilities and metadata."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str = ""
    version: str = "0.0.1"

    # Hexagonal capabilities
    capabilities: list[Capability] = Field(default_factory=list)

    # Communication endpoints
    endpoints: list[Endpoint] = Field(default_factory=list)

    # Cluster/Federation info
    cluster_id: Optional[str] = None
    voltran_id: Optional[str] = None

    # Health
    health: HealthStatus = HealthStatus.UNKNOWN
    last_heartbeat: datetime = Field(default_factory=datetime.now)

    # Metadata
    metadata: dict[str, Any] = Field(default_factory=dict)

    def get_inbound_ports(self) -> list[Capability]:
        """Get all inbound port capabilities."""
        return [c for c in self.capabilities if c.direction == PortDirection.INBOUND]

    def get_outbound_ports(self) -> list[Capability]:
        """Get all outbound port capabilities."""
        return [c for c in self.capabilities if c.direction == PortDirection.OUTBOUND]

    def has_capability(self, contract_id: str, direction: PortDirection) -> bool:
        """Check if module has a specific capability."""
        return any(
            c.contract_id == contract_id and c.direction == direction for c in self.capabilities
        )


class ModuleQuery(BaseModel):
    """Query parameters for finding modules."""

    contract_id: Optional[str] = None
    name_pattern: Optional[str] = None
    cluster_id: Optional[str] = None
    voltran_id: Optional[str] = None
    health_status: Optional[HealthStatus] = None
    include_federated: bool = True


class Cluster(BaseModel):
    """Module cluster - group of modules that can act as one."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str = ""

    # Modules in this cluster
    module_ids: list[str] = Field(default_factory=list)

    # Virtual module when cluster is fused
    virtual_module: Optional[ModuleDescriptor] = None
    is_fused: bool = False

    # Parent Voltran
    voltran_id: Optional[str] = None

    # Nested clusters support
    parent_cluster_id: Optional[str] = None
    child_cluster_ids: list[str] = Field(default_factory=list)

    def module_count(self) -> int:
        """Get number of modules in cluster."""
        return len(self.module_ids)


class VoltranNode(BaseModel):
    """Single Voltran instance."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str = ""

    # Role and status
    role: NodeRole = NodeRole.STANDALONE
    health: HealthStatus = HealthStatus.UNKNOWN

    # Local modules (module_id -> descriptor)
    local_modules: dict[str, ModuleDescriptor] = Field(default_factory=dict)

    # Local clusters (cluster_id -> cluster)
    local_clusters: dict[str, Cluster] = Field(default_factory=dict)

    # Federation membership
    federation_id: Optional[str] = None
    known_peers: set[str] = Field(default_factory=set)

    # Communication endpoints
    discovery_endpoint: Optional[Endpoint] = None
    federation_endpoint: Optional[Endpoint] = None

    # Metadata
    started_at: datetime = Field(default_factory=datetime.now)
    last_sync: Optional[datetime] = None

    def module_count(self) -> int:
        """Get number of local modules."""
        return len(self.local_modules)

    def is_federated(self) -> bool:
        """Check if node is part of a federation."""
        return self.role in (NodeRole.FEDERATION_MEMBER, NodeRole.FEDERATION_LEADER)

    def is_leader(self) -> bool:
        """Check if node is federation leader."""
        return self.role == NodeRole.FEDERATION_LEADER


class Federation(BaseModel):
    """Federation of multiple Voltran nodes."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    name: str = ""

    # Member Voltran nodes (node_id -> node)
    members: dict[str, VoltranNode] = Field(default_factory=dict)
    leader_id: Optional[str] = None

    # Global module registry (module_id -> descriptor)
    global_registry: dict[str, ModuleDescriptor] = Field(default_factory=dict)

    # Virtual Voltran when federation acts as one
    virtual_voltran: Optional[VoltranNode] = None

    # Metadata
    created_at: datetime = Field(default_factory=datetime.now)

    def member_count(self) -> int:
        """Get number of federation members."""
        return len(self.members)

    def total_module_count(self) -> int:
        """Get total number of modules across all members."""
        return len(self.global_registry)


# Exception classes
class VoltranError(Exception):
    """Base exception for Voltran errors."""

    pass


class ModuleNotFoundError(VoltranError):
    """Module not found in registry."""

    def __init__(self, module_id: str):
        self.module_id = module_id
        super().__init__(f"Module not found: {module_id}")


class ClusterNotFoundError(VoltranError):
    """Cluster not found."""

    def __init__(self, cluster_id: str):
        self.cluster_id = cluster_id
        super().__init__(f"Cluster not found: {cluster_id}")


class FederationError(VoltranError):
    """Federation related error."""

    pass


class FederationConnectionError(FederationError):
    """Failed to connect to federation."""

    def __init__(self, endpoint: str, reason: str = ""):
        self.endpoint = endpoint
        self.reason = reason
        super().__init__(f"Failed to connect to federation at {endpoint}: {reason}")


class CircularDependencyError(VoltranError):
    """Circular dependency detected in module graph."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Circular dependency detected: {' -> '.join(cycle)}")


class PermissionDeniedError(VoltranError):
    """Permission denied error."""

    def __init__(
        self,
        message: str,
        subject_id: Optional[str] = None,
        permission: Optional[str] = None,
        role: Optional[str] = None,
    ):
        self.subject_id = subject_id
        self.permission = permission
        self.role = role
        super().__init__(message)


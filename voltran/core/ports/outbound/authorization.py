"""Authorization outbound port interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import uuid4


class PermissionEffect(str, Enum):
    """Permission effect - allow or deny."""
    
    ALLOW = "allow"
    DENY = "deny"


class ResourceType(str, Enum):
    """Resource types for authorization."""
    
    MODULE = "module"
    ENDPOINT = "endpoint"
    CONTRACT = "contract"
    CLUSTER = "cluster"
    FEDERATION = "federation"
    NODE = "node"
    SYSTEM = "system"


@dataclass
class Permission:
    """A permission definition."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    resource_type: ResourceType = ResourceType.SYSTEM
    resource_id: Optional[str] = None
    action: str = ""  # read, write, execute, delete, etc.
    effect: PermissionEffect = PermissionEffect.ALLOW
    
    # Conditions (optional)
    conditions: dict[str, Any] = field(default_factory=dict)
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    
    def to_string(self) -> str:
        """Convert to permission string format."""
        parts = [self.resource_type.value]
        if self.resource_id:
            parts.append(self.resource_id)
        parts.append(self.action)
        return ":".join(parts)
    
    @classmethod
    def from_string(cls, permission_str: str, effect: PermissionEffect = PermissionEffect.ALLOW) -> "Permission":
        """Create permission from string format (e.g., 'module:payment:read')."""
        parts = permission_str.split(":")
        if len(parts) < 2:
            raise ValueError(f"Invalid permission format: {permission_str}")
        
        resource_type = ResourceType(parts[0])
        action = parts[-1]
        resource_id = ":".join(parts[1:-1]) if len(parts) > 2 else None
        
        return cls(
            name=permission_str,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            effect=effect,
        )


@dataclass
class Role:
    """A role definition with permissions."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    permissions: list[str] = field(default_factory=list)  # Permission names/strings
    
    # Inherited roles
    inherited_roles: list[str] = field(default_factory=list)  # Role names
    
    # Metadata
    created_at: datetime = field(default_factory=datetime.now)
    is_system: bool = False  # System roles cannot be deleted


@dataclass
class Policy:
    """An authorization policy (rule)."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    name: str = ""
    description: str = ""
    
    # Subjects (who)
    subjects: list[str] = field(default_factory=list)  # User IDs, role names, or "*" for all
    
    # Resources (what)
    resources: list[str] = field(default_factory=list)  # Resource patterns (e.g., "module:*", "endpoint:payment:*")
    
    # Actions (how)
    actions: list[str] = field(default_factory=list)  # Actions (e.g., "read", "write", "*")
    
    # Effect
    effect: PermissionEffect = PermissionEffect.ALLOW
    
    # Conditions (optional)
    conditions: dict[str, Any] = field(default_factory=dict)
    
    # Priority (higher = evaluated first)
    priority: int = 0
    
    # Metadata
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class AuthorizationContext:
    """Context for authorization decision."""
    
    # Subject (who is requesting)
    subject_id: str = ""  # User ID, module ID, or service ID
    subject_type: str = "user"  # user, module, service, system
    roles: list[str] = field(default_factory=list)
    
    # Resource (what is being accessed)
    resource_type: ResourceType = ResourceType.SYSTEM
    resource_id: Optional[str] = None
    
    # Action
    action: str = ""
    
    # Additional context
    metadata: dict[str, Any] = field(default_factory=dict)
    
    # Request context
    request_id: Optional[str] = None
    ip_address: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class AuthorizationResult:
    """Result of an authorization check."""
    
    allowed: bool
    reason: str = ""
    matched_policy: Optional[str] = None
    matched_permission: Optional[str] = None
    
    # Additional info
    context: dict[str, Any] = field(default_factory=dict)


class IAuthorizationPort(ABC):
    """
    Outbound port for authorization and access control.
    
    This port defines the interface for:
    - Permission management
    - Role management
    - Policy management
    - Authorization checks
    - Access control decisions
    """

    # === Permission Management ===

    @abstractmethod
    async def create_permission(self, permission: Permission) -> str:
        """
        Create a new permission.
        
        Args:
            permission: Permission definition
            
        Returns:
            Permission ID
        """
        pass

    @abstractmethod
    async def get_permission(self, permission_id: str) -> Optional[Permission]:
        """
        Get a permission by ID.
        
        Args:
            permission_id: Permission ID
            
        Returns:
            Permission if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_permission_by_name(self, name: str) -> Optional[Permission]:
        """
        Get a permission by name.
        
        Args:
            name: Permission name
            
        Returns:
            Permission if found, None otherwise
        """
        pass

    @abstractmethod
    async def list_permissions(
        self,
        resource_type: Optional[ResourceType] = None,
    ) -> list[Permission]:
        """
        List all permissions.
        
        Args:
            resource_type: Optional filter by resource type
            
        Returns:
            List of permissions
        """
        pass

    @abstractmethod
    async def delete_permission(self, permission_id: str) -> bool:
        """
        Delete a permission.
        
        Args:
            permission_id: Permission ID
            
        Returns:
            True if deleted, False if not found
        """
        pass

    # === Role Management ===

    @abstractmethod
    async def create_role(self, role: Role) -> str:
        """
        Create a new role.
        
        Args:
            role: Role definition
            
        Returns:
            Role ID
        """
        pass

    @abstractmethod
    async def get_role(self, role_id: str) -> Optional[Role]:
        """
        Get a role by ID.
        
        Args:
            role_id: Role ID
            
        Returns:
            Role if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_role_by_name(self, name: str) -> Optional[Role]:
        """
        Get a role by name.
        
        Args:
            name: Role name
            
        Returns:
            Role if found, None otherwise
        """
        pass

    @abstractmethod
    async def list_roles(self) -> list[Role]:
        """
        List all roles.
        
        Returns:
            List of roles
        """
        pass

    @abstractmethod
    async def update_role(self, role_id: str, role: Role) -> bool:
        """
        Update a role.
        
        Args:
            role_id: Role ID
            role: Updated role definition
            
        Returns:
            True if updated, False if not found
        """
        pass

    @abstractmethod
    async def delete_role(self, role_id: str) -> bool:
        """
        Delete a role.
        
        Args:
            role_id: Role ID
            
        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def assign_role(self, subject_id: str, role_name: str) -> bool:
        """
        Assign a role to a subject.
        
        Args:
            subject_id: Subject ID (user, module, etc.)
            role_name: Role name to assign
            
        Returns:
            True if assigned, False if role not found
        """
        pass

    @abstractmethod
    async def revoke_role(self, subject_id: str, role_name: str) -> bool:
        """
        Revoke a role from a subject.
        
        Args:
            subject_id: Subject ID
            role_name: Role name to revoke
            
        Returns:
            True if revoked, False if not found
        """
        pass

    @abstractmethod
    async def get_subject_roles(self, subject_id: str) -> list[str]:
        """
        Get all roles for a subject (including inherited).
        
        Args:
            subject_id: Subject ID
            
        Returns:
            List of role names
        """
        pass

    # === Policy Management ===

    @abstractmethod
    async def create_policy(self, policy: Policy) -> str:
        """
        Create a new policy.
        
        Args:
            policy: Policy definition
            
        Returns:
            Policy ID
        """
        pass

    @abstractmethod
    async def get_policy(self, policy_id: str) -> Optional[Policy]:
        """
        Get a policy by ID.
        
        Args:
            policy_id: Policy ID
            
        Returns:
            Policy if found, None otherwise
        """
        pass

    @abstractmethod
    async def list_policies(
        self,
        enabled_only: bool = False,
    ) -> list[Policy]:
        """
        List all policies.
        
        Args:
            enabled_only: Only return enabled policies
            
        Returns:
            List of policies
        """
        pass

    @abstractmethod
    async def update_policy(self, policy_id: str, policy: Policy) -> bool:
        """
        Update a policy.
        
        Args:
            policy_id: Policy ID
            policy: Updated policy definition
            
        Returns:
            True if updated, False if not found
        """
        pass

    @abstractmethod
    async def delete_policy(self, policy_id: str) -> bool:
        """
        Delete a policy.
        
        Args:
            policy_id: Policy ID
            
        Returns:
            True if deleted, False if not found
        """
        pass

    # === Authorization Checks ===

    @abstractmethod
    async def authorize(
        self,
        context: AuthorizationContext,
    ) -> AuthorizationResult:
        """
        Check if a subject is authorized to perform an action.
        
        This is the main authorization method. It evaluates:
        1. Direct permissions
        2. Role-based permissions
        3. Policies (in priority order)
        
        Args:
            context: Authorization context
            
        Returns:
            AuthorizationResult with decision
        """
        pass

    @abstractmethod
    async def has_permission(
        self,
        subject_id: str,
        permission: str,
        resource_id: Optional[str] = None,
    ) -> bool:
        """
        Check if subject has a specific permission.
        
        Args:
            subject_id: Subject ID
            permission: Permission string (e.g., "module:payment:read")
            resource_id: Optional specific resource ID
            
        Returns:
            True if permitted
        """
        pass

    @abstractmethod
    async def has_role(
        self,
        subject_id: str,
        role_name: str,
    ) -> bool:
        """
        Check if subject has a specific role.
        
        Args:
            subject_id: Subject ID
            role_name: Role name
            
        Returns:
            True if subject has the role
        """
        pass

    # === Lifecycle ===

    @abstractmethod
    async def start(self) -> None:
        """Start the authorization system."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the authorization system."""
        pass

    @abstractmethod
    async def reset(self) -> None:
        """Reset all authorization data (useful for testing)."""
        pass


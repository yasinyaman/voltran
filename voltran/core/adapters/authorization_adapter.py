"""In-memory authorization adapter implementation."""

from collections import defaultdict
from datetime import datetime
from typing import Optional

from voltran.core.ports.outbound.authorization import (
    AuthorizationContext,
    AuthorizationResult,
    IAuthorizationPort,
    Permission,
    PermissionEffect,
    Policy,
    ResourceType,
    Role,
)


class InMemoryAuthorizationAdapter(IAuthorizationPort):
    """
    In-memory implementation of authorization port.
    
    Stores all authorization data in memory - suitable for development
    and single-node deployments. For production with multiple nodes,
    use a distributed adapter (Redis, database, etc.)
    """

    def __init__(self, voltran_id: str = ""):
        """
        Initialize authorization adapter.
        
        Args:
            voltran_id: Voltran node ID
        """
        self._voltran_id = voltran_id
        
        # Storage
        self._permissions: dict[str, Permission] = {}
        self._permissions_by_name: dict[str, Permission] = {}
        self._roles: dict[str, Role] = {}
        self._roles_by_name: dict[str, Role] = {}
        self._policies: dict[str, Policy] = {}
        
        # Subject -> Roles mapping
        self._subject_roles: dict[str, set[str]] = defaultdict(set)
        
        # System roles
        self._system_roles: set[str] = set()
        
        self._running = False

    # === Permission Management ===

    async def create_permission(self, permission: Permission) -> str:
        """Create a new permission."""
        self._permissions[permission.id] = permission
        self._permissions_by_name[permission.name] = permission
        return permission.id

    async def get_permission(self, permission_id: str) -> Optional[Permission]:
        """Get a permission by ID."""
        return self._permissions.get(permission_id)

    async def get_permission_by_name(self, name: str) -> Optional[Permission]:
        """Get a permission by name."""
        return self._permissions_by_name.get(name)

    async def list_permissions(
        self,
        resource_type: Optional[ResourceType] = None,
    ) -> list[Permission]:
        """List all permissions."""
        permissions = list(self._permissions.values())
        if resource_type:
            permissions = [p for p in permissions if p.resource_type == resource_type]
        return permissions

    async def delete_permission(self, permission_id: str) -> bool:
        """Delete a permission."""
        if permission_id in self._permissions:
            permission = self._permissions[permission_id]
            del self._permissions[permission_id]
            if permission.name in self._permissions_by_name:
                del self._permissions_by_name[permission.name]
            return True
        return False

    # === Role Management ===

    async def create_role(self, role: Role) -> str:
        """Create a new role."""
        self._roles[role.id] = role
        self._roles_by_name[role.name] = role
        if role.is_system:
            self._system_roles.add(role.name)
        return role.id

    async def get_role(self, role_id: str) -> Optional[Role]:
        """Get a role by ID."""
        return self._roles.get(role_id)

    async def get_role_by_name(self, name: str) -> Optional[Role]:
        """Get a role by name."""
        return self._roles_by_name.get(name)

    async def list_roles(self) -> list[Role]:
        """List all roles."""
        return list(self._roles.values())

    async def update_role(self, role_id: str, role: Role) -> bool:
        """Update a role."""
        if role_id in self._roles:
            old_role = self._roles[role_id]
            role.id = role_id  # Preserve ID
            self._roles[role_id] = role
            
            # Update name mapping
            if old_role.name != role.name:
                if old_role.name in self._roles_by_name:
                    del self._roles_by_name[old_role.name]
                self._roles_by_name[role.name] = role
            
            # Update system roles
            if role.is_system:
                self._system_roles.add(role.name)
            elif old_role.name in self._system_roles:
                self._system_roles.discard(old_role.name)
            
            return True
        return False

    async def delete_role(self, role_id: str) -> bool:
        """Delete a role."""
        if role_id in self._roles:
            role = self._roles[role_id]
            if role.is_system:
                return False  # Cannot delete system roles
            
            del self._roles[role_id]
            if role.name in self._roles_by_name:
                del self._roles_by_name[role.name]
            
            # Remove from all subjects
            for subject_id in list(self._subject_roles.keys()):
                self._subject_roles[subject_id].discard(role.name)
                if not self._subject_roles[subject_id]:
                    del self._subject_roles[subject_id]
            
            return True
        return False

    async def assign_role(self, subject_id: str, role_name: str) -> bool:
        """Assign a role to a subject."""
        if role_name not in self._roles_by_name:
            return False
        
        self._subject_roles[subject_id].add(role_name)
        return True

    async def revoke_role(self, subject_id: str, role_name: str) -> bool:
        """Revoke a role from a subject."""
        if subject_id in self._subject_roles:
            self._subject_roles[subject_id].discard(role_name)
            if not self._subject_roles[subject_id]:
                del self._subject_roles[subject_id]
            return True
        return False

    async def get_subject_roles(self, subject_id: str) -> list[str]:
        """Get all roles for a subject (including inherited)."""
        direct_roles = list(self._subject_roles.get(subject_id, set()))
        all_roles = set(direct_roles)
        
        # Resolve inherited roles
        visited = set()
        to_process = list(direct_roles)
        
        while to_process:
            role_name = to_process.pop(0)
            if role_name in visited:
                continue
            visited.add(role_name)
            
            role = self._roles_by_name.get(role_name)
            if role:
                for inherited_name in role.inherited_roles:
                    if inherited_name not in all_roles:
                        all_roles.add(inherited_name)
                        to_process.append(inherited_name)
        
        return list(all_roles)

    # === Policy Management ===

    async def create_policy(self, policy: Policy) -> str:
        """Create a new policy."""
        self._policies[policy.id] = policy
        return policy.id

    async def get_policy(self, policy_id: str) -> Optional[Policy]:
        """Get a policy by ID."""
        return self._policies.get(policy_id)

    async def list_policies(
        self,
        enabled_only: bool = False,
    ) -> list[Policy]:
        """List all policies."""
        policies = list(self._policies.values())
        if enabled_only:
            policies = [p for p in policies if p.enabled]
        # Sort by priority (higher first)
        policies.sort(key=lambda p: p.priority, reverse=True)
        return policies

    async def update_policy(self, policy_id: str, policy: Policy) -> bool:
        """Update a policy."""
        if policy_id in self._policies:
            policy.id = policy_id  # Preserve ID
            self._policies[policy_id] = policy
            return True
        return False

    async def delete_policy(self, policy_id: str) -> bool:
        """Delete a policy."""
        if policy_id in self._policies:
            del self._policies[policy_id]
            return True
        return False

    # === Authorization Checks ===

    async def authorize(
        self,
        context: AuthorizationContext,
    ) -> AuthorizationResult:
        """
        Check if a subject is authorized to perform an action.
        
        Evaluation order:
        1. Policies (by priority, DENY takes precedence)
        2. Role-based permissions
        3. Direct permissions
        """
        # Get all roles for subject
        subject_roles = await self.get_subject_roles(context.subject_id)
        
        # Build permission string
        permission_parts = [context.resource_type.value]
        if context.resource_id:
            permission_parts.append(context.resource_id)
        permission_parts.append(context.action)
        permission_str = ":".join(permission_parts)
        
        # 1. Check policies (highest priority)
        policies = await self.list_policies(enabled_only=True)
        for policy in policies:
            if self._policy_matches(policy, context, subject_roles, permission_str):
                return AuthorizationResult(
                    allowed=(policy.effect == PermissionEffect.ALLOW),
                    reason=f"Policy: {policy.name}",
                    matched_policy=policy.id,
                )
        
        # 2. Check role-based permissions
        for role_name in subject_roles:
            role = self._roles_by_name.get(role_name)
            if role:
                for perm_str in role.permissions:
                    if self._permission_matches(perm_str, permission_str, context.resource_id):
                        return AuthorizationResult(
                            allowed=True,
                            reason=f"Role: {role_name}",
                            matched_permission=perm_str,
                        )
        
        # 3. Check direct permissions (if any)
        # This would require a subject -> permissions mapping
        
        # Default: deny
        return AuthorizationResult(
            allowed=False,
            reason="No matching policy or permission found",
        )

    async def has_permission(
        self,
        subject_id: str,
        permission: str,
        resource_id: Optional[str] = None,
    ) -> bool:
        """Check if subject has a specific permission."""
        try:
            perm = Permission.from_string(permission)
            if resource_id:
                perm.resource_id = resource_id
            
            context = AuthorizationContext(
                subject_id=subject_id,
                resource_type=perm.resource_type,
                resource_id=perm.resource_id,
                action=perm.action,
            )
            
            result = await self.authorize(context)
            return result.allowed
        except ValueError:
            return False

    async def has_role(
        self,
        subject_id: str,
        role_name: str,
    ) -> bool:
        """Check if subject has a specific role."""
        roles = await self.get_subject_roles(subject_id)
        return role_name in roles

    def _policy_matches(
        self,
        policy: Policy,
        context: AuthorizationContext,
        subject_roles: list[str],
        permission_str: str,
    ) -> bool:
        """Check if a policy matches the context."""
        # Check subjects
        if "*" not in policy.subjects:
            if context.subject_id not in policy.subjects:
                # Check roles
                if not any(role in policy.subjects for role in subject_roles):
                    return False
        
        # Check resources
        if "*" not in policy.resources:
            resource_pattern = f"{context.resource_type.value}:{context.resource_id or '*'}"
            if not any(self._pattern_matches(pattern, resource_pattern) for pattern in policy.resources):
                return False
        
        # Check actions
        if "*" not in policy.actions:
            if context.action not in policy.actions:
                return False
        
        return True

    def _permission_matches(
        self,
        pattern: str,
        permission_str: str,
        resource_id: Optional[str],
    ) -> bool:
        """Check if a permission pattern matches."""
        return self._pattern_matches(pattern, permission_str)

    def _pattern_matches(self, pattern: str, value: str) -> bool:
        """Check if a pattern matches a value (supports wildcards)."""
        if pattern == "*":
            return True
        
        # Simple wildcard matching
        if "*" in pattern:
            parts = pattern.split("*")
            if len(parts) == 2:
                return value.startswith(parts[0]) and value.endswith(parts[1])
            elif len(parts) == 1:
                return value.startswith(parts[0]) or value.endswith(parts[0])
        
        return pattern == value

    # === Lifecycle ===

    async def start(self) -> None:
        """Start the authorization system."""
        if self._running:
            return
        
        # Create default system roles
        await self._create_default_roles()
        
        self._running = True

    async def stop(self) -> None:
        """Stop the authorization system."""
        self._running = False

    async def reset(self) -> None:
        """Reset all authorization data."""
        self._permissions.clear()
        self._permissions_by_name.clear()
        self._roles.clear()
        self._roles_by_name.clear()
        self._policies.clear()
        self._subject_roles.clear()
        self._system_roles.clear()

    async def _create_default_roles(self) -> None:
        """Create default system roles."""
        # Admin role - full access
        admin_role = Role(
            name="admin",
            description="Full system access",
            permissions=[
                "system:*:*",
                "module:*:*",
                "endpoint:*:*",
                "cluster:*:*",
                "federation:*:*",
            ],
            is_system=True,
        )
        await self.create_role(admin_role)
        
        # Viewer role - read-only
        viewer_role = Role(
            name="viewer",
            description="Read-only access",
            permissions=[
                "system:*:read",
                "module:*:read",
                "endpoint:*:read",
            ],
            is_system=True,
        )
        await self.create_role(viewer_role)
        
        # Module owner role - full access to own module
        module_owner_role = Role(
            name="module_owner",
            description="Full access to owned module",
            permissions=[
                "module:*:read",
                "module:*:write",
                "endpoint:*:read",
                "endpoint:*:execute",
            ],
            is_system=True,
        )
        await self.create_role(module_owner_role)


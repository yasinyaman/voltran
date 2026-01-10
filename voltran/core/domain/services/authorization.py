"""Authorization domain service."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

import structlog

from voltran.core.domain.models import ModuleDescriptor, PermissionDeniedError
from voltran.core.domain.services.authorization_loader import AuthorizationRulesLoader
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

logger = structlog.get_logger(__name__)


class AuthorizationService:
    """
    Domain service for authorization and access control.
    
    Provides high-level authorization operations including:
    - Permission checks
    - Role management
    - Policy management
    - Module-level authorization
    - Endpoint-level authorization
    
    Usage:
        auth = AuthorizationService(adapter)
        await auth.start()
        
        # Check permission
        result = await auth.authorize(
            subject_id="user-123",
            resource_type=ResourceType.MODULE,
            resource_id="payment-module",
            action="read",
        )
        
        if result.allowed:
            # Access granted
            pass
    """

    def __init__(
        self,
        authorization_port: IAuthorizationPort,
        voltran_id: str = "",
    ):
        """
        Initialize authorization service.
        
        Args:
            authorization_port: Authorization port implementation
            voltran_id: Voltran node ID
        """
        self._auth = authorization_port
        self._voltran_id = voltran_id
        self._running = False

    async def start(self) -> None:
        """Start the authorization service."""
        if self._running:
            return

        await self._auth.start()
        self._running = True

        logger.info(
            "authorization_service_started",
            voltran_id=self._voltran_id,
        )

    async def stop(self) -> None:
        """Stop the authorization service."""
        await self._auth.stop()
        self._running = False

        logger.info("authorization_service_stopped")

    # === Permission Management ===

    async def create_permission(
        self,
        name: str,
        resource_type: ResourceType,
        action: str,
        resource_id: Optional[str] = None,
        description: str = "",
    ) -> str:
        """
        Create a new permission.
        
        Args:
            name: Permission name
            resource_type: Resource type
            action: Action (read, write, execute, etc.)
            resource_id: Optional specific resource ID
            description: Permission description
            
        Returns:
            Permission ID
        """
        permission = Permission(
            name=name,
            description=description,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
        )
        return await self._auth.create_permission(permission)

    async def get_permission(self, permission_id: str) -> Optional[Permission]:
        """Get a permission by ID."""
        return await self._auth.get_permission(permission_id)

    async def list_permissions(
        self,
        resource_type: Optional[ResourceType] = None,
    ) -> list[Permission]:
        """List all permissions."""
        return await self._auth.list_permissions(resource_type)

    # === Role Management ===

    async def create_role(
        self,
        name: str,
        permissions: list[str],
        description: str = "",
        inherited_roles: Optional[list[str]] = None,
    ) -> str:
        """
        Create a new role.
        
        Args:
            name: Role name
            permissions: List of permission strings
            description: Role description
            inherited_roles: Optional list of role names to inherit
            
        Returns:
            Role ID
        """
        role = Role(
            name=name,
            description=description,
            permissions=permissions,
            inherited_roles=inherited_roles or [],
        )
        return await self._auth.create_role(role)

    async def get_role(self, role_id: str) -> Optional[Role]:
        """Get a role by ID."""
        return await self._auth.get_role(role_id)

    async def get_role_by_name(self, name: str) -> Optional[Role]:
        """Get a role by name."""
        return await self._auth.get_role_by_name(name)

    async def list_roles(self) -> list[Role]:
        """List all roles."""
        return await self._auth.list_roles()

    async def assign_role(self, subject_id: str, role_name: str) -> bool:
        """
        Assign a role to a subject.
        
        Args:
            subject_id: Subject ID (user, module, etc.)
            role_name: Role name to assign
            
        Returns:
            True if assigned
        """
        return await self._auth.assign_role(subject_id, role_name)

    async def revoke_role(self, subject_id: str, role_name: str) -> bool:
        """
        Revoke a role from a subject.
        
        Args:
            subject_id: Subject ID
            role_name: Role name to revoke
            
        Returns:
            True if revoked
        """
        return await self._auth.revoke_role(subject_id, role_name)

    async def get_subject_roles(self, subject_id: str) -> list[str]:
        """
        Get all roles for a subject.
        
        Args:
            subject_id: Subject ID
            
        Returns:
            List of role names
        """
        return await self._auth.get_subject_roles(subject_id)

    # === Policy Management ===

    async def create_policy(
        self,
        name: str,
        subjects: list[str],
        resources: list[str],
        actions: list[str],
        effect: PermissionEffect = PermissionEffect.ALLOW,
        description: str = "",
        priority: int = 0,
    ) -> str:
        """
        Create a new policy.
        
        Args:
            name: Policy name
            subjects: List of subject IDs or "*" for all
            resources: List of resource patterns (e.g., "module:*")
            actions: List of actions or "*" for all
            effect: Allow or deny
            description: Policy description
            priority: Policy priority (higher = evaluated first)
            
        Returns:
            Policy ID
        """
        policy = Policy(
            name=name,
            description=description,
            subjects=subjects,
            resources=resources,
            actions=actions,
            effect=effect,
            priority=priority,
        )
        return await self._auth.create_policy(policy)

    async def get_policy(self, policy_id: str) -> Optional[Policy]:
        """Get a policy by ID."""
        return await self._auth.get_policy(policy_id)

    async def list_policies(self, enabled_only: bool = False) -> list[Policy]:
        """List all policies."""
        return await self._auth.list_policies(enabled_only)

    # === Authorization Checks ===

    async def authorize(
        self,
        subject_id: str,
        resource_type: ResourceType,
        action: str,
        resource_id: Optional[str] = None,
        roles: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> AuthorizationResult:
        """
        Check if a subject is authorized to perform an action.
        
        Args:
            subject_id: Subject ID (user, module, etc.)
            resource_type: Resource type
            action: Action to perform
            resource_id: Optional specific resource ID
            roles: Optional list of roles (if not provided, fetched from adapter)
            metadata: Optional additional context
            
        Returns:
            AuthorizationResult with decision
        """
        # Get roles if not provided
        if roles is None:
            roles = await self._auth.get_subject_roles(subject_id)
        
        context = AuthorizationContext(
            subject_id=subject_id,
            roles=roles,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            metadata=metadata or {},
        )
        
        return await self._auth.authorize(context)

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
        return await self._auth.has_permission(subject_id, permission, resource_id)

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
        return await self._auth.has_role(subject_id, role_name)

    # === Convenience Methods ===

    async def can_access_module(
        self,
        subject_id: str,
        module: ModuleDescriptor,
        action: str = "read",
    ) -> bool:
        """
        Check if subject can access a module.
        
        Args:
            subject_id: Subject ID
            module: Module descriptor
            action: Action (read, write, execute)
            
        Returns:
            True if permitted
        """
        result = await self.authorize(
            subject_id=subject_id,
            resource_type=ResourceType.MODULE,
            resource_id=module.id,
            action=action,
        )
        return result.allowed

    async def can_access_endpoint(
        self,
        subject_id: str,
        contract_id: str,
        action: str = "execute",
    ) -> bool:
        """
        Check if subject can access an endpoint/contract.
        
        Args:
            subject_id: Subject ID
            contract_id: Contract ID
            action: Action (execute, read, write)
            
        Returns:
            True if permitted
        """
        result = await self.authorize(
            subject_id=subject_id,
            resource_type=ResourceType.CONTRACT,
            resource_id=contract_id,
            action=action,
        )
        return result.allowed

    async def require_permission(
        self,
        subject_id: str,
        permission: str,
        resource_id: Optional[str] = None,
    ) -> None:
        """
        Require a permission (raises exception if not permitted).
        
        Args:
            subject_id: Subject ID
            permission: Permission string
            resource_id: Optional resource ID
            
        Raises:
            PermissionDeniedError: If not permitted
        """
        if not await self.has_permission(subject_id, permission, resource_id):
            from voltran.core.domain.models import PermissionDeniedError
            
            raise PermissionDeniedError(
                f"Permission denied: {permission}",
                subject_id=subject_id,
                permission=permission,
            )

    async def require_role(
        self,
        subject_id: str,
        role_name: str,
    ) -> None:
        """
        Require a role (raises exception if not present).
        
        Args:
            subject_id: Subject ID
            role_name: Role name
            
        Raises:
            PermissionDeniedError: If role not present
        """
        if not await self.has_role(subject_id, role_name):
            raise PermissionDeniedError(
                f"Role required: {role_name}",
                subject_id=subject_id,
                role=role_name,
            )

    # === Transaction-Level Authorization ===

    @asynccontextmanager
    async def authorization_guard(
        self,
        subject_id: str,
        permission: Optional[str] = None,
        role: Optional[str] = None,
        resource_type: Optional[ResourceType] = None,
        resource_id: Optional[str] = None,
        action: Optional[str] = None,
    ):
        """
        Context manager for transaction-level authorization.
        
        Usage:
            async with auth.authorization_guard(
                subject_id="user-123",
                permission="module:payment:write",
            ):
                # Bu blok içindeki işlemler yetki kontrolünden geçer
                await process_payment()
        
        Args:
            subject_id: Subject ID
            permission: Optional permission to check
            role: Optional role to check
            resource_type: Optional resource type
            resource_id: Optional resource ID
            action: Optional action
            
        Raises:
            PermissionDeniedError: If authorization fails
        """
        # Pre-check authorization
        if permission:
            if not await self.has_permission(subject_id, permission, resource_id):
                raise PermissionDeniedError(
                    f"Permission denied: {permission}",
                    subject_id=subject_id,
                    permission=permission,
                )
        
        if role:
            if not await self.has_role(subject_id, role):
                raise PermissionDeniedError(
                    f"Role required: {role}",
                    subject_id=subject_id,
                    role=role,
                )
        
        if resource_type and action:
            result = await self.authorize(
                subject_id=subject_id,
                resource_type=resource_type,
                resource_id=resource_id,
                action=action,
            )
            if not result.allowed:
                raise PermissionDeniedError(
                    f"Authorization denied: {resource_type.value}:{action}",
                    subject_id=subject_id,
                )
        
        # Yield control
        try:
            yield
        finally:
            # Post-transaction checks can be added here
            pass

    async def check_permission(
        self,
        subject_id: str,
        permission: str,
        resource_id: Optional[str] = None,
        raise_on_deny: bool = True,
    ) -> bool:
        """
        Check permission and optionally raise exception.
        
        Usage:
            # Check without exception
            if await auth.check_permission("user-123", "module:payment:write", raise_on_deny=False):
                await process_payment()
            
            # Check with exception
            await auth.check_permission("user-123", "module:payment:write")
        
        Args:
            subject_id: Subject ID
            permission: Permission string
            resource_id: Optional resource ID
            raise_on_deny: If True, raise exception on deny
            
        Returns:
            True if permitted
            
        Raises:
            PermissionDeniedError: If raise_on_deny=True and permission denied
        """
        has_perm = await self.has_permission(subject_id, permission, resource_id)
        
        if not has_perm and raise_on_deny:
            raise PermissionDeniedError(
                f"Permission denied: {permission}",
                subject_id=subject_id,
                permission=permission,
            )
        
        return has_perm

    async def check_role(
        self,
        subject_id: str,
        role_name: str,
        raise_on_deny: bool = True,
    ) -> bool:
        """
        Check role and optionally raise exception.
        
        Usage:
            if await auth.check_role("user-123", "admin", raise_on_deny=False):
                await admin_operation()
        
        Args:
            subject_id: Subject ID
            role_name: Role name
            raise_on_deny: If True, raise exception on deny
            
        Returns:
            True if subject has role
            
        Raises:
            PermissionDeniedError: If raise_on_deny=True and role not present
        """
        has_role = await self.has_role(subject_id, role_name)
        
        if not has_role and raise_on_deny:
            raise PermissionDeniedError(
                f"Role required: {role_name}",
                subject_id=subject_id,
                role=role_name,
            )
        
        return has_role

    # === Rules Loading ===

    async def load_rules_from_file(
        self,
        file_path: str | Path,
        clear_existing: bool = False,
    ) -> dict[str, Any]:
        """
        Load authorization rules from a YAML or JSON file.
        
        File format:
            permissions:
              - name: payment:write
                resource_type: module
                action: write
                description: Payment write permission
            roles:
              - name: payment_manager
                permissions:
                  - module:payment:read
                  - module:payment:write
                description: Payment manager role
            policies:
              - name: Deny Normal Users
                subjects: ["normal_user"]
                resources: ["module:payment:*"]
                actions: ["write"]
                effect: deny
                priority: 100
            assignments:
              - subject_id: user-123
                role: payment_manager
        
        Args:
            file_path: Path to rules file (YAML or JSON)
            clear_existing: If True, clear existing rules before loading
            
        Returns:
            Dictionary with loaded rules summary
            
        Raises:
            FileNotFoundError: If file does not exist
            ValueError: If file format is invalid
        """
        loader = AuthorizationRulesLoader()
        rules_data = loader.load_from_file(file_path)
        parsed = loader.parse_rules(rules_data)
        
        if clear_existing:
            await self._auth.reset()
            logger.info("authorization_rules_cleared")
        
        # Load permissions
        permission_count = 0
        for permission in parsed["permissions"]:
            await self._auth.create_permission(permission)
            permission_count += 1
        
        # Load roles
        role_count = 0
        for role in parsed["roles"]:
            await self._auth.create_role(role)
            role_count += 1
        
        # Load policies
        policy_count = 0
        for policy in parsed["policies"]:
            await self._auth.create_policy(policy)
            policy_count += 1
        
        # Apply assignments
        assignment_count = 0
        for assignment in parsed["assignments"]:
            subject_id = assignment.get("subject_id")
            role_name = assignment.get("role")
            if subject_id and role_name:
                await self._auth.assign_role(subject_id, role_name)
                assignment_count += 1
        
        logger.info(
            "authorization_rules_loaded",
            file_path=str(file_path),
            permissions=permission_count,
            roles=role_count,
            policies=policy_count,
            assignments=assignment_count,
        )
        
        return {
            "permissions": permission_count,
            "roles": role_count,
            "policies": policy_count,
            "assignments": assignment_count,
        }

    async def export_rules_to_file(
        self,
        file_path: str | Path,
        format: str = "yaml",
    ) -> None:
        """
        Export current authorization rules to a file.
        
        Args:
            file_path: Path to output file
            format: Output format ("yaml" or "json")
            
        Raises:
            ValueError: If format is not supported
        """
        path = Path(file_path)
        
        # Collect all rules
        permissions = await self._auth.list_permissions()
        roles = await self._auth.list_roles()
        policies = await self._auth.list_policies(enabled_only=False)
        
        # Build export data
        export_data = {
            "permissions": [
                {
                    "name": p.name,
                    "description": p.description,
                    "resource_type": p.resource_type.value,
                    "resource_id": p.resource_id,
                    "action": p.action,
                    "effect": p.effect.value,
                    "conditions": p.conditions,
                }
                for p in permissions
            ],
            "roles": [
                {
                    "name": r.name,
                    "description": r.description,
                    "permissions": r.permissions,
                    "inherited_roles": r.inherited_roles,
                    "is_system": r.is_system,
                }
                for r in roles
            ],
            "policies": [
                {
                    "name": p.name,
                    "description": p.description,
                    "subjects": p.subjects,
                    "resources": p.resources,
                    "actions": p.actions,
                    "effect": p.effect.value,
                    "conditions": p.conditions,
                    "priority": p.priority,
                    "enabled": p.enabled,
                }
                for p in policies
            ],
            "assignments": [],  # Note: Subject-role assignments are not easily exportable
        }
        
        # Write to file
        if format.lower() == "yaml":
            try:
                import yaml
            except ImportError:
                raise ImportError(
                    "PyYAML is required for YAML export. Install with: pip install pyyaml"
                )
            
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                yaml.dump(export_data, f, default_flow_style=False, sort_keys=False)
        elif format.lower() == "json":
            import json
            
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'yaml' or 'json'")
        
        logger.info(
            "authorization_rules_exported",
            file_path=str(file_path),
            format=format,
            permissions=len(permissions),
            roles=len(roles),
            policies=len(policies),
        )


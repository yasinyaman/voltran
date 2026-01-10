"""Authorization helpers for Voltran modules."""

from contextlib import asynccontextmanager
from typing import Any, Optional

from voltran.core.domain.models import PermissionDeniedError
from voltran.core.ports.outbound.authorization import ResourceType


class AuthorizationGuard:
    """
    Helper class for transaction-level authorization in modules.
    
    Usage:
        guard = AuthorizationGuard(module_instance)
        
        # Check permission
        async with guard.require_permission("module:payment:write", subject_id="user-123"):
            await process_payment()
        
        # Check role
        async with guard.require_role("admin", subject_id="user-123"):
            await admin_operation()
        
        # Check resource access
        async with guard.require_access(
            ResourceType.MODULE,
            "payment-module-id",
            "write",
            subject_id="user-123",
        ):
            await update_payment()
    """

    def __init__(self, module_instance: Any):
        """
        Initialize authorization guard.
        
        Args:
            module_instance: Module instance with _voltran_context
        """
        self._module = module_instance
        
        if not hasattr(module_instance, "_voltran_context"):
            raise RuntimeError(
                "Module context not available. Ensure module is registered with Voltran."
            )
        
        context = module_instance._voltran_context
        self._auth = getattr(context, "authorization", None)
        
        if not self._auth:
            raise RuntimeError("Authorization service not available")

    @asynccontextmanager
    async def require_permission(
        self,
        permission: str,
        subject_id: str,
        resource_id: Optional[str] = None,
    ):
        """
        Context manager to require a permission for a transaction.
        
        Usage:
            async with guard.require_permission("module:payment:write", "user-123"):
                # Transaction code here
                await process_payment()
        
        Args:
            permission: Permission string
            subject_id: Subject ID
            resource_id: Optional resource ID
            
        Raises:
            PermissionDeniedError: If permission denied
        """
        async with self._auth.authorization_guard(
            subject_id=subject_id,
            permission=permission,
            resource_id=resource_id,
        ):
            yield

    @asynccontextmanager
    async def require_role(
        self,
        role: str,
        subject_id: str,
    ):
        """
        Context manager to require a role for a transaction.
        
        Usage:
            async with guard.require_role("admin", "user-123"):
                # Admin operation here
                await admin_operation()
        
        Args:
            role: Role name
            subject_id: Subject ID
            
        Raises:
            PermissionDeniedError: If role not present
        """
        async with self._auth.authorization_guard(
            subject_id=subject_id,
            role=role,
        ):
            yield

    @asynccontextmanager
    async def require_access(
        self,
        resource_type: ResourceType,
        action: str,
        subject_id: str,
        resource_id: Optional[str] = None,
    ):
        """
        Context manager to require resource access for a transaction.
        
        Usage:
            async with guard.require_access(
                ResourceType.MODULE,
                "write",
                "user-123",
                resource_id="payment-module-id",
            ):
                # Resource access code here
                await update_module()
        
        Args:
            resource_type: Resource type
            action: Action to perform
            subject_id: Subject ID
            resource_id: Optional resource ID
            
        Raises:
            PermissionDeniedError: If access denied
        """
        async with self._auth.authorization_guard(
            subject_id=subject_id,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
        ):
            yield

    async def check_permission(
        self,
        permission: str,
        subject_id: str,
        resource_id: Optional[str] = None,
        raise_on_deny: bool = True,
    ) -> bool:
        """
        Check permission (helper method).
        
        Args:
            permission: Permission string
            subject_id: Subject ID
            resource_id: Optional resource ID
            raise_on_deny: If True, raise exception on deny
            
        Returns:
            True if permitted
        """
        return await self._auth.check_permission(
            subject_id=subject_id,
            permission=permission,
            resource_id=resource_id,
            raise_on_deny=raise_on_deny,
        )

    async def check_role(
        self,
        role: str,
        subject_id: str,
        raise_on_deny: bool = True,
    ) -> bool:
        """
        Check role (helper method).
        
        Args:
            role: Role name
            subject_id: Subject ID
            raise_on_deny: If True, raise exception on deny
            
        Returns:
            True if subject has role
        """
        return await self._auth.check_role(
            subject_id=subject_id,
            role_name=role,
            raise_on_deny=raise_on_deny,
        )


def get_auth_guard(module_instance: Any) -> Optional[AuthorizationGuard]:
    """
    Get authorization guard for a module instance.
    
    Args:
        module_instance: Module instance
        
    Returns:
        AuthorizationGuard if authorization is enabled, None otherwise
    """
    try:
        return AuthorizationGuard(module_instance)
    except (RuntimeError, AttributeError):
        return None


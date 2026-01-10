"""Decorators for defining Voltran modules and ports."""

import functools
import inspect
from typing import Any, Callable, Optional, TypeVar

from voltran.core.domain.models import Capability, PortDirection, PermissionDeniedError

# Type variables for decorated functions
F = TypeVar("F", bound=Callable[..., Any])


# Module metadata storage
_MODULE_REGISTRY: dict[type, dict[str, Any]] = {}


def voltran_module(
    name: str,
    version: str = "1.0.0",
    description: str = "",
) -> Callable[[type], type]:
    """
    Decorator to mark a class as a Voltran module.
    
    Usage:
        @voltran_module(name="my-service", version="1.0.0")
        class MyModule:
            @inbound_port(contract="my.service.v1")
            async def handle_request(self, request):
                return {"status": "ok"}
    
    Args:
        name: Module name for discovery
        version: Module version (semver format)
        description: Optional module description
        
    Returns:
        Decorated class with Voltran metadata
    """

    def decorator(cls: type) -> type:
        # Store module metadata
        _MODULE_REGISTRY[cls] = {
            "name": name,
            "version": version,
            "description": description,
            "inbound_ports": [],
            "outbound_ports": [],
        }
        
        # Scan for port decorators
        for attr_name in dir(cls):
            attr = getattr(cls, attr_name, None)
            if attr is None:
                continue
                
            # Check for inbound port
            if hasattr(attr, "_voltran_inbound_port"):
                port_info = attr._voltran_inbound_port
                _MODULE_REGISTRY[cls]["inbound_ports"].append(port_info)
                
            # Check for outbound port
            if hasattr(attr, "_voltran_outbound_port"):
                port_info = attr._voltran_outbound_port
                _MODULE_REGISTRY[cls]["outbound_ports"].append(port_info)
        
        # Add helper method to get capabilities
        def get_capabilities(self: Any) -> list[Capability]:
            capabilities: list[Capability] = []
            
            for port in _MODULE_REGISTRY[cls]["inbound_ports"]:
                capabilities.append(
                    Capability(
                        name=port["name"],
                        contract_id=port["contract"],
                        direction=PortDirection.INBOUND,
                        protocol=port.get("protocol", "grpc"),
                    )
                )
            
            for port in _MODULE_REGISTRY[cls]["outbound_ports"]:
                capabilities.append(
                    Capability(
                        name=port["name"],
                        contract_id=port["contract"],
                        direction=PortDirection.OUTBOUND,
                        protocol=port.get("protocol", "grpc"),
                    )
                )
            
            return capabilities
        
        cls._voltran_module = _MODULE_REGISTRY[cls]
        cls._get_capabilities = get_capabilities
        
        return cls

    return decorator


def inbound_port(
    contract: str,
    name: Optional[str] = None,
    protocol: str = "grpc",
    description: str = "",
) -> Callable[[F], F]:
    """
    Decorator to mark a method as an inbound port.
    
    Inbound ports are entry points that other modules can call.
    
    Usage:
        @inbound_port(contract="payment.process.v1")
        async def process_payment(self, request):
            return {"status": "success"}
    
    Args:
        contract: Contract ID that this port implements
        name: Optional port name (defaults to method name)
        protocol: Communication protocol (grpc, rest, etc.)
        description: Optional port description
        
    Returns:
        Decorated method with port metadata
    """

    def decorator(func: F) -> F:
        port_name = name or func.__name__
        
        # Validate async
        if not inspect.iscoroutinefunction(func):
            raise TypeError(
                f"Inbound port '{port_name}' must be an async function"
            )
        
        # Store port metadata
        func._voltran_inbound_port = {  # type: ignore
            "name": port_name,
            "contract": contract,
            "protocol": protocol,
            "description": description,
            "handler": func,
        }
        
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Add any pre/post processing here
            return await func(self, *args, **kwargs)
        
        # Preserve metadata on wrapper
        wrapper._voltran_inbound_port = func._voltran_inbound_port  # type: ignore
        
        return wrapper  # type: ignore

    return decorator


def outbound_port(
    contract: str,
    name: Optional[str] = None,
    protocol: str = "grpc",
    required: bool = True,
    description: str = "",
) -> Callable[[F], F]:
    """
    Decorator to mark a method as an outbound port.
    
    Outbound ports are dependencies that this module requires.
    The SDK will automatically route calls to the appropriate provider.
    
    Usage:
        @outbound_port(contract="notification.send.v1")
        async def send_notification(self, message):
            # SDK will route this to a module providing this contract
            pass
    
    Args:
        contract: Contract ID that this port requires
        name: Optional port name (defaults to method name)
        protocol: Communication protocol
        required: Whether this dependency is required
        description: Optional port description
        
    Returns:
        Decorated method with port metadata and auto-routing
    """

    def decorator(func: F) -> F:
        port_name = name or func.__name__
        
        # Store port metadata
        func._voltran_outbound_port = {  # type: ignore
            "name": port_name,
            "contract": contract,
            "protocol": protocol,
            "required": required,
            "description": description,
        }
        
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Check if module has a discovery context
            if hasattr(self, "_voltran_context"):
                context = self._voltran_context
                
                # Find provider for this contract
                provider = await context.find_provider(contract)
                
                if provider is None:
                    if required:
                        raise RuntimeError(
                            f"No provider found for required contract: {contract}"
                        )
                    return None
                
                # Route call to provider
                return await context.call_provider(
                    provider, port_name, *args, **kwargs
                )
            
            # Fallback to original implementation
            return await func(self, *args, **kwargs)
        
        # Preserve metadata on wrapper
        wrapper._voltran_outbound_port = func._voltran_outbound_port  # type: ignore
        
        return wrapper  # type: ignore

    return decorator


def get_module_metadata(cls: type) -> Optional[dict[str, Any]]:
    """
    Get Voltran module metadata from a class.
    
    Args:
        cls: Class to get metadata from
        
    Returns:
        Module metadata if decorated, None otherwise
    """
    return _MODULE_REGISTRY.get(cls)


def is_voltran_module(cls: type) -> bool:
    """
    Check if a class is a Voltran module.
    
    Args:
        cls: Class to check
        
    Returns:
        True if class is decorated with @voltran_module
    """
    return cls in _MODULE_REGISTRY


def get_inbound_ports(instance: Any) -> list[dict[str, Any]]:
    """
    Get all inbound ports from a module instance.
    
    Args:
        instance: Module instance
        
    Returns:
        List of inbound port metadata
    """
    ports: list[dict[str, Any]] = []
    
    for attr_name in dir(instance):
        attr = getattr(instance, attr_name, None)
        if attr and hasattr(attr, "_voltran_inbound_port"):
            ports.append(attr._voltran_inbound_port)
    
    return ports


def get_outbound_ports(instance: Any) -> list[dict[str, Any]]:
    """
    Get all outbound ports from a module instance.
    
    Args:
        instance: Module instance
        
    Returns:
        List of outbound port metadata
    """
    ports: list[dict[str, Any]] = []
    
    for attr_name in dir(instance):
        attr = getattr(instance, attr_name, None)
        if attr and hasattr(attr, "_voltran_outbound_port"):
            ports.append(attr._voltran_outbound_port)
    
    return ports


def require_permission(
    permission: str,
    resource_id: Optional[str] = None,
    subject_id_key: str = "subject_id",
) -> Callable[[F], F]:
    """
    Decorator to require a permission for a method.
    
    The method must receive a subject_id parameter (or custom key).
    The authorization service will be checked from the module context.
    
    Usage:
        @inbound_port(contract="payment.process.v1")
        @require_permission("module:payment:write")
        async def process_payment(self, request, subject_id: str):
            return {"status": "success"}
    
    Args:
        permission: Permission string (e.g., "module:payment:read")
        resource_id: Optional resource ID (can be from kwargs)
        subject_id_key: Key name for subject_id in function kwargs
        
    Returns:
        Decorated method with permission check
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get authorization service from context
            if not hasattr(self, "_voltran_context"):
                raise RuntimeError(
                    "Module context not available. Ensure module is registered with Voltran."
                )
            
            context = self._voltran_context
            if not hasattr(context, "authorization"):
                # Authorization not enabled, allow
                return await func(self, *args, **kwargs)
            
            # Get subject_id from kwargs
            subject_id = kwargs.get(subject_id_key)
            if not subject_id:
                raise PermissionDeniedError(
                    f"Subject ID not provided (key: {subject_id_key})",
                    permission=permission,
                )
            
            # Check permission
            auth_service = context.authorization
            has_perm = await auth_service.has_permission(
                subject_id=subject_id,
                permission=permission,
                resource_id=resource_id or kwargs.get("resource_id"),
            )
            
            if not has_perm:
                raise PermissionDeniedError(
                    f"Permission denied: {permission}",
                    subject_id=subject_id,
                    permission=permission,
                )
            
            return await func(self, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


def require_role(
    role: str,
    subject_id_key: str = "subject_id",
) -> Callable[[F], F]:
    """
    Decorator to require a role for a method.
    
    The method must receive a subject_id parameter (or custom key).
    
    Usage:
        @inbound_port(contract="admin.config.v1")
        @require_role("admin")
        async def update_config(self, request, subject_id: str):
            return {"status": "updated"}
    
    Args:
        role: Role name
        subject_id_key: Key name for subject_id in function kwargs
        
    Returns:
        Decorated method with role check
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            # Get authorization service from context
            if not hasattr(self, "_voltran_context"):
                raise RuntimeError(
                    "Module context not available. Ensure module is registered with Voltran."
                )
            
            context = self._voltran_context
            if not hasattr(context, "authorization"):
                # Authorization not enabled, allow
                return await func(self, *args, **kwargs)
            
            # Get subject_id from kwargs
            subject_id = kwargs.get(subject_id_key)
            if not subject_id:
                raise PermissionDeniedError(
                    f"Subject ID not provided (key: {subject_id_key})",
                    role=role,
                )
            
            # Check role
            auth_service = context.authorization
            has_role = await auth_service.has_role(
                subject_id=subject_id,
                role_name=role,
            )
            
            if not has_role:
                raise PermissionDeniedError(
                    f"Role required: {role}",
                    subject_id=subject_id,
                    role=role,
                )
            
            return await func(self, *args, **kwargs)

        return wrapper  # type: ignore

    return decorator


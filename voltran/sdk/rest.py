"""
REST API Decorator Module.

Minimum kod ile class method'larını REST endpoint'lerine dönüştürür.

Kullanım:
    from voltran import RestService, get, post, put, delete

    @RestService("/api/users")
    class UserService:
        @get("/")
        async def list_users(self) -> list[dict]:
            return [{"id": 1, "name": "Ali"}]
        
        @get("/{user_id}")
        async def get_user(self, user_id: int) -> dict:
            return {"id": user_id, "name": "Ali"}
        
        @post("/")
        async def create_user(self, name: str, email: str) -> dict:
            return {"id": 1, "name": name, "email": email}
    
    # FastAPI'ye mount et
    app = FastAPI()
    UserService().mount(app)
"""

import inspect
import re
from functools import wraps
from typing import Any, Callable, get_type_hints

from fastapi import APIRouter, FastAPI, Request
from pydantic import BaseModel, Field, create_model


# =========================================================================
# Route Metadata
# =========================================================================

_ROUTE_ATTR = "__rest_route__"
_SERVICE_ATTR = "__rest_service__"


class RouteInfo:
    """Route metadata container."""
    
    __slots__ = ("path", "method", "summary", "tags", "response_model")
    
    def __init__(
        self,
        path: str,
        method: str,
        summary: str = "",
        tags: list[str] | None = None,
        response_model: type | None = None,
    ):
        self.path = path
        self.method = method
        self.summary = summary
        self.tags = tags or []
        self.response_model = response_model


class ServiceInfo:
    """Service metadata container."""
    
    __slots__ = ("prefix", "tags")
    
    def __init__(self, prefix: str, tags: list[str] | None = None):
        self.prefix = prefix.rstrip("/")
        self.tags = tags or []


# =========================================================================
# Method Decorators
# =========================================================================


def _route_decorator(method: str, path: str, **kwargs) -> Callable:
    """Generic route decorator factory."""
    
    def decorator(func: Callable) -> Callable:
        # Docstring'den summary çek
        summary = kwargs.get("summary", "")
        if not summary and func.__doc__:
            summary = func.__doc__.strip().split("\n")[0]
        
        route_info = RouteInfo(
            path=path,
            method=method,
            summary=summary,
            tags=kwargs.get("tags"),
            response_model=kwargs.get("response_model"),
        )
        setattr(func, _ROUTE_ATTR, route_info)
        return func
    
    return decorator


def get(path: str, **kwargs) -> Callable:
    """GET endpoint decorator."""
    return _route_decorator("GET", path, **kwargs)


def post(path: str, **kwargs) -> Callable:
    """POST endpoint decorator."""
    return _route_decorator("POST", path, **kwargs)


def put(path: str, **kwargs) -> Callable:
    """PUT endpoint decorator."""
    return _route_decorator("PUT", path, **kwargs)


def patch(path: str, **kwargs) -> Callable:
    """PATCH endpoint decorator."""
    return _route_decorator("PATCH", path, **kwargs)


def delete(path: str, **kwargs) -> Callable:
    """DELETE endpoint decorator."""
    return _route_decorator("DELETE", path, **kwargs)


# =========================================================================
# Class Decorator
# =========================================================================


def RestService(prefix: str = "", tags: list[str] | None = None):
    """
    REST Service class decorator.
    
    Method'ları REST endpoint'lerine dönüştürür ve mount() metodu ekler.
    
    Args:
        prefix: URL prefix (örn: "/api/users")
        tags: OpenAPI tag'leri
    
    Example:
        @RestService("/api/users")
        class UserService:
            @get("/")
            async def list_users(self): ...
    """
    
    def decorator(cls: type) -> type:
        service_info = ServiceInfo(prefix, tags)
        setattr(cls, _SERVICE_ATTR, service_info)
        
        # mount() metodu ekle
        original_init = cls.__init__
        
        def new_init(self, *args, **kwargs):
            original_init(self, *args, **kwargs)
            self._router: APIRouter | None = None
        
        cls.__init__ = new_init
        cls.mount = _create_mount_method()
        cls.router = property(_get_router)
        
        return cls
    
    return decorator


def _get_router(self) -> APIRouter:
    """Router property getter."""
    if self._router is None:
        self._router = _build_router(self)
    return self._router


def _create_mount_method() -> Callable:
    """Mount metodu oluştur."""
    
    def mount(self, app: FastAPI) -> APIRouter:
        """
        Service'i FastAPI uygulamasına mount et.
        
        Args:
            app: FastAPI uygulaması
        
        Returns:
            Oluşturulan APIRouter
        """
        router = self.router
        app.include_router(router)
        return router
    
    return mount


def _build_router(instance: Any) -> APIRouter:
    """Instance'dan APIRouter oluştur."""
    cls = type(instance)
    service_info: ServiceInfo = getattr(cls, _SERVICE_ATTR)
    
    router = APIRouter(
        prefix=service_info.prefix,
        tags=service_info.tags or [cls.__name__],
    )
    
    # Class'ın kendi method'larını tara (property'leri atla)
    for name in dir(cls):
        if name.startswith("_"):
            continue
        
        # Property'leri atla (router, mount gibi)
        if name in ("router", "mount"):
            continue
        
        # Class attribute'unu al
        cls_attr = getattr(cls, name, None)
        if cls_attr is None:
            continue
        
        # Property'leri atla
        if isinstance(cls_attr, property):
            continue
        
        # Instance method'u al
        method = getattr(instance, name, None)
        if method is None or not callable(method):
            continue
        
        # Route decorator'ı var mı?
        # Method'un unbound versiyonunda ara
        unbound = getattr(cls, name, None)
        if unbound is None:
            continue
        
        route_info: RouteInfo | None = getattr(unbound, _ROUTE_ATTR, None)
        if route_info is None:
            continue
        
        # Endpoint oluştur
        endpoint = _create_endpoint(method, route_info)
        
        # Router'a ekle
        router_method = getattr(router, route_info.method.lower())
        router_method(
            route_info.path,
            summary=route_info.summary,
            response_model=route_info.response_model,
            tags=route_info.tags or None,
        )(endpoint)
    
    return router


def _create_endpoint(method: Callable, route_info: RouteInfo) -> Callable:
    """Method'dan FastAPI endpoint oluştur."""
    sig = inspect.signature(method)
    
    # Path parametrelerini bul
    path_params = set()
    for match in re.finditer(r"\{(\w+)\}", route_info.path):
        path_params.add(match.group(1))
    
    # Body gerekli mi?
    needs_body = route_info.method in ("POST", "PUT", "PATCH")
    
    # Body ve query parametrelerini bul
    body_params = []
    query_params = []
    path_param_list = []
    
    for param_name, param in sig.parameters.items():
        if param_name == "self":
            continue
        if param_name in path_params:
            path_param_list.append(param)
        elif needs_body:
            body_params.append(param)
        else:
            query_params.append(param)
    
    if needs_body and body_params:
        # Dinamik Pydantic model oluştur (create_model ile)
        field_definitions = {}
        for param in body_params:
            annotation = param.annotation if param.annotation != inspect.Parameter.empty else Any
            if param.default != inspect.Parameter.empty:
                # Optional field with default
                field_definitions[param.name] = (annotation, param.default)
            else:
                # Required field
                field_definitions[param.name] = (annotation, ...)
        
        # Benzersiz model adı
        model_name = f"_{method.__name__}_Body"
        BodyModel = create_model(model_name, **field_definitions)
        
        # Closure için değişkenleri yakala
        _body_params = [p.name for p in body_params]
        _method = method
        
        async def endpoint_with_body(body: BodyModel, **path_kwargs):  # type: ignore
            # Body'den parametreleri al
            kwargs = {}
            for param_name in _body_params:
                kwargs[param_name] = getattr(body, param_name)
            
            # Path parametrelerini ekle
            kwargs.update(path_kwargs)
            
            return await _method(**kwargs)
        
        # Signature oluştur: body + path params
        new_params = [
            inspect.Parameter("body", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=BodyModel)
        ]
        for param in path_param_list:
            new_params.append(param.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD))
        
        endpoint_with_body.__signature__ = inspect.Signature(new_params)  # type: ignore
        endpoint_with_body.__name__ = method.__name__
        endpoint_with_body.__doc__ = method.__doc__
        
        return endpoint_with_body
    else:
        # GET/DELETE için sadece path/query parametreleri
        _method = method
        _all_params = path_param_list + query_params
        _param_names = [p.name for p in _all_params]
        
        async def endpoint_no_body(**kwargs):
            return await _method(**kwargs)
        
        # Signature oluştur: path + query params
        new_params = []
        for param in _all_params:
            new_params.append(param.replace(kind=inspect.Parameter.POSITIONAL_OR_KEYWORD))
        
        endpoint_no_body.__signature__ = inspect.Signature(new_params)  # type: ignore
        endpoint_no_body.__name__ = method.__name__
        endpoint_no_body.__doc__ = method.__doc__
        
        return endpoint_no_body


# =========================================================================
# Exports
# =========================================================================

__all__ = [
    "RestService",
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "RouteInfo",
    "ServiceInfo",
]


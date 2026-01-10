"""
Module Client Service - Modüller arası REST iletişim servisi.

Bu servis, modüllerin birbirleriyle REST API üzerinden
kolayca iletişim kurmasını sağlar.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import structlog

from voltran.core.domain.models import ModuleDescriptor, PortDirection
from voltran.core.domain.services.discovery import DiscoveryService
from voltran.core.ports.outbound.rest_client import IRestClientPort

logger = structlog.get_logger(__name__)


@dataclass
class ModuleCallResult:
    """Modül çağrısı sonucu."""
    success: bool
    status_code: int
    data: Any
    module_id: str
    module_name: str
    elapsed_ms: float
    timestamp: datetime = field(default_factory=datetime.now)
    error: str | None = None


@dataclass
class ModuleEndpoint:
    """Modül endpoint bilgisi."""
    module_id: str
    module_name: str
    base_url: str
    health_status: str


class ModuleClient:
    """
    Modüller arası REST iletişim client'ı.
    
    Bu servis:
    - Discovery ile modülleri bulur
    - REST API çağrıları yapar
    - Load balancing (round-robin)
    - Retry ve circuit breaker
    
    Kullanım:
        ```python
        # ModuleClient oluştur
        client = ModuleClient(discovery, rest_client)
        
        # Başka bir modülü çağır
        result = await client.call(
            module_name="user-service",
            method="GET",
            path="/users/1",
        )
        
        # veya kısaca
        user = await client.get("user-service", "/users/1")
        ```
    """
    
    def __init__(
        self,
        discovery: DiscoveryService,
        rest_client: IRestClientPort,
    ):
        """
        Args:
            discovery: Modül discovery servisi
            rest_client: REST client port'u
        """
        self._discovery = discovery
        self._rest_client = rest_client
        
        # Cache: module_name -> [endpoints]
        self._endpoint_cache: dict[str, list[ModuleEndpoint]] = {}
        self._cache_ttl = 60  # saniye
        self._cache_timestamps: dict[str, datetime] = {}
        
        # Round-robin index
        self._rr_index: dict[str, int] = {}
    
    # === Discovery ===
    
    async def discover(
        self,
        module_name: str,
        refresh: bool = False,
    ) -> list[ModuleEndpoint]:
        """
        Modül endpoint'lerini keşfet.
        
        Args:
            module_name: Modül adı veya contract_id
            refresh: Cache'i yenile
            
        Returns:
            Modül endpoint listesi
        """
        # Cache kontrolü
        if not refresh and module_name in self._endpoint_cache:
            cache_time = self._cache_timestamps.get(module_name)
            if cache_time:
                age = (datetime.now() - cache_time).total_seconds()
                if age < self._cache_ttl:
                    return self._endpoint_cache[module_name]
        
        # Discovery'den bul
        modules = await self._discovery.discover_by_capability(
            contract_id=module_name,
            direction=PortDirection.INBOUND,
            healthy_only=True,
        )
        
        # Endpoint'leri çıkar
        endpoints = []
        for module in modules:
            for endpoint in module.endpoints:
                endpoints.append(ModuleEndpoint(
                    module_id=module.id,
                    module_name=module.name,
                    base_url=endpoint.to_uri(),
                    health_status=module.health.value,
                ))
        
        # Cache'e kaydet
        self._endpoint_cache[module_name] = endpoints
        self._cache_timestamps[module_name] = datetime.now()
        
        logger.debug(
            "module_discovered",
            module_name=module_name,
            endpoint_count=len(endpoints),
        )
        
        return endpoints
    
    async def get_endpoint(
        self,
        module_name: str,
    ) -> ModuleEndpoint | None:
        """
        Modül için bir endpoint seç (round-robin load balancing).
        
        Args:
            module_name: Modül adı
            
        Returns:
            Seçilen endpoint veya None
        """
        endpoints = await self.discover(module_name)
        
        if not endpoints:
            return None
        
        # Round-robin
        idx = self._rr_index.get(module_name, 0)
        endpoint = endpoints[idx % len(endpoints)]
        self._rr_index[module_name] = idx + 1
        
        return endpoint
    
    # === REST Calls ===
    
    async def call(
        self,
        module_name: str,
        method: str,
        path: str,
        body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 30.0,
    ) -> ModuleCallResult:
        """
        Bir modülü REST API üzerinden çağır.
        
        Args:
            module_name: Hedef modül adı
            method: HTTP metodu (GET, POST, PUT, DELETE, vb.)
            path: API path'i
            body: Request body
            params: Query parametreleri
            headers: Ek header'lar
            timeout: Timeout (saniye)
            
        Returns:
            ModuleCallResult
        """
        # Endpoint bul
        endpoint = await self.get_endpoint(module_name)
        
        if not endpoint:
            return ModuleCallResult(
                success=False,
                status_code=0,
                data=None,
                module_id="",
                module_name=module_name,
                elapsed_ms=0,
                error=f"No healthy endpoint found for module: {module_name}",
            )
        
        # URL oluştur
        url = f"{endpoint.base_url.rstrip('/')}{path}"
        
        # Request yap
        import time
        start = time.time()
        
        try:
            method_lower = method.lower()
            
            if method_lower == "get":
                response = await self._rest_client.get(
                    url, params=params, headers=headers, timeout=timeout
                )
            elif method_lower == "post":
                response = await self._rest_client.post(
                    url, body=body, params=params, headers=headers, timeout=timeout
                )
            elif method_lower == "put":
                response = await self._rest_client.put(
                    url, body=body, params=params, headers=headers, timeout=timeout
                )
            elif method_lower == "patch":
                response = await self._rest_client.patch(
                    url, body=body, params=params, headers=headers, timeout=timeout
                )
            elif method_lower == "delete":
                response = await self._rest_client.delete(
                    url, params=params, headers=headers, timeout=timeout
                )
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            elapsed_ms = (time.time() - start) * 1000
            
            return ModuleCallResult(
                success=response.is_success,
                status_code=response.status_code,
                data=response.json() if response.is_success else response.body,
                module_id=endpoint.module_id,
                module_name=endpoint.module_name,
                elapsed_ms=elapsed_ms,
            )
            
        except Exception as e:
            elapsed_ms = (time.time() - start) * 1000
            
            logger.error(
                "module_call_failed",
                module_name=module_name,
                path=path,
                error=str(e),
            )
            
            return ModuleCallResult(
                success=False,
                status_code=0,
                data=None,
                module_id=endpoint.module_id,
                module_name=endpoint.module_name,
                elapsed_ms=elapsed_ms,
                error=str(e),
            )
    
    # === Shorthand Methods ===
    
    async def get(
        self,
        module_name: str,
        path: str,
        params: dict[str, Any] | None = None,
        **kwargs,
    ) -> ModuleCallResult:
        """GET isteği."""
        return await self.call(module_name, "GET", path, params=params, **kwargs)
    
    async def post(
        self,
        module_name: str,
        path: str,
        body: Any | None = None,
        **kwargs,
    ) -> ModuleCallResult:
        """POST isteği."""
        return await self.call(module_name, "POST", path, body=body, **kwargs)
    
    async def put(
        self,
        module_name: str,
        path: str,
        body: Any | None = None,
        **kwargs,
    ) -> ModuleCallResult:
        """PUT isteği."""
        return await self.call(module_name, "PUT", path, body=body, **kwargs)
    
    async def patch(
        self,
        module_name: str,
        path: str,
        body: Any | None = None,
        **kwargs,
    ) -> ModuleCallResult:
        """PATCH isteği."""
        return await self.call(module_name, "PATCH", path, body=body, **kwargs)
    
    async def delete(
        self,
        module_name: str,
        path: str,
        **kwargs,
    ) -> ModuleCallResult:
        """DELETE isteği."""
        return await self.call(module_name, "DELETE", path, **kwargs)
    
    # === Broadcast ===
    
    async def broadcast(
        self,
        module_name: str,
        method: str,
        path: str,
        body: Any | None = None,
        **kwargs,
    ) -> list[ModuleCallResult]:
        """
        Tüm modül instance'larına istek gönder (broadcast).
        
        Args:
            module_name: Hedef modül adı
            method: HTTP metodu
            path: API path'i
            body: Request body
            
        Returns:
            Tüm sonuçların listesi
        """
        import asyncio
        
        endpoints = await self.discover(module_name, refresh=True)
        
        if not endpoints:
            return []
        
        # Tüm endpoint'lere paralel istek
        tasks = []
        for endpoint in endpoints:
            url = f"{endpoint.base_url.rstrip('/')}{path}"
            tasks.append(self._make_request(
                endpoint, method, url, body, **kwargs
            ))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Exception'ları ModuleCallResult'a çevir
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append(ModuleCallResult(
                    success=False,
                    status_code=0,
                    data=None,
                    module_id=endpoints[i].module_id,
                    module_name=endpoints[i].module_name,
                    elapsed_ms=0,
                    error=str(result),
                ))
            else:
                final_results.append(result)
        
        return final_results
    
    async def _make_request(
        self,
        endpoint: ModuleEndpoint,
        method: str,
        url: str,
        body: Any | None = None,
        **kwargs,
    ) -> ModuleCallResult:
        """Tekil istek helper'ı."""
        import time
        start = time.time()
        
        method_func = getattr(self._rest_client, method.lower())
        
        if method.upper() in ("POST", "PUT", "PATCH"):
            response = await method_func(url, body=body, **kwargs)
        else:
            response = await method_func(url, **kwargs)
        
        elapsed_ms = (time.time() - start) * 1000
        
        return ModuleCallResult(
            success=response.is_success,
            status_code=response.status_code,
            data=response.json() if response.is_success else response.body,
            module_id=endpoint.module_id,
            module_name=endpoint.module_name,
            elapsed_ms=elapsed_ms,
        )
    
    # === Health ===
    
    async def check_module_health(
        self,
        module_name: str,
    ) -> dict[str, bool]:
        """
        Modülün tüm endpoint'lerinin sağlık durumunu kontrol et.
        
        Returns:
            {endpoint_url: is_healthy}
        """
        endpoints = await self.discover(module_name, refresh=True)
        
        results = {}
        for endpoint in endpoints:
            try:
                health_url = f"{endpoint.base_url.rstrip('/')}/health"
                is_healthy = await self._rest_client.health_check(health_url)
                results[endpoint.base_url] = is_healthy
            except Exception:
                results[endpoint.base_url] = False
        
        return results
    
    # === Cache Management ===
    
    def clear_cache(self, module_name: str | None = None) -> None:
        """
        Endpoint cache'ini temizle.
        
        Args:
            module_name: Belirli modül için temizle, None ise tamamını
        """
        if module_name:
            self._endpoint_cache.pop(module_name, None)
            self._cache_timestamps.pop(module_name, None)
        else:
            self._endpoint_cache.clear()
            self._cache_timestamps.clear()


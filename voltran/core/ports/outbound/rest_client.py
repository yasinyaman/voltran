"""
REST Client Port - Outbound port for REST API communication.

Bu port, her app/modülün dış REST API'lerle iletişim kurmasını sağlar.
Voltran'dan gelen merkezi REST client altyapısıdır.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections.abc import AsyncIterator
from typing import Any


class HttpMethod(Enum):
    """HTTP metodları."""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"


@dataclass
class HttpRequest:
    """HTTP istek modeli."""
    method: HttpMethod
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    body: Any | None = None
    timeout: float = 30.0
    
    # Request metadata
    request_id: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class HttpResponse:
    """HTTP yanıt modeli."""
    status_code: int
    headers: dict[str, str]
    body: Any
    
    # Response metadata
    request_id: str = ""
    elapsed_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def is_success(self) -> bool:
        """2xx status code kontrolü."""
        return 200 <= self.status_code < 300
    
    @property
    def is_client_error(self) -> bool:
        """4xx status code kontrolü."""
        return 400 <= self.status_code < 500
    
    @property
    def is_server_error(self) -> bool:
        """5xx status code kontrolü."""
        return 500 <= self.status_code < 600
    
    def json(self) -> Any:
        """Body'yi JSON olarak döndür."""
        if isinstance(self.body, dict):
            return self.body
        if isinstance(self.body, str):
            import json
            return json.loads(self.body)
        return self.body


@dataclass
class RestClientConfig:
    """REST client konfigürasyonu."""
    base_url: str = ""
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # Headers
    default_headers: dict[str, str] = field(default_factory=dict)
    
    # Auth
    auth_type: str | None = None  # "basic", "bearer", "api_key"
    auth_credentials: dict[str, str] = field(default_factory=dict)
    
    # SSL
    verify_ssl: bool = True
    ca_bundle: str | None = None
    
    # Proxy
    proxy_url: str | None = None


@dataclass
class RestClientError:
    """REST client hatası."""
    code: str
    message: str
    status_code: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


class IRestClientPort(ABC):
    """
    REST Client Port Interface.
    
    Bu interface, her app/modülün dış REST API'lerle iletişim kurmasını sağlar.
    Voltran'dan gelen merkezi REST client altyapısıdır.
    
    Hexagonal architecture'da "driven port" (secondary port) olarak görev yapar.
    
    Kullanım:
        ```python
        class MyApp:
            def __init__(self, rest_client: IRestClientPort):
                self.rest = rest_client
            
            async def fetch_users(self):
                response = await self.rest.get("/api/users")
                return response.json()
        ```
    """
    
    # === Lifecycle ===
    
    @abstractmethod
    async def configure(self, config: RestClientConfig) -> None:
        """
        Client'ı yapılandır.
        
        Args:
            config: REST client konfigürasyonu
        """
        ...
    
    @abstractmethod
    async def close(self) -> None:
        """Client bağlantılarını kapat."""
        ...
    
    @abstractmethod
    async def is_ready(self) -> bool:
        """Client hazır mı kontrol et."""
        ...
    
    # === HTTP Methods ===
    
    @abstractmethod
    async def get(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """
        HTTP GET isteği.
        
        Args:
            url: İstek URL'i (base_url'e göre relative veya absolute)
            params: Query parametreleri
            headers: Ek header'lar
            timeout: İstek timeout'u (saniye)
            
        Returns:
            HttpResponse
        """
        ...
    
    @abstractmethod
    async def post(
        self,
        url: str,
        body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """
        HTTP POST isteği.
        
        Args:
            url: İstek URL'i
            body: Request body (JSON serializable)
            params: Query parametreleri
            headers: Ek header'lar
            timeout: İstek timeout'u
            
        Returns:
            HttpResponse
        """
        ...
    
    @abstractmethod
    async def put(
        self,
        url: str,
        body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """HTTP PUT isteği."""
        ...
    
    @abstractmethod
    async def patch(
        self,
        url: str,
        body: Any | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """HTTP PATCH isteği."""
        ...
    
    @abstractmethod
    async def delete(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> HttpResponse:
        """HTTP DELETE isteği."""
        ...
    
    @abstractmethod
    async def request(self, request: HttpRequest) -> HttpResponse:
        """
        Genel HTTP isteği.
        
        Args:
            request: HttpRequest objesi
            
        Returns:
            HttpResponse
        """
        ...
    
    # === Streaming ===
    
    @abstractmethod
    async def stream(
        self,
        url: str,
        method: HttpMethod = HttpMethod.GET,
        body: Any | None = None,
        headers: dict[str, str] | None = None,
    ) -> AsyncIterator[bytes]:
        """
        Streaming HTTP isteği.
        
        Args:
            url: İstek URL'i
            method: HTTP metodu
            body: Request body
            headers: Ek header'lar
            
        Yields:
            Response chunk'ları
        """
        ...
    
    # === Batch Operations ===
    
    @abstractmethod
    async def batch(
        self,
        requests: list[HttpRequest],
        concurrent: bool = True,
    ) -> list[HttpResponse]:
        """
        Toplu HTTP istekleri.
        
        Args:
            requests: İstek listesi
            concurrent: Paralel mi çalışsın
            
        Returns:
            Yanıt listesi
        """
        ...
    
    # === Health Check ===
    
    @abstractmethod
    async def health_check(self, url: str | None = None) -> bool:
        """
        Hedef servisin sağlık durumunu kontrol et.
        
        Args:
            url: Health check URL'i (default: base_url + /health)
            
        Returns:
            Servis erişilebilir mi
        """
        ...
    
    # === Interceptors ===
    
    @abstractmethod
    def add_request_interceptor(
        self,
        interceptor: "RequestInterceptor",
    ) -> None:
        """İstek interceptor'ı ekle."""
        ...
    
    @abstractmethod
    def add_response_interceptor(
        self,
        interceptor: "ResponseInterceptor",
    ) -> None:
        """Yanıt interceptor'ı ekle."""
        ...
    
    # === Metrics ===
    
    @abstractmethod
    async def get_metrics(self) -> dict[str, Any]:
        """
        Client metriklerini döndür.
        
        Returns:
            {
                "total_requests": int,
                "successful_requests": int,
                "failed_requests": int,
                "avg_response_time_ms": float,
                "active_connections": int,
            }
        """
        ...


# === Interceptor Types ===

class RequestInterceptor(ABC):
    """İstek interceptor interface'i."""
    
    @abstractmethod
    async def intercept(self, request: HttpRequest) -> HttpRequest:
        """
        İsteği işle ve (değiştirilmiş) isteği döndür.
        
        Args:
            request: Gelen istek
            
        Returns:
            İşlenmiş istek
        """
        ...


class ResponseInterceptor(ABC):
    """Yanıt interceptor interface'i."""
    
    @abstractmethod
    async def intercept(
        self,
        request: HttpRequest,
        response: HttpResponse,
    ) -> HttpResponse:
        """
        Yanıtı işle ve (değiştirilmiş) yanıtı döndür.
        
        Args:
            request: Orijinal istek
            response: Gelen yanıt
            
        Returns:
            İşlenmiş yanıt
        """
        ...


# === Built-in Interceptors ===

class AuthInterceptor(RequestInterceptor):
    """
    Authentication interceptor.
    
    İsteklere auth header'ı ekler.
    """
    
    def __init__(
        self,
        auth_type: str,
        credentials: dict[str, str],
    ):
        """
        Args:
            auth_type: "basic", "bearer", "api_key"
            credentials: Auth bilgileri
        """
        self.auth_type = auth_type
        self.credentials = credentials
    
    async def intercept(self, request: HttpRequest) -> HttpRequest:
        """Auth header'ı ekle."""
        if self.auth_type == "bearer":
            token = self.credentials.get("token", "")
            request.headers["Authorization"] = f"Bearer {token}"
        
        elif self.auth_type == "basic":
            import base64
            username = self.credentials.get("username", "")
            password = self.credentials.get("password", "")
            encoded = base64.b64encode(
                f"{username}:{password}".encode()
            ).decode()
            request.headers["Authorization"] = f"Basic {encoded}"
        
        elif self.auth_type == "api_key":
            key_name = self.credentials.get("key_name", "X-API-Key")
            key_value = self.credentials.get("key_value", "")
            request.headers[key_name] = key_value
        
        return request


class LoggingRequestInterceptor(RequestInterceptor):
    """
    Request logging interceptor.
    
    İstekleri loglar.
    """
    
    def __init__(self, logger: Any = None):
        self.logger = logger
    
    async def intercept(self, request: HttpRequest) -> HttpRequest:
        """İsteği logla."""
        if self.logger:
            self.logger.debug(
                "rest_request",
                method=request.method.value,
                url=request.url,
                request_id=request.request_id,
            )
        return request


class LoggingResponseInterceptor(ResponseInterceptor):
    """
    Response logging interceptor.
    
    Yanıtları loglar.
    """
    
    def __init__(self, logger: Any = None):
        self.logger = logger
    
    async def intercept(
        self,
        request: HttpRequest,
        response: HttpResponse,
    ) -> HttpResponse:
        """Yanıtı logla."""
        if self.logger:
            self.logger.debug(
                "rest_response",
                method=request.method.value,
                url=request.url,
                status_code=response.status_code,
                elapsed_ms=response.elapsed_ms,
                request_id=request.request_id,
            )
        return response


class RetryInterceptor(ResponseInterceptor):
    """
    Retry interceptor.
    
    Başarısız istekleri yeniden dener.
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        retry_statuses: list[int] = None,
        retry_delay: float = 1.0,
    ):
        self.max_retries = max_retries
        self.retry_statuses = retry_statuses or [500, 502, 503, 504]
        self.retry_delay = retry_delay
        self._retry_counts: dict[str, int] = {}
    
    async def intercept(
        self,
        request: HttpRequest,
        response: HttpResponse,
    ) -> HttpResponse:
        """Retry kontrolü yap."""
        # Bu interceptor client tarafından özel olarak handle edilir
        # Burada sadece response'u pass-through yaparız
        return response


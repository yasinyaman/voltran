"""Auto-discovery service using mDNS/Bonjour (zeroconf)."""

import asyncio
import socket
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional
from uuid import uuid4

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class DiscoveredService:
    """Discovered Voltran service on the network."""
    
    name: str
    voltran_id: str
    host: str
    port: int
    service_type: str
    properties: dict[str, Any] = field(default_factory=dict)
    discovered_at: datetime = field(default_factory=datetime.now)
    
    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "voltran_id": self.voltran_id,
            "host": self.host,
            "port": self.port,
            "service_type": self.service_type,
            "properties": self.properties,
        }


ServiceCallback = Callable[[DiscoveredService], None]


class AutoDiscoveryService:
    """
    Zero-configuration service discovery using mDNS/DNS-SD (Bonjour).
    
    This service allows Voltran nodes to:
    - Automatically announce themselves on the local network
    - Discover other Voltran nodes without manual configuration
    - Receive notifications when new nodes appear/disappear
    
    Uses the zeroconf library for mDNS/DNS-SD implementation.
    """
    
    # Default service type for Voltran
    SERVICE_TYPE = "_voltran._tcp.local."
    
    def __init__(
        self,
        voltran_id: str,
        name: str,
        port: int,
        host: Optional[str] = None,
        service_type: str = SERVICE_TYPE,
        properties: Optional[dict[str, Any]] = None,
    ):
        """
        Initialize auto-discovery service.
        
        Args:
            voltran_id: Unique ID of this Voltran node
            name: Human-readable name for this node
            port: Port this node is listening on
            host: Host/IP address (auto-detected if not provided)
            service_type: mDNS service type
            properties: Additional properties to advertise
        """
        self._voltran_id = voltran_id
        self._name = name
        self._port = port
        self._host = host or self._get_local_ip()
        self._service_type = service_type
        self._properties = properties or {}
        
        # Zeroconf instances
        self._zeroconf: Any = None
        self._service_info: Any = None
        self._browser: Any = None
        
        # Discovered services
        self._discovered: dict[str, DiscoveredService] = {}
        
        # Callbacks
        self._on_service_added: list[ServiceCallback] = []
        self._on_service_removed: list[ServiceCallback] = []
        
        # State
        self._running = False

    @property
    def voltran_id(self) -> str:
        return self._voltran_id

    @property
    def discovered_services(self) -> dict[str, DiscoveredService]:
        return self._discovered.copy()

    def on_service_added(self, callback: ServiceCallback) -> None:
        """Register callback for when a service is discovered."""
        self._on_service_added.append(callback)

    def on_service_removed(self, callback: ServiceCallback) -> None:
        """Register callback for when a service disappears."""
        self._on_service_removed.append(callback)

    async def start(self) -> None:
        """Start the auto-discovery service."""
        if self._running:
            return

        try:
            from zeroconf import IPVersion, ServiceInfo, Zeroconf
            from zeroconf.asyncio import AsyncServiceBrowser, AsyncZeroconf
            
            logger.info(
                "auto_discovery_starting",
                name=self._name,
                host=self._host,
                port=self._port,
            )

            # Create async zeroconf instance
            self._zeroconf = AsyncZeroconf(ip_version=IPVersion.V4Only)
            
            # Prepare properties
            properties = {
                b"voltran_id": self._voltran_id.encode(),
                b"name": self._name.encode(),
                b"version": b"0.1.0",
                **{k.encode(): str(v).encode() for k, v in self._properties.items()},
            }
            
            # Create service info
            self._service_info = ServiceInfo(
                self._service_type,
                f"{self._name}.{self._service_type}",
                addresses=[socket.inet_aton(self._host)],
                port=self._port,
                properties=properties,
                server=f"{self._name}.local.",
            )
            
            # Register service
            await self._zeroconf.async_register_service(self._service_info)
            
            # Start browsing for other services
            self._browser = AsyncServiceBrowser(
                self._zeroconf.zeroconf,
                self._service_type,
                handlers=[self._on_service_state_change],
            )
            
            self._running = True
            
            logger.info(
                "auto_discovery_started",
                service_name=f"{self._name}.{self._service_type}",
            )

        except ImportError:
            logger.warning(
                "zeroconf_not_installed",
                message="Install zeroconf for auto-discovery: pip install zeroconf",
            )
            # Continue without auto-discovery
            self._running = True
        except Exception as e:
            logger.error("auto_discovery_start_failed", error=str(e))
            raise

    async def stop(self) -> None:
        """Stop the auto-discovery service."""
        if not self._running:
            return

        try:
            if self._browser:
                self._browser.cancel()
                self._browser = None

            if self._zeroconf and self._service_info:
                await self._zeroconf.async_unregister_service(self._service_info)
                await self._zeroconf.async_close()

            self._zeroconf = None
            self._service_info = None
            self._running = False
            
            logger.info("auto_discovery_stopped")

        except Exception as e:
            logger.error("auto_discovery_stop_error", error=str(e))

    async def discover_once(self, timeout: float = 3.0) -> list[DiscoveredService]:
        """
        Perform a one-time discovery scan.
        
        Args:
            timeout: How long to wait for responses
            
        Returns:
            List of discovered services
        """
        if not self._running:
            await self.start()
        
        # Wait for discovery
        await asyncio.sleep(timeout)
        
        return list(self._discovered.values())

    def _on_service_state_change(
        self,
        zeroconf: Any,
        service_type: str,
        name: str,
        state_change: Any,
    ) -> None:
        """Handle service state changes (sync callback from zeroconf)."""
        from zeroconf import ServiceStateChange
        
        asyncio.create_task(
            self._handle_service_change(zeroconf, service_type, name, state_change)
        )

    async def _handle_service_change(
        self,
        zeroconf: Any,
        service_type: str,
        name: str,
        state_change: Any,
    ) -> None:
        """Handle service state changes asynchronously."""
        from zeroconf import ServiceStateChange
        
        try:
            if state_change == ServiceStateChange.Added:
                await self._handle_service_added(zeroconf, service_type, name)
            elif state_change == ServiceStateChange.Removed:
                await self._handle_service_removed(name)
            elif state_change == ServiceStateChange.Updated:
                await self._handle_service_added(zeroconf, service_type, name)
                
        except Exception as e:
            logger.error(
                "service_change_error",
                name=name,
                error=str(e),
            )

    async def _handle_service_added(
        self,
        zeroconf: Any,
        service_type: str,
        name: str,
    ) -> None:
        """Handle new service discovery."""
        from zeroconf import ServiceInfo
        
        info = ServiceInfo(service_type, name)
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: info.request(zeroconf, 3000),
        )
        
        if not info.addresses:
            return
        
        # Parse properties
        properties = {}
        voltran_id = ""
        service_name = name.replace(f".{service_type}", "")
        
        if info.properties:
            for key, value in info.properties.items():
                key_str = key.decode() if isinstance(key, bytes) else key
                value_str = value.decode() if isinstance(value, bytes) else str(value)
                
                if key_str == "voltran_id":
                    voltran_id = value_str
                else:
                    properties[key_str] = value_str
        
        # Skip self
        if voltran_id == self._voltran_id:
            return
        
        # Create discovered service
        host = socket.inet_ntoa(info.addresses[0])
        service = DiscoveredService(
            name=properties.get("name", service_name),
            voltran_id=voltran_id or str(uuid4()),
            host=host,
            port=info.port or 0,
            service_type=service_type,
            properties=properties,
        )
        
        # Store and notify
        is_new = voltran_id not in self._discovered
        self._discovered[voltran_id] = service
        
        if is_new:
            logger.info(
                "service_discovered",
                name=service.name,
                voltran_id=voltran_id[:8] + "...",
                host=host,
                port=info.port,
            )
            
            for callback in self._on_service_added:
                try:
                    callback(service)
                except Exception as e:
                    logger.error("callback_error", error=str(e))

    async def _handle_service_removed(self, name: str) -> None:
        """Handle service removal."""
        # Find by name
        removed_id = None
        removed_service = None
        
        for vid, service in self._discovered.items():
            if name.startswith(service.name):
                removed_id = vid
                removed_service = service
                break
        
        if removed_id and removed_service:
            del self._discovered[removed_id]
            
            logger.info(
                "service_removed",
                name=removed_service.name,
                voltran_id=removed_id[:8] + "...",
            )
            
            for callback in self._on_service_removed:
                try:
                    callback(removed_service)
                except Exception as e:
                    logger.error("callback_error", error=str(e))

    def _get_local_ip(self) -> str:
        """Get local IP address."""
        try:
            # Create a socket to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "127.0.0.1"


class BroadcastDiscovery:
    """
    Simple UDP broadcast-based discovery (fallback when zeroconf unavailable).
    
    Uses UDP broadcast to discover services on the local network.
    """
    
    BROADCAST_PORT = 5354
    MAGIC = b"VOLTRAN"
    
    def __init__(
        self,
        voltran_id: str,
        name: str,
        port: int,
        broadcast_port: int = BROADCAST_PORT,
    ):
        self._voltran_id = voltran_id
        self._name = name
        self._port = port
        self._broadcast_port = broadcast_port
        
        self._discovered: dict[str, DiscoveredService] = {}
        self._running = False
        self._socket: Optional[socket.socket] = None
        self._broadcast_task: Optional[asyncio.Task[None]] = None
        self._listen_task: Optional[asyncio.Task[None]] = None

    async def start(self) -> None:
        """Start broadcast discovery."""
        if self._running:
            return

        self._running = True
        
        # Create UDP socket
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._socket.setblocking(False)
        
        try:
            self._socket.bind(("", self._broadcast_port))
        except OSError:
            # Port in use, try another
            self._socket.bind(("", 0))
        
        # Start tasks
        self._broadcast_task = asyncio.create_task(self._broadcast_loop())
        self._listen_task = asyncio.create_task(self._listen_loop())
        
        logger.info(
            "broadcast_discovery_started",
            port=self._broadcast_port,
        )

    async def stop(self) -> None:
        """Stop broadcast discovery."""
        self._running = False
        
        if self._broadcast_task:
            self._broadcast_task.cancel()
        if self._listen_task:
            self._listen_task.cancel()
        
        if self._socket:
            self._socket.close()
        
        logger.info("broadcast_discovery_stopped")

    async def _broadcast_loop(self) -> None:
        """Periodically broadcast presence."""
        import json
        
        while self._running:
            try:
                message = {
                    "magic": self.MAGIC.decode(),
                    "voltran_id": self._voltran_id,
                    "name": self._name,
                    "port": self._port,
                }
                data = json.dumps(message).encode()
                
                if self._socket:
                    self._socket.sendto(
                        data,
                        ("<broadcast>", self._broadcast_port),
                    )
                
                await asyncio.sleep(5)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("broadcast_error", error=str(e))
                await asyncio.sleep(5)

    async def _listen_loop(self) -> None:
        """Listen for broadcasts from other nodes."""
        import json
        
        loop = asyncio.get_event_loop()
        
        while self._running:
            try:
                if not self._socket:
                    break
                
                data, addr = await loop.sock_recvfrom(self._socket, 1024)
                message = json.loads(data.decode())
                
                if message.get("magic") != self.MAGIC.decode():
                    continue
                
                voltran_id = message.get("voltran_id", "")
                
                # Skip self
                if voltran_id == self._voltran_id:
                    continue
                
                # Create discovered service
                service = DiscoveredService(
                    name=message.get("name", "unknown"),
                    voltran_id=voltran_id,
                    host=addr[0],
                    port=message.get("port", 0),
                    service_type="broadcast",
                )
                
                if voltran_id not in self._discovered:
                    logger.info(
                        "peer_discovered_broadcast",
                        name=service.name,
                        host=service.host,
                    )
                
                self._discovered[voltran_id] = service
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.error("listen_error", error=str(e))
                await asyncio.sleep(1)

    @property
    def discovered_services(self) -> dict[str, DiscoveredService]:
        return self._discovered.copy()


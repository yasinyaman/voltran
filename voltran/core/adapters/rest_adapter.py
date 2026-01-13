"""REST API messaging adapter implementation."""

import asyncio
import json
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

import structlog

from voltran.core.ports.outbound.messaging import IMessagingPort, Message, MessageHandler

logger = structlog.get_logger(__name__)


class RestMessagingAdapter(IMessagingPort):
    """
    REST API-based messaging adapter.
    
    This adapter uses HTTP/REST for communication between
    Voltran nodes. It provides a simple, widely-compatible
    protocol for module communication.
    
    Features:
    - HTTP endpoints for each module
    - JSON payload serialization
    - Request-response pattern via POST
    - Webhook-based subscriptions
    """

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8080,
        voltran_id: str = "",
        base_path: str = "/api/v1",
    ):
        """
        Initialize REST adapter.
        
        Args:
            host: HTTP server host
            port: HTTP server port
            voltran_id: ID of this Voltran node
            base_path: Base path for API endpoints
        """
        self._host = host
        self._port = port
        self._voltran_id = voltran_id or str(uuid4())
        self._base_path = base_path
        self._connected = False
        
        # HTTP client session
        self._session: Any = None  # aiohttp.ClientSession
        
        # HTTP server app
        self._app: Any = None  # aiohttp.web.Application
        self._runner: Any = None
        self._site: Any = None
        
        # Subscription handlers
        self._subscriptions: dict[str, MessageHandler] = {}
        self._topic_handlers: dict[str, list[MessageHandler]] = {}
        
        # Pending requests
        self._pending_requests: dict[str, asyncio.Future[Any]] = {}
        self._pending_responses: dict[str, Any] = {}
        
        # Known peers (voltran_id -> base_url)
        self._peers: dict[str, str] = {}

    @property
    def voltran_id(self) -> str:
        """Get Voltran node ID."""
        return self._voltran_id

    @property
    def base_url(self) -> str:
        """Get base URL for this adapter."""
        return f"http://{self._host}:{self._port}{self._base_path}"

    def register_peer(self, voltran_id: str, base_url: str) -> None:
        """
        Register a peer Voltran's REST endpoint.
        
        Args:
            voltran_id: ID of peer Voltran
            base_url: Base URL of peer's REST API
        """
        self._peers[voltran_id] = base_url
        logger.info("peer_registered", voltran_id=voltran_id, base_url=base_url)

    async def connect(self) -> None:
        """Start HTTP server and client."""
        if self._connected:
            return

        try:
            import aiohttp
            from aiohttp import web
            
            logger.info(
                "rest_connecting",
                host=self._host,
                port=self._port,
            )

            # Create HTTP client session
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                json_serialize=lambda x: json.dumps(x, default=str),
            )

            # Create HTTP server
            self._app = web.Application()
            self._setup_routes()

            # Start server
            self._runner = web.AppRunner(self._app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()

            self._connected = True

            logger.info(
                "rest_connected",
                host=self._host,
                port=self._port,
                base_url=self.base_url,
            )

        except ImportError:
            logger.warning("aiohttp_not_installed", message="aiohttp not installed")
            # Fallback to mock mode
            self._connected = True
        except Exception as e:
            logger.error("rest_connection_failed", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Stop HTTP server and close client."""
        if not self._connected:
            return

        # Close client session
        if self._session:
            await self._session.close()
            self._session = None

        # Stop server
        if self._site:
            await self._site.stop()
        if self._runner:
            await self._runner.cleanup()

        self._app = None
        self._runner = None
        self._site = None
        self._connected = False

        logger.info("rest_disconnected")

    async def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    async def publish(self, topic: str, payload: Any) -> None:
        """Publish message to topic (broadcast to all subscribers)."""
        if not self._connected:
            raise ConnectionError("Not connected")

        message = Message(
            topic=topic,
            payload=payload,
            source_voltran_id=self._voltran_id,
            timestamp=datetime.now(),
        )

        # Deliver to local handlers
        await self._deliver_to_handlers(topic, message)

        # Broadcast to peers (webhook style)
        for peer_id, peer_url in self._peers.items():
            try:
                await self._post_to_peer(peer_url, "/webhook", message)
            except Exception as e:
                logger.warning(
                    "peer_publish_failed",
                    peer_id=peer_id,
                    error=str(e),
                )

        logger.debug("rest_message_published", topic=topic)

    async def subscribe(self, topic: str, handler: MessageHandler) -> str:
        """Subscribe to topic."""
        subscription_id = f"sub-{uuid4()}"
        
        # Store handler
        self._subscriptions[subscription_id] = handler
        
        # Add to topic handlers
        if topic not in self._topic_handlers:
            self._topic_handlers[topic] = []
        self._topic_handlers[topic].append(handler)

        logger.info(
            "rest_subscribed",
            topic=topic,
            subscription_id=subscription_id,
        )

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from topic."""
        if subscription_id not in self._subscriptions:
            return False

        handler = self._subscriptions.pop(subscription_id)
        
        # Remove from all topic handlers
        for handlers in self._topic_handlers.values():
            if handler in handlers:
                handlers.remove(handler)

        logger.info("rest_unsubscribed", subscription_id=subscription_id)
        return True

    async def request(
        self,
        target_voltran_id: str,
        payload: Any,
        timeout: float = 30.0,
    ) -> Any:
        """Send HTTP request to target Voltran and get response."""
        if not self._connected:
            raise ConnectionError("Not connected")

        # Get peer URL
        peer_url = self._peers.get(target_voltran_id)
        if not peer_url:
            raise ValueError(f"Unknown peer: {target_voltran_id}")

        message = Message(
            topic=f"voltran.{target_voltran_id}.request",
            payload=payload,
            source_voltran_id=self._voltran_id,
            timestamp=datetime.now(),
        )

        # Send HTTP POST request
        try:
            response = await self._post_to_peer(
                peer_url,
                "/request",
                message,
                timeout=timeout,
            )
            return response.get("payload")
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Request to {target_voltran_id} timed out after {timeout}s"
            )

    async def respond(self, original_message: Message, response: Any) -> None:
        """Send response to request (handled in request handler)."""
        # For REST, responses are returned directly from the request handler
        # This method is for compatibility with the interface
        if original_message.correlation_id:
            self._pending_responses[original_message.correlation_id] = response

    # === HTTP Server Routes ===

    def _setup_routes(self) -> None:
        """Setup HTTP server routes."""
        from aiohttp import web

        # Health check
        self._app.router.add_get(
            f"{self._base_path}/health",
            self._handle_health,
        )

        # Info endpoint
        self._app.router.add_get(
            f"{self._base_path}/info",
            self._handle_info,
        )

        # Request endpoint (for request-response)
        self._app.router.add_post(
            f"{self._base_path}/request",
            self._handle_request,
        )

        # Webhook endpoint (for pub/sub)
        self._app.router.add_post(
            f"{self._base_path}/webhook",
            self._handle_webhook,
        )

        # Module call endpoint
        self._app.router.add_post(
            f"{self._base_path}/modules/{{module_id}}/call/{{method}}",
            self._handle_module_call,
        )

        # Discovery endpoint
        self._app.router.add_get(
            f"{self._base_path}/modules",
            self._handle_list_modules,
        )

        # Register peer endpoint
        self._app.router.add_post(
            f"{self._base_path}/peers",
            self._handle_register_peer,
        )

    async def _handle_health(self, request: Any) -> Any:
        """Handle health check request."""
        from aiohttp import web
        
        return web.json_response({
            "status": "healthy",
            "voltran_id": self._voltran_id,
            "timestamp": datetime.now().isoformat(),
        })

    async def _handle_info(self, request: Any) -> Any:
        """Handle info request."""
        from aiohttp import web
        
        return web.json_response({
            "voltran_id": self._voltran_id,
            "base_url": self.base_url,
            "peers": list(self._peers.keys()),
            "subscriptions": len(self._subscriptions),
        })

    async def _handle_request(self, request: Any) -> Any:
        """Handle incoming request-response."""
        from aiohttp import web
        
        try:
            data = await request.json()
            message = self._deserialize_message(data)
            
            # Find handler for this request
            topic = message.topic
            response_payload: Any = None
            self._pending_responses.pop(message.correlation_id, None)
            
            # Deliver to handlers and collect response
            if topic in self._topic_handlers:
                for handler in self._topic_handlers[topic]:
                    try:
                        handler_result = await handler(message)
                        if handler_result is not None and response_payload is None:
                            response_payload = handler_result
                        pending = self._pending_responses.pop(
                            message.correlation_id, None
                        )
                        if pending is not None:
                            response_payload = pending
                            break
                    except Exception as e:
                        logger.error("request_handler_error", error=str(e))
            
            # Generic request handling
            if response_payload is None and message.payload.get("action") == "get_modules":
                # Return modules (to be implemented with discovery integration)
                response_payload = {"modules": []}
            
            return web.json_response({
                "success": True,
                "payload": response_payload,
                "correlation_id": message.correlation_id,
            })
            
        except Exception as e:
            logger.error("request_handling_error", error=str(e))
            return web.json_response(
                {"success": False, "error": str(e)},
                status=500,
            )

    async def _handle_webhook(self, request: Any) -> Any:
        """Handle incoming webhook (pub/sub message)."""
        from aiohttp import web
        
        try:
            data = await request.json()
            message = self._deserialize_message(data)
            
            # Deliver to local handlers
            await self._deliver_to_handlers(message.topic, message)
            
            return web.json_response({"success": True})
            
        except Exception as e:
            logger.error("webhook_handling_error", error=str(e))
            return web.json_response(
                {"success": False, "error": str(e)},
                status=500,
            )

    async def _handle_module_call(self, request: Any) -> Any:
        """Handle direct module method call."""
        from aiohttp import web
        
        module_id = request.match_info["module_id"]
        method = request.match_info["method"]
        
        try:
            data = await request.json()
            
            # Create message for handlers
            message = Message(
                topic=f"voltran.module.{module_id}.{method}",
                payload={
                    "action": "call",
                    "module_id": module_id,
                    "method": method,
                    "args": data.get("args", []),
                    "kwargs": data.get("kwargs", {}),
                },
                source_voltran_id=request.headers.get("X-Voltran-ID", "unknown"),
            )
            
            # Deliver to handlers
            await self._deliver_to_handlers(message.topic, message)
            
            return web.json_response({
                "success": True,
                "module_id": module_id,
                "method": method,
            })
            
        except Exception as e:
            logger.error("module_call_error", error=str(e))
            return web.json_response(
                {"success": False, "error": str(e)},
                status=500,
            )

    async def _handle_list_modules(self, request: Any) -> Any:
        """Handle list modules request."""
        from aiohttp import web
        
        # This should be integrated with discovery service
        return web.json_response({
            "voltran_id": self._voltran_id,
            "modules": [],  # To be populated by discovery integration
        })

    async def _handle_register_peer(self, request: Any) -> Any:
        """Handle peer registration."""
        from aiohttp import web
        
        try:
            data = await request.json()
            peer_id = data.get("voltran_id")
            peer_url = data.get("base_url")
            
            if peer_id and peer_url:
                self.register_peer(peer_id, peer_url)
                return web.json_response({
                    "success": True,
                    "registered": peer_id,
                })
            else:
                return web.json_response(
                    {"success": False, "error": "Missing voltran_id or base_url"},
                    status=400,
                )
                
        except Exception as e:
            return web.json_response(
                {"success": False, "error": str(e)},
                status=500,
            )

    # === Helper Methods ===

    async def _post_to_peer(
        self,
        peer_url: str,
        path: str,
        message: Message,
        timeout: float = 30.0,
    ) -> dict:
        """Send POST request to peer."""
        if not self._session:
            return {}

        try:
            import aiohttp
            timeout_obj = aiohttp.ClientTimeout(total=timeout)
        except ImportError:
            timeout_obj = None  # type: ignore

        url = f"{peer_url}{path}"
        data = self._serialize_message(message)
        
        kwargs: dict[str, Any] = {
            "json": data,
            "headers": {
                "Content-Type": "application/json",
                "X-Voltran-ID": self._voltran_id,
                "X-Correlation-ID": message.correlation_id,
            },
        }
        if timeout_obj is not None:
            kwargs["timeout"] = timeout_obj
        
        async with self._session.post(url, **kwargs) as response:
            return await response.json()

    async def _deliver_to_handlers(self, topic: str, message: Message) -> None:
        """Deliver message to matching handlers."""
        # Exact match
        if topic in self._topic_handlers:
            for handler in self._topic_handlers[topic]:
                try:
                    await handler(message)
                except Exception as e:
                    logger.error(
                        "handler_error",
                        topic=topic,
                        error=str(e),
                    )

        # Wildcard matching (simple prefix)
        for pattern, handlers in self._topic_handlers.items():
            if pattern.endswith(".*") and topic.startswith(pattern[:-2]):
                for handler in handlers:
                    try:
                        await handler(message)
                    except Exception as e:
                        logger.error(
                            "handler_error",
                            topic=topic,
                            pattern=pattern,
                            error=str(e),
                        )

    def _serialize_message(self, message: Message) -> dict:
        """Serialize message to dictionary."""
        return {
            "topic": message.topic,
            "payload": message.payload,
            "source_voltran_id": message.source_voltran_id,
            "timestamp": message.timestamp.isoformat(),
            "correlation_id": message.correlation_id,
            "reply_to": message.reply_to,
        }

    def _deserialize_message(self, data: dict) -> Message:
        """Deserialize dictionary to message."""
        return Message(
            topic=data["topic"],
            payload=data["payload"],
            source_voltran_id=data["source_voltran_id"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            correlation_id=data.get("correlation_id", ""),
            reply_to=data.get("reply_to"),
        )



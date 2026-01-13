"""NATS messaging adapter implementation."""

import asyncio
import json
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

import structlog

from voltran.core.ports.outbound.messaging import IMessagingPort, Message, MessageHandler

logger = structlog.get_logger(__name__)


class NatsMessagingAdapter(IMessagingPort):
    """
    NATS-based messaging adapter.
    
    This adapter uses NATS for pub/sub communication across
    Voltran federation. It supports topic-based messaging,
    wildcards, and request-response pattern.
    """

    def __init__(
        self,
        servers: list[str] | None = None,
        voltran_id: str = "",
    ):
        """
        Initialize NATS adapter.
        
        Args:
            servers: List of NATS server URLs (e.g., ["nats://localhost:4222"])
            voltran_id: ID of this Voltran node
        """
        self._servers = servers or ["nats://localhost:4222"]
        self._voltran_id = voltran_id or str(uuid4())
        self._nc: Any = None  # nats.Client
        self._connected = False
        
        # Subscription tracking
        self._subscriptions: dict[str, tuple[Any, MessageHandler]] = {}
        self._pending_requests: dict[str, asyncio.Future[Any]] = {}

    @property
    def voltran_id(self) -> str:
        """Get Voltran node ID."""
        return self._voltran_id

    async def connect(self) -> None:
        """Establish NATS connection."""
        if self._connected:
            return

        try:
            import nats
            
            logger.info(
                "nats_connecting",
                servers=self._servers,
            )

            self._nc = await nats.connect(
                servers=self._servers,
                reconnect_time_wait=2,
                max_reconnect_attempts=10,
                error_cb=self._error_callback,
                disconnected_cb=self._disconnected_callback,
                reconnected_cb=self._reconnected_callback,
            )
            self._connected = True

            # Subscribe to response topic
            await self._setup_response_handler()

            logger.info(
                "nats_connected",
                servers=self._servers,
            )

        except ImportError:
            logger.warning("nats_not_installed", message="nats-py not installed, using mock")
            self._connected = True
        except Exception as e:
            logger.error("nats_connection_failed", error=str(e))
            raise

    async def disconnect(self) -> None:
        """Disconnect from NATS."""
        if not self._connected:
            return

        # Unsubscribe all
        for sub_id, (sub, _) in list(self._subscriptions.items()):
            try:
                if sub:
                    await sub.unsubscribe()
            except Exception:
                pass
        self._subscriptions.clear()

        # Drain and close
        if self._nc:
            try:
                await self._nc.drain()
            except Exception:
                pass
            self._nc = None

        self._connected = False
        logger.info("nats_disconnected")

    async def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected and (self._nc is None or self._nc.is_connected)

    async def publish(self, topic: str, payload: Any) -> None:
        """Publish message to topic."""
        if not self._connected:
            raise ConnectionError("Not connected to NATS")

        message = Message(
            topic=topic,
            payload=payload,
            source_voltran_id=self._voltran_id,
            timestamp=datetime.now(),
        )

        data = self._serialize(message)

        if self._nc:
            await self._nc.publish(topic, data)
        
        logger.debug(
            "nats_message_published",
            topic=topic,
            size=len(data),
        )

    async def subscribe(self, topic: str, handler: MessageHandler) -> str:
        """Subscribe to topic with pattern support."""
        if not self._connected:
            raise ConnectionError("Not connected to NATS")

        subscription_id = f"sub-{uuid4()}"

        async def message_handler(msg: Any) -> None:
            try:
                message = self._deserialize(msg.data)
                if msg.reply:
                    message.reply_to = msg.reply
                await handler(message)
            except Exception as e:
                logger.error(
                    "nats_handler_error",
                    error=str(e),
                    topic=topic,
                )

        sub = None
        if self._nc:
            sub = await self._nc.subscribe(topic, cb=message_handler)
        
        self._subscriptions[subscription_id] = (sub, handler)

        logger.info(
            "nats_subscribed",
            topic=topic,
            subscription_id=subscription_id,
        )

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from topic."""
        if subscription_id not in self._subscriptions:
            return False

        sub, _ = self._subscriptions.pop(subscription_id)
        if sub:
            await sub.unsubscribe()

        logger.info("nats_unsubscribed", subscription_id=subscription_id)
        return True

    async def request(
        self,
        target_voltran_id: str,
        payload: Any,
        timeout: float = 30.0,
    ) -> Any:
        """Send request and wait for response using NATS request-reply."""
        if not self._connected:
            raise ConnectionError("Not connected to NATS")

        topic = f"voltran.{target_voltran_id}.request"
        
        message = Message(
            topic=topic,
            payload=payload,
            source_voltran_id=self._voltran_id,
            timestamp=datetime.now(),
            reply_to=f"voltran.{self._voltran_id}.response",
        )

        data = self._serialize(message)

        if self._nc:
            try:
                response = await self._nc.request(topic, data, timeout=timeout)
                return self._deserialize(response.data).payload
            except asyncio.TimeoutError:
                raise TimeoutError(
                    f"Request to {target_voltran_id} timed out after {timeout}s"
                )
        else:
            # Mock response for testing without NATS
            return {"status": "mock_response"}

    async def respond(self, original_message: Message, response: Any) -> None:
        """Send response to request."""
        if not original_message.reply_to:
            logger.warning(
                "nats_no_reply_to",
                correlation_id=original_message.correlation_id,
            )
            return

        response_message = Message(
            topic=original_message.reply_to,
            payload=response,
            source_voltran_id=self._voltran_id,
            correlation_id=original_message.correlation_id,
        )

        await self.publish(original_message.reply_to, response_message.payload)

    async def _setup_response_handler(self) -> None:
        """Setup handler for responses."""
        response_topic = f"voltran.{self._voltran_id}.response"

        async def handle_response(msg: Any) -> None:
            message = self._deserialize(msg.data)
            if message.correlation_id in self._pending_requests:
                future = self._pending_requests.pop(message.correlation_id)
                future.set_result(message.payload)

        if self._nc:
            await self._nc.subscribe(response_topic, cb=handle_response)

    async def _error_callback(self, e: Exception) -> None:
        """Handle NATS errors."""
        logger.error("nats_error", error=str(e))

    async def _disconnected_callback(self) -> None:
        """Handle NATS disconnection."""
        logger.warning("nats_disconnected_event")

    async def _reconnected_callback(self) -> None:
        """Handle NATS reconnection."""
        logger.info("nats_reconnected_event")

    def _serialize(self, message: Message) -> bytes:
        """Serialize message to bytes."""
        data = {
            "topic": message.topic,
            "payload": message.payload,
            "source_voltran_id": message.source_voltran_id,
            "timestamp": message.timestamp.isoformat(),
            "correlation_id": message.correlation_id,
            "reply_to": message.reply_to,
        }
        return json.dumps(data, default=str).encode("utf-8")

    def _deserialize(self, data: bytes) -> Message:
        """Deserialize bytes to message."""
        obj = json.loads(data.decode("utf-8"))
        return Message(
            topic=obj["topic"],
            payload=obj["payload"],
            source_voltran_id=obj["source_voltran_id"],
            timestamp=datetime.fromisoformat(obj["timestamp"]),
            correlation_id=obj.get("correlation_id", ""),
            reply_to=obj.get("reply_to"),
        )

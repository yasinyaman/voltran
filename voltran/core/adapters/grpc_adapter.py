"""gRPC messaging adapter implementation."""

import asyncio
from datetime import datetime
from typing import Any, Optional
from uuid import uuid4

import structlog

from voltran.core.ports.outbound.messaging import IMessagingPort, Message, MessageHandler

logger = structlog.get_logger(__name__)


class GrpcMessagingAdapter(IMessagingPort):
    """
    gRPC-based messaging adapter.
    
    This adapter uses gRPC for point-to-point communication between
    Voltran nodes. It supports request-response pattern and direct
    messaging to specific nodes.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 50051,
        voltran_id: str = "",
    ):
        """
        Initialize gRPC adapter.
        
        Args:
            host: gRPC server host
            port: gRPC server port
            voltran_id: ID of this Voltran node
        """
        self._host = host
        self._port = port
        self._voltran_id = voltran_id or str(uuid4())
        self._connected = False
        
        # In-memory message handling for now
        # In real implementation, this would use grpc channels
        self._subscriptions: dict[str, tuple[str, MessageHandler]] = {}
        self._pending_requests: dict[str, asyncio.Future[Any]] = {}
        self._message_queue: asyncio.Queue[Message] = asyncio.Queue()
        self._processor_task: Optional[asyncio.Task[None]] = None

    @property
    def voltran_id(self) -> str:
        """Get Voltran node ID."""
        return self._voltran_id

    async def connect(self) -> None:
        """Establish gRPC connection."""
        if self._connected:
            return

        logger.info(
            "grpc_connecting",
            host=self._host,
            port=self._port,
        )

        # Start message processor
        self._processor_task = asyncio.create_task(self._process_messages())
        self._connected = True

        logger.info(
            "grpc_connected",
            host=self._host,
            port=self._port,
        )

    async def disconnect(self) -> None:
        """Disconnect from gRPC."""
        if not self._connected:
            return

        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass

        self._connected = False
        self._subscriptions.clear()
        self._pending_requests.clear()

        logger.info("grpc_disconnected")

    async def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected

    async def publish(self, topic: str, payload: Any) -> None:
        """Publish message to topic."""
        if not self._connected:
            raise ConnectionError("Not connected to gRPC")

        message = Message(
            topic=topic,
            payload=payload,
            source_voltran_id=self._voltran_id,
            timestamp=datetime.now(),
        )

        await self._message_queue.put(message)

        logger.debug(
            "grpc_message_published",
            topic=topic,
            correlation_id=message.correlation_id,
        )

    async def subscribe(self, topic: str, handler: MessageHandler) -> str:
        """Subscribe to topic."""
        subscription_id = f"sub-{uuid4()}"
        self._subscriptions[subscription_id] = (topic, handler)

        logger.info(
            "grpc_subscribed",
            topic=topic,
            subscription_id=subscription_id,
        )

        return subscription_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from topic."""
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            logger.info("grpc_unsubscribed", subscription_id=subscription_id)
            return True
        return False

    async def request(
        self,
        target_voltran_id: str,
        payload: Any,
        timeout: float = 30.0,
    ) -> Any:
        """Send request and wait for response."""
        if not self._connected:
            raise ConnectionError("Not connected to gRPC")

        correlation_id = str(uuid4())
        future: asyncio.Future[Any] = asyncio.Future()
        self._pending_requests[correlation_id] = future

        # Create request message
        message = Message(
            topic=f"voltran.{target_voltran_id}.request",
            payload=payload,
            source_voltran_id=self._voltran_id,
            correlation_id=correlation_id,
            reply_to=f"voltran.{self._voltran_id}.response",
        )

        await self._message_queue.put(message)

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            del self._pending_requests[correlation_id]
            raise TimeoutError(
                f"Request to {target_voltran_id} timed out after {timeout}s"
            )

    async def respond(self, original_message: Message, response: Any) -> None:
        """Send response to request."""
        if not original_message.reply_to:
            logger.warning(
                "grpc_no_reply_to",
                correlation_id=original_message.correlation_id,
            )
            return

        response_payload = self._inject_correlation_id(
            response, original_message.correlation_id
        )
        response_message = Message(
            topic=original_message.reply_to,
            payload=response_payload,
            source_voltran_id=self._voltran_id,
            correlation_id=original_message.correlation_id,
        )

        await self._message_queue.put(response_message)

    async def _process_messages(self) -> None:
        """Process messages from queue."""
        while True:
            try:
                message = await self._message_queue.get()

                # Check if this is a response to a pending request
                if message.correlation_id in self._pending_requests:
                    future = self._pending_requests.pop(message.correlation_id)
                    future.set_result(message.payload)
                    continue

                # Dispatch to subscribers
                for subscription_topic, handler in self._subscriptions.values():
                    if not self._topic_matches(subscription_topic, message.topic):
                        continue
                    try:
                        await handler(message)
                    except Exception as e:
                        logger.error(
                            "grpc_handler_error",
                            error=str(e),
                            topic=message.topic,
                            subscription_topic=subscription_topic,
                        )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("grpc_processor_error", error=str(e))

    def _topic_matches(self, pattern: str, topic: str) -> bool:
        """Check if a subscription pattern matches a topic."""
        if pattern == topic:
            return True
        if pattern.endswith(".*"):
            return topic.startswith(pattern[:-2])
        return False

    def _inject_correlation_id(self, payload: Any, correlation_id: str) -> Any:
        """Ensure correlation_id is included in dict payloads."""
        if correlation_id and isinstance(payload, dict) and "correlation_id" not in payload:
            return {**payload, "correlation_id": correlation_id}
        return payload

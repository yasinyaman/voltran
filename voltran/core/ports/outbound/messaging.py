"""Messaging outbound port interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Awaitable, Callable, Optional
from uuid import uuid4


@dataclass
class Message:
    """Message container for inter-Voltran communication."""

    topic: str
    payload: Any
    source_voltran_id: str
    timestamp: datetime = field(default_factory=datetime.now)
    correlation_id: str = field(default_factory=lambda: str(uuid4()))
    reply_to: Optional[str] = None


MessageHandler = Callable[[Message], Awaitable[None]]


class IMessagingPort(ABC):
    """
    Outbound port for inter-Voltran messaging.
    
    This port defines the interface for:
    - Publishing messages to topics
    - Subscribing to topics
    - Request-response pattern
    - Point-to-point messaging
    """

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to messaging system.
        """
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """
        Disconnect from messaging system.
        """
        pass

    @abstractmethod
    async def is_connected(self) -> bool:
        """
        Check if connected to messaging system.
        
        Returns:
            True if connected, False otherwise
        """
        pass

    @abstractmethod
    async def publish(self, topic: str, payload: Any) -> None:
        """
        Publish a message to a topic.
        
        Args:
            topic: Topic to publish to
            payload: Message payload (will be serialized)
        """
        pass

    @abstractmethod
    async def subscribe(self, topic: str, handler: MessageHandler) -> str:
        """
        Subscribe to a topic.
        
        Args:
            topic: Topic pattern to subscribe to (supports wildcards)
            handler: Async handler function for received messages
            
        Returns:
            Subscription ID for unsubscribing
        """
        pass

    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic.
        
        Args:
            subscription_id: ID returned from subscribe()
            
        Returns:
            True if unsubscribed, False if not found
        """
        pass

    @abstractmethod
    async def request(
        self,
        target_voltran_id: str,
        payload: Any,
        timeout: float = 30.0,
    ) -> Any:
        """
        Send a request and wait for response (request-response pattern).
        
        Args:
            target_voltran_id: ID of target Voltran node
            payload: Request payload
            timeout: Timeout in seconds
            
        Returns:
            Response payload
            
        Raises:
            TimeoutError: If no response received within timeout
        """
        pass

    @abstractmethod
    async def respond(self, original_message: Message, response: Any) -> None:
        """
        Send a response to a request message.
        
        Args:
            original_message: The request message to respond to
            response: Response payload
        """
        pass


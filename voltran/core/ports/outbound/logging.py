"""Logging outbound port interface."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from uuid import uuid4


class LogLevel(str, Enum):
    """Log levels."""
    
    TRACE = "trace"
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    
    @property
    def severity(self) -> int:
        """Get numeric severity (higher = more severe)."""
        severities = {
            "trace": 0,
            "debug": 10,
            "info": 20,
            "warning": 30,
            "error": 40,
            "critical": 50,
        }
        return severities.get(self.value, 0)


@dataclass
class LogEntry:
    """Structured log entry."""
    
    id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: datetime = field(default_factory=datetime.now)
    level: LogLevel = LogLevel.INFO
    message: str = ""
    logger_name: str = ""
    
    # Context and structured data
    context: dict[str, Any] = field(default_factory=dict)
    
    # Source information
    module_id: Optional[str] = None
    voltran_id: Optional[str] = None
    service_name: Optional[str] = None
    
    # Tracing
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    
    # Error details
    exception_type: Optional[str] = None
    exception_message: Optional[str] = None
    stack_trace: Optional[str] = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "id": self.id,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "logger_name": self.logger_name,
            "context": self.context,
            "module_id": self.module_id,
            "voltran_id": self.voltran_id,
            "service_name": self.service_name,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "parent_span_id": self.parent_span_id,
            "exception_type": self.exception_type,
            "exception_message": self.exception_message,
            "stack_trace": self.stack_trace,
        }


@dataclass
class LogQuery:
    """Query parameters for log search."""
    
    level: Optional[LogLevel] = None
    min_level: Optional[LogLevel] = None
    logger_name: Optional[str] = None
    message_contains: Optional[str] = None
    module_id: Optional[str] = None
    voltran_id: Optional[str] = None
    service_name: Optional[str] = None
    trace_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 100
    offset: int = 0
    context_filters: dict[str, Any] = field(default_factory=dict)


@dataclass
class LogStats:
    """Log statistics."""
    
    total_count: int = 0
    counts_by_level: dict[str, int] = field(default_factory=dict)
    counts_by_service: dict[str, int] = field(default_factory=dict)
    error_rate: float = 0.0
    logs_per_minute: float = 0.0
    first_log_time: Optional[datetime] = None
    last_log_time: Optional[datetime] = None


LogHandler = Callable[[LogEntry], None]


class ILoggingPort(ABC):
    """
    Outbound port for structured logging.
    
    This port defines the interface for:
    - Writing structured log entries
    - Querying logs
    - Log aggregation and stats
    - Log export
    """

    # === Log Writing ===

    @abstractmethod
    async def log(self, entry: LogEntry) -> None:
        """
        Write a log entry.
        
        Args:
            entry: Log entry to write
        """
        pass

    @abstractmethod
    async def trace(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """
        Write a trace level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            **context: Additional context fields
        """
        pass

    @abstractmethod
    async def debug(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """
        Write a debug level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            **context: Additional context fields
        """
        pass

    @abstractmethod
    async def info(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """
        Write an info level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            **context: Additional context fields
        """
        pass

    @abstractmethod
    async def warning(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """
        Write a warning level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            **context: Additional context fields
        """
        pass

    @abstractmethod
    async def error(
        self,
        message: str,
        logger_name: str = "",
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """
        Write an error level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            exception: Optional exception to log
            **context: Additional context fields
        """
        pass

    @abstractmethod
    async def critical(
        self,
        message: str,
        logger_name: str = "",
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """
        Write a critical level log.
        
        Args:
            message: Log message
            logger_name: Logger name
            exception: Optional exception to log
            **context: Additional context fields
        """
        pass

    # === Log Querying ===

    @abstractmethod
    async def query(self, query: LogQuery) -> list[LogEntry]:
        """
        Query logs with filters.
        
        Args:
            query: Query parameters
            
        Returns:
            List of matching log entries
        """
        pass

    @abstractmethod
    async def get_by_id(self, log_id: str) -> Optional[LogEntry]:
        """
        Get a specific log entry by ID.
        
        Args:
            log_id: Log entry ID
            
        Returns:
            Log entry if found, None otherwise
        """
        pass

    @abstractmethod
    async def get_by_trace(self, trace_id: str) -> list[LogEntry]:
        """
        Get all logs for a trace.
        
        Args:
            trace_id: Trace ID
            
        Returns:
            List of log entries for the trace
        """
        pass

    @abstractmethod
    async def get_recent(
        self,
        limit: int = 100,
        min_level: Optional[LogLevel] = None,
    ) -> list[LogEntry]:
        """
        Get most recent logs.
        
        Args:
            limit: Maximum number of entries
            min_level: Minimum log level filter
            
        Returns:
            List of recent log entries
        """
        pass

    # === Aggregation ===

    @abstractmethod
    async def get_stats(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> LogStats:
        """
        Get log statistics.
        
        Args:
            start_time: Start of time range
            end_time: End of time range
            
        Returns:
            Log statistics
        """
        pass

    @abstractmethod
    async def count(self, query: Optional[LogQuery] = None) -> int:
        """
        Count logs matching query.
        
        Args:
            query: Optional query parameters
            
        Returns:
            Number of matching logs
        """
        pass

    # === Context Management ===

    @abstractmethod
    def set_context(self, **context: Any) -> None:
        """
        Set default context for all logs.
        
        Args:
            **context: Context key-value pairs
        """
        pass

    @abstractmethod
    def get_context(self) -> dict[str, Any]:
        """
        Get current default context.
        
        Returns:
            Current context dictionary
        """
        pass

    @abstractmethod
    def clear_context(self) -> None:
        """Clear default context."""
        pass

    # === Handlers ===

    @abstractmethod
    def on_log(
        self,
        handler: LogHandler,
        min_level: Optional[LogLevel] = None,
    ) -> str:
        """
        Register a log handler.
        
        Args:
            handler: Function to call for each log
            min_level: Minimum level to trigger handler
            
        Returns:
            Handler ID for removal
        """
        pass

    @abstractmethod
    def remove_handler(self, handler_id: str) -> bool:
        """
        Remove a log handler.
        
        Args:
            handler_id: Handler ID to remove
            
        Returns:
            True if removed, False if not found
        """
        pass

    # === Export ===

    @abstractmethod
    async def export_json(
        self,
        query: Optional[LogQuery] = None,
    ) -> list[dict[str, Any]]:
        """
        Export logs as JSON.
        
        Args:
            query: Optional query to filter logs
            
        Returns:
            List of log dictionaries
        """
        pass

    @abstractmethod
    async def export_jsonl(
        self,
        query: Optional[LogQuery] = None,
    ) -> str:
        """
        Export logs as JSON Lines format.
        
        Args:
            query: Optional query to filter logs
            
        Returns:
            JSON Lines formatted string
        """
        pass

    # === Lifecycle ===

    @abstractmethod
    async def start(self) -> None:
        """Start the logging system."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Stop the logging system."""
        pass

    @abstractmethod
    async def flush(self) -> None:
        """Flush any buffered logs."""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all logs (useful for testing)."""
        pass


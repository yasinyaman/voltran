"""Logging domain service."""

import traceback
from contextvars import ContextVar
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Optional, TypeVar
from uuid import uuid4

import structlog

from voltran.core.ports.outbound.logging import (
    ILoggingPort,
    LogEntry,
    LogHandler,
    LogLevel,
    LogQuery,
    LogStats,
)

# Context variable for trace propagation
_current_trace: ContextVar[Optional[str]] = ContextVar("current_trace", default=None)
_current_span: ContextVar[Optional[str]] = ContextVar("current_span", default=None)

logger = structlog.get_logger(__name__)

F = TypeVar("F", bound=Callable[..., Any])


class LoggingService:
    """
    Domain service for structured logging.
    
    Provides high-level logging operations including:
    - Structured logging with context
    - Trace/span propagation
    - Log querying and aggregation
    - Module-scoped loggers
    
    Usage:
        logging_svc = LoggingService(adapter)
        await logging_svc.start()
        
        # Create scoped logger
        log = logging_svc.get_logger("my-module")
        await log.info("Something happened", user_id="123")
        
        # Query logs
        errors = await logging_svc.query(LogQuery(min_level=LogLevel.ERROR))
    """

    def __init__(
        self,
        logging_port: ILoggingPort,
        voltran_id: str = "",
        node_name: str = "",
    ):
        """
        Initialize logging service.
        
        Args:
            logging_port: Logging port implementation
            voltran_id: Voltran node ID
            node_name: Node name for logs
        """
        self._logging = logging_port
        self._voltran_id = voltran_id
        self._node_name = node_name
        self._running = False

    async def start(self) -> None:
        """Start the logging service."""
        if self._running:
            return

        await self._logging.start()
        self._running = True

        await self.info(
            "logging_service_started",
            logger_name="voltran.logging",
            voltran_id=self._voltran_id,
            node_name=self._node_name,
        )

    async def stop(self) -> None:
        """Stop the logging service."""
        await self.info(
            "logging_service_stopping",
            logger_name="voltran.logging",
        )
        await self._logging.flush()
        await self._logging.stop()
        self._running = False

    # === Scoped Logger Factory ===

    def get_logger(
        self,
        name: str,
        module_id: Optional[str] = None,
    ) -> "ScopedLogger":
        """
        Get a scoped logger for a module or component.
        
        Args:
            name: Logger name (typically module name)
            module_id: Optional module ID
            
        Returns:
            ScopedLogger instance
        """
        return ScopedLogger(
            logging_service=self,
            logger_name=name,
            module_id=module_id,
        )

    # === Direct Logging Methods ===

    async def log(self, entry: LogEntry) -> None:
        """Write a log entry directly."""
        # Inject trace context if available
        entry.trace_id = entry.trace_id or _current_trace.get()
        entry.span_id = entry.span_id or _current_span.get()
        entry.voltran_id = entry.voltran_id or self._voltran_id

        await self._logging.log(entry)

    async def trace(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """Write a trace level log."""
        await self._write_log(LogLevel.TRACE, message, logger_name, context)

    async def debug(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """Write a debug level log."""
        await self._write_log(LogLevel.DEBUG, message, logger_name, context)

    async def info(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """Write an info level log."""
        await self._write_log(LogLevel.INFO, message, logger_name, context)

    async def warning(
        self,
        message: str,
        logger_name: str = "",
        **context: Any,
    ) -> None:
        """Write a warning level log."""
        await self._write_log(LogLevel.WARNING, message, logger_name, context)

    async def error(
        self,
        message: str,
        logger_name: str = "",
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Write an error level log."""
        await self._write_log(LogLevel.ERROR, message, logger_name, context, exception)

    async def critical(
        self,
        message: str,
        logger_name: str = "",
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Write a critical level log."""
        await self._write_log(
            LogLevel.CRITICAL, message, logger_name, context, exception
        )

    async def _write_log(
        self,
        level: LogLevel,
        message: str,
        logger_name: str,
        context: dict[str, Any],
        exception: Optional[Exception] = None,
    ) -> None:
        """Internal method to write a log entry."""
        entry = LogEntry(
            level=level,
            message=message,
            logger_name=logger_name,
            context=context,
            voltran_id=self._voltran_id,
            trace_id=_current_trace.get(),
            span_id=_current_span.get(),
        )

        if exception:
            entry.exception_type = type(exception).__name__
            entry.exception_message = str(exception)
            entry.stack_trace = traceback.format_exc()

        await self.log(entry)

    # === Trace Context ===

    def create_trace(self) -> str:
        """
        Create a new trace ID and set it in context.
        
        Returns:
            New trace ID
        """
        trace_id = str(uuid4())
        _current_trace.set(trace_id)
        return trace_id

    def create_span(self, parent_span_id: Optional[str] = None) -> str:
        """
        Create a new span ID and set it in context.
        
        Args:
            parent_span_id: Optional parent span
            
        Returns:
            New span ID
        """
        span_id = str(uuid4())
        _current_span.set(span_id)
        return span_id

    def set_trace_context(
        self,
        trace_id: Optional[str] = None,
        span_id: Optional[str] = None,
    ) -> None:
        """
        Set trace context from external source.
        
        Args:
            trace_id: Trace ID to set
            span_id: Span ID to set
        """
        if trace_id:
            _current_trace.set(trace_id)
        if span_id:
            _current_span.set(span_id)

    def get_trace_context(self) -> tuple[Optional[str], Optional[str]]:
        """
        Get current trace context.
        
        Returns:
            Tuple of (trace_id, span_id)
        """
        return _current_trace.get(), _current_span.get()

    def clear_trace_context(self) -> None:
        """Clear trace context."""
        _current_trace.set(None)
        _current_span.set(None)

    def traced(self, func: F) -> F:
        """
        Decorator to create trace context for an async function.
        
        Usage:
            @logging_svc.traced
            async def my_handler(request):
                # logs in this function will have trace context
                return response
        """

        @wraps(func)
        async def wrapper(*args, **kwargs):
            trace_id = self.create_trace()
            span_id = self.create_span()
            try:
                return await func(*args, **kwargs)
            finally:
                self.clear_trace_context()

        return wrapper  # type: ignore

    # === Querying ===

    async def query(self, query: LogQuery) -> list[LogEntry]:
        """Query logs with filters."""
        return await self._logging.query(query)

    async def get_by_id(self, log_id: str) -> Optional[LogEntry]:
        """Get a specific log entry by ID."""
        return await self._logging.get_by_id(log_id)

    async def get_by_trace(self, trace_id: str) -> list[LogEntry]:
        """Get all logs for a trace."""
        return await self._logging.get_by_trace(trace_id)

    async def get_recent(
        self,
        limit: int = 100,
        min_level: Optional[LogLevel] = None,
    ) -> list[LogEntry]:
        """Get most recent logs."""
        return await self._logging.get_recent(limit, min_level)

    async def get_errors(self, limit: int = 100) -> list[LogEntry]:
        """Get recent error logs."""
        return await self._logging.get_recent(limit, LogLevel.ERROR)

    # === Aggregation ===

    async def get_stats(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> LogStats:
        """Get log statistics."""
        return await self._logging.get_stats(start_time, end_time)

    async def count(self, query: Optional[LogQuery] = None) -> int:
        """Count logs matching query."""
        return await self._logging.count(query)

    # === Handlers ===

    def on_log(
        self,
        handler: LogHandler,
        min_level: Optional[LogLevel] = None,
    ) -> str:
        """Register a log handler."""
        return self._logging.on_log(handler, min_level)

    def on_error(self, handler: LogHandler) -> str:
        """Register an error log handler."""
        return self._logging.on_log(handler, LogLevel.ERROR)

    def remove_handler(self, handler_id: str) -> bool:
        """Remove a log handler."""
        return self._logging.remove_handler(handler_id)

    # === Export ===

    async def export_json(
        self,
        query: Optional[LogQuery] = None,
    ) -> list[dict[str, Any]]:
        """Export logs as JSON."""
        return await self._logging.export_json(query)

    async def export_jsonl(
        self,
        query: Optional[LogQuery] = None,
    ) -> str:
        """Export logs as JSON Lines format."""
        return await self._logging.export_jsonl(query)

    # === Dashboard ===

    async def get_dashboard(self) -> dict[str, Any]:
        """
        Get dashboard-ready log summary.
        
        Returns:
            Dictionary with log statistics and recent entries
        """
        stats = await self.get_stats()
        recent_errors = await self.get_errors(limit=10)

        return {
            "voltran_id": self._voltran_id,
            "node_name": self._node_name,
            "stats": {
                "total_count": stats.total_count,
                "counts_by_level": stats.counts_by_level,
                "error_rate_percent": round(stats.error_rate * 100, 2),
                "logs_per_minute": round(stats.logs_per_minute, 2),
            },
            "recent_errors": [
                {
                    "id": e.id,
                    "timestamp": e.timestamp.isoformat(),
                    "message": e.message,
                    "logger": e.logger_name,
                    "exception": e.exception_type,
                }
                for e in recent_errors
            ],
            "timestamp": datetime.now().isoformat(),
        }


class ScopedLogger:
    """
    Scoped logger for a specific module or component.
    
    Automatically injects module_id and logger_name into all logs.
    """

    def __init__(
        self,
        logging_service: LoggingService,
        logger_name: str,
        module_id: Optional[str] = None,
    ):
        """
        Initialize scoped logger.
        
        Args:
            logging_service: Parent logging service
            logger_name: Logger name
            module_id: Optional module ID
        """
        self._service = logging_service
        self._logger_name = logger_name
        self._module_id = module_id
        self._context: dict[str, Any] = {}

    def bind(self, **context: Any) -> "ScopedLogger":
        """
        Create a new logger with additional bound context.
        
        Args:
            **context: Context to add
            
        Returns:
            New ScopedLogger with bound context
        """
        new_logger = ScopedLogger(
            logging_service=self._service,
            logger_name=self._logger_name,
            module_id=self._module_id,
        )
        new_logger._context = {**self._context, **context}
        return new_logger

    async def trace(self, message: str, **context: Any) -> None:
        """Write a trace level log."""
        await self._log(LogLevel.TRACE, message, context)

    async def debug(self, message: str, **context: Any) -> None:
        """Write a debug level log."""
        await self._log(LogLevel.DEBUG, message, context)

    async def info(self, message: str, **context: Any) -> None:
        """Write an info level log."""
        await self._log(LogLevel.INFO, message, context)

    async def warning(self, message: str, **context: Any) -> None:
        """Write a warning level log."""
        await self._log(LogLevel.WARNING, message, context)

    async def error(
        self,
        message: str,
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Write an error level log."""
        await self._log(LogLevel.ERROR, message, context, exception)

    async def critical(
        self,
        message: str,
        exception: Optional[Exception] = None,
        **context: Any,
    ) -> None:
        """Write a critical level log."""
        await self._log(LogLevel.CRITICAL, message, context, exception)

    async def exception(
        self,
        message: str,
        exc: Exception,
        **context: Any,
    ) -> None:
        """Write an error log with exception details."""
        await self.error(message, exception=exc, **context)

    async def _log(
        self,
        level: LogLevel,
        message: str,
        context: dict[str, Any],
        exception: Optional[Exception] = None,
    ) -> None:
        """Internal log method."""
        merged_context = {**self._context, **context}

        entry = LogEntry(
            level=level,
            message=message,
            logger_name=self._logger_name,
            context=merged_context,
            module_id=self._module_id,
        )

        if exception:
            entry.exception_type = type(exception).__name__
            entry.exception_message = str(exception)
            entry.stack_trace = traceback.format_exc()

        await self._service.log(entry)


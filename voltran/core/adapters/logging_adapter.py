"""In-memory and file logging adapter implementations."""

import asyncio
import json
import traceback
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional
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


class InMemoryLoggingAdapter(ILoggingPort):
    """
    In-memory implementation of logging port.
    
    Stores logs in memory with configurable max size.
    Suitable for development and testing.
    """

    def __init__(
        self,
        voltran_id: str = "",
        service_name: str = "voltran",
        max_entries: int = 10000,
        min_level: LogLevel = LogLevel.DEBUG,
    ):
        """
        Initialize logging adapter.
        
        Args:
            voltran_id: Voltran node ID
            service_name: Service name for logs
            max_entries: Maximum log entries to keep
            min_level: Minimum log level to record
        """
        self._voltran_id = voltran_id
        self._service_name = service_name
        self._max_entries = max_entries
        self._min_level = min_level
        
        # Log storage (deque for efficient FIFO)
        self._logs: deque[LogEntry] = deque(maxlen=max_entries)
        self._logs_by_id: dict[str, LogEntry] = {}
        
        # Default context
        self._context: dict[str, Any] = {}
        
        # Handlers
        self._handlers: dict[str, tuple[LogHandler, Optional[LogLevel]]] = {}
        
        # Stats
        self._counts_by_level: dict[str, int] = {level.value: 0 for level in LogLevel}
        self._counts_by_service: dict[str, int] = {}
        
        # Structlog integration
        self._structlog = structlog.get_logger(__name__)
        
        self._running = False

    # === Log Writing ===

    async def log(self, entry: LogEntry) -> None:
        """Write a log entry."""
        # Check minimum level
        if entry.level.severity < self._min_level.severity:
            return
        
        # Add default context
        entry.context = {**self._context, **entry.context}
        entry.voltran_id = entry.voltran_id or self._voltran_id
        entry.service_name = entry.service_name or self._service_name
        
        # Evict oldest entry if at capacity
        if self._logs.maxlen is not None and len(self._logs) >= self._logs.maxlen:
            dropped = self._logs.popleft()
            self._logs_by_id.pop(dropped.id, None)
            if dropped.level.value in self._counts_by_level:
                self._counts_by_level[dropped.level.value] = max(
                    0, self._counts_by_level[dropped.level.value] - 1
                )
            if dropped.service_name:
                remaining = self._counts_by_service.get(dropped.service_name, 0) - 1
                if remaining > 0:
                    self._counts_by_service[dropped.service_name] = remaining
                elif dropped.service_name in self._counts_by_service:
                    del self._counts_by_service[dropped.service_name]

        # Store log
        self._logs.append(entry)
        self._logs_by_id[entry.id] = entry
        
        # Update stats
        self._counts_by_level[entry.level.value] += 1
        if entry.service_name:
            self._counts_by_service[entry.service_name] = (
                self._counts_by_service.get(entry.service_name, 0) + 1
            )
        
        # Notify handlers
        for handler_id, (handler, min_level) in self._handlers.items():
            if min_level is None or entry.level.severity >= min_level.severity:
                try:
                    handler(entry)
                except Exception:
                    pass
        
        # Also log to structlog
        self._log_to_structlog(entry)

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
        await self._write_log(LogLevel.CRITICAL, message, logger_name, context, exception)

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
        )
        
        if exception:
            entry.exception_type = type(exception).__name__
            entry.exception_message = str(exception)
            entry.stack_trace = traceback.format_exc()
        
        await self.log(entry)

    def _log_to_structlog(self, entry: LogEntry) -> None:
        """Forward log to structlog."""
        log_method = getattr(self._structlog, entry.level.value, self._structlog.info)
        
        extra = {
            "log_id": entry.id,
            **entry.context,
        }
        
        if entry.trace_id:
            extra["trace_id"] = entry.trace_id
        if entry.module_id:
            extra["module_id"] = entry.module_id
        if entry.exception_type:
            extra["exception"] = f"{entry.exception_type}: {entry.exception_message}"
        
        log_method(entry.message, **extra)

    # === Log Querying ===

    async def query(self, query: LogQuery) -> list[LogEntry]:
        """Query logs with filters."""
        results = []
        
        for entry in reversed(self._logs):  # Newest first
            if self._matches_query(entry, query):
                results.append(entry)
                if len(results) >= query.offset + query.limit:
                    break
        
        # Apply offset and limit
        return results[query.offset:query.offset + query.limit]

    async def get_by_id(self, log_id: str) -> Optional[LogEntry]:
        """Get a specific log entry by ID."""
        return self._logs_by_id.get(log_id)

    async def get_by_trace(self, trace_id: str) -> list[LogEntry]:
        """Get all logs for a trace."""
        return [
            entry for entry in self._logs
            if entry.trace_id == trace_id
        ]

    async def get_recent(
        self,
        limit: int = 100,
        min_level: Optional[LogLevel] = None,
    ) -> list[LogEntry]:
        """Get most recent logs."""
        results = []
        
        for entry in reversed(self._logs):
            if min_level and entry.level.severity < min_level.severity:
                continue
            results.append(entry)
            if len(results) >= limit:
                break
        
        return results

    def _matches_query(self, entry: LogEntry, query: LogQuery) -> bool:
        """Check if entry matches query filters."""
        if query.level and entry.level != query.level:
            return False
        
        if query.min_level and entry.level.severity < query.min_level.severity:
            return False
        
        if query.logger_name and entry.logger_name != query.logger_name:
            return False
        
        if query.message_contains and query.message_contains.lower() not in entry.message.lower():
            return False
        
        if query.module_id and entry.module_id != query.module_id:
            return False
        
        if query.voltran_id and entry.voltran_id != query.voltran_id:
            return False
        
        if query.service_name and entry.service_name != query.service_name:
            return False
        
        if query.trace_id and entry.trace_id != query.trace_id:
            return False
        
        if query.start_time and entry.timestamp < query.start_time:
            return False
        
        if query.end_time and entry.timestamp > query.end_time:
            return False
        
        # Check context filters
        for key, value in query.context_filters.items():
            if entry.context.get(key) != value:
                return False
        
        return True

    # === Aggregation ===

    async def get_stats(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> LogStats:
        """Get log statistics."""
        filtered_logs = list(self._logs)
        
        if start_time or end_time:
            filtered_logs = [
                entry for entry in filtered_logs
                if (not start_time or entry.timestamp >= start_time) and
                   (not end_time or entry.timestamp <= end_time)
            ]
        
        if not filtered_logs:
            return LogStats()
        
        # Calculate counts
        counts_by_level: dict[str, int] = {}
        counts_by_service: dict[str, int] = {}
        error_count = 0
        
        for entry in filtered_logs:
            counts_by_level[entry.level.value] = counts_by_level.get(entry.level.value, 0) + 1
            if entry.service_name:
                counts_by_service[entry.service_name] = counts_by_service.get(entry.service_name, 0) + 1
            if entry.level.severity >= LogLevel.ERROR.severity:
                error_count += 1
        
        # Calculate rate
        first_log = filtered_logs[0]
        last_log = filtered_logs[-1]
        time_span = (last_log.timestamp - first_log.timestamp).total_seconds() / 60
        logs_per_minute = len(filtered_logs) / time_span if time_span > 0 else 0
        
        return LogStats(
            total_count=len(filtered_logs),
            counts_by_level=counts_by_level,
            counts_by_service=counts_by_service,
            error_rate=error_count / len(filtered_logs) if filtered_logs else 0,
            logs_per_minute=logs_per_minute,
            first_log_time=first_log.timestamp,
            last_log_time=last_log.timestamp,
        )

    async def count(self, query: Optional[LogQuery] = None) -> int:
        """Count logs matching query."""
        if query is None:
            return len(self._logs)
        
        count = 0
        for entry in self._logs:
            if self._matches_query(entry, query):
                count += 1
        return count

    # === Context Management ===

    def set_context(self, **context: Any) -> None:
        """Set default context for all logs."""
        self._context.update(context)

    def get_context(self) -> dict[str, Any]:
        """Get current default context."""
        return self._context.copy()

    def clear_context(self) -> None:
        """Clear default context."""
        self._context.clear()

    # === Handlers ===

    def on_log(
        self,
        handler: LogHandler,
        min_level: Optional[LogLevel] = None,
    ) -> str:
        """Register a log handler."""
        handler_id = str(uuid4())
        self._handlers[handler_id] = (handler, min_level)
        return handler_id

    def remove_handler(self, handler_id: str) -> bool:
        """Remove a log handler."""
        if handler_id in self._handlers:
            del self._handlers[handler_id]
            return True
        return False

    # === Export ===

    async def export_json(
        self,
        query: Optional[LogQuery] = None,
    ) -> list[dict[str, Any]]:
        """Export logs as JSON."""
        if query:
            logs = await self.query(query)
        else:
            logs = list(self._logs)
        
        return [entry.to_dict() for entry in logs]

    async def export_jsonl(
        self,
        query: Optional[LogQuery] = None,
    ) -> str:
        """Export logs as JSON Lines format."""
        logs = await self.export_json(query)
        return "\n".join(json.dumps(log) for log in logs)

    # === Lifecycle ===

    async def start(self) -> None:
        """Start the logging system."""
        self._running = True

    async def stop(self) -> None:
        """Stop the logging system."""
        self._running = False

    async def flush(self) -> None:
        """Flush any buffered logs (no-op for in-memory)."""
        pass

    async def clear(self) -> None:
        """Clear all logs."""
        self._logs.clear()
        self._logs_by_id.clear()
        self._counts_by_level = {level.value: 0 for level in LogLevel}
        self._counts_by_service.clear()


class FileLoggingAdapter(InMemoryLoggingAdapter):
    """
    File-based logging adapter.
    
    Extends in-memory adapter with file persistence.
    Writes logs to JSONL files with rotation.
    """

    def __init__(
        self,
        voltran_id: str = "",
        service_name: str = "voltran",
        log_dir: str = "logs",
        max_file_size_mb: int = 10,
        max_files: int = 10,
        flush_interval: float = 1.0,
        **kwargs,
    ):
        """
        Initialize file logging adapter.
        
        Args:
            voltran_id: Voltran node ID
            service_name: Service name for logs
            log_dir: Directory for log files
            max_file_size_mb: Max size per log file
            max_files: Max number of log files to keep
            flush_interval: Seconds between flushes
            **kwargs: Additional args for InMemoryLoggingAdapter
        """
        super().__init__(voltran_id, service_name, **kwargs)
        
        self._log_dir = Path(log_dir)
        self._max_file_size = max_file_size_mb * 1024 * 1024
        self._max_files = max_files
        self._flush_interval = flush_interval
        
        # File state
        self._current_file: Optional[Path] = None
        self._current_file_size = 0
        self._buffer: list[LogEntry] = []
        self._flush_task: Optional[asyncio.Task] = None

    async def log(self, entry: LogEntry) -> None:
        """Write a log entry."""
        await super().log(entry)
        
        # Add to buffer for file writing
        self._buffer.append(entry)

    async def start(self) -> None:
        """Start the logging system."""
        await super().start()
        
        # Create log directory
        self._log_dir.mkdir(parents=True, exist_ok=True)
        
        # Start new log file
        self._rotate_file()
        
        # Start flush task
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Stop the logging system."""
        # Flush remaining logs
        await self.flush()
        
        # Stop flush task
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        await super().stop()

    async def flush(self) -> None:
        """Flush buffered logs to file."""
        if not self._buffer or not self._current_file:
            return
        
        entries = self._buffer.copy()
        self._buffer.clear()
        
        # Write to file
        with open(self._current_file, "a") as f:
            for entry in entries:
                line = json.dumps(entry.to_dict()) + "\n"
                f.write(line)
                self._current_file_size += len(line)
        
        # Check rotation
        if self._current_file_size >= self._max_file_size:
            self._rotate_file()

    def _rotate_file(self) -> None:
        """Create a new log file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._current_file = self._log_dir / f"voltran_{timestamp}.jsonl"
        self._current_file_size = 0
        
        # Clean up old files
        self._cleanup_old_files()

    def _cleanup_old_files(self) -> None:
        """Remove old log files beyond max_files."""
        log_files = sorted(
            self._log_dir.glob("voltran_*.jsonl"),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        
        for old_file in log_files[self._max_files:]:
            old_file.unlink()

    async def _flush_loop(self) -> None:
        """Background loop for flushing logs."""
        while self._running:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.flush()
            except Exception:
                pass

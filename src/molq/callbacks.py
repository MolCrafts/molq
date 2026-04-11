"""Event system for job lifecycle notifications.

This module provides a simple pub/sub EventBus that allows registering
callbacks for job status changes. Handlers are called synchronously
but errors in handlers are isolated — a failing callback never breaks
the monitoring loop.
"""

import logging
from collections import defaultdict
from collections.abc import Callable
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


class EventType(str, Enum):
    """Job lifecycle event types."""

    STATUS_CHANGE = "status_change"
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"
    JOB_TIMEOUT = "job_timeout"
    JOB_TIMED_OUT = "job_timed_out"
    JOB_LOST = "job_lost"
    ALL_COMPLETED = "all_completed"


class EventBus:
    """Pub/sub bus for job lifecycle events.

    Handlers are called synchronously in registration order.
    Exceptions in handlers are logged but do not propagate.
    """

    def __init__(self) -> None:
        self._handlers: dict[EventType, list[Callable]] = defaultdict(list)

    def on(self, event: EventType, handler: Callable) -> None:
        """Register a callback for an event type.

        Args:
            event: Event type to listen for.
            handler: Callable that receives the event data.
        """
        self._handlers[event].append(handler)

    def off(self, event: EventType, handler: Callable) -> None:
        """Remove a previously registered callback.

        Args:
            event: Event type.
            handler: The handler to remove.
        """
        handlers = self._handlers.get(event, [])
        self._handlers[event] = [h for h in handlers if h is not handler]

    def emit(self, event: EventType, data: Any = None) -> None:
        """Dispatch an event to all registered handlers.

        Args:
            event: Event type to emit.
            data: Event payload (StatusChange, JobRecord, or None).
        """
        for handler in self._handlers.get(event, []):
            try:
                handler(data)
            except Exception:
                logger.exception(
                    "Handler %s failed for event %s", handler.__name__, event
                )

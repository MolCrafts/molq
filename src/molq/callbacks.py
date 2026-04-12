"""Event system for job lifecycle notifications.

This module provides a simple pub/sub EventBus that allows registering
callbacks for job status changes. Handlers are called synchronously
but errors in handlers are isolated — a failing callback never breaks
the monitoring loop.
"""

import threading
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import mollog

logger = mollog.get_logger(__name__)


class EventType(StrEnum):
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


@dataclass(frozen=True)
class EventPayload:
    """Lifecycle event payload."""

    event: EventType
    job_id: str | None = None
    transition: Any = None
    record: Any = None
    data: Any = None


class EventBus:
    """Pub/sub bus for job lifecycle events.

    Handlers are called synchronously in registration order.
    Exceptions in handlers are logged but do not propagate.
    """

    def __init__(self) -> None:
        self._handlers: dict[EventType, list[Callable]] = defaultdict(list)
        self._lock = threading.Lock()

    def on(self, event: EventType, handler: Callable) -> None:
        """Register a callback for an event type.

        Args:
            event: Event type to listen for.
            handler: Callable that receives the event data.
        """
        with self._lock:
            self._handlers[event].append(handler)

    def off(self, event: EventType, handler: Callable) -> None:
        """Remove a previously registered callback.

        Args:
            event: Event type.
            handler: The handler to remove.
        """
        with self._lock:
            handlers = self._handlers.get(event, [])
            self._handlers[event] = [h for h in handlers if h is not handler]

    def emit(self, event: EventType, data: Any = None) -> None:
        """Dispatch an event to all registered handlers.

        Args:
            event: Event type to emit.
            data: Event payload (StatusChange, JobRecord, or None).
        """
        # Snapshot the handler list under the lock, then dispatch outside it
        # so that handlers may freely on()/off() without deadlocking or
        # mutating the list we are iterating.
        with self._lock:
            handlers = list(self._handlers.get(event, []))
        for handler in handlers:
            try:
                handler(data)
            except Exception:
                logger.exception(
                    "Handler %s failed for event %s",
                    getattr(handler, "__name__", repr(handler)),
                    event,
                )

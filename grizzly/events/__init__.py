"""Logic for grizzly specific events."""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Optional

from locust.event import EventHook

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse, P
    from grizzly.users import GrizzlyUser


GrizzlyEventHandlerFunc = Callable[..., None]


class GrizzlyEventHandlerClass(metaclass=ABCMeta):
    user: GrizzlyUser
    event_hook: EventHook

    def __init__(self, user: GrizzlyUser) -> None:
        self.user = user
        self.event_hook = user.events.request

    @abstractmethod
    def __call__(
        self,
        name: str,
        context: GrizzlyResponse,
        request: RequestTask,
        exception: Optional[Exception] = None,
        **kwargs: Any,
    ) -> None:
        ...


GrizzlyEventHandler = GrizzlyEventHandlerFunc | GrizzlyEventHandlerClass


class GrizzlyEventHook(EventHook):
    """Override locust.events.EventHook to get types, and not to catch any exceptions."""

    _handlers: list[GrizzlyEventHandler]

    def __init__(self) -> None:
        self._handlers = []

    def add_listener(self, handler: GrizzlyEventHandler) -> None:
        super().add_listener(handler)

    def remove_listener(self, handler: GrizzlyEventHandler) -> None:
        super().remove_listener(handler)

    def fire(self, *, reverse: bool = False, **kwargs: Any) -> None:
        handlers = reversed(self._handlers) if reverse else self._handlers

        for handler in handlers:
            handler(**kwargs)


@dataclass
class GrizzlyEvents:
    keystore_request: GrizzlyEventHook = field(init=False, default_factory=GrizzlyEventHook)
    testdata_request: GrizzlyEventHook = field(init=False, default_factory=GrizzlyEventHook)


def event(hook: GrizzlyEventHook, measurement: str | None = None, tags: dict[str, str] | None = None) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            start = perf_counter()
            return_value: Any = None

            try:
                return_value = func(*args, **kwargs)
            finally:
                response_time = (perf_counter() - start) * 1000
                timestamp = datetime.now(timezone.utc).isoformat()

                hook.fire(
                    reverse=False,
                    timestamp=timestamp,
                    response_time=response_time,
                    return_value=return_value,
                    tags=tags,
                    measurement=measurement,
                    kwargs=kwargs,
                )

            return return_value

        return wrapper

    return decorator

from .request_logger import RequestLogger
from .response_handler import ResponseHandler

events = GrizzlyEvents()

__all__ = [
    'RequestLogger',
    'ResponseHandler',
]

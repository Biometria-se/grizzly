"""Logic for grizzly specific events."""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, List, Optional

from locust.event import EventHook

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types import GrizzlyResponse
    from grizzly.users import GrizzlyUser

class GrizzlyEventHook(EventHook):
    """Override locust.events.EventHook to get types, and not to catch any exceptions."""

    _handlers: List[GrizzlyEventHandler]

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


class GrizzlyEventHandler(metaclass=ABCMeta):
    user: GrizzlyUser
    event_hook: EventHook

    def __init__(self, user: GrizzlyUser) -> None:
        self.user = user
        self.event_hook = user.event_hook

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


from .request_logger import RequestLogger
from .response_handler import ResponseHandler

__all__ = [
    'RequestLogger',
    'ResponseHandler',
]

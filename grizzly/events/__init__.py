"""Logic for grizzly specific events."""
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Optional, Protocol

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


class GrizzlyInternalEventHandler(Protocol):
    def __call__(self, *,
        timestamp: str,
        metrics: dict[str, Any],
        tags: dict[str, str | None],
        measurement: str,
    ) -> None: ...


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


class GrizzlyInternalEventHook(GrizzlyEventHook):
    name: str

    def __init__(self, *args: Any, name: str, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.name = name

    def add_listener(
            self,
            handler: GrizzlyInternalEventHandler,  # type: ignore[override]
    ) -> None:
        return super().add_listener(handler)

    def fire(  # type: ignore[override]
        self,
        *,
        reverse: bool = False,
        timestamp: str,
        metrics: dict[str, Any],
        tags: dict[str, str | None],
        measurement: str | None,
    ) -> None:
        super().fire(
            reverse=reverse,
            timestamp=timestamp,
            metrics=metrics,
            tags=tags,
            measurement=measurement,
        )

def grizzly_internal_event_hook_factory(name: str) -> Callable[[], GrizzlyInternalEventHook]:
    def wrapper() -> GrizzlyInternalEventHook:
        return GrizzlyInternalEventHook(name=name)

    return wrapper

@dataclass
class GrizzlyEvents:
    keystore_request: GrizzlyInternalEventHook = field(init=False, default_factory=grizzly_internal_event_hook_factory('request_keystore'))
    testdata_request: GrizzlyInternalEventHook = field(init=False, default_factory=grizzly_internal_event_hook_factory('request_testdata'))
    user_event: GrizzlyInternalEventHook = field(init=False, default_factory=grizzly_internal_event_hook_factory('user_event'))


class GrizzlyEventDecoder(metaclass=ABCMeta):
    arg: str | int

    def __init__(self, arg: str | int) -> None:
        self.arg = arg

    @abstractmethod
    def __call__(
        self,
        *args: Any,
        tags: dict[str, str | None] | None,
        return_value: Any,
        exception: Exception | None,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], dict[str, str | None]]:
        ...


def event(
        hook: GrizzlyInternalEventHook,
        *,
        decoder: GrizzlyEventDecoder,
        measurement: str | None = None,
        tags: dict[str, str | None] | None = None,
    ) -> Callable[..., Any]:

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Any:
            start = perf_counter()
            return_value: Any = None
            exception: Exception | None = None

            try:
                return_value = func(*args, **kwargs)
            except Exception as e:
                exception = e
                raise
            finally:
                response_time = (perf_counter() - start) * 1000
                timestamp = datetime.now(timezone.utc).isoformat()

                with suppress(Exception):
                    metrics, decoded_tags = decoder(*args, tags=tags, return_value=return_value, exception=exception, **kwargs)

                    if tags is not None:
                        decoded_tags.update(tags)

                    _measurement = hook.name if measurement is None else measurement

                    metrics.update({'response_time': response_time})

                    hook.fire(
                        reverse=False,
                        timestamp=timestamp,
                        tags=decoded_tags,
                        measurement=_measurement,
                        metrics=metrics,
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

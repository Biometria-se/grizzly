"""Additional custom gevent functionality."""
from __future__ import annotations

from functools import wraps
from typing import Any, Callable

from gevent import Greenlet, getcurrent


class GreenletWithExceptionCatching(Greenlet):
    """Catch exceptions thrown by a function executing in a greenlet."""

    started_from: Greenlet

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize Greenlet object, with custom property from which greenlet this greenlet was started."""
        super().__init__(*args, **kwargs)
        self.started_from = getcurrent()

    def handle_exception(self, error: Exception) -> None:
        """Handle exception thrown, by throwing it from the greenlet that started this greenlet."""
        self.started_from.throw(error)

    def wrap_exceptions(self, func: Callable) -> Callable:
        """Make sure exceptions is thrown from the correct place, so it can be handled."""
        @wraps(func)
        def exception_handler(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exception:
                self.wrap_exceptions(self.handle_exception)(exception)
                return exception  # pragma: no cover

        return exception_handler

    def spawn(self, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        """Spawn a greenlet executing the function, in a way that any exceptions can be handled where it was spawned."""
        return super().spawn(
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

    def spawn_blocking(self, func: Callable, *args: Any, **kwargs: Any) -> None:
        """Spawn a greenlet executing the function and wait for the function to finish.
        Get the result of the executed function, if there was an exception raised, it will be
        re-raised by `get`.
        """
        greenlet = self.spawn(func, *args, **kwargs)
        greenlet.join()
        greenlet.get()

    def spawn_later(self, seconds: int, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        """Spawn a greenlet `seconds` in the future, in a way that any exceptions can be handled where it was spawned."""
        return super().spawn_later(
            seconds,
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

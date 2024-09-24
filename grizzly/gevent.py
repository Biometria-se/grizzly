"""Additional custom gevent functionality."""
from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from gevent import Greenlet, getcurrent

if TYPE_CHECKING:
    import logging
    from collections.abc import Generator


class GreenletWithExceptionCatching(Greenlet):
    """Catch exceptions thrown by a function executing in a greenlet."""

    started_from: Greenlet
    logger: logging.Logger
    message: str
    ignore_exceptions: list[type[Exception]]


    def __init__(self, *args: Any, logger: logging.Logger, ignore_exceptions: list[type[Exception]], **kwargs: Any) -> None:
        """Initialize Greenlet object, with custom property from which greenlet this greenlet was started."""
        super().__init__(*args, **kwargs)
        self.started_from = getcurrent()
        self.logger = logger
        self.ignore_exceptions = ignore_exceptions

    def handle_exception(self, error: Exception) -> None:
        """Handle exception thrown, by throwing it from the greenlet that started this greenlet."""
        if error.__class__ not in self.ignore_exceptions:
            self.logger.exception(self.message)

        self.started_from.throw(error)

    def wrap_exceptions(self, func: Callable) -> Callable:
        """Make sure exceptions is thrown from the correct place, so it can be handled."""
        @wraps(func)
        def exception_handler(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exception:
                return self.wrap_exceptions(self.handle_exception)(exception)

        return exception_handler

    def spawn(self, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        """Spawn a greenlet executing the function, in a way that any exceptions can be handled where it was spawned."""
        return Greenlet.spawn(
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

    @contextmanager
    def spawn_blocking(self, func: Callable, message: str, *args: Any, **kwargs: Any) -> Generator[Greenlet, None, None]:
        """Spawn a greenlet executing the function and wait for the function to finish.
        Get the result of the executed function, if there was an exception raised, it will be
        re-raised by `get`.
        """
        self.message = message
        greenlet = self.spawn(func, *args, **kwargs)

        yield greenlet

        greenlet.join()
        greenlet.get()

    def spawn_later(self, seconds: int, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        """Spawn a greenlet `seconds` in the future, in a way that any exceptions can be handled where it was spawned."""
        return Greenlet.spawn_later(
            seconds,
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

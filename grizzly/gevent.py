"""Additional custom gevent functionality."""
from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable

from gevent import Greenlet, getcurrent

if TYPE_CHECKING:
    import logging
    from collections.abc import Generator


class GreenletFactory:
    """Catch exceptions thrown by a function executing in a greenlet."""

    started_from: Greenlet
    logger: logging.Logger
    ignore_exceptions: list[type[Exception]]
    index: int
    total: int
    description: str

    def __init__(self, *args: Any, logger: logging.Logger, ignore_exceptions: list[type[Exception]] | None = None, **kwargs: Any) -> None:
        """Initialize Greenlet object, with custom property from which greenlet this greenlet was started."""
        super().__init__(*args, **kwargs)
        self.started_from = getcurrent()
        self.logger = logger
        self.ignore_exceptions = ignore_exceptions if ignore_exceptions is not None else []
        self.index = -1
        self.total = -1
        self.description = ''

    def handle_exception(self, exception: Exception) -> None:
        """Handle exception thrown, by throwing it from the greenlet that started this greenlet."""
        if exception.__class__ not in self.ignore_exceptions and self.total > 0:
            message = f'task {self.index} of {self.total} failed: {self.description}'
            self.logger.exception(message)

        self.started_from.throw(exception)

    def wrap_exceptions(self, func: Callable) -> Callable:
        """Make sure exceptions is thrown from the correct place, so it can be handled."""
        @wraps(func)
        def exception_handler(*args: Any, **kwargs: Any) -> Any:
            try:
                result = func(*args, **kwargs)

                if self.total > 0:
                    message = f'task {self.index} of {self.total} executed: {self.description}'
                    self.logger.debug(message)
            except Exception as exception:
                self.wrap_exceptions(self.handle_exception)(exception)
                return exception
            else:
                return result

        return exception_handler

    def spawn(self, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        """Spawn a greenlet executing the function, in a way that any exceptions can be handled where it was spawned."""
        return Greenlet.spawn(
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

    @contextmanager
    def spawn_task(self, func: Callable, index: int, total: int, description: str, *args: Any, **kwargs: Any) -> Generator[Greenlet, None, None]:
        """Spawn a greenlet executing the function and wait for the function to finish.
        Get the result of the executed function, if there was an exception raised, it will be
        re-raised by `get`.
        """
        self.index = index
        self.total = total
        self.description = description

        greenlet = self.spawn(func, *args, **kwargs)

        yield greenlet

        greenlet.join()
        greenlet.get()

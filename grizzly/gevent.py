"""Additional custom gevent functionality."""

from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
from typing import TYPE_CHECKING, Any

from gevent import Greenlet, Timeout, getcurrent

from grizzly.exceptions import TaskTimeoutError
from grizzly.types import FailureAction

if TYPE_CHECKING:  # pragma: no cover
    import logging
    from collections.abc import Callable, Generator

    from grizzly.scenarios import GrizzlyScenario


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

            exc_info: Exception | None = exception
            if isinstance(exception, FailureAction.get_failure_exceptions()):
                exc_info = None

            self.logger.error(message, exc_info=exc_info)

        self.started_from.throw(exception)

    def wrap_exceptions(self, func: Callable) -> Callable:
        """Make sure exceptions is thrown from the correct place, so it can be handled."""
        metadata = getattr(func, '__grizzly_metadata__', {})
        timeout = metadata.get('timeout')

        @wraps(func)
        def exception_handler(*args: Any, **kwargs: Any) -> Any:
            try:
                with Timeout(seconds=timeout, exception=TaskTimeoutError(f'task took more than {timeout} seconds')):
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
    def spawn_task(
        self,
        scenario: GrizzlyScenario,
        task: Callable,
        index: int,
        total: int,
        description: str,
        *args: Any,
        **kwargs: Any,
    ) -> Generator[Greenlet, None, None]:
        """Spawn a greenlet executing the function and wait for the function to finish.
        Get the result of the executed function, if there was an exception raised, it will be
        re-raised by `get`.
        """
        self.index = index
        self.total = total
        self.description = description

        args = (scenario, *args)

        greenlet = self.spawn(task, *args, **kwargs)

        yield greenlet

        greenlet.join()
        greenlet.get()

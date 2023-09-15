from typing import Any, Callable, Dict, Tuple
from functools import wraps

from gevent import Greenlet, getcurrent


class GreenletWithExceptionCatching(Greenlet):
    started_from: Greenlet

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)
        self.started_from = getcurrent()

    def handle_exception(self, error: Exception) -> None:
        self.started_from.throw(error)

    def wrap_exceptions(self, func: Callable) -> Callable:
        @wraps(func)
        def exception_handler(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exception:
                self.wrap_exceptions(self.handle_exception)(exception)
                return exception  # pragma: no cover

        return exception_handler

    def spawn(self, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        return super().spawn(
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

    def spawn_later(self, seconds: int, func: Callable, *args: Any, **kwargs: Any) -> Greenlet:
        return super().spawn_later(
            seconds,
            self.wrap_exceptions(func),
            *args,
            **kwargs,
        )

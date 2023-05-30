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
                return exception

        return exception_handler

    def spawn(self, func: Callable, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Greenlet:
        func_wrap = self.wrap_exceptions(func)
        return super().spawn(func_wrap, *args, **kwargs)

    def spawn_later(self, seconds: int, func: Callable, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Greenlet:
        func_wrap = self.wrap_exceptions(func)
        return super().spawn_later(seconds, func_wrap, *args, **kwargs)

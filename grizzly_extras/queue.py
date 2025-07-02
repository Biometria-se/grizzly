"""Custom queue implementations."""

from __future__ import annotations

from collections import deque
from contextlib import suppress
from time import perf_counter
from typing import TYPE_CHECKING, TypeVar
from unittest.mock import ANY

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable, Iterable

T = TypeVar('T')


class VolatileDeque(deque[T]):
    timeout: float
    queue: deque[tuple[float, T]]

    def __init__(self, /, timeout: float) -> None:
        self.timeout = timeout
        self.queue = deque()

    def __expire(self) -> None:
        now = perf_counter()
        for timestamp, _value in list(self.queue):
            if now - timestamp < self.timeout:
                continue

            with suppress(Exception):
                self.queue.remove((timestamp, _value))

    def __repr__(self) -> str:
        return repr(self.queue)

    def __contains__(self, value: object) -> bool:
        # first remove values that has timed out
        self.__expire()

        try:
            _ = self.queue.index((ANY, value))  # type: ignore[arg-type]
        except ValueError:
            return False
        else:
            return True

    def __len__(self) -> int:
        return len(self.queue)

    def __append(self, value: T, func: Callable[[tuple[float, T]], None]) -> None:
        timestamp = perf_counter()

        func((timestamp, value))

    def append(self, value: T, /) -> None:
        self.__append(value, self.queue.append)

    def appendleft(self, value: T, /) -> None:
        self.__append(value, self.queue.appendleft)

    def __extend(self, iterable: Iterable[T], func: Callable[[Iterable[tuple[float, T]]], None]) -> None:
        timestamp = perf_counter()
        volatile_iterable: Iterable[tuple[float, T]] = iter([(timestamp, value) for value in iterable])

        func(volatile_iterable)

    def extend(self, iterable: Iterable[T], /) -> None:
        self.__extend(iterable, self.queue.extend)

    def extendleft(self, iterable: Iterable[T], /) -> None:
        self.__extend(iterable, self.queue.extendleft)

    def insert(self, index: int, value: T, /) -> None:
        timestamp = perf_counter()
        self.queue.insert(index, (timestamp, value))

    def index(self, item: T, start: int = 0, *args: int) -> int:
        return self.queue.index((ANY, item), start, *args)

    def __pop(self, func: Callable[[], tuple[float, T]]) -> T:
        value: T | None = None

        while value is None:
            timestamp, value = func()
            if perf_counter() - timestamp >= self.timeout:
                value = None

        return value

    def pop(self) -> T:  # type: ignore[override]
        return self.__pop(self.queue.pop)

    def popleft(self) -> T:
        return self.__pop(self.queue.popleft)

    def remove(self, value: T) -> None:
        self.queue.remove((ANY, value))

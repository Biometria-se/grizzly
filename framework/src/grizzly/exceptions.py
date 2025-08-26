"""Custom grizzly exceptions."""

from __future__ import annotations

from random import uniform
from typing import TYPE_CHECKING, Any

from gevent import sleep as gsleep
from grizzly_common.exceptions import StopScenario
from locust.exception import StopUser
from typing_extensions import Self

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable
    from types import TracebackType

    from grizzly.types.behave import Scenario, Step


__all__ = [
    'StopScenario',
    'StopUser',
]


class ResponseHandlerError(Exception):
    message: str | None = None

    def __init__(self, message: str | None = None) -> None:
        self.message = message


class RestartScenario(Exception):  # noqa: N818
    pass


class RestartIteration(Exception):  # noqa: N818
    max_retries: int | None

    def __init__(self, *args: object, max_retries: int | None = None) -> None:
        super().__init__(*args)
        self.max_retries = max_retries


class RetryTask(Exception):  # noqa: N818
    pass


class TaskTimeoutError(Exception):
    pass


class AssertionErrors(Exception):  # noqa: N818
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.errors: list[AssertionError] = []
        self._index = 0

    def append(self, error: AssertionError) -> None:
        self.errors.append(error)

    def __iter__(self) -> AssertionErrors:
        return self

    def __next__(self) -> AssertionError:
        if self._index < len(self.errors):
            error = self.errors[self._index]
            self._index += 1
            return error

        self._index = 0

        raise StopIteration

    def __len__(self) -> int:
        return len(self.errors)


class StepError(AssertionError):
    def __init__(self, error: AssertionError | str, step: Step) -> None:
        if isinstance(error, AssertionError):
            error = str(error)

        super().__init__(error)

        self.step = step
        self.error = error

    def __str__(self) -> str:
        return '\n'.join([f'    {self.step.keyword} {self.step.name} # {self.step.location}', f'    ! {self.error!s}'])


class ScenarioError(AssertionError):
    def __init__(self, error: AssertionError, scenario: Scenario) -> None:
        self.scenario = scenario
        self.error = error

    def __str__(self) -> str:
        return f'! {self.error!s}'


class FeatureError(Exception):
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __str__(self) -> str:
        return f'{self.error!s}'


class retry:
    def __init__(
        self,
        *,
        retries: int = 3,
        exceptions: tuple[type[Exception], ...],
        backoff: float | None = None,
        failure_exception: type[Exception] | Exception | None = None,
    ) -> None:
        self.retries = retries
        self.exceptions = exceptions
        self.backoff = backoff
        self.failure_exception = failure_exception
        self.retry = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        return exc is None

    def execute(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        backoff_time: float = 0.0

        while self.retry < self.retries:
            self.retry += 1

            try:
                result = func(*args, **kwargs)
            except self.exceptions as e:
                if self.retry >= self.retries:
                    if self.failure_exception is None:
                        raise
                    raise self.failure_exception from e

                if self.backoff is not None:
                    backoff_time += uniform(0.5, 1.5) + self.backoff  # noqa: S311
                    gsleep(backoff_time)
            else:
                return result

        return None

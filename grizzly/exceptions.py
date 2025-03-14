"""Custom grizzly exceptions."""
from __future__ import annotations

from random import uniform
from typing import TYPE_CHECKING, Any, Callable, Optional

from gevent import sleep as gsleep
from locust.exception import StopUser
from typing_extensions import Self

from grizzly_extras.exceptions import StopScenario

if TYPE_CHECKING:  # pragma: no cover
    from types import TracebackType

    from grizzly.context import GrizzlyContextScenario
    from grizzly.types.behave import Scenario, Step


__all__ = [
    'StopScenario',
    'StopUser',
]


class ResponseHandlerError(Exception):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
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


def failure_handler(exception: Exception | None, scenario: GrizzlyContextScenario) -> None:
    # no failure, just return
    if exception is None:
        return

    # failure action has already been decided, let it through
    from grizzly.types import FailureAction
    if isinstance(exception, FailureAction.get_failure_exceptions()):
        raise exception

    # always raise StopUser when these unhandled exceptions has occured
    if isinstance(exception, (NotImplementedError, KeyError, IndexError, AttributeError, TypeError, SyntaxError)):
        raise StopUser from exception

    # custom actions based on failure
    for failure_type, failure_action in scenario.failure_handling.items():
        if failure_type is None:
            continue

        # continue test for this specific error, i.e. ignore it
        if failure_action is None:
            return

        if (isinstance(failure_type, str) and failure_type in str(exception)) or exception.__class__ is failure_type:
            raise failure_action from exception

    # no match, raise the default if it has been set
    default_exception = scenario.failure_handling.get(None, None)

    if default_exception is not None:
        raise default_exception from exception


class retry:
    def __init__(self, *, retries: int = 3, exceptions: tuple[type[Exception], ...], backoff: float | None = None) -> None:
        self.retries = retries
        self.exceptions = exceptions
        self.backoff = backoff
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
            except self.exceptions:
                if self.retry >= self.retries:
                    raise

                if self.backoff is not None:
                    backoff_time += uniform(0.5, 1.5) + self.backoff  # noqa: S311
                    gsleep(backoff_time)
            else:
                return result

        return None

"""Custom grizzly exceptions."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from locust.exception import StopUser

from grizzly_extras.exceptions import StopScenario

if TYPE_CHECKING:  # pragma: no cover
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


class RetryTask(Exception):  # noqa: N818
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

    # always raise StopUser when these unhandled exceptions has occured
    if isinstance(exception, (NotImplementedError, KeyError, IndexError, AttributeError, TypeError, SyntaxError)):
        raise StopUser from exception

    # custom actions based on failure
    for failure_type, failure_action in scenario.failure_handling.items():
        if failure_type is None:
            continue

        if (isinstance(failure_type, str) and failure_type in str(exception)) or exception.__class__ is failure_type:
            raise failure_action from exception

    # no match, raise the default if it has been set
    default_exception = scenario.failure_handling.get(None, None)

    if default_exception is not None:
        raise default_exception from exception

"""Custom grizzly exceptions."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from locust.exception import StopUser

from grizzly_extras.async_message import AsyncMessageAbort

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types.behave import Scenario, Step


__all__ = [
    'StopUser',
]


class ResponseHandlerError(Exception):
    message: Optional[str] = None

    def __init__(self, message: Optional[str] = None) -> None:
        self.message = message


class RestartScenario(Exception):  # noqa: N818
    pass


class StopScenario(AsyncMessageAbort):
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
        return '\n'.join([f'{self.step.keyword} {self.step.name} # {self.step.location}', f'    ! {self.error!s}'])


class ScenarioError(AssertionError):
    def __init__(self, error: AssertionError, scenario: Scenario) -> None:
        self.scenario = scenario
        self.error = error

    def __str__(self) -> str:
        return f'    ! {self.error!s}'


class FeatureError(Exception):
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __str__(self) -> str:
        return f'    {self.error!s}'

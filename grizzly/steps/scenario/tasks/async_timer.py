"""Module contains step implementations for the [Async timer][grizzly.tasks.async_timer] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import AsyncTimerTask
from grizzly.types.behave import Context, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@then('start document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_start(context: Context, name: str, tid: str, version: str) -> None:
    """Start an asynchrounous timer.

    See [Async timer][grizzly.tasks.async_timer] task documentation for more information.

    Example:
    ```gherkin
    Scenario: input
        Then start document timer with name "Creation time" for id "{{ document_id }}" and version "{{ document_version }}"
    ```

    Start a timer in one scenario. One specific instance of a timer is the combination of `name`, `tid` and `version`. So `tid`
    can be reused, as long as it has another `version`.

    Args:
        name (str): name of the timer which will be shown in request statistics
        tid (str): unique id for the timer
        version (str): version of `tid`

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'start'))


@then('stop document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_stop(context: Context, name: str, tid: str, version: str) -> None:
    """Stop an asynchrounous timer, with a known name.

    This is needed if the combination of `tid` and `version` the timer was started with is not
    unique for the timer.

    See [Async timer][grizzly.tasks.async_timer] task documentation for more information.

    Example:
    ```gherkin
    Scenario: output
        Then stop document timer with name "Creation time" for id "{{ document_id }}" and version "{{ document_version }}"
    ```

    Stop timer from another scenario (or the same as it was started).

    Args:
        name (str): name of the timer which will be shown in request statistics
        tid (str): unique id for the timer
        version (str): version of `tid`

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'stop'))

"""@anchor pydoc:grizzly.steps.scenario.tasks.async_timer Asynchronous Timer
This module contains step implementations for the {@pylink grizzly.tasks.async_timer} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import AsyncTimerTask
from grizzly.types.behave import Context, then


@then('start document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_start(context: Context, name: str, tid: str, version: str) -> None:
    """Start an asynchrounous timer.

    See {@pylink grizzly.tasks.async_timer} task documentation for more information.

    Example:
    ```gherkin
    Scenario: input
        Then start document timer with name "Creation time" for id "{{ document_id }}" and version "{{ document_version }}"
    ```

    Start a timer in one scenario.

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'start'))


@then('stop document timer with name "{name}" for id "{tid}" and version "{version}"')
def step_task_async_timer_stop(context: Context, name: str, tid: str, version: str) -> None:
    """Stop an asynchrounous timer, with a known name.

    This is needed if the combination of `tid` and `version` the timer was started with is not
    unique for the timer.

    See {@pylink grizzly.tasks.async_timer} task documentation for more information.

    Example:
    ```gherkin
    Scenario: output
        Then stop document timer with name "Creation time" for id "{{ document_id }}" and version "{{ document_version }}"
    ```

    Stop a, known, timer in another scenario

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    grizzly.scenario.tasks.add(AsyncTimerTask(name, tid, version, 'stop'))

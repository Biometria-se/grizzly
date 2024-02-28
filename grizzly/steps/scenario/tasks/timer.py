"""@anchor pydoc:grizzly.steps.scenario.tasks.timer Timer
This module contains step implementations for the {@pylink grizzly.tasks.timer} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import TimerTask
from grizzly.types.behave import Context, then


@then('start timer with name "{name}"')
def step_task_timer_start(context: Context, name: str) -> None:
    """Create an instance of the {@pylink grizzly.tasks.timer} task.

    Starts a timer to measure the "request time" for all tasks between the start and stop of the timer.

    See {@pylink grizzly.tasks.timer} task documentation for more information.

    Example:
    ```gherkin
    Then start timer with name "parsing-xml"
    ...
    And stop timer with name "parsing-xml"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert name not in grizzly.scenario.tasks.tmp.timers, f'timer with name {name} has already been defined'

    task = TimerTask(name=name)

    grizzly.scenario.tasks.tmp.timers.update({
        name: task,
    })

    grizzly.scenario.tasks.add(task)


@then('stop timer with name "{name}"')
def step_task_timer_stop(context: Context, name: str) -> None:
    """Add the instance created by {@pylink grizzly.steps.scenario.tasks.timer.step_task_timer_start} to the list of scenario tasks.

    See {@pylink grizzly.tasks.timer} task documentation for more information.

    Example:
    ```gherkin
    Then start timer with name "parsing-xml"
    ...
    And stop timer with name "parsing-xml"
    ```

    """
    grizzly = cast(GrizzlyContext, context.grizzly)

    task = grizzly.scenario.tasks.tmp.timers.get(name, None)

    assert task is not None, f'timer with name {name} has not been defined'

    grizzly.scenario.tasks.add(task)
    grizzly.scenario.tasks.tmp.timers.update({name: None})

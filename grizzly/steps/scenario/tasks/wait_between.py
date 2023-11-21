"""@anchor pydoc:grizzly.steps.scenario.tasks.wait_between Wait between
This module contains step implementations for the {@pylink grizzly.tasks.wait_between} task.
"""
from __future__ import annotations

from typing import cast

from grizzly.context import GrizzlyContext
from grizzly.tasks import WaitBetweenTask
from grizzly.types.behave import Context, given


@given('wait "{min_time:g}..{max_time:g}" seconds between tasks')
def step_task_wait_between_random(context: Context, min_time: float, max_time: float) -> None:
    """Create an instance of the {@pylink grizzly.tasks.wait_between} task.

    Set number of, randomly, seconds the {@pylink grizzly.users} will wait between executing each task.

    See {@pylink grizzly.tasks.wait_between} task documentation for more information.

    Example:
    ```gherkin
    And wait "1.4..1.7" seconds between tasks
    # wait between 1.4 and 1.7 seconds
    Then get request with name "test-get-1" from endpoint "..."
    # wait between 1.4 and 1.7 seconds
    Then get request with name "test-get-2" from endpoint "..."
    # wait between 1.4 and 1.7 seconds
    And wait "0.1" seconds between tasks
    # wait 0.1 seconds
    Then get request with name "test-get-3" from endpoint "..."
    # wait 0.1 seconds
    ...
    ```
    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    if min_time > max_time:
        min_time, max_time = max_time, min_time

    grizzly.scenario.tasks.add(WaitBetweenTask(min_time=min_time, max_time=max_time))


@given('wait "{time:g}" seconds between tasks')
def step_task_wait_between_constant(context: Context, time: float) -> None:
    """Create an instance of the {@pylink grizzly.tasks.wait_between} task.

    Set number of, constant, seconds the {@pylink grizzly.users} will wait between executing each task.

    See {@pylink grizzly.tasks.wait_between} task documentation for more information.

    Example:
    ```gherkin
    And wait "1.4" seconds between tasks
    # wait 1.4 seconds
    Then get request with name "test-get-1" from endpoint "..."
    # wait 1.4 seconds
    Then get request with name "test-get-2" from endpoint "..."
    # wait 1.4 seconds
    And wait "0.1" seconds between tasks
    # wait 0.1 seconds
    Then get request with name "test-get-3" from endpoint "..."
    # wait 0.1 seconds
    ...
    ```
    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    grizzly.scenario.tasks.add(WaitBetweenTask(time))

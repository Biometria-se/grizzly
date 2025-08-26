"""Module contains step implementations for the [Async group][grizzly.tasks.async_group] task."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.tasks import AsyncRequestGroupTask
from grizzly.types.behave import Context, given, then

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@given('an async request group with name "{name}"')
def step_task_async_group_open(context: Context, name: str) -> None:
    """Create an instance of the [Async group][grizzly.tasks.async_group] task.

    All [Request][grizzly.tasks.request] tasks created after this step will be added to the request group, until the group is closed.

    See [Async group][grizzly.tasks.async_group] task documentation for more information.

    Example:
    ```gherkin
    Given an async request group with name "async-group-1"
    Then post request with name "test-post-2" to endpoint "/api/test"
        \"\"\"
        {
            "value": "good news everyone!"
        }
        \"\"\"

    Then get request with name "test-get-1" from endpoint "/api/test"
    And close async request group
    ```

    In this example, the `put` and `get` requests will run asynchronously, and both requests will block following requests until both are finished.
    [Async group][grizzly.tasks.async_group] tasks cannot be nested.

    Args:
        name (str): unique name for the group, used in request statistics

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    assert grizzly.scenario.tasks.tmp.async_group is None, f'async request group "{grizzly.scenario.tasks.tmp.async_group.name}" has not been closed'

    grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name=name)


@then('close async request group')
def step_task_async_group_close(context: Context) -> None:
    """Close the instance created in [Async group start][grizzly.steps.scenario.tasks.async_group.step_task_async_group_open].

    Add the [Async group][grizzly.tasks.async_group] task to the list of tasks that the scenario is going to execute.

    See [Async group][grizzly.tasks.async_group] task documentation for more information.

    Example:
    ```gherkin
    Given an async request group with name "async-group-1"
    Then post request with name "test-post-2" to endpoint "/api/test"
        \"\"\"
        {
            "value": "good news everyone!"
        }
        \"\"\"

    Then get request with name "test-get-1" from endpoint "/api/test"
    And close async request group
    ```

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    async_group = grizzly.scenario.tasks.tmp.async_group

    assert async_group is not None, 'no async request group is open'
    assert len(async_group.tasks) > 0, f'there are no request tasks in async group "{async_group.name}"'

    grizzly.scenario.tasks.tmp.async_group = None
    grizzly.scenario.tasks.add(async_group)

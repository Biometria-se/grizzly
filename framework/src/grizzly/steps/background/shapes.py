"""Module contains step implementations that describes how the load for all scenarios in a feature will look like."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

import parse
from grizzly_common.text import permutation
from locust.dispatch import UsersDispatcher as WeightedUsersDispatcher

from grizzly.testdata.utils import resolve_variable
from grizzly.types.behave import Context, given, register_type
from grizzly.utils import has_template

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext


@parse.with_pattern(r'(user[s]?)')
@permutation(vector=(False, True))
def parse_user_gramatical_number(text: str) -> str:
    return text.strip()


register_type(
    UserGramaticalNumber=parse_user_gramatical_number,
)


@given('"{value}" {grammar:UserGramaticalNumber}')
def step_shapes_user_count(context: Context, value: str, **_kwargs: Any) -> None:
    """Set number of users that will generate load.

    Example:
    ```gherkin
    Given "5" users
    Given "1" user
    Given "{{ user_count }}"
    ```

    Args:
        value (str): number of users locust should create, supports [templating][framework.usage.variables.templating] that renders to an `int`

    """
    grizzly = cast('GrizzlyContext', context.grizzly)

    if grizzly.setup.dispatcher_class is not None and grizzly.setup.dispatcher_class != WeightedUsersDispatcher:
        message = 'this step cannot be used in combination with step(s) `step_shapes_fixed_user_count*`'
        raise AssertionError(message)

    assert value[0] != '$', 'this expression does not support $conf or $env variables'
    user_count = max(int(round(float(resolve_variable(grizzly.scenario, value)), 0)), 1)

    if has_template(value):
        for scenario in grizzly.scenarios:
            scenario.orphan_templates.append(value)

    assert user_count >= 0, f'{value} resolved to {user_count} users, which is not valid'

    if grizzly.setup.spawn_rate is not None:
        assert user_count >= grizzly.setup.spawn_rate, f'spawn rate ({grizzly.setup.spawn_rate}) cannot be greater than user count ({user_count})'

    grizzly.setup.user_count = user_count
    grizzly.setup.dispatcher_class = WeightedUsersDispatcher


@given('spawn rate is "{value}" {grammar:UserGramaticalNumber} per second')
def step_shapes_spawn_rate(context: Context, value: str, **_kwargs: Any) -> None:
    """Set rate in which locust shall swarm new user instances.

    Example:
    ```gherkin
    And spawn rate is "5" users per second
    And spawn rate is "1" user per second
    And spawn rate is "0.1" users per second
    ```

    Args:
        value (str): number of users locust should create, supports [templating][framework.usage.variables.templating] that renders to a `float`

    """
    assert isinstance(value, str), f'{value} is not a string'
    assert value[0] != '$', 'this expression does not support $conf or $env variables'
    grizzly = cast('GrizzlyContext', context.grizzly)
    spawn_rate = max(float(resolve_variable(grizzly.scenario, value)), 0.01)

    if has_template(value):
        for scenario in grizzly.scenarios:
            scenario.orphan_templates.append(value)

    assert spawn_rate > 0.0, f'{value} resolved to {spawn_rate} users, which is not valid'

    if grizzly.setup.user_count is not None:
        assert int(spawn_rate) <= grizzly.setup.user_count, 'spawn rate cannot be greater than user count'

    grizzly.setup.spawn_rate = spawn_rate

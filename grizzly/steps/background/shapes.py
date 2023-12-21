"""@anchor pydoc:grizzly.steps.background.shapes Shapes
This module contains step implementations that describes how the load for all scenarios in a feature will look like.
"""
from __future__ import annotations

from typing import Any, cast

import parse

from grizzly.context import GrizzlyContext
from grizzly.testdata.utils import resolve_variable
from grizzly.types.behave import Context, given, register_type
from grizzly.utils import has_template
from grizzly_extras.text import permutation


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
        user_count (int): Number of users locust should create
        grammar (UserGramaticalNumber): one of `user`, `users`
    """
    grizzly = cast(GrizzlyContext, context.grizzly)
    assert value[0] != '$', 'this expression does not support $conf or $env variables'
    user_count = max(int(round(float(resolve_variable(grizzly, value)), 0)), 1)

    if has_template(value):
        grizzly.scenario.orphan_templates.append(value)

    assert user_count >= 0, f'{value} resolved to {user_count} users, which is not valid'

    if grizzly.setup.spawn_rate is not None:
        assert user_count >= grizzly.setup.spawn_rate, f'spawn rate ({grizzly.setup.spawn_rate}) can not be greater than user count ({user_count})'

    grizzly.setup.user_count = user_count


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
        spawn_rate (float): number of users per second
        grammar (UserGramaticalNumber): one of `user`, `users`
    """
    assert isinstance(value, str), f'{value} is not a string'
    assert value[0] != '$', 'this expression does not support $conf or $env variables'
    grizzly = cast(GrizzlyContext, context.grizzly)
    spawn_rate = max(float(resolve_variable(grizzly, value)), 0.01)

    if has_template(value):
        grizzly.scenario.orphan_templates.append(value)

    assert spawn_rate > 0.0, f'{value} resolved to {spawn_rate} users, which is not valid'

    if grizzly.setup.user_count is not None:
        assert int(spawn_rate) <= grizzly.setup.user_count, 'spawn rate can not be greater than user count'

    grizzly.setup.spawn_rate = spawn_rate

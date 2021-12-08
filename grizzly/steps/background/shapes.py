'''This module contains step implementations that describes the actual load all scenarios in a feature will generate.'''
from typing import Any, Dict, cast

import parse

from behave.runner import Context
from behave import register_type, given  # pylint: disable=no-name-in-module

from ...context import GrizzlyContext
from ...testdata.utils import resolve_variable


@parse.with_pattern(r'(user[s]?)')
def parse_user_gramatical_number(text: str) -> str:
    return text.strip()


register_type(
    UserGramaticalNumber=parse_user_gramatical_number,
)


@given(u'"{value}" {grammar:UserGramaticalNumber}')
def step_shapes_user_count(context: Context, value: str, **kwargs: Dict[str, Any]) -> None:
    '''Set number of users that will generate load.

    ```gherkin
    Given "5" users
    Given "1" user
    Given "$conf::load.user.count"
    ```

    Args:
        user_count (int): Number of users locust should create
    '''
    grizzly = cast(GrizzlyContext, context.grizzly)
    should_resolve = '{{' in value and '}}' in value or value[0] == '$'
    user_count = int(round(float(resolve_variable(grizzly, value)), 0))

    if should_resolve and user_count < 1:
        user_count = 1

    assert user_count >= 0, f'{value} resolved to {user_count} users, which is not valid'

    if grizzly.setup.spawn_rate is not None:
        assert user_count >= grizzly.setup.spawn_rate, f'spawn rate ({grizzly.setup.spawn_rate}) can not be greater than user count ({user_count})'

    grizzly.setup.user_count = user_count


@given(u'spawn rate is "{value}" {grammar:UserGramaticalNumber} per second')
def step_shapes_spawn_rate(context: Context, value: str, **kwargs: Dict[str, Any]) -> None:
    '''Set rate in which locust shall swarm new user instances.

    ```gherkin
    And spawn rate is "5" users per second
    And spawn rate is "1" user per second
    And spawn rate is "0.1" users per second
    ```

    Args:
        spawn_rate (float): number of users per second
    '''
    assert isinstance(value, str), f'{value} is not a string'
    grizzly = cast(GrizzlyContext, context.grizzly)
    should_resolve = '{{' in value and '}}' in value or value[0] == '$'
    spawn_rate = float(resolve_variable(grizzly, value))

    if should_resolve and spawn_rate < 0.01:
        spawn_rate = 0.01

    assert spawn_rate > 0.0, f'{value} resolved to {spawn_rate} users, which is not valid'

    if grizzly.setup.user_count is not None:
        assert int(spawn_rate) <= grizzly.setup.user_count, f'spawn rate can not be greater than user count'

    grizzly.setup.spawn_rate = spawn_rate

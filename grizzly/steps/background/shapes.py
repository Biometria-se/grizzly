'''This module contains step implementations that describes the actual load all scenarios in a feature will generate.'''
from typing import cast

import parse

from behave.runner import Context
from behave import register_type, given  # pylint: disable=no-name-in-module

from ...context import LocustContext


@parse.with_pattern(r'(user[s]?)')
def parse_user_gramatical_number(text: str) -> str:
    return text.strip()


register_type(
    UserGramaticalNumber=parse_user_gramatical_number,
)


@given(u'"{user_count:d}" {user_number:UserGramaticalNumber}')
def step_shapes_user_count(context: Context, user_count: int, user_number: str) -> None:
    '''Set number of users that will generate load.

    ```gherkin
    Given "5" users
    Given "1" user
    Given "$conf::load.user.count"
    ```

    Args:
        user_count (int): Number of users locust should create
    '''
    if user_count > 1:
        assert user_number == 'users', 'when user_count is greater than 1, use "users"'
    else:
        assert user_number == 'user', 'when user_count is 1, use "user"'

    context_locust = cast(LocustContext, context.locust)

    if context_locust.setup.spawn_rate is not None:
        assert user_count > context_locust.setup.spawn_rate, f'spawn rate can not be greater than user count'

    context_locust.setup.user_count = user_count


@given(u'spawn rate is "{spawn_rate:d}" {user_number:UserGramaticalNumber} per second')
def step_shapes_spawn_rate(context: Context, spawn_rate: int, user_number: str) -> None:
    '''Set rate in which locust shall swarm new user instances.

    ```gherkin
    And spawn rate is "5" users per second
    And spawn rate is "1" user per second
    ```

    Args:
        spawn_rate (int): number of users per second
    '''
    context_locust = cast(LocustContext, context.locust)
    if spawn_rate > 1:
        assert user_number == 'users', f'when user_count is greater than 1, use "users"'
    else:
        assert user_number == 'user', f'when user_count is 1, use "user"'

    if context_locust.setup.user_count is not None:
        assert spawn_rate <= context_locust.setup.user_count, f'spawn rate can not be greater than user count'

    context_locust.setup.spawn_rate = spawn_rate

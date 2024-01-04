"""@anchor pydoc:grizzly.steps.scenario.shapes Shapes
This module contains step implementations that describes how the load for this scenarios in a feature will look like.
"""
from __future__ import annotations

from typing import Any, Optional, cast

import parse
from locust.dispatch import FixedUsersDispatcher

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

def _shape_fixed_user_count(context: Context, value: str, sticky_tag: Optional[str]) -> None:
    grizzly = cast(GrizzlyContext, context.grizzly)

    if (grizzly.setup.dispatcher_class is not None and grizzly.setup.dispatcher_class != FixedUsersDispatcher) or grizzly.setup.user_count is not None:
        message = 'this step cannot be used in combination with step `step_shapes_user_count`'
        raise AssertionError(message)


    assert value[0] != '$', 'this expression does not support $conf or $env variables'
    user_count = max(int(round(float(resolve_variable(grizzly, value)), 0)), 1)

    if has_template(value):
        grizzly.scenario.orphan_templates.append(value)

    assert user_count >= 0, f'{value} resolved to {user_count} users, which is not valid'

    if grizzly.setup.spawn_rate is not None:
        assert user_count >= grizzly.setup.spawn_rate, f'spawn rate ({grizzly.setup.spawn_rate}) can not be greater than user count ({user_count})'

    grizzly.setup.dispatcher_class = FixedUsersDispatcher
    grizzly.scenario.user.fixed_count = user_count
    grizzly.scenario.user.sticky_tag = sticky_tag

@given('scenario is assigned "{value}" {grammar:UserGramaticalNumber} with tag "{sticky_tag}"')
def step_shapes_fixed_user_count_sticky_tag(context: Context, value: str, sticky_tag: str, **_kwargs: Any) -> None:
    """Set number of users that will execute the scenario, with a tag.

    Scenarios with same tag value will only spawn on the same workers. This makes it possible to isolate users of the same type
    to a set of workers when running distributed. This is required when using {@pylink grizzly.users.messagequeue} user in more than
    one scenario, which uses different certificates (due to a problem with the native libraries having more than one SSL context per process).

    When this step is used, any weight value for the user will be ignored.

    Example:
    ```gherkin
    Scenario: first
        Given scenario "5" users with tag "foobar"

    Scenario: second
        Given scenario "1" user with tag "foobar"

    Scenario: third
        Given scenario "{{ user_count }}" users
    ```

    This example would require at minimum 2 workers. Scenario `first` and `second` will run, exclusively, on the same set of workers while scenario
    `third` will run on a different set of workers.

    Args:
        user_count (int): Number of users locust should create
        grammar (UserGramaticalNumber): one of `user`, `users`
        sticky_tag (str): Unique string value that groups scenarios to a set of workers
    """
    _shape_fixed_user_count(context, value, sticky_tag)


@given('scenario is assigned "{value}" {grammar:UserGramaticalNumber}')
def step_shapes_fixed_user_count(context: Context, value: str, **_kwargs: Any) -> None:
    """Set number of users that will execute the scenario.

    When this step is used, any weight value for the user will be ignored.

    Example:
    ```gherkin
    Given scenario "5" users
    Given scenario "1" user
    Given scenario "{{ user_count }}" users
    ```

    Args:
        user_count (int): Number of users locust should create
        grammar (UserGramaticalNumber): one of `user`, `users`
    """
    _shape_fixed_user_count(context, value, None)

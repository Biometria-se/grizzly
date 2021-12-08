from typing import cast

import pytest

from parse import compile
from behave.runner import Context

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import


def test_parse_user_gramatical_number() -> None:
    p = compile(
        'we have {user:d} {user_number:UserGramaticalNumber}',
        extra_types=dict(UserGramaticalNumber=parse_user_gramatical_number),
    )

    assert p.parse('we have 1 user')['user_number'] == 'user'
    assert p.parse('we have 2 users')['user_number'] == 'users'
    assert p.parse('we have 3 people') is None


@pytest.mark.usefixtures('behave_context')
def test_step_shapes_user_count(behave_context: Context) -> None:
    step_impl = step_shapes_user_count

    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    assert grizzly.setup.user_count == 0

    step_impl(behave_context, '10', 'user')
    assert grizzly.setup.user_count == 10

    step_impl(behave_context, '10', 'users')
    assert grizzly.setup.user_count == 10

    step_impl(behave_context, '1', 'user')
    assert grizzly.setup.user_count == 1

    step_impl(behave_context, '1', 'users')
    assert grizzly.setup.user_count == 1

    grizzly.setup.spawn_rate = 10

    with pytest.raises(AssertionError):
        step_impl(behave_context, '1', 'user')

    grizzly.setup.spawn_rate = 4

    with pytest.raises(AssertionError) as ae:
        step_impl(behave_context, '{{ user_count }}', 'user')
    assert 'value contained variable "user_count" which has not been set' in str(ae)

    grizzly.state.variables['user_count'] = 5
    step_impl(behave_context, '{{ user_count }}', 'user')
    assert grizzly.setup.user_count == 5

    grizzly.setup.spawn_rate = None

    step_impl(behave_context, '{{ user_count * 0.1 }}', 'user')
    assert grizzly.setup.user_count == 1


@pytest.mark.usefixtures('behave_context')
def test_step_shapes_spawn_rate(behave_context: Context) -> None:
    step_impl = step_shapes_spawn_rate

    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    assert grizzly.setup.spawn_rate is None

    # spawn_rate must be <= user_count
    with pytest.raises(AssertionError):
        step_impl(behave_context, '1', 'user')

    grizzly.setup.user_count = 10
    step_impl(behave_context, '0.1', 'users')
    assert grizzly.setup.spawn_rate == 0.1

    step_impl(behave_context, '10', 'users')
    assert grizzly.setup.spawn_rate == 10

    grizzly.setup.user_count = 1

    with pytest.raises(AssertionError):
        step_impl(behave_context, '10', 'users')

    with pytest.raises(AssertionError) as ae:
        step_impl(behave_context, '{{ spawn_rate }}', 'users')
    assert 'value contained variable "spawn_rate" which has not been set' in str(ae)

    grizzly.setup.spawn_rate = None
    grizzly.state.variables['spawn_rate'] = 1
    step_impl(behave_context, '{{ spawn_rate }}', 'users')
    assert grizzly.setup.spawn_rate == 1.0

    step_impl(behave_context, '{{ spawn_rate / 1000 }}', 'users')
    assert grizzly.setup.spawn_rate == 0.01


from typing import cast

import pytest

from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from tests.fixtures import BehaveFixture


def test_parse_user_gramatical_number() -> None:
    p = compile(
        'we have {user:d} {user_number:UserGramaticalNumber}',
        extra_types=dict(UserGramaticalNumber=parse_user_gramatical_number),
    )

    assert parse_user_gramatical_number.__vector__ == (False, True,)

    assert p.parse('we have 1 user')['user_number'] == 'user'
    assert p.parse('we have 2 users')['user_number'] == 'users'
    assert p.parse('we have 3 people') is None


def test_step_shapes_user_count(behave_fixture: BehaveFixture) -> None:
    step_impl = step_shapes_user_count

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    assert grizzly.setup.user_count == 0

    step_impl(behave, '10', grammar='user')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '1', grammar='user')
    assert grizzly.setup.user_count == 1

    step_impl(behave, '1', grammar='users')
    assert grizzly.setup.user_count == 1

    grizzly.setup.spawn_rate = 10

    with pytest.raises(AssertionError):
        step_impl(behave, '1', grammar='user')

    grizzly.setup.spawn_rate = 4

    with pytest.raises(AssertionError) as ae:
        step_impl(behave, '{{ user_count }}', grammar='user')
    assert str(ae.value) == 'value contained variable "user_count" which has not been declared'

    grizzly.state.variables['user_count'] = 5
    step_impl(behave, '{{ user_count }}', grammar='user')
    assert grizzly.setup.user_count == 5

    grizzly.setup.spawn_rate = None

    step_impl(behave, '{{ user_count * 0.1 }}', grammar='user')
    assert grizzly.setup.user_count == 1

    with pytest.raises(AssertionError) as ae:
        step_impl(behave, '$conf::user.count', grammar='users')
    assert 'this expression does not support $conf or $env variables' == str(ae.value)


def test_step_shapes_spawn_rate(behave_fixture: BehaveFixture) -> None:
    step_impl = step_shapes_spawn_rate

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    assert grizzly.setup.spawn_rate is None

    # spawn_rate must be <= user_count
    with pytest.raises(AssertionError):
        step_impl(behave, '1', grammar='user')

    grizzly.setup.user_count = 10
    step_impl(behave, '0.1', grammar='users')
    assert grizzly.setup.spawn_rate == 0.1

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.spawn_rate == 10

    grizzly.setup.user_count = 1

    with pytest.raises(AssertionError):
        step_impl(behave, '10', grammar='users')

    with pytest.raises(AssertionError) as ae:
        step_impl(behave, '{{ spawn_rate }}', grammar='users')
    assert str(ae.value) == 'value contained variable "spawn_rate" which has not been declared'

    grizzly.setup.spawn_rate = None
    grizzly.state.variables['spawn_rate'] = 1
    step_impl(behave, '{{ spawn_rate }}', grammar='users')
    assert grizzly.setup.spawn_rate == 1.0

    step_impl(behave, '{{ spawn_rate / 1000 }}', grammar='users')
    assert grizzly.setup.spawn_rate == 0.01

    with pytest.raises(AssertionError) as ae:
        step_impl(behave, '$conf::user.rate', grammar='users')
    assert 'this expression does not support $conf or $env variables' == str(ae.value)

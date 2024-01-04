"""Unit tests of grizzly.steps.background.shapes."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from locust.dispatch import FixedUsersDispatcher, WeightedUsersDispatcher
from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.steps import *

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_parse_user_gramatical_number() -> None:
    p = compile(
        'we have {user:d} {user_number:UserGramaticalNumber}',
        extra_types={'UserGramaticalNumber': parse_user_gramatical_number},
    )

    assert parse_user_gramatical_number.__vector__ == (False, True)

    assert p.parse('we have 1 user')['user_number'] == 'user'
    assert p.parse('we have 2 users')['user_number'] == 'users'
    assert p.parse('we have 3 people') is None


def test_step_shapes_user_count(behave_fixture: BehaveFixture) -> None:
    step_impl = step_shapes_user_count

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    assert grizzly.setup.user_count is None

    step_impl(behave, '10', grammar='user')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '1', grammar='user')
    assert grizzly.setup.user_count == 1

    step_impl(behave, '1', grammar='users')
    assert grizzly.setup.user_count == 1
    assert grizzly.setup.dispatcher_class == WeightedUsersDispatcher

    grizzly.setup.dispatcher_class = FixedUsersDispatcher

    with pytest.raises(AssertionError, match='this step cannot be used in combination with'):
        step_impl(behave, '1', grammar='users')

    grizzly.setup.dispatcher_class = None
    grizzly.setup.spawn_rate = 10

    with pytest.raises(AssertionError, match=r'spawn rate \(10\) can not be greater than user count \(1\)'):
        step_impl(behave, '1', grammar='user')

    grizzly.setup.spawn_rate = 4

    with pytest.raises(AssertionError, match='value contained variable "user_count" which has not been declared'):
        step_impl(behave, '{{ user_count }}', grammar='user')

    grizzly.state.variables['user_count'] = 5
    step_impl(behave, '{{ user_count }}', grammar='user')
    assert grizzly.setup.user_count == 5

    grizzly.setup.spawn_rate = None

    step_impl(behave, '{{ user_count * 0.1 }}', grammar='user')
    assert grizzly.setup.user_count == 1
    assert grizzly.setup.dispatcher_class == WeightedUsersDispatcher

    with pytest.raises(AssertionError, match=r'this expression does not support \$conf or \$env variables'):
        step_impl(behave, '$conf::user.count', grammar='users')


def test_step_shapes_spawn_rate(behave_fixture: BehaveFixture) -> None:
    step_impl = step_shapes_spawn_rate

    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    assert grizzly.setup.spawn_rate is None

    grizzly.setup.user_count = 5

    # spawn_rate must be <= user_count
    with pytest.raises(AssertionError, match='spawn rate can not be greater than user count'):
        step_impl(behave, '10', grammar='users')

    grizzly.setup.user_count = 10
    step_impl(behave, '0.1', grammar='users')
    assert grizzly.setup.spawn_rate == 0.1

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.spawn_rate == 10

    grizzly.setup.user_count = 1

    with pytest.raises(AssertionError, match='spawn rate can not be greater than user count'):
        step_impl(behave, '10', grammar='users')

    with pytest.raises(AssertionError, match='value contained variable "spawn_rate" which has not been declared'):
        step_impl(behave, '{{ spawn_rate }}', grammar='users')

    grizzly.setup.spawn_rate = None
    grizzly.state.variables['spawn_rate'] = 1
    step_impl(behave, '{{ spawn_rate }}', grammar='users')
    assert grizzly.setup.spawn_rate == 1.0

    step_impl(behave, '{{ spawn_rate / 1000 }}', grammar='users')
    assert grizzly.setup.spawn_rate == 0.01

    with pytest.raises(AssertionError, match=r'this expression does not support \$conf or \$env variables'):
        step_impl(behave, '$conf::user.rate', grammar='users')

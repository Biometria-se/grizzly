"""Unit tests of grizzly.steps.background.shapes."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from grizzly.locust import FixedUsersDispatcher, UsersDispatcher
from grizzly.steps import *
from parse import compile as parse_compile

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_parse_user_gramatical_number() -> None:
    p = parse_compile(
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
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    assert grizzly.setup.user_count is None

    step_impl(behave, '10', grammar='user')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.user_count == 10

    step_impl(behave, '1', grammar='user')
    assert grizzly.setup.user_count == 1

    step_impl(behave, '1', grammar='users')
    assert grizzly.setup.user_count == 1
    assert grizzly.setup.dispatcher_class == UsersDispatcher

    grizzly.setup.dispatcher_class = FixedUsersDispatcher

    step_impl(behave, '1', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='this step cannot be used in combination with')]}
    delattr(behave, 'exceptions')

    grizzly.setup.dispatcher_class = None
    grizzly.setup.spawn_rate = 10

    step_impl(behave, '1', grammar='user')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='spawn rate (10) cannot be greater than user count (1)')]}
    delattr(behave, 'exceptions')

    grizzly.setup.spawn_rate = 4

    step_impl(behave, '{{ user_count }}', grammar='user')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nuser_count')]}
    delattr(behave, 'exceptions')

    grizzly.scenario.variables['user_count'] = 5
    step_impl(behave, '{{ user_count }}', grammar='user')
    assert grizzly.setup.user_count == 5

    grizzly.setup.spawn_rate = None

    step_impl(behave, '{{ user_count * 0.1 }}', grammar='user')
    assert grizzly.setup.user_count == 1
    assert grizzly.setup.dispatcher_class == UsersDispatcher

    step_impl(behave, '$conf::user.count', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='this expression does not support $conf or $env variables')]}
    delattr(behave, 'exceptions')


def test_step_shapes_spawn_rate(behave_fixture: BehaveFixture) -> None:
    step_impl = step_shapes_spawn_rate

    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    assert grizzly.setup.spawn_rate is None

    grizzly.setup.user_count = 5

    # spawn_rate must be <= user_count
    step_impl(behave, '10', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='spawn rate cannot be greater than user count')]}
    delattr(behave, 'exceptions')

    grizzly.setup.user_count = 10
    step_impl(behave, '0.1', grammar='users')
    assert grizzly.setup.spawn_rate == 0.1

    step_impl(behave, '10', grammar='users')
    assert grizzly.setup.spawn_rate == 10

    grizzly.setup.user_count = 1

    step_impl(behave, '10', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='spawn rate cannot be greater than user count')]}
    delattr(behave, 'exceptions')

    step_impl(behave, '{{ spawn_rate }}', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nspawn_rate')]}
    delattr(behave, 'exceptions')

    grizzly.setup.spawn_rate = None
    grizzly.scenario.variables['spawn_rate'] = 1
    step_impl(behave, '{{ spawn_rate }}', grammar='users')
    assert grizzly.setup.spawn_rate == 1.0

    step_impl(behave, '{{ spawn_rate / 1000 }}', grammar='users')
    assert grizzly.setup.spawn_rate == 0.01

    step_impl(behave, '$conf::user.rate', grammar='users')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='this expression does not support $conf or $env variables')]}
    delattr(behave, 'exceptions')

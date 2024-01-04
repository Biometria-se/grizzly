"""Unit tests of grizzly.steps.scenario.shapes."""
from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from locust.dispatch import FixedUsersDispatcher, WeightedUsersDispatcher
from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.steps.scenario.shapes import _shape_fixed_user_count, parse_user_gramatical_number

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


def test__shape_fixed_user_count(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert grizzly.setup.user_count == 0
    assert grizzly.setup.dispatcher_class is None

    grizzly.scenarios.create(behave_fixture.create_scenario('first'))
    _shape_fixed_user_count(behave, '10', None)
    assert grizzly.scenario.user.fixed_count == 10
    assert grizzly.scenario.user.sticky_tag is None
    assert grizzly.setup.dispatcher_class == FixedUsersDispatcher

    grizzly.scenarios.create(behave_fixture.create_scenario('second'))
    grizzly.setup.dispatcher_class = WeightedUsersDispatcher

    with pytest.raises(AssertionError, match='this step cannot be used in combination with ...'):
        _shape_fixed_user_count(behave, '10', None)

    grizzly.setup.dispatcher_class = None
    _shape_fixed_user_count(behave, '20', 'foobar')
    assert grizzly.scenario.user.fixed_count == 20
    assert grizzly.scenario.user.sticky_tag == 'foobar'
    assert grizzly.setup.dispatcher_class == FixedUsersDispatcher

    grizzly.scenarios.create(behave_fixture.create_scenario('third'))
    grizzly.state.variables['user_count'] = 5
    _shape_fixed_user_count(behave, '{{ user_count }}', 'bar')
    assert grizzly.scenario.user.fixed_count == 5
    assert grizzly.scenario.user.sticky_tag == 'bar'


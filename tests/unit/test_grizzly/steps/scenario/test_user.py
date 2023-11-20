"""Unit tests of grizzly.steps.scenario.user."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import *

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_user_type(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    step_user_type(behave, 'RestApi', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type(behave, 'ServiceBus', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    with pytest.raises(AssertionError, match='value contained variable "host" which has not been declared'):
        step_user_type(behave, 'RestApi', '{{ host }}')

    grizzly.state.variables['host'] = 'http://example.io:1337'
    step_user_type(behave, 'RestApi', '{{ host }}')

    assert grizzly.scenario.context['host'] == 'http://example.io:1337'

    try:
        environ['TARGET_HOST'] = 'http://host.docker.internal'
        step_user_type(behave, 'RestApi', '$env::TARGET_HOST$')
        assert grizzly.scenario.context['host'] == 'http://host.docker.internal'
    finally:
        with suppress(KeyError):
            del environ['TARGET_HOST']

    grizzly.state.configuration['target.host'] = 'http://conf.example.io'
    step_user_type(behave, 'RestApi', '$conf::target.host$')
    assert grizzly.scenario.context['host'] == 'http://conf.example.io'


def test_step_user_type_with_weight(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    step_user_type_with_weight(behave, 'RestApi', '1', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.user.weight == 1
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type_with_weight(behave, 'ServiceBus', '2', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.user.weight == 2
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    with pytest.raises(AssertionError, match='weight value -1 resolved to -1, which is not valid'):
        step_user_type_with_weight(behave, 'RestApi', '-1', 'http://localhost:8000')

    with pytest.raises(AssertionError, match='weight value 0 resolved to 0, which is not valid'):
        step_user_type_with_weight(behave, 'RestApi', '0', 'http://localhost:8000')

    with pytest.raises(AssertionError, match='value contained variable "weight" which has not been declared'):
        step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')

    grizzly.state.variables['weight'] = 3
    step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')
    assert grizzly.scenario.user.weight == 3

    grizzly.state.variables['weight'] = 0
    with pytest.raises(AssertionError, match='weight value {{ weight }} resolved to 0, which is not valid'):
        step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')

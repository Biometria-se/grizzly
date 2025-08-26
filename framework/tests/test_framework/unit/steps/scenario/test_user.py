"""Unit tests of grizzly.steps.scenario.user."""

from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.locust import FixedUsersDispatcher
from grizzly.steps import *
from grizzly.steps.scenario.user import _setup_user

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test__setup_user_validation(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError, match='cannot combine fixed user count with user weights'):
        _setup_user(behave, 'Dummy', 'dummy://foobar', weight='200', user_count='200')

    grizzly.setup.user_count = 200

    with pytest.raises(AssertionError, match='this step cannot be used in combination with step'):
        _setup_user(behave, 'Dummy', 'dummy://foobar', user_count='200')


def test_step_user_type_with_count_and_tag(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    step_user_type_with_count_and_tag(behave, '100', 'RestApi', 'foobar', 'http://localhost:8000', _grammar='users')

    assert grizzly.setup.dispatcher_class == FixedUsersDispatcher
    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'
    assert grizzly.scenario.user.fixed_count == 100
    assert grizzly.scenario.user.sticky_tag == 'foobar'
    assert grizzly.scenario.user.weight == 1


def test_setup_user_type_with_count(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context
    grizzly.scenario.variables['max_users'] = '10'

    step_user_type_with_count(behave, '{{ max_users * 0.1 }}', 'ServiceBus', 'sb://localhost:8000', _grammar='user')

    assert grizzly.setup.dispatcher_class == FixedUsersDispatcher
    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.context['host'] == 'sb://localhost:8000'
    assert grizzly.scenario.user.fixed_count == 1
    assert grizzly.scenario.user.sticky_tag is None
    assert grizzly.scenario.user.weight == 1


def test_step_user_type(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    assert behave.exceptions == {}

    step_user_type(behave, 'RestApi', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type(behave, 'ServiceBus', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'
    assert grizzly.scenario.user.fixed_count is None
    assert grizzly.scenario.user.sticky_tag is None
    assert grizzly.setup.dispatcher_class is None

    step_user_type(behave, 'RestApi', '{{ host }}')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nhost')]}

    grizzly.scenario.variables['host'] = 'http://example.io:1337'
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
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context
    assert behave.exceptions == {}

    step_user_type_with_weight(behave, 'RestApi', '1', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.user.weight == 1
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type_with_weight(behave, 'ServiceBus', '2', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.user.weight == 2
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type_with_weight(behave, 'RestApi', '-1', 'http://localhost:8000')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='weight value -1 resolved to -1, which is not valid')]}

    step_user_type_with_weight(behave, 'RestApi', '0', 'http://localhost:8000')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='weight value -1 resolved to -1, which is not valid'),
            ANY(AssertionError, message='weight value 0 resolved to 0, which is not valid'),
        ],
    }

    step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='weight value -1 resolved to -1, which is not valid'),
            ANY(AssertionError, message='weight value 0 resolved to 0, which is not valid'),
            ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nweight'),
        ],
    }

    grizzly.scenario.variables['weight'] = 3
    step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')
    assert grizzly.scenario.user.weight == 3

    grizzly.scenario.variables['weight'] = 0
    step_user_type_with_weight(behave, 'RestApi', '{{ weight }}', 'http://localhost:8000')

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='weight value -1 resolved to -1, which is not valid'),
            ANY(AssertionError, message='weight value 0 resolved to 0, which is not valid'),
            ANY(AssertionError, message='variables have been found in templates, but have not been declared:\nweight'),
            ANY(AssertionError, message='weight value {{ weight }} resolved to 0, which is not valid'),
        ],
    }

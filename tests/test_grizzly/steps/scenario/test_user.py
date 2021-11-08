from typing import cast
from os import environ

import pytest

from behave.runner import Context

from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.context import GrizzlyContext

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import


@pytest.mark.usefixtures('behave_context')
def test_step_user_type(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    step_user_type(behave_context, 'RestApi', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type(behave_context, 'ServiceBus', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    with pytest.raises(AssertionError):
        step_user_type(behave_context, 'RestApi', '{{ host }}')

    grizzly.state.variables['host'] = 'http://test.ru:1337'
    step_user_type(behave_context, 'RestApi', '{{ host }}')

    assert grizzly.scenario.context['host'] == 'http://test.ru:1337'

    try:
        environ['TARGET_HOST'] = 'http://host.docker.internal'
        step_user_type(behave_context, 'RestApi', '$env::TARGET_HOST')
        assert grizzly.scenario.context['host'] == 'http://host.docker.internal'
    finally:
        try:
            del environ['TARGET_HOST']
        except KeyError:
            pass

    grizzly.state.configuration['target.host'] = 'http://conf.host.nu'
    step_user_type(behave_context, 'RestApi', '$conf::target.host')
    assert grizzly.scenario.context['host'] == 'http://conf.host.nu'

@pytest.mark.usefixtures('behave_context')
def test_step_user_type_with_weight(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    assert hasattr(grizzly.scenario, 'user')
    assert not hasattr(grizzly.scenario.user, 'class_name')
    assert 'host' not in grizzly.scenario.context

    step_user_type_with_weight(behave_context, 'RestApi', '1', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'RestApiUser'
    assert grizzly.scenario.user.weight == 1
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    step_user_type_with_weight(behave_context, 'ServiceBus', '2', 'http://localhost:8000')

    assert grizzly.scenario.user.class_name == 'ServiceBusUser'
    assert grizzly.scenario.user.weight == 2
    assert grizzly.scenario.context['host'] == 'http://localhost:8000'

    with pytest.raises(AssertionError):
        step_user_type_with_weight(behave_context, 'RestApi', '-1', 'http://localhost:8000')

    with pytest.raises(AssertionError):
        step_user_type_with_weight(behave_context, 'RestApi', '0', 'http://localhost:8000')

    with pytest.raises(AssertionError):
        step_user_type_with_weight(behave_context, 'RestApi', '{{ weight }}', 'http://localhost:8000')

    grizzly.state.variables['weight'] = 3
    step_user_type_with_weight(behave_context, 'RestApi', '{{ weight }}', 'http://localhost:8000')
    assert grizzly.scenario.user.weight == 3

    grizzly.state.variables['weight'] = 0
    with pytest.raises(AssertionError):
        step_user_type_with_weight(behave_context, 'RestApi', '{{ weight }}', 'http://localhost:8000')

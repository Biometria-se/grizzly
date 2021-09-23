from typing import cast
from os import environ

import pytest

from behave.runner import Context

from grizzly.steps import *  # pylint: disable=unused-wildcard-import
from grizzly.context import LocustContext

from ...fixtures import behave_context


@pytest.mark.usefixtures('behave_context')
def test_step_user_type(behave_context: Context) -> None:
    context_locust = cast(LocustContext, behave_context.locust)

    assert not hasattr(context_locust.scenario, 'user_class_name')
    assert 'host' not in context_locust.scenario.context

    step_user_type(behave_context, 'RestApi', 'http://localhost:8000')

    assert context_locust.scenario.user_class_name == 'RestApiUser'
    assert context_locust.scenario.context['host'] == 'http://localhost:8000'

    step_user_type(behave_context, 'ServiceBus', 'http://localhost:8000')

    assert context_locust.scenario.user_class_name == 'ServiceBusUser'
    assert context_locust.scenario.context['host'] == 'http://localhost:8000'

    with pytest.raises(AssertionError):
        step_user_type(behave_context, 'RestApi', '{{ host }}')

    context_locust.state.variables['host'] = 'http://test.ru:1337'
    step_user_type(behave_context, 'RestApi', '{{ host }}')

    assert context_locust.scenario.context['host'] == 'http://test.ru:1337'

    try:
        environ['TARGET_HOST'] = 'http://host.docker.internal'
        step_user_type(behave_context, 'RestApi', '$env::TARGET_HOST')
        assert context_locust.scenario.context['host'] == 'http://host.docker.internal'
    finally:
        try:
            del environ['TARGET_HOST']
        except KeyError:
            pass

    context_locust.state.configuration['target.host'] = 'http://conf.host.nu'
    step_user_type(behave_context, 'RestApi', '$conf::target.host')
    assert context_locust.scenario.context['host'] == 'http://conf.host.nu'

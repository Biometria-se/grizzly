"""Unit tests of grizzly.steps.scenario.tasks.request."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest
from grizzly.steps import (
    step_task_request_file_with_name,
    step_task_request_file_with_name_endpoint,
    step_task_request_text_with_name,
    step_task_request_text_with_name_endpoint,
)
from grizzly.tasks import RequestTask
from grizzly.types import RequestDirection, RequestMethod
from parse import compile as parse_compile

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture


def test_parse_method() -> None:
    p = parse_compile(
        'value {method:Method} world',
        extra_types={
            'Method': RequestMethod.from_string,
        },
    )

    assert RequestMethod.get_vector() == (False, True)

    for method in RequestMethod:
        actual = p.parse(f'value {method.name.lower()} world')['method']
        assert actual == method

    with pytest.raises(AssertionError, match='"ASDF" is not a valid value of RequestMethod'):
        p.parse('value asdf world')


def test_parse_direction() -> None:
    p = parse_compile(
        'value {direction:Direction} world',
        extra_types={
            'Direction': RequestDirection.from_string,
        },
    )

    assert RequestDirection.get_vector() == (False, True)

    for direction in RequestDirection:
        actual = p.parse(f'value {direction.name} world')['direction']
        assert actual == direction

    with pytest.raises(AssertionError, match='"ASDF" is not a valid value of RequestDirection'):
        p.parse('value asdf world')


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_file_with_name_endpoint(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_file_with_name_endpoint_wrong_direction(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'{method.name} is not allowed')]}
    delattr(behave, 'exceptions')


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_file_with_name(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no endpoint specified')]}
    delattr(behave, 'exceptions')

    step_task_request_file_with_name_endpoint(behave, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
    step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_file_with_name_wrong_direction(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'{method.name} is not allowed')]}
    delattr(behave, 'exceptions')


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_text_with_name_endpoint_to(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave.text = '{}'

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, RequestTask)
    assert task.source == '{}'

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'"from endpoint" is not allowed for {method.name}, use "to endpoint"')]}
    delattr(behave, 'exceptions')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_endpoint_from(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave.text = '{}'

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'step text is not allowed for {method.name}')]}
    delattr(behave, 'exceptions')

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'step text is not allowed for {method.name}')]}
    delattr(behave, 'exceptions')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_endpoint_no_text(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave
    behave.text = None

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message=f'"to endpoint" is not allowed for {method.name}, use "from endpoint"')]}
    delattr(behave, 'exceptions')


def test_step_task_request_text_with_name_endpoint_no_direction(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_task_request_text_with_name_endpoint(behave, 'GET', 'test-name', 'asdf', '/api/test')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='invalid direction specified in expression')]}
    delattr(behave, 'exceptions')


def test_step_task_request_text_with_name(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    behave.text = '{}'

    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')
    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='no endpoint specified')]}

    step_task_request_text_with_name_endpoint(behave, RequestMethod.POST, 'test-name', RequestDirection.TO, '/api/test')

    behave.text = None
    step_task_request_text_with_name(behave, RequestMethod.GET, 'test-name')
    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='no endpoint specified'),
            ANY(AssertionError, message='cannot use endpoint from previous request, it has a different request method'),
        ],
    }

    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')
    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='no endpoint specified'),
            ANY(AssertionError, message='cannot use endpoint from previous request, it has a different request method'),
            ANY(AssertionError, message='step text is mandatory for POST'),
        ],
    }

    behave.text = '{}'
    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

from typing import cast

import pytest

from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, RequestDirection
from grizzly.steps import (
    step_task_request_file_with_name_endpoint,
    step_task_request_file_with_name,
    step_task_request_text_with_name_endpoint,
    step_task_request_text_with_name,
)

from tests.fixtures import BehaveFixture


def test_parse_method() -> None:
    p = compile(
        'value {method:Method} world',
        extra_types=dict(
            Method=RequestMethod.from_string,
        ),
    )

    assert RequestMethod.get_vector() == (False, True,)

    for method in RequestMethod:
        actual = p.parse(f'value {method.name.lower()} world')['method']
        assert actual == method

    with pytest.raises(ValueError):
        p.parse('value asdf world')


def test_parse_direction() -> None:
    p = compile(
        'value {direction:Direction} world',
        extra_types=dict(
            Direction=RequestDirection.from_string,
        ),
    )

    assert RequestDirection.get_vector() == (False, True,)

    for direction in RequestDirection:
        actual = p.parse(f'value {direction.name} world')['direction']
        assert actual == direction

    with pytest.raises(ValueError):
        p.parse('value asdf world')


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_file_with_name_endpoint(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_file_with_name_endpoint_wrong_direction(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    with pytest.raises(AssertionError) as ae:
        step_task_request_file_with_name_endpoint(behave, method, '{}', 'the_name', 'the_container')
    assert f'{method.name} is not allowed' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_file_with_name(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(ValueError):
        step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')

    step_task_request_file_with_name_endpoint(behave, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
    step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_file_with_name_wrong_direction(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    with pytest.raises(AssertionError) as ae:
        # step_request_to_payload_file_with_name_endpoint(behave, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
        step_task_request_file_with_name(behave, method, '{}', f'{method.name}-test')
    assert f'{method.name} is not allowed' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_text_with_name_endpoint_to(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.text = '{}'

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert f'"from endpoint" is not allowed for {method.name}, use "to endpoint"' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_endpoint_from(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert f'step text is not allowed for {method.name}' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert f'step text is not allowed for {method.name}' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_endpoint_no_text(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.text = None

    step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert f'"to endpoint" is not allowed for {method.name}, use "from endpoint"' in str(ae)


def test_step_task_request_text_with_name_endpoint_no_direction(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_endpoint(behave, 'GET', 'test-name', 'asdf', '/api/test')
    assert 'invalid direction specified in expression' in str(ae)


def test_step_task_request_text_with_name(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    behave.text = '{}'

    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    step_task_request_text_with_name_endpoint(behave, RequestMethod.POST, 'test-name', RequestDirection.TO, '/api/test')

    behave.text = None
    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.GET, 'test-name')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    behave.text = '{}'
    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

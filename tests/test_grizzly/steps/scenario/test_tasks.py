import pytest

from behave.runner import Context
from parse import compile

from grizzly.types import RequestMethod
from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context  # pylint: disable=unused-import


def test_parse_method() -> None:
    p = compile(
        'value {method:Method} world',
        extra_types=dict(
            Method=parse_method,
        ),
    )

    for method in RequestMethod:
        assert p.parse(f'value {method.name} world')['method'] == method

    with pytest.raises(ValueError):
        p.parse('value asdf world')


def test_parse_direction() -> None:
    p = compile(
        'value {direction:Direction} world',
        extra_types=dict(
            Direction=parse_direction,
        ),
    )

    for direction in RequestDirection:
        assert p.parse(f'value {direction.name} world')['direction'] == direction

    with pytest.raises(ValueError):
        p.parse('value asdf world')


@pytest.mark.usefixtures('behave_context')
def test_step_task_request_file_with_name_endpoint(behave_context: Context) -> None:
    for method in RequestDirection.TO.methods:
        step_task_request_file_with_name_endpoint(behave_context, method, '{}', 'the_name', 'the_container')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            step_task_request_file_with_name_endpoint(behave_context, method, '{}', 'the_name', 'the_container')


@pytest.mark.usefixtures('behave_context')
def test_step_task_request_file_with_name(behave_context: Context) -> None:
    for method in RequestDirection.TO.methods:
        with pytest.raises(ValueError):
            step_task_request_file_with_name(behave_context, method, '{}', f'{method.name}-test')

    for method in RequestDirection.TO.methods:
        step_task_request_file_with_name_endpoint(behave_context, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
        step_task_request_file_with_name(behave_context, method, '{}', f'{method.name}-test')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            # step_request_to_payload_file_with_name_endpoint(behave_context, method, '{}', f'{method.name}-test', f'/api/test/{method.name.lower()}')
            step_task_request_file_with_name(behave_context, method, '{}', f'{method.name}-test')



@pytest.mark.usefixtures('behave_context')
def test_step_task_request_text_with_name_to_endpoint(behave_context: Context) -> None:
    behave_context.text = '{}'

    for method in RequestDirection.TO.methods:
        step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.TO, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.FROM, '/api/test')

    for method in RequestDirection.FROM.methods:
        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.TO, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.FROM, '/api/test')

    behave_context.text = None

    for method in RequestDirection.FROM.methods:
        step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.FROM, '/api/test')

        with pytest.raises(AssertionError):
            step_task_request_text_with_name_to_endpoint(behave_context, method, 'test-name', RequestDirection.TO, '/api/test')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name_to_endpoint(behave_context, 'GET', 'test-name', 'asdf', '/api/test')


def test_step_task_request_text_with_name(behave_context: Context) -> None:
    behave_context.text = '{}'

    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave_context, RequestMethod.POST, 'test-name')

    step_task_request_text_with_name_to_endpoint(behave_context, RequestMethod.POST, 'test-name', RequestDirection.TO, '/api/test')

    behave_context.text = None
    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave_context, RequestMethod.GET, 'test-name')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name(behave_context, RequestMethod.POST, 'test-name')

    behave_context.text = '{}'
    step_task_request_text_with_name(behave_context, RequestMethod.POST, 'test-name')


@pytest.mark.usefixtures('behave_context')
def test_step_task_wait_seconds(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    with pytest.raises(AssertionError):
        step_task_wait_seconds(behave_context, -1.0)

    step_task_wait_seconds(behave_context, 1.337)

    assert isinstance(grizzly.scenario.tasks[-1], SleepTask)
    assert grizzly.scenario.tasks[-1].sleep == 1.337


@pytest.mark.usefixtures('behave_context')
def test_step_print_message(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    step_task_print_message(behave_context, 'hello {{ world }}')

    assert isinstance(grizzly.scenario.tasks[-1], PrintTask)
    assert grizzly.scenario.tasks[-1].message == 'hello {{ world }}'

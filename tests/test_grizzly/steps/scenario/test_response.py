from typing import Generator, cast

import pytest

from parse import compile
from behave.runner import Context
from behave.model import Table, Row

from grizzly.types import RequestMethod, str_response_content_type
from grizzly.task import RequestTask, SleepTask
from grizzly.steps import *  # pylint: disable=unused-wildcard-import

from ...fixtures import behave_context, locust_environment  # pylint: disable=unused-import

@pytest.fixture
def request_task_context(behave_context: Context) -> Generator[Context, None, None]:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
    grizzly.scenario.tasks.append(request)

    yield behave_context

    behave_context.grizzly.destroy()


def test_parse_negative() -> None:
    p = compile(
        'value {condition:Condition} world',
        extra_types=dict(
            Condition=parse_condition,
        ),
    )

    assert p.parse('value is world')['condition'] == True
    assert p.parse('value is not world')['condition'] == False
    assert p.parse('value equals world') is None


def test_parse_response_target() -> None:
    p = compile(
        'save response {target:ResponseTarget}',
        extra_types=dict(
            ResponseTarget=parse_response_target,
        ),
    )
    assert p.parse('save response metadata')['target'] == ResponseTarget.METADATA
    assert p.parse('save response payload')['target'] == ResponseTarget.PAYLOAD
    assert p.parse('save response test') is None

    with pytest.raises(ValueError):
        parse_response_target('asdf')


def test_parse_response_content_type() -> None:
    p = compile(
        'content type is "{content_type:ResponseContentType}"',
        extra_types=dict(
            ResponseContentType=str_response_content_type,
        ),
    )

    tests = [
        (ResponseContentType.JSON, ['json', 'application/json']),
        (ResponseContentType.XML, ['xml', 'application/xml']),
        (ResponseContentType.PLAIN, ['plain', 'text/plain']),
    ]

    for test_type, values in tests:
        for value in values:
            assert p.parse(f'content type is "{value}"')['content_type'] == test_type

    with pytest.raises(ValueError) as e:
        p.parse('content type is "image/png"')
    assert 'is an unknown response content type' in str(e)


def test_step_response_save_matches_metadata(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_save_matches(request_task_context, ResponseTarget.METADATA, '', '', '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError):
        step_response_save_matches(request_task_context, ResponseTarget.METADATA, '$.test.value', '.*ary$', 'test')

    try:
        grizzly.state.variables['test'] = 'none'
        step_response_save_matches(request_task_context, ResponseTarget.METADATA, '$.test.value', '.*ary$', 'test')

        assert len(request.response.handlers.metadata) == 1
        assert len(request.response.handlers.payload) == 0
    finally:
        del grizzly.state.variables['test']


def test_step_response_save_matches_payload(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_save_matches(request_task_context, ResponseTarget.PAYLOAD, '', '', '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError):
        step_response_save_matches(request_task_context, ResponseTarget.PAYLOAD, '$.test.value', '.*ary$', 'test')

    try:
        grizzly.state.variables['test'] = 'none'

        step_response_save_matches(request_task_context, ResponseTarget.PAYLOAD, '$.test.value', '.*ary$', 'test')
        assert len(request.response.handlers.metadata) == 0
        assert len(request.response.handlers.payload) == 1
    finally:
        del grizzly.state.variables['test']


def test_step_response_save_metadata(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_save(request_task_context, ResponseTarget.METADATA, '', '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError):
        step_response_save(request_task_context, ResponseTarget.METADATA, '$.test.value', 'test')

    try:
        grizzly.state.variables['test'] = 'none'

        step_response_save(request_task_context, ResponseTarget.METADATA, '$.test.value', 'test')
        assert len(request.response.handlers.metadata) == 1
        assert len(request.response.handlers.payload) == 0
    finally:
        del grizzly.state.variables['test']


def test_step_response_save_payload(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_save(request_task_context, ResponseTarget.PAYLOAD, '', '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError):
        step_response_save(request_task_context, ResponseTarget.PAYLOAD, '$.test.value', 'test')

    try:
        grizzly.state.variables['test'] = 'none'

        step_response_save(request_task_context, ResponseTarget.PAYLOAD, '$.test.value', 'test')
        assert len(request.response.handlers.metadata) == 0
        assert len(request.response.handlers.payload) == 1
    finally:
        del grizzly.state.variables['test']


def test_step_response_validate_metadata(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_validate(request_task_context, ResponseTarget.METADATA, '', True, '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    step_response_validate(request_task_context, ResponseTarget.METADATA, '$.test.value', True, '.*test')
    assert len(request.response.handlers.metadata) == 1
    assert len(request.response.handlers.payload) == 0


def test_step_response_validate_payload(request_task_context: Context) -> None:
    grizzly = cast(GrizzlyContext, request_task_context.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks[0])

    with pytest.raises(ValueError):
        step_response_validate(request_task_context, ResponseTarget.PAYLOAD, '', True, '')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    step_response_validate(request_task_context, ResponseTarget.PAYLOAD, '$.test.value', True, '.*test')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 1


@pytest.mark.usefixtures('behave_context')
def test_step_response_allow_status_codes(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)
    with pytest.raises(AssertionError):
        step_response_allow_status_codes(behave_context, '-200')

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.add_scenario('test')
    grizzly.scenario.add_task(request)

    step_response_allow_status_codes(behave_context, '-200')
    assert request.response.status_codes == []

    step_response_allow_status_codes(behave_context, '200,302')
    assert request.response.status_codes == [200, 302]


@pytest.mark.usefixtures('behave_context')
def test_step_response_allow_status_codes_table(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    with pytest.raises(AssertionError):
        step_response_allow_status_codes_table(behave_context)

    rows: List[Row] = []
    rows.append(Row(['test'], ['-200,400']))
    rows.append(Row(['test'], ['302']))
    behave_context.table = Table(['test'], rows=rows)

    with pytest.raises(AssertionError):
        step_response_allow_status_codes_table(behave_context)

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.add_scenario('test')
    grizzly.scenario.add_task(request)

    # more rows in data table then there are requests
    with pytest.raises(AssertionError):
        step_response_allow_status_codes_table(behave_context)

    request = RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/test')
    grizzly.scenario.add_task(request)


    # data table column "code" does not exist
    with pytest.raises(AssertionError):
        step_response_allow_status_codes_table(behave_context)

    request = RequestTask(RequestMethod.GET, name='no-code', endpoint='/api/test')
    grizzly.scenario.tasks.insert(0, request)

    rows = []
    '''
    | status   |
    | -200,400 | # name=test
    | 302      | # name=test-get
    '''
    column_name = 'status'
    rows.append(Row([column_name], ['-200,400']))
    rows.append(Row([column_name], ['302']))
    behave_context.table = Table([column_name], rows=rows)

    step_response_allow_status_codes_table(behave_context)
    assert cast(RequestTask, grizzly.scenario.tasks[0]).response.status_codes == [200]
    assert cast(RequestTask, grizzly.scenario.tasks[1]).response.status_codes == [400]
    assert cast(RequestTask, grizzly.scenario.tasks[2]).response.status_codes == [200, 302]


@pytest.mark.usefixtures('behave_context')
def test_step_response_content_type(behave_context: Context) -> None:
    grizzly = cast(GrizzlyContext, behave_context.grizzly)

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave_context, ResponseContentType.JSON)
    assert 'There are no requests in the scenario' in str(ae)

    grizzly.scenario.add_task(SleepTask(sleep=1.0))

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave_context, ResponseContentType.JSON)
    assert 'Latest task in scenario is not a request' in str(ae)

    request = RequestTask(RequestMethod.POST, 'test-request', endpoint='queue:INCOMMING.MESSAGE')

    assert request.response.content_type == ResponseContentType.GUESS

    grizzly.scenario.add_task(request)

    for content_type in ResponseContentType:
        if content_type == ResponseContentType.GUESS:
            continue
        step_response_content_type(behave_context, content_type)
        assert request.response.content_type == content_type

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave_context, ResponseContentType.GUESS)
    assert 'It is now allowed to set GUESS with this step' in str(ae)

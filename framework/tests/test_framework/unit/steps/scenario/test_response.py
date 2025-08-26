"""Unit tests of grizzly.steps.scenario.response."""

from __future__ import annotations

from itertools import product
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.steps import *
from grizzly.tasks import ExplicitWaitTask, RequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestMethod, ResponseTarget, StrDict
from grizzly.types.behave import Row, Table
from grizzly_common.transformer import TransformerContentType
from parse import compile as parse_compile

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture


def test_parse_condition() -> None:
    p = parse_compile(
        'value {condition:Condition} world',
        extra_types={
            'Condition': parse_condition,
        },
    )

    assert parse_condition.__vector__ == (False, True)

    assert p.parse('value is world')['condition']
    assert not p.parse('value is not world')['condition']
    assert p.parse('value equals world') is None


def test_parse_response_target() -> None:
    p = parse_compile(
        'save response {target:ResponseTarget}',
        extra_types={
            'ResponseTarget': ResponseTarget.from_string,
        },
    )
    assert ResponseTarget.get_vector() == (False, True)
    actual = p.parse('save response metadata')['target']
    assert actual == ResponseTarget.METADATA
    actual = p.parse('save response payload')['target']
    assert actual == ResponseTarget.PAYLOAD

    with pytest.raises(AssertionError, match='"TEST" is not a valid value of ResponseTarget'):
        p.parse('save response test')


def test_parse_response_content_type() -> None:
    p = parse_compile(
        'content type is "{content_type:TransformerContentType}"',
        extra_types={
            'TransformerContentType': TransformerContentType.from_string,
        },
    )

    assert TransformerContentType.get_vector() == (False, True)

    tests = [
        (TransformerContentType.JSON, ['json', 'application/json']),
        (TransformerContentType.XML, ['xml', 'application/xml']),
        (TransformerContentType.PLAIN, ['plain', 'text/plain']),
    ]

    for test_type, values in tests:
        for value in values:
            actual = p.parse(f'content type is "{value}"')['content_type']
            assert actual == test_type

    with pytest.raises(ValueError, match='is an unknown response content type'):
        p.parse('content type is "image/png"')


@pytest.mark.parametrize(
    ('response_target', 'step_impl'),
    product(ResponseTarget, [step_response_save_matches, step_response_save_matches_optional, step_response_save, step_response_save_optional]),
)
def test_step_response_save(grizzly_fixture: GrizzlyFixture, response_target: ResponseTarget, step_impl: Callable[..., None]) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly
    request = cast('RequestTask', grizzly.scenario.tasks()[0])
    kwargs: StrDict = {
        'context': behave,
        'target': response_target,
        'expression': '$.test.value',
        'variable': '',
    }

    if step_impl.__name__.replace('_optional', '') == 'step_response_save_matches':
        kwargs.update({'match_with': '.*ary$'})

    if step_impl.__name__.endswith('_optional'):
        kwargs.update({'default_value': 'foobar'})

    assert behave.exceptions == {}

    step_impl(**kwargs)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='variable "" has not been declared')]}

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    kwargs.update({'variable': 'test'})

    step_impl(**kwargs)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='variable "" has not been declared'),
            ANY(AssertionError, message='variable "test" has not been declared'),
        ],
    }

    try:
        grizzly.scenario.variables['test'] = 'none'

        step_impl(**kwargs)

        assert behave.exceptions == {
            behave.scenario.name: [
                ANY(AssertionError, message='variable "" has not been declared'),
                ANY(AssertionError, message='variable "test" has not been declared'),
                ANY(AssertionError, message='content type is not set for latest request'),
            ],
        }

        request.response.content_type = TransformerContentType.JSON
        step_impl(**kwargs)

        handlers_metadata_count = 1 if response_target == ResponseTarget.METADATA else 0
        handlers_payload_count = 0 if response_target == ResponseTarget.METADATA else 1

        assert len(request.response.handlers.metadata) == handlers_metadata_count
        assert len(request.response.handlers.payload) == handlers_payload_count
    finally:
        request.response.content_type = TransformerContentType.UNDEFINED
        del grizzly.scenario.variables['test']


@pytest.mark.parametrize('response_target', ResponseTarget)
def test_step_response_validate(grizzly_fixture: GrizzlyFixture, response_target: ResponseTarget) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    request = cast('RequestTask', grizzly.scenario.tasks()[0])

    assert behave.exceptions == {}

    step_response_validate(behave, response_target, '', True, '')  # noqa: FBT003

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='expression is empty')]}

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    step_response_validate(behave, response_target, '$.test.value', True, '.*test')  # noqa: FBT003

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='expression is empty'),
            ANY(AssertionError, message='content type is not set for latest request'),
        ],
    }

    request.response.content_type = TransformerContentType.JSON
    step_response_validate(behave, response_target, '$.test.value', True, '.*test')  # noqa: FBT003

    handlers_metadata_count = 1 if response_target == ResponseTarget.METADATA else 0
    handlers_payload_count = 0 if response_target == ResponseTarget.METADATA else 1

    assert len(request.response.handlers.metadata) == handlers_metadata_count
    assert len(request.response.handlers.payload) == handlers_payload_count


@pytest.mark.parametrize(
    'request_type',
    [
        RequestTask,
        HttpClientTask,
    ],
)
def test_step_response_allow_status_codes(grizzly_fixture: GrizzlyFixture, request_type: type[RequestTask | HttpClientTask]) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    behave.scenario = grizzly.scenario.behave
    grizzly.scenario.tasks.clear()

    if request_type is RequestTask:
        request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    else:
        task_cls = type('HttpClientTaskTest', (HttpClientTask,), {'__scenario__': grizzly.scenario})
        request = task_cls(RequestDirection.TO, 'http://example.org', 'test', source='foobar')

    assert behave.exceptions == {}

    step_response_allow_status_codes(behave, '-200')

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='there are no requests in the scenario')]}

    grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test'))
    grizzly.scenario.tasks.add(request)

    step_response_allow_status_codes(behave, '-200')
    assert request.response.status_codes == []

    step_response_allow_status_codes(behave, '200,302')
    assert request.response.status_codes == [200, 302]

    grizzly.scenario.tasks.add(LogMessageTask(message='hello world'))
    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name='conditional',
        condition='{{ value | int == 0}}',
    )
    grizzly.scenario.tasks.tmp.conditional.switch(pointer=True)

    grizzly.scenario.tasks.add(request)
    step_response_allow_status_codes(behave, '-200,-302,500')
    assert request.response.status_codes == [500]


def test_step_response_allow_status_codes_table(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    step_response_allow_status_codes_table(behave)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='step table is missing')]}

    rows: list[Row] = []
    rows.append(Row(['test'], ['-200,400']))
    rows.append(Row(['test'], ['302']))
    behave.table = Table(['test'], rows=rows)

    step_response_allow_status_codes_table(behave)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='step table is missing'),
            ANY(AssertionError, message='there are no request tasks in the scenario'),
        ],
    }

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))
    grizzly.scenario.tasks.add(request)

    # more rows in data table then there are requests
    step_response_allow_status_codes_table(behave)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='step table is missing'),
            ANY(AssertionError, message='there are no request tasks in the scenario'),
            ANY(AssertionError, message='step table has more rows than there are request tasks'),
        ],
    }

    request = RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)

    # step table column "code" does not exist
    step_response_allow_status_codes_table(behave)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='step table is missing'),
            ANY(AssertionError, message='there are no request tasks in the scenario'),
            ANY(AssertionError, message='step table has more rows than there are request tasks'),
            ANY(AssertionError, message='step table does not have column "status"'),
        ],
    }

    request = RequestTask(RequestMethod.GET, name='no-code', endpoint='/api/test')
    grizzly.scenario.tasks().insert(0, request)

    rows = []
    """
    Create the following table:
    | status   |
    | -200,400 | # name=test
    | 302      | # name=test-get
    """
    column_name = 'status'
    rows.append(Row([column_name], ['-200,400']))
    rows.append(Row([column_name], ['302']))
    behave.table = Table([column_name], rows=rows)

    step_response_allow_status_codes_table(behave)
    assert cast('RequestTask', grizzly.scenario.tasks()[0]).response.status_codes == [200]
    assert cast('RequestTask', grizzly.scenario.tasks()[1]).response.status_codes == [400]
    assert cast('RequestTask', grizzly.scenario.tasks()[2]).response.status_codes == [200, 302]

    grizzly.scenario.tasks.clear()

    grizzly.scenario.tasks.add(LogMessageTask(message='hello world'))
    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name='conditional',
        condition='{{ value | int == 0}}',
    )
    grizzly.scenario.tasks.tmp.conditional.switch(pointer=True)

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)
    request = RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)
    request = RequestTask(RequestMethod.GET, name='no-code', endpoint='/api/test')
    grizzly.scenario.tasks().insert(0, request)

    step_response_allow_status_codes_table(behave)
    assert cast('RequestTask', grizzly.scenario.tasks()[0]).response.status_codes == [200]
    assert cast('RequestTask', grizzly.scenario.tasks()[1]).response.status_codes == [400]
    assert cast('RequestTask', grizzly.scenario.tasks()[2]).response.status_codes == [200, 302]


def test_step_response_content_type(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    behave.scenario = grizzly.scenario.behave

    assert behave.exceptions == {}

    step_response_content_type(behave, TransformerContentType.JSON)

    assert behave.exceptions == {behave.scenario.name: [ANY(AssertionError, message='there are no tasks in the scenario')]}

    grizzly.scenario.tasks.add(ExplicitWaitTask(time_expression='1.0'))

    step_response_content_type(behave, TransformerContentType.JSON)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='there are no tasks in the scenario'),
            ANY(AssertionError, message='latest task in scenario is not a request task'),
        ],
    }

    request: RequestTask = RequestTask(RequestMethod.POST, 'test-request', endpoint='queue:INCOMMING.MESSAGE')

    assert getattr(request.response, 'content_type', None) == TransformerContentType.UNDEFINED

    grizzly.scenario.tasks.add(request)

    for content_type in TransformerContentType:
        if content_type == TransformerContentType.UNDEFINED:
            continue
        step_response_content_type(behave, content_type)
        assert request.response.content_type == content_type

    step_response_content_type(behave, TransformerContentType.UNDEFINED)

    assert behave.exceptions == {
        behave.scenario.name: [
            ANY(AssertionError, message='there are no tasks in the scenario'),
            ANY(AssertionError, message='latest task in scenario is not a request task'),
            ANY(AssertionError, message='it is not allowed to set UNDEFINED with this step'),
        ],
    }

    request = RequestTask(RequestMethod.POST, 'test-request', endpoint='queue:INCOMING.MESSAGE | content_type="application/xml"')

    assert request.response.content_type == TransformerContentType.XML
    assert request.endpoint == 'queue:INCOMING.MESSAGE'

    grizzly.scenario.tasks.add(request)

    for content_type in [content_type for content_type in TransformerContentType if content_type != TransformerContentType.UNDEFINED]:
        step_response_content_type(behave, content_type)
        assert request.response.content_type == content_type

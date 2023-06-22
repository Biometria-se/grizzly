from typing import List, cast

import pytest

from parse import compile

from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, ResponseTarget
from grizzly.types.behave import Table, Row
from grizzly.tasks import RequestTask, WaitTask
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from grizzly_extras.transformer import TransformerContentType

from tests.fixtures import GrizzlyFixture, BehaveFixture


def test_parse_condition() -> None:
    p = compile(
        'value {condition:Condition} world',
        extra_types=dict(
            Condition=parse_condition,
        ),
    )

    assert parse_condition.__vector__ == (False, True,)

    assert p.parse('value is world')['condition']
    assert not p.parse('value is not world')['condition']
    assert p.parse('value equals world') is None


def test_parse_response_target() -> None:
    p = compile(
        'save response {target:ResponseTarget}',
        extra_types=dict(
            ResponseTarget=ResponseTarget.from_string,
        ),
    )
    assert ResponseTarget.get_vector() == (False, True,)
    actual = p.parse('save response metadata')['target']
    assert actual == ResponseTarget.METADATA
    actual = p.parse('save response payload')['target']
    assert actual == ResponseTarget.PAYLOAD

    with pytest.raises(ValueError):
        p.parse('save response test') is None


def test_parse_response_content_type() -> None:
    p = compile(
        'content type is "{content_type:TransformerContentType}"',
        extra_types=dict(
            TransformerContentType=TransformerContentType.from_string,
        ),
    )

    assert TransformerContentType.get_vector() == (False, True, )

    tests = [
        (TransformerContentType.JSON, ['json', 'application/json']),
        (TransformerContentType.XML, ['xml', 'application/xml']),
        (TransformerContentType.PLAIN, ['plain', 'text/plain']),
    ]

    for test_type, values in tests:
        for value in values:
            actual = p.parse(f'content type is "{value}"')['content_type']
            assert actual == test_type

    with pytest.raises(ValueError) as e:
        p.parse('content type is "image/png"')
    assert 'is an unknown response content type' in str(e)


def test_step_response_save_matches_metadata(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave
    grizzly = grizzly_fixture.grizzly
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_save_matches(behave, ResponseTarget.METADATA, '', '', '')
    assert 'variable "" has not been declared' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError) as ve:
        step_response_save_matches(behave, ResponseTarget.METADATA, '$.test.value', '.*ary$', 'test')
    assert 'variable "test" has not been declared' in str(ve)

    try:
        grizzly.state.variables['test'] = 'none'

        with pytest.raises(ValueError) as ve:
            step_response_save_matches(behave, ResponseTarget.METADATA, '$.test.value', '.*ary$', 'test')
        assert 'content type is not set for latest request' in str(ve)

        request.response.content_type = TransformerContentType.JSON
        step_response_save_matches(behave, ResponseTarget.METADATA, '$.test.value', '.*ary$', 'test')

        assert len(request.response.handlers.metadata) == 1
        assert len(request.response.handlers.payload) == 0
    finally:
        request.response.content_type = TransformerContentType.UNDEFINED
        del grizzly.state.variables['test']


def test_step_response_save_matches_payload(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_save_matches(behave, ResponseTarget.PAYLOAD, '', '', '')
    assert 'variable "" has not been declared' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError) as ve:
        step_response_save_matches(behave, ResponseTarget.PAYLOAD, '$.test.value', '.*ary$', 'test')
    assert 'variable "test" has not been declared' in str(ve)

    try:
        grizzly.state.variables['test'] = 'none'
        with pytest.raises(ValueError) as ve:
            step_response_save_matches(behave, ResponseTarget.PAYLOAD, '$.test.value', '.*ary$', 'test')
        assert 'content type is not set for latest request' in str(ve)

        request.response.content_type = TransformerContentType.JSON

        step_response_save_matches(behave, ResponseTarget.PAYLOAD, '$.test.value', '.*ary$', 'test')
        assert len(request.response.handlers.metadata) == 0
        assert len(request.response.handlers.payload) == 1
    finally:
        request.response.content_type = TransformerContentType.UNDEFINED
        del grizzly.state.variables['test']


def test_step_response_save_metadata(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_save(behave, ResponseTarget.METADATA, '', '')
    assert 'variable "" has not been declared' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError) as ve:
        step_response_save(behave, ResponseTarget.METADATA, '$.test.value', 'test')
    assert 'variable "test" has not been declared' in str(ve)

    try:
        grizzly.state.variables['test'] = 'none'
        with pytest.raises(ValueError) as ve:
            step_response_save(behave, ResponseTarget.METADATA, '$.test.value', 'test')
        assert 'content type is not set for latest request' in str(ve)

        request.response.content_type = TransformerContentType.JSON

        step_response_save(behave, ResponseTarget.METADATA, '$.test.value', 'test')
        assert len(request.response.handlers.metadata) == 1
        assert len(request.response.handlers.payload) == 0
    finally:
        request.response.content_type = TransformerContentType.UNDEFINED
        del grizzly.state.variables['test']


def test_step_response_save_payload(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_save(behave, ResponseTarget.PAYLOAD, '', '')
    assert 'variable "" has not been declared' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError) as ve:
        step_response_save(behave, ResponseTarget.PAYLOAD, '$.test.value', 'test')
    assert 'variable "test" has not been declared' in str(ve)

    try:
        grizzly.state.variables['test'] = 'none'
        with pytest.raises(ValueError) as ve:
            step_response_save(behave, ResponseTarget.PAYLOAD, '$.test.value', 'test')
        assert 'content type is not set for latest request' in str(ve)

        request.response.content_type = TransformerContentType.JSON

        step_response_save(behave, ResponseTarget.PAYLOAD, '$.test.value', 'test')
        assert len(request.response.handlers.metadata) == 0
        assert len(request.response.handlers.payload) == 1
    finally:
        request.response.content_type = TransformerContentType.UNDEFINED
        del grizzly.state.variables['test']


def test_step_response_validate_metadata(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_validate(behave, ResponseTarget.METADATA, '', True, '')
    assert 'expression is empty' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    with pytest.raises(ValueError) as ve:
        step_response_validate(behave, ResponseTarget.METADATA, '$.test.value', True, '.*test')
    assert 'content type is not set for latest request' in str(ve)

    request.response.content_type = TransformerContentType.JSON
    step_response_validate(behave, ResponseTarget.METADATA, '$.test.value', True, '.*test')

    assert len(request.response.handlers.metadata) == 1
    assert len(request.response.handlers.payload) == 0


def test_step_response_validate_payload(grizzly_fixture: GrizzlyFixture) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    request = cast(RequestTask, grizzly.scenario.tasks()[0])

    with pytest.raises(ValueError) as ve:
        step_response_validate(behave, ResponseTarget.PAYLOAD, '', True, '')
    assert 'expression is empty' in str(ve)

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 0

    request.response.content_type = TransformerContentType.JSON

    step_response_validate(behave, ResponseTarget.PAYLOAD, '$.test.value', True, '.*test')

    assert len(request.response.handlers.metadata) == 0
    assert len(request.response.handlers.payload) == 1


def test_step_response_allow_status_codes(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
    with pytest.raises(AssertionError) as ve:
        step_response_allow_status_codes(behave, '-200')
    assert 'there are no requests in the scenario' in str(ve)

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))
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
    grizzly.scenario.tasks.tmp.conditional.switch(True)

    grizzly.scenario.tasks.add(request)
    step_response_allow_status_codes(behave, '-200,-302,500')
    assert request.response.status_codes == [500]


def test_step_response_allow_status_codes_table(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_response_allow_status_codes_table(behave)
    assert 'step data table is mandatory' in str(ae)

    rows: List[Row] = []
    rows.append(Row(['test'], ['-200,400']))
    rows.append(Row(['test'], ['302']))
    behave.table = Table(['test'], rows=rows)

    with pytest.raises(AssertionError) as ae:
        step_response_allow_status_codes_table(behave)
    assert 'there are no requests in the scenario' in str(ae)

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.scenarios.create(behave_fixture.create_scenario('test'))
    grizzly.scenario.tasks.add(request)

    # more rows in data table then there are requests
    with pytest.raises(AssertionError) as ae:
        step_response_allow_status_codes_table(behave)
    assert 'data table has more rows than there are requests' in str(ae)

    request = RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)

    # data table column "code" does not exist
    with pytest.raises(AssertionError) as ae:
        step_response_allow_status_codes_table(behave)
    assert 'data table does not have column "status"' in str(ae)

    request = RequestTask(RequestMethod.GET, name='no-code', endpoint='/api/test')
    grizzly.scenario.tasks().insert(0, request)

    rows = []
    '''
    | status   |
    | -200,400 | # name=test
    | 302      | # name=test-get
    '''
    column_name = 'status'
    rows.append(Row([column_name], ['-200,400']))
    rows.append(Row([column_name], ['302']))
    behave.table = Table([column_name], rows=rows)

    step_response_allow_status_codes_table(behave)
    assert cast(RequestTask, grizzly.scenario.tasks()[0]).response.status_codes == [200]
    assert cast(RequestTask, grizzly.scenario.tasks()[1]).response.status_codes == [400]
    assert cast(RequestTask, grizzly.scenario.tasks()[2]).response.status_codes == [200, 302]

    grizzly.scenario.tasks.clear()

    grizzly.scenario.tasks.add(LogMessageTask(message='hello world'))
    grizzly.scenario.tasks.tmp.conditional = ConditionalTask(
        name='conditional',
        condition='{{ value | int == 0}}',
    )
    grizzly.scenario.tasks.tmp.conditional.switch(True)

    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)
    request = RequestTask(RequestMethod.GET, name='test-get', endpoint='/api/test')
    grizzly.scenario.tasks.add(request)
    request = RequestTask(RequestMethod.GET, name='no-code', endpoint='/api/test')
    grizzly.scenario.tasks().insert(0, request)

    step_response_allow_status_codes_table(behave)
    assert cast(RequestTask, grizzly.scenario.tasks()[0]).response.status_codes == [200]
    assert cast(RequestTask, grizzly.scenario.tasks()[1]).response.status_codes == [400]
    assert cast(RequestTask, grizzly.scenario.tasks()[2]).response.status_codes == [200, 302]


def test_step_response_content_type(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave, TransformerContentType.JSON)
    assert 'There are no requests in the scenario' in str(ae)

    grizzly.scenario.tasks.add(WaitTask(time_expression='1.0'))

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave, TransformerContentType.JSON)
    assert 'Latest task in scenario is not a request' in str(ae)

    request: RequestTask = RequestTask(RequestMethod.POST, 'test-request', endpoint='queue:INCOMMING.MESSAGE')

    assert getattr(request.response, 'content_type', None) == TransformerContentType.UNDEFINED

    grizzly.scenario.tasks.add(request)

    for content_type in TransformerContentType:
        if content_type == TransformerContentType.UNDEFINED:
            continue
        step_response_content_type(behave, content_type)
        assert request.response.content_type == content_type

    with pytest.raises(AssertionError) as ae:
        step_response_content_type(behave, TransformerContentType.UNDEFINED)
    assert 'It is not allowed to set UNDEFINED with this step' in str(ae)

    request = RequestTask(RequestMethod.POST, 'test-request', endpoint='queue:INCOMING.MESSAGE | content_type="application/xml"')

    assert request.response.content_type == TransformerContentType.XML
    assert request.endpoint == 'queue:INCOMING.MESSAGE'

    grizzly.scenario.tasks.add(request)

    for content_type in TransformerContentType:
        if content_type == TransformerContentType.UNDEFINED:
            continue
        step_response_content_type(behave, content_type)
        assert request.response.content_type == content_type

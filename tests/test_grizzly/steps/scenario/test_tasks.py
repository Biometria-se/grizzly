from typing import cast

import pytest

from parse import compile
from json import dumps as jsondumps

from behave.model import Table, Row
from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, RequestDirection
from grizzly.tasks import TransformerTask, LogMessage, WaitTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from grizzly_extras.transformer import TransformerContentType

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from ....fixtures import BehaveFixture


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


def test_step_task_request_with_name_to_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    assert len(grizzly.scenario.tasks) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.POST, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step is only valid for request methods with direction FROM' in str(ae)

    behave.text = 'foo bar'
    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step does not have support for step text' in str(ae)

    behave.text = None

    with pytest.raises(ValueError) as ve:
        step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'content type must be specified for request' in str(ve)

    step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test | content_type=json', '$.`this`[?status="ready"]')

    assert len(grizzly.scenario.tasks) == 1

    rows: List[Row] = []
    rows.append(Row(['endpoint'], ['{{ variable }}']))
    rows.append(Row(['endpoint'], ['foo']))
    rows.append(Row(['endpoint'], ['bar']))
    behave.table = Table(['endpoint'], rows=rows)

    step_task_request_with_name_to_endpoint_until(behave, RequestMethod.GET, 'test', '/api/{{ endpoint }} | content_type=json', '$.`this`[?status="{{ endpoint }}"]')

    assert len(grizzly.scenario.tasks) == 4
    tasks = cast(List[UntilRequestTask], grizzly.scenario.tasks)

    templates: List[str] = []

    assert tasks[-1].request.endpoint == '/api/bar'
    assert tasks[-1].condition == '$.`this`[?status="bar"]'
    templates += tasks[-1].get_templates()
    assert tasks[-2].request.endpoint == '/api/foo'
    assert tasks[-2].condition == '$.`this`[?status="foo"]'
    templates += tasks[-2].get_templates()
    assert tasks[-3].request.endpoint == '/api/{{ variable }}'
    assert tasks[-3].condition == '$.`this`[?status="{{ variable }}"]'
    templates += tasks[-3].get_templates()

    assert len(templates) == 2
    assert sorted(templates) == sorted([
        '$.`this`[?status="{{ variable }}"]',
        '/api/{{ variable }}',
    ])


@pytest.mark.parametrize('method', RequestDirection.TO.methods)
def test_step_task_request_file_with_name_endpoint(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
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
def test_step_task_request_text_with_name_to_endpoint_to(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert f'"from endpoint" is not allowed for {method.name}, use "to endpoint"' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_to_endpoint_from(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert f'step text is not allowed for {method.name}' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')
    assert f'step text is not allowed for {method.name}' in str(ae)


@pytest.mark.parametrize('method', RequestDirection.FROM.methods)
def test_step_task_request_text_with_name_to_endpoint_no_text(behave_fixture: BehaveFixture, method: RequestMethod) -> None:
    behave = behave_fixture.context
    behave.text = None

    step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.FROM, '/api/test')

    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_to_endpoint(behave, method, 'test-name', RequestDirection.TO, '/api/test')
    assert f'"to endpoint" is not allowed for {method.name}, use "from endpoint"' in str(ae)


def test_step_task_request_text_with_name_to_endpoint_no_direction(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    with pytest.raises(AssertionError) as ae:
        step_task_request_text_with_name_to_endpoint(behave, 'GET', 'test-name', 'asdf', '/api/test')
    assert 'invalid direction specified in expression' in str(ae)


def test_step_task_request_text_with_name(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    behave.text = '{}'

    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    step_task_request_text_with_name_to_endpoint(behave, RequestMethod.POST, 'test-name', RequestDirection.TO, '/api/test')

    behave.text = None
    with pytest.raises(ValueError):
        step_task_request_text_with_name(behave, RequestMethod.GET, 'test-name')

    with pytest.raises(AssertionError):
        step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')

    behave.text = '{}'
    step_task_request_text_with_name(behave, RequestMethod.POST, 'test-name')


def test_step_task_wait_seconds(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError):
        step_task_wait_seconds(behave, -1.0)

    step_task_wait_seconds(behave, 1.337)

    assert isinstance(grizzly.scenario.tasks[-1], WaitTask)
    assert grizzly.scenario.tasks[-1].time == 1.337


def test_step_task_print_message(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    step_task_print_message(behave, 'hello {{ world }}')

    assert isinstance(grizzly.scenario.tasks[-1], LogMessage)
    assert grizzly.scenario.tasks[-1].message == 'hello {{ world }}'


def test_step_task_transform_json(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(ValueError) as ve:
        step_task_transform(
            behave,
            jsondumps({
                'document': {
                    'id': 'DOCUMENT_8483-1',
                    'title': 'TPM Report 2020',
                },
            }),
            TransformerContentType.JSON,
            '$.document.id',
            'document_id',
        )
    assert 'TransformerTask: document_id has not been initialized' in str(ve)

    grizzly.state.variables['document_id'] = 'None'
    step_task_transform(
        behave,
        jsondumps({
            'document': {
                'id': 'DOCUMENT_8483-1',
                'title': 'TPM Report 2020',
            },
        }),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    task = grizzly.scenario.tasks[-1]
    assert isinstance(task, TransformerTask)
    assert task.content_type == TransformerContentType.JSON
    assert task.expression == '$.document.id'
    assert task.variable == 'document_id'

    assert len(grizzly.scenario.orphan_templates) == 0

    step_task_transform(
        behave,
        jsondumps({
            'document': {
                'id': 'DOCUMENT_8483-1',
                'title': 'TPM Report {{ year }}',
            },
        }),
        TransformerContentType.JSON,
        '$.document.id',
        'document_id',
    )

    templates = grizzly.scenario.tasks[-1].get_templates()

    assert len(templates) == 1
    assert templates[-1] == jsondumps({
        'document': {
            'id': 'DOCUMENT_8483-1',
            'title': 'TPM Report {{ year }}',
        },
    })


def test_step_task_transform_xml(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(ValueError) as ve:
        step_task_transform(
            behave,
            '''<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report 2022</title>
</document>
            ''',
            TransformerContentType.XML,
            '/document/id/text()',
            'document_id',
        )
    assert 'TransformerTask: document_id has not been initialized' in str(ve)

    grizzly.state.variables['document_id'] = 'None'
    step_task_transform(
        behave,
        '''<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report 2022</title>
</document>
        ''',
        TransformerContentType.XML,
        '/document/id/text()',
        'document_id',
    )

    task = grizzly.scenario.tasks[-1]
    assert isinstance(task, TransformerTask)
    assert task.content_type == TransformerContentType.XML
    assert task.expression == '/document/id/text()'
    assert task.variable == 'document_id'

    assert len(grizzly.scenario.orphan_templates) == 0

    step_task_transform(
        behave,
        '''<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report {{ year }}</title>
</document>
        ''',
        TransformerContentType.XML,
        '/document/id/text()',
        'document_id',
    )

    templates = grizzly.scenario.tasks[-1].get_templates()

    assert len(templates) == 1
    assert templates[-1] == '''<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report {{ year }}</title>
</document>
        '''


def test_step_task_client_get_endpoint(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint(behave, 'obscure.example.com', 'test')
    assert 'could not find scheme in "obscure.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint(behave, 'obscure://obscure.example.com', 'test')
    assert 'no client task registered for obscure' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint(behave, 'http://www.example.org', 'test')
    assert 'HttpClientTask: variable test has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint(behave, 'mq://mq.example.org', 'test')
        assert 'MessageQueueClientTask: variable test has not been initialized' in str(ve)

    grizzly.state.variables['test'] = 'none'

    assert len(grizzly.scenario.tasks) == 0
    step_task_client_get_endpoint(behave, 'http://www.example.org', 'test')
    assert len(grizzly.scenario.tasks) == 1
    assert isinstance(grizzly.scenario.tasks[-1], HttpClientTask)

    grizzly.state.variables['endpoint_url'] = 'https://example.org'
    step_task_client_get_endpoint(behave, 'https://{{ endpoint_url }}', 'test')

    task = grizzly.scenario.tasks[-1]
    assert task.endpoint == '{{ endpoint_url }}'


def test_step_task_date(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    with pytest.raises(AssertionError) as ae:
        step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')
    assert 'variable date_variable has not been initialized' in str(ae)

    grizzly.state.variables['date_variable'] = 'none'

    step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')

    assert len(grizzly.scenario.tasks) == 1
    assert isinstance(grizzly.scenario.tasks[-1], DateTask)

    task = grizzly.scenario.tasks[-1]
    assert task.value == '{{ datetime.now() }}'
    assert task.variable == 'date_variable'
    assert task.arguments.get('offset') == '1D'
    templates = task.get_templates()
    assert len(templates) == 1
    assert templates[0] == '{{ datetime.now() }}'


def test_step_task_client_put_endpoint_file_destination(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file.json', 'http://example.org/put', 'uploaded-file.json')
    assert 'step text is not allowed for this step expression' in str(ae.value)

    behave.text = None

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'uploaded-file-{{ suffix }}.json')
    assert 'source file cannot be a template' == str(ae.value)

    step_task_client_put_endpoint_file_destination(behave, 'file-test.json', 'http://{{ url }}', 'uploaded-file-{{ suffix }}.json')

    assert len(grizzly.scenario.tasks) == 1
    task = grizzly.scenario.tasks[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination == 'uploaded-file-{{ suffix }}.json'
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert len(templates) == 2
    assert sorted(templates) == sorted([
        '{{ url }}',
        'uploaded-file-{{ suffix }}.json',
    ])


def test_step_task_async_group_start(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    assert getattr(grizzly.scenario, 'async_group', '') is None

    step_task_async_group_start(behave, 'async-test-1')

    assert grizzly.scenario.async_group is not None
    assert grizzly.scenario.async_group.name == 'async-test-1'

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_start(behave, 'async-test-2')
    assert str(ae.value) == 'async request group "async-test-1" has not been closed'


def test_step_task_async_group_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly

    assert len(grizzly.scenario.tasks) == 0
    assert getattr(grizzly.scenario, 'async_group', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'no async request group is open'

    step_task_async_group_start(behave, 'async-test-1')

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'there are no requests in async group "async-test-1"'
    assert grizzly.scenario.async_group is not None

    step_task_request_text_with_name_to_endpoint(behave, RequestMethod.GET, 'test', direction=RequestDirection.FROM, endpoint='/api/test')
    assert len(grizzly.scenario.tasks) == 0

    step_task_async_group_close(behave)

    assert len(grizzly.scenario.tasks) == 1
    assert grizzly.scenario.async_group is None

from typing import List, cast

import pytest

from parse import compile
from json import dumps as jsondumps

from grizzly.context import GrizzlyContext
from grizzly.types import RequestMethod, RequestDirection
from grizzly.types.behave import Table, Row
from grizzly.tasks import TransformerTask, LogMessageTask, WaitTask, TimerTask, TaskWaitTask, ConditionalTask, KeystoreTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.steps import *  # pylint: disable=unused-wildcard-import  # noqa: F403

from grizzly_extras.transformer import TransformerContentType

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

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


def test_step_task_request_with_name_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_endpoint_until(behave, RequestMethod.POST, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step is only valid for request methods with direction FROM' in str(ae)

    behave.text = 'foo bar'
    with pytest.raises(AssertionError) as ae:
        step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'this step does not have support for step text' in str(ae)

    behave.text = None

    with pytest.raises(ValueError) as ve:
        step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')
    assert 'content type must be specified for request' in str(ve)

    step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test | content_type=json', '$.`this`[?status="ready"]')

    assert len(grizzly.scenario.tasks()) == 1

    rows: List[Row] = []
    rows.append(Row(['endpoint'], ['{{ variable }}']))
    rows.append(Row(['endpoint'], ['foo']))
    rows.append(Row(['endpoint'], ['bar']))
    behave.table = Table(['endpoint'], rows=rows)

    step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/{{ endpoint }} | content_type=json', '$.`this`[?status="{{ endpoint }}"]')

    assert len(grizzly.scenario.tasks()) == 4
    tasks = cast(List[UntilRequestTask], grizzly.scenario.tasks())

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


def test_step_task_wait_seconds(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_wait_seconds(behave, '-1.0')
    assert str(ae.value) == 'wait time cannot be less than 0.0 seconds'

    with pytest.raises(AssertionError) as ae:
        step_task_wait_seconds(behave, 'foobar')
    assert str(ae.value) == '"foobar" is not a template nor a float'

    step_task_wait_seconds(behave, '1.337')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, WaitTask)
    assert task.time_expression == '1.337'

    grizzly.state.variables['wait_time'] = '126'

    step_task_wait_seconds(behave, '{{ wait_time }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, WaitTask)
    assert task.time_expression == '{{ wait_time }}'


def test_step_task_log_message(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    step_task_log_message(behave, 'hello {{ world }}')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, LogMessageTask)
    assert task.message == 'hello {{ world }}'


def test_step_task_transform_json(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

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

    task = grizzly.scenario.tasks()[-1]
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

    templates = grizzly.scenario.tasks()[-1].get_templates()

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
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

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

    task = grizzly.scenario.tasks()[-1]
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

    templates = grizzly.scenario.tasks()[-1].get_templates()

    assert len(templates) == 1
    assert templates[-1] == '''<?xml version="1.0" encoding="utf-8"?>
<document>
    <id>DOCUMENT_8483-1</id>
    <title>TPM Report {{ year }}</title>
</document>
        '''


def test_step_task_client_get_endpoint_payload_metadata(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload_metadata(behave, 'obscure.example.com', 'step-name', 'test', 'metadata')
    assert 'could not find scheme in "obscure.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload_metadata(behave, 'obscure://obscure.example.com', 'step-name', 'test', 'metadata')
    assert 'no client task registered for obscure' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert 'HttpClientTask: variable test has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert 'MessageQueueClientTask: variable test has not been initialized' in str(ve)

    grizzly.state.variables['test'] = 'none'

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert 'HttpClientTask: variable metadata has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload_metadata(behave, 'mq://mq.example.org', 'step-name', 'test', 'metadata')
        assert 'MessageQueueClientTask: variable metadata has not been initialized' in str(ve)

    grizzly.state.variables['metadata'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_get_endpoint_payload_metadata(behave, 'http://www.example.org', 'step-name', 'test', 'metadata')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.state.variables['endpoint_url'] = 'https://example.org'
    step_task_client_get_endpoint_payload_metadata(behave, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }} {{ metadata }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError) as nie:
        step_task_client_get_endpoint_payload_metadata(behave, 'https://{{ endpoint_url }}', 'step-name', 'test', 'metadata')
    assert str(nie.value) == 'HttpClientTask has not implemented support for step text'


def test_step_task_client_get_endpoint_payload(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload(behave, 'obscure.example.com', 'step-name', 'test')
    assert 'could not find scheme in "obscure.example.com"' in str(ae)

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_payload(behave, 'obscure://obscure.example.com', 'step-name', 'test')
    assert 'no client task registered for obscure' in str(ae)

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_payload(behave, 'http://www.example.org', 'step-name', 'test')
    assert 'HttpClientTask: variable test has not been initialized' in str(ve)

    if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
        with pytest.raises(ValueError) as ve:
            step_task_client_get_endpoint_payload(behave, 'mq://mq.example.org', 'step-name', 'test')
        assert 'MessageQueueClientTask: variable test has not been initialized' in str(ve)

    grizzly.state.variables['test'] = 'none'

    assert len(grizzly.scenario.tasks()) == 0
    step_task_client_get_endpoint_payload(behave, 'http://www.example.org', 'step-name', 'test')
    assert len(grizzly.scenario.tasks()) == 1

    grizzly.state.variables['endpoint_url'] = 'https://example.org'
    step_task_client_get_endpoint_payload(behave, 'https://{{ endpoint_url }}', 'step-name', 'test')

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, HttpClientTask)
    assert task.endpoint == '{{ endpoint_url }}'
    assert sorted(task.get_templates()) == sorted(['{{ endpoint_url }}', '{{ test }}'])

    behave.text = '1=1'
    with pytest.raises(NotImplementedError) as nie:
        step_task_client_get_endpoint_payload(behave, 'https://{{ endpoint_url }}', 'step-name', 'test')
    assert str(nie.value) == 'HttpClientTask has not implemented support for step text'


def test_step_task_client_get_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_until(behave, 'api.example.io/api/test', 'step-name', '$.`this`[?status=true]')
    assert str(ae.value) == 'could not find scheme in "api.example.io/api/test"'

    with pytest.raises(AssertionError) as ae:
        step_task_client_get_endpoint_until(behave, 'foo://api.example.io/api/test', 'step-name', '$.`this`[?status=true]')
    assert str(ae.value) == 'no client task registered for foo'

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test', 'step-name', '$.`this`[?status=true]')
    assert str(ve.value) == 'content type must be specified for request'

    with pytest.raises(ValueError) as ve:
        step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test | content_type=json', 'step-name', '$.`this`[?success=true] | foo=bar, bar=foo')
    assert str(ve.value) == 'unsupported arguments foo, bar'

    step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test | content_type=json', 'step-name', '$.`this`[?success=true]')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, UntilRequestTask)
    assert isinstance(task.request, HttpClientTask)
    assert task.request.content_type == TransformerContentType.JSON
    assert task.request.endpoint == 'https://api.example.io/api/test'

    grizzly.state.configuration['test.host'] = 'https://api.example.com'

    step_task_client_get_endpoint_until(behave, 'https://$conf::test.host$/api/test | content_type=json', 'step-name', '$.`this`[success=false]')

    assert len(grizzly.scenario.tasks()) == 2
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, UntilRequestTask)
    assert isinstance(task.request, HttpClientTask)
    assert task.request.content_type == TransformerContentType.JSON
    assert task.request.endpoint == 'https://api.example.com/api/test'

    behave.text = '1=1'
    with pytest.raises(NotImplementedError) as nie:
        step_task_client_get_endpoint_until(behave, 'https://$conf::test.host$/api/test | content_type=json', 'step-name', '$.`this`[success=false]')
    assert str(nie.value) == 'HttpClientTask has not implemented support for step text'


def test_step_task_date(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError) as ae:
        step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')
    assert 'variable date_variable has not been initialized' in str(ae)

    grizzly.state.variables['date_variable'] = 'none'

    step_task_date(behave, '{{ datetime.now() }} | offset=1D', 'date_variable')

    assert len(grizzly.scenario.tasks()) == 1

    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, DateTask)
    assert task.value == '{{ datetime.now() }}'
    assert task.variable == 'date_variable'
    assert task.arguments.get('offset') == '1D'
    templates = task.get_templates()
    assert len(templates) == 1
    assert templates[0] == '{{ datetime.now() }}'


def test_step_task_client_put_endpoint_file_destination(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file.json', 'http://example.org/put', 'step-name', 'uploaded-file.json')
    assert 'step text is not allowed for this step expression' in str(ae.value)

    behave.text = None

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file_destination(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')
    assert 'source file cannot be a template' == str(ae.value)

    step_task_client_put_endpoint_file_destination(behave, 'file-test.json', 'http://{{ url }}', 'step-name', 'uploaded-file-{{ suffix }}.json')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

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


def test_step_task_client_put_endpoint_file(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    behave.text = 'hello'

    assert len(grizzly.scenario.orphan_templates) == 0
    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file(behave, 'file.json', 'http://example.org/put', 'step-name')
    assert 'step text is not allowed for this step expression' in str(ae.value)

    behave.text = None

    with pytest.raises(AssertionError) as ae:
        step_task_client_put_endpoint_file(behave, 'file-{{ suffix }}.json', 'http://{{ url }}', 'step-name')
    assert 'source file cannot be a template' == str(ae.value)

    step_task_client_put_endpoint_file(behave, 'file-test.json', 'http://{{ url }}', 'step-name')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, HttpClientTask)
    assert task.source == 'file-test.json'
    assert task.destination is None
    assert task.endpoint == '{{ url }}'

    templates = task.get_templates()
    assert len(templates) == 1
    assert sorted(templates) == sorted([
        '{{ url }}',
    ])


def test_step_task_async_group_start(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    step_task_async_group_start(behave, 'async-test-1')

    assert grizzly.scenario.tasks.tmp.async_group is not None
    assert grizzly.scenario.tasks.tmp.async_group.name == 'async-test-1'

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_start(behave, 'async-test-2')
    assert str(ae.value) == 'async request group "async-test-1" has not been closed'


def test_step_task_async_group_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0
    assert getattr(grizzly.scenario.tasks.tmp, 'async_group', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'no async request group is open'

    step_task_async_group_start(behave, 'async-test-1')

    with pytest.raises(AssertionError) as ae:
        step_task_async_group_close(behave)
    assert str(ae.value) == 'there are no requests in async group "async-test-1"'
    assert grizzly.scenario.tasks.tmp.async_group is not None

    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test', direction=RequestDirection.FROM, endpoint='/api/test')
    assert len(grizzly.scenario.tasks) == 0  # OK here

    step_task_async_group_close(behave)

    assert len(grizzly.scenario.tasks()) == 1
    assert grizzly.scenario.tasks.tmp.async_group is None


def test_step_task_timer_start_and_stop(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert grizzly.scenario.tasks.tmp.timers == {}

    with pytest.raises(AssertionError) as ae:
        step_task_timer_stop(behave, 'test-timer-1')
    assert str(ae.value) == 'timer with name test-timer-1 has not been defined'

    step_task_timer_start(behave, 'test-timer-1')

    timer = grizzly.scenario.tasks.tmp.timers.get('test-timer-1', None)
    assert isinstance(timer, TimerTask)
    assert timer.name == 'test-timer-1'
    assert grizzly.scenario.tasks()[-1] is timer

    with pytest.raises(AssertionError) as ae:
        step_task_timer_start(behave, 'test-timer-1')
    assert str(ae.value) == 'timer with name test-timer-1 has already been defined'

    step_task_timer_stop(behave, 'test-timer-1')

    assert grizzly.scenario.tasks.tmp.timers == {
        'test-timer-1': None,
    }

    assert grizzly.scenario.tasks()[-2] is grizzly.scenario.tasks()[-1]


def test_step_task_request_wait_between(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_between(behave, 1.4, 1.7)

    assert len(grizzly.scenario.tasks()) == 1

    task = cast(TaskWaitTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 1.4
    assert task.max_time == 1.7

    step_task_wait_between(behave, 30, 20)

    assert len(grizzly.scenario.tasks()) == 2

    task = cast(TaskWaitTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 20
    assert task.max_time == 30


def test_step_task_wait_constant(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    step_task_wait_constant(behave, 10)

    assert len(grizzly.scenario.tasks()) == 1

    task = cast(TaskWaitTask, grizzly.scenario.tasks()[-1])
    assert task.min_time == 10
    assert task.max_time is None


def test_step_task_conditional_if(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    step_task_conditional_if(behave, '{{ value | int == 10 }}', 'conditional-1')

    assert grizzly.scenario.tasks.tmp.conditional is not None
    assert grizzly.scenario.tasks.tmp.conditional.name == 'conditional-1'
    assert grizzly.scenario.tasks.tmp.conditional._pointer
    assert grizzly.scenario.tasks.tmp.conditional.tasks == {True: []}

    step_task_wait_constant(behave, 1.4)
    step_task_log_message(behave, 'hello world')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get', RequestDirection.FROM, '/api/test')
    step_task_async_group_start(behave, 'async-group')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-1', RequestDirection.FROM, '/api/test')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-2', RequestDirection.FROM, '/api/test')
    step_task_async_group_close(behave)

    assert list(grizzly.scenario.tasks.tmp.conditional.tasks.keys()) == [True]
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[True]) == 4

    with pytest.raises(AssertionError) as ae:
        step_task_conditional_if(behave, '{{ value | int == 20 }}', 'conditional-2')
    assert str(ae.value) == 'cannot create a new conditional while "conditional-1" is still open'


def test_step_task_conditional_else(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_conditional_else(behave)
    assert str(ae.value) == 'there are no open conditional, you need to create one first'

    test_step_task_conditional_if(behave_fixture)

    assert grizzly.scenario.tasks.tmp.conditional is not None

    step_task_conditional_else(behave)

    assert grizzly.scenario.tasks.tmp.conditional.tasks.get(False, None) == []

    step_task_wait_constant(behave, 3.7)
    step_task_log_message(behave, 'foo bar')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-async-get-3', RequestDirection.FROM, '/api/test')

    assert list(grizzly.scenario.tasks.tmp.conditional.tasks.keys()) == [True, False]
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[True]) == 4
    assert len(grizzly.scenario.tasks.tmp.conditional.tasks[False]) == 3


def test_step_task_conditional_end(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'conditional', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_conditional_end(behave)
    assert str(ae.value) == 'there are no open conditional, you need to create one before closing it'

    test_step_task_conditional_else(behave_fixture)

    assert grizzly.scenario.tasks.tmp.conditional is not None

    step_task_conditional_end(behave)

    assert len(grizzly.scenario.tasks()) == 1
    conditional = cast(ConditionalTask, grizzly.scenario.tasks()[-1])

    assert conditional.name == 'conditional-1'
    assert conditional.condition == '{{ value | int == 10 }}'
    assert list(conditional.tasks.keys()) == [True, False]
    assert len(conditional.tasks[True]) == 4
    assert len(conditional.tasks[False]) == 3


def test_step_task_loop(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert getattr(grizzly.scenario.tasks.tmp, 'loop', '') is None

    with pytest.raises(ValueError) as ve:
        step_task_loop_start(behave, '["hello", "world"]', 'foobar', 'test-loop')
    assert str(ve.value) == 'LoopTask: foobar has not been initialized'

    step_setup_variable_value(behave, 'foobar', 'none')

    step_task_loop_start(behave, '["hello", "world"]', 'foobar', 'test-loop')

    assert grizzly.scenario.tasks.tmp.loop is not None
    assert len(grizzly.scenario.tasks()) == 0
    assert len(grizzly.scenario.tasks) == 0  # OK here

    assert grizzly.scenario.tasks.tmp.loop.name == 'test-loop'
    assert grizzly.scenario.tasks.tmp.loop.values == '["hello", "world"]'
    assert grizzly.scenario.tasks.tmp.loop.variable == 'foobar'
    assert len(grizzly.scenario.tasks.tmp.loop.tasks) == 0

    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get-1', RequestDirection.FROM, '/api/test/1')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-get-2', RequestDirection.FROM, '/api/test/2')

    step_task_conditional_if(behave, '{{ value | int > 0 }}', 'test-conditional')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-cond-get-1', RequestDirection.FROM, '/api/test/1')
    step_task_request_text_with_name_endpoint(behave, RequestMethod.GET, 'test-cond-get-2', RequestDirection.FROM, '/api/test/2')
    step_task_conditional_end(behave)

    assert len(grizzly.scenario.tasks.tmp.loop.tasks) == 3
    assert len(grizzly.scenario.tasks()) == 3

    step_task_loop_end(behave)

    assert len(grizzly.scenario.tasks()) == 1
    assert getattr(grizzly.scenario.tasks.tmp, 'loop', '') is None

    with pytest.raises(AssertionError) as ae:
        step_task_loop_end(behave)
    assert str(ae.value) == 'there are no open loop, you need to create one before closing it'


def test_step_task_keystore_get(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    grizzly.state.variables.update({'foobar': 'none'})

    step_task_keystore_get(behave, 'foobar', 'foobar')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value is None


def test_step_task_keystore_get_default(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    grizzly.state.variables.update({'foobar': 'none'})

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', "{'hello': 'world'}")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == {'hello': 'world'}

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', '"hello"')

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == 'hello'

    step_task_keystore_get_default(behave, 'barfoo', 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'barfoo'
    assert task.action == 'get'
    assert task.action_context == 'foobar'
    assert task.default_value == 'hello'

    with pytest.raises(AssertionError) as ae:
        step_task_keystore_get_default(behave, 'barfoo', 'foobar', 'hello')
    assert str(ae.value) == '"hello" is not valid JSON'


def test_step_task_keystore_set(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = behave_fixture.grizzly
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    grizzly.scenario.tasks.clear()

    with pytest.raises(AssertionError) as ae:
        step_task_keystore_set(behave, 'foobar', 'hello')
    assert str(ae.value) == '"hello" is not valid JSON'

    step_task_keystore_set(behave, 'foobar', "'hello'")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == 'hello'

    step_task_keystore_set(behave, 'foobar', "['hello', 'world']")

    task = grizzly.scenario.tasks()[-1]

    assert isinstance(task, KeystoreTask)
    assert task.key == 'foobar'
    assert task.action == 'set'
    assert task.action_context == ['hello', 'world']

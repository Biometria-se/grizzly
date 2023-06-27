import os
import json
import shutil

from typing import List, cast

import pytest

from _pytest.tmpdir import TempPathFactory
from locust.clients import ResponseContextManager
from requests.models import Response

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.tasks.async_group import AsyncRequestGroupTask
from grizzly.types import RequestMethod, ResponseTarget, ResponseAction
from grizzly.types.behave import Table, Row
from grizzly.tasks import RequestTask, WaitTask
from grizzly.testdata.utils import templatingfilter
from grizzly.steps._helpers import (
    add_validation_handler,
    add_save_handler,
    add_request_task,
    add_request_task_response_status_codes,
    get_task_client,
    normalize_step_name,
    _add_response_handler,
    is_template,
)
from grizzly.tasks.clients import ClientTask, client
from grizzly.exceptions import ResponseHandlerError

from grizzly_extras.transformer import TransformerContentType

from tests.helpers import TestUser
from tests.fixtures import BehaveFixture, GrizzlyFixture, LocustFixture


def test_add_request_task_response_status_codes() -> None:
    request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')

    assert request.response.status_codes == [200]

    add_request_task_response_status_codes(request, '-200')
    assert request.response.status_codes == []

    add_request_task_response_status_codes(request, '200,302, 400')
    assert request.response.status_codes == [200, 302, 400]


@pytest.mark.parametrize('as_async', [False, True])
def test_add_request_task(grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory, as_async: bool) -> None:
    behave = grizzly_fixture.behave.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test scenario'))
    grizzly.scenario.context['host'] = 'http://test'

    if as_async:
        grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='async-test-1')
        name_prefix = f'{grizzly.scenario.tasks.tmp.async_group.name}:'
    else:
        name_prefix = ''

    tasks = grizzly.scenario.tasks()

    @templatingfilter
    def uppercase(value: str) -> str:
        return value.upper()

    tasks.clear()

    assert len(tasks) == 0

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}')

    assert len(tasks) == 0

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='http://test/api/v1/test')

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', endpoint='/api/v1/test')

    assert add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='/api/v1/test') == []

    assert len(tasks) == 1
    assert isinstance(tasks[0], RequestTask)
    assert tasks[0].name == f'{name_prefix}<unknown>'

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test')

    assert add_request_task(behave, method=RequestMethod.from_string('POST'), source='{}', name='test') == []

    assert len(tasks) == 2
    assert isinstance(tasks[1], RequestTask)
    assert tasks[0].endpoint == tasks[1].endpoint
    assert tasks[1].name == f'{name_prefix}test'

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test', endpoint='/api/v2/test')

    assert add_request_task(behave, method=RequestMethod.POST, source='{}', name='test', endpoint='/api/v2/test') == []

    assert len(tasks) == 3
    assert isinstance(tasks[2], RequestTask)
    assert tasks[1].endpoint != tasks[2].endpoint
    assert tasks[2].name == f'{name_prefix}test'

    template_path = grizzly_fixture.request_task.context_root
    template_name = grizzly_fixture.request_task.relative_path
    template_full_path = os.path.join(template_path, template_name)

    assert add_request_task(behave, method=RequestMethod.SEND, source=template_full_path, name='my_blob', endpoint='my_container') == []

    with open(template_full_path, 'r') as fd:
        template_source = json.dumps(json.load(fd))

    assert len(tasks) == 4
    assert isinstance(tasks[-1], RequestTask)
    task = tasks[-1]
    assert task.source == template_source
    assert task.endpoint == 'my_container'
    assert task.name == f'{name_prefix}my_blob'
    assert task.response.content_type == TransformerContentType.UNDEFINED

    with pytest.raises(ValueError):
        add_request_task(behave, method=RequestMethod.POST, source='{}', name='test')

    assert add_request_task(behave, method=RequestMethod.SEND, source=template_full_path, name='my_blob2') == []
    assert len(tasks) == 5
    assert isinstance(tasks[-1], RequestTask)
    assert isinstance(tasks[-2], RequestTask)
    assert tasks[-1].source == template_source
    assert tasks[-1].endpoint == tasks[-2].endpoint
    assert tasks[-1].name == f'{name_prefix}my_blob2'

    try:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = os.path.dirname(test_context)
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        behave.config.base_dir = test_context_root
        test_template = test_context / 'template.j2.json'
        test_template.write_text('{{ hello_world }}')

        rows: List[Row] = []
        rows.append(Row(['test'], ['-200,400']))
        rows.append(Row(['test'], ['302']))
        behave.table = Table(['test'], rows=rows)

        if not as_async:
            tasks.clear()
            tasks.append(WaitTask(time_expression='1.0'))

            with pytest.raises(ValueError) as e:
                add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json')
            assert 'previous task was not a request' in str(e)

            assert add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json', name='test', endpoint='/api/test') == []

            assert add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json', endpoint='/api/test') == []
            assert tasks[-1].name == f'{name_prefix}template'

        tasks.clear()

        test_datatable_template = test_context / 'datatable_template.j2.json'
        test_datatable_template.write_text('Hello {{ name }} and good {{ time_of_day }}!')

        values = [
            ['bob', 'morning', '{{ AtomicRandomString.object }} is garbage'],
            ['alice', 'noon', 'i like {{ fruit | uppercase }}'],
            ['chad', 'evening', 'have you tried {{ AtomicDate.action }} it off and on again?'],
            ['dave', 'night', 'yabba {{ AtomicCsvReader.response.word }} doo'],
        ]

        rows = []
        for value in values:
            rows.append(Row(['name', 'time_of_day', 'quote'], value))
        behave.table = Table(['name', 'time_of_day', 'quote'], rows=rows)

        assert add_request_task(behave, method=RequestMethod.SEND, source='datatable_template.j2.json', name='quote={{ quote }}', endpoint='/api/test/{{ time_of_day }}') == []

        assert len(tasks) == 4

        for i, t in enumerate(tasks):
            request = cast(RequestTask, t)
            name, time_of_day, quote = values[i]
            assert request.name == f'{name_prefix}quote={quote}'
            assert request.endpoint == f'/api/test/{time_of_day}'
            assert request.source == f'Hello {name} and good {time_of_day}!'
    finally:
        del os.environ['GRIZZLY_CONTEXT_ROOT']
        shutil.rmtree(test_context_root)

    with pytest.raises(ValueError) as ve:
        add_request_task(behave, method=RequestMethod.GET, endpoint='hello | world:False', source=None, name='hello-world')
    assert 'incorrect format in arguments: "world:False"' in str(ve)

    with pytest.raises(ValueError) as ve:
        add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | content_type=asdf', name='hello-world')
    assert '"asdf" is an unknown response content type' in str(ve)

    assert add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | content_type=json', name='hello-world') == []

    task = tasks[-1]
    assert task.endpoint == 'hello world'
    assert task.response.content_type == TransformerContentType.JSON
    assert task.arguments is not None
    assert 'content_type' not in task.arguments

    assert add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | expression=$.test.value, content_type=json', name='hello-world') == []

    task = tasks[-1]
    assert task.endpoint == 'hello world'
    assert task.response.content_type == TransformerContentType.JSON
    assert task.arguments is not None
    assert task.arguments['expression'] == '$.test.value'
    assert 'content_type' not in task.arguments

    assert add_request_task(behave, method=RequestMethod.GET, source=None, endpoint=None, name='world-hello') == []

    task = tasks[-1]
    assert task.endpoint == 'hello world'
    assert task.response.content_type == TransformerContentType.JSON

    with pytest.raises(AssertionError) as ae:
        add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='$conf::test.endpoint$', name='foo-bar')
    assert 'configuration variable "test.endpoint" is not set' in str(ae)

    grizzly.state.configuration['test.endpoint'] = '/foo/bar'
    add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='$conf::test.endpoint$', name='foo-bar')

    task = tasks[-1]
    assert task.endpoint == '/foo/bar'
    assert task.response.content_type == TransformerContentType.UNDEFINED

    add_request_task(behave, method=RequestMethod.GET, source=None, endpoint=None, name='foo-bar')

    task = tasks[-1]
    assert task.endpoint == '/foo/bar'
    assert task.response.content_type == TransformerContentType.UNDEFINED

    assert len(tasks) == 24

    behave.table = None

    tasks_outside = add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='/api/foo/bar', name='foo-bar', in_scenario=False)

    assert len(tasks) == 24
    assert len(tasks_outside) == 1
    assert tasks_outside[0][0].endpoint == '/api/foo/bar'
    assert tasks_outside[0][1] == {}

    task = tasks[-1]
    assert task.endpoint == '/foo/bar'
    assert task.response.content_type == TransformerContentType.UNDEFINED


@pytest.mark.parametrize('as_async', [False, True])
def test_add_save_handler(behave_fixture: BehaveFixture, locust_fixture: LocustFixture, as_async: bool) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    scenario = GrizzlyContextScenario(index=2, behave=behave_fixture.create_scenario('test scenario'))
    grizzly.scenarios.append(scenario)

    TestUser.__scenario__ = scenario
    TestUser.host = 'https://example.io'
    user = TestUser(locust_fixture.environment)

    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    response_context_manager._entered = True

    if as_async:
        grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='async-test-2')

    tasks = grizzly.scenario.tasks()

    assert len(tasks) == 0
    assert len(user.context_variables) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', 'test-variable')

    assert len(user.context_variables) == 0

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    task = cast(RequestTask, tasks[0])

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '', 'test', 'test-variable')

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')

    try:
        grizzly.state.variables['test-variable-metadata'] = 'none'
        task.response.content_type = TransformerContentType.JSON
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 0
    finally:
        del grizzly.state.variables['test-variable-metadata']

    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')

    try:
        grizzly.state.variables['test-variable-payload'] = 'none'

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 1
    finally:
        del grizzly.state.variables['test-variable-payload']

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'metadata'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'

    payload_handler((TransformerContentType.JSON, {'test': {'value': 'payload'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    assert user.context_variables.get('test-variable-metadata', None) == 'metadata'
    assert user.context_variables.get('test-variable-payload', None) == 'payload'

    metadata_handler((TransformerContentType.JSON, {'test': {'name': 'metadata'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-metadata', 'metadata') is None

    payload_handler((TransformerContentType.JSON, {'test': {'name': 'payload'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
    response_context_manager._manual_result = None
    assert user.context_variables.get('test-variable-payload', 'payload') is None

    # previous non RequestTask task
    tasks.append(WaitTask(time_expression='1.0'))

    grizzly.state.variables['test'] = 'none'
    with pytest.raises(ValueError):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test')

    # remove non RequestTask task
    tasks.pop()

    # add_save_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.SAVE, '$test.value', '.*', variable=None)
    assert 'variable is not set' in str(e)

    try:
        grizzly.state.variables['test']

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100', '.*', 'test')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 2

        handler = task.response.handlers.payload[-1]

        assert handler.expression == '$.test.value'
        assert handler.expected_matches == 100
        assert not handler.as_json

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=-1, as_json=True', '.*', 'test')
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 3

        handler = task.response.handlers.payload[-1]

        assert handler.expression == '$.test.value'
        assert handler.expected_matches == -1
        assert handler.as_json

        with pytest.raises(ValueError) as ve:
            add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100, foobar=False, hello=world', '.*', 'test')
        assert str(ve.value) == 'unsupported arguments foobar, hello'

        cast(RequestTask, tasks[-1]).response.content_type = TransformerContentType.UNDEFINED

        with pytest.raises(ValueError) as ve:
            add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100', '.*', 'test')
        assert str(ve.value) == 'content type is not set for latest request'

    finally:
        del grizzly.state.variables['test']


@pytest.mark.parametrize('as_async', [False, True])
def test_add_validation_handler(behave_fixture: BehaveFixture, locust_fixture: LocustFixture, as_async: bool) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    scenario = GrizzlyContextScenario(index=1, behave=behave_fixture.create_scenario('test scenario'))
    grizzly.scenarios.append(scenario)

    TestUser.__scenario__ = scenario
    TestUser.host = 'http://example.io'
    user = TestUser(locust_fixture.environment)

    response = Response()
    response._content = '{}'.encode('utf-8')
    response.status_code = 200
    response_context_manager = ResponseContextManager(response, None, None)
    response_context_manager._entered = True

    if as_async:
        grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='test-async-3')

    tasks = grizzly.scenario.tasks()
    assert len(tasks) == 0

    # not preceeded by a request source
    with pytest.raises(ValueError):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', False)

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    # empty expression, fail
    with pytest.raises(ValueError):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '', 'test', False)

    # add metadata response handler
    task = cast(RequestTask, tasks[0])
    task.response.content_type = TransformerContentType.JSON
    add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 0

    # add payload response handler
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', 'test', False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 1

    metadata_handler = list(task.response.handlers.metadata)[0]
    payload_handler = list(task.response.handlers.payload)[0]

    # test that they validates
    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None
    payload_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), user, response_context_manager)
    assert response_context_manager._manual_result is None

    # test that they validates, negative
    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
    response_context_manager._manual_result = None

    payload_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), user, response_context_manager)
    assert isinstance(getattr(response_context_manager, '_manual_result', None), ResponseHandlerError)
    response_context_manager._manual_result = None

    # add a second payload response handler
    user.add_context({'variables': {'property': 'name', 'name': 'bob'}})
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.{{ property }}', '{{ name }}', False)
    assert len(task.response.handlers.payload) == 2

    # test that they validates
    for handler in task.response.handlers.payload:
        handler((TransformerContentType.JSON, {'test': {'value': 'test', 'name': 'bob'}}), user, response_context_manager)
        assert response_context_manager._manual_result is None

    # add_validation_handler calling _add_response_handler incorrectly
    with pytest.raises(ValueError) as e:
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.VALIDATE, '$.test', 'value', condition=None)
    assert 'condition is not set' in str(e)


def test_normalize_step_name() -> None:
    expected = 'this is just a "" of text with quoted ""'
    actual = normalize_step_name('this is just a "string" of text with quoted "words"')

    assert expected == actual


def test_get_task_client_error(grizzly_fixture: GrizzlyFixture) -> None:
    with pytest.raises(AssertionError) as ae:
        get_task_client(grizzly_fixture.grizzly, '')
    assert 'could not find scheme in ""' == str(ae.value)

    with pytest.raises(AssertionError) as ae:
        get_task_client(grizzly_fixture.grizzly, 'obscure://obscure.example.io')
    assert 'no client task registered for obscure' == str(ae.value)


@pytest.mark.parametrize('test_scheme', client.available.keys())
def test_get_task_client(grizzly_fixture: GrizzlyFixture, test_scheme: str) -> None:
    task_client = get_task_client(grizzly_fixture.grizzly, f'{test_scheme}://example.net')

    assert task_client is not None
    assert issubclass(task_client, ClientTask)


def test_is_template() -> None:
    assert is_template('{{ hello_world }}')
    assert not is_template('{{ hello_world')
    assert not is_template('hello_world }}')
    assert is_template('is {{ this }} really a template?')

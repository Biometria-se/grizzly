"""Unit tests for grizzly.steps._helpers."""

from __future__ import annotations

import json
import os
from contextlib import suppress
from itertools import product
from pathlib import Path
from typing import TYPE_CHECKING, cast

import pytest
from grizzly.exceptions import ResponseHandlerError
from grizzly.steps._helpers import (
    _add_response_handler,
    add_request_response_status_codes,
    add_request_task,
    add_save_handler,
    add_validation_handler,
    get_task_client,
    normalize_step_name,
)
from grizzly.tasks import ExplicitWaitTask, RequestTask
from grizzly.tasks.async_group import AsyncRequestGroupTask
from grizzly.tasks.clients import ClientTask, HttpClientTask, client
from grizzly.testdata.filters import templatingfilter
from grizzly.types import RequestDirection, RequestMethod, ResponseAction, ResponseTarget
from grizzly.types.behave import Row, Table
from grizzly_common.transformer import TransformerContentType
from jinja2.filters import FILTERS

from test_framework.helpers import rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory
    from grizzly.context import GrizzlyContext

    from test_framework.fixtures import GrizzlyFixture


@pytest.mark.parametrize(
    'request_type',
    [
        RequestTask,
        HttpClientTask,
    ],
)
def test_add_request_task_response_status_codes(grizzly_fixture: GrizzlyFixture, request_type: type[RequestTask | HttpClientTask]) -> None:
    grizzly = grizzly_fixture.grizzly

    if request_type is RequestTask:
        request = RequestTask(RequestMethod.SEND, name='test', endpoint='/api/test')
    else:
        task_cls = type('HttpClientTaskTest', (HttpClientTask,), {'__scenario__': grizzly.scenario})
        request = task_cls(RequestDirection.TO, 'http://example.org', 'test', source='foobar')

    assert request.response.status_codes == [200]

    add_request_response_status_codes(request, '-200')
    assert request.response.status_codes == []

    add_request_response_status_codes(request, '200,302, 400')
    assert request.response.status_codes == [200, 302, 400]


@pytest.mark.parametrize('request_type,', ['sync', 'async'])
def test_add_request_task(grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory, *, request_type: str) -> None:  # noqa: PLR0915
    behave = grizzly_fixture.behave.context
    grizzly = cast('GrizzlyContext', behave.grizzly)
    grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test scenario'))
    grizzly.scenario.context['host'] = 'http://test'
    as_async = request_type == 'async'

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

    with pytest.raises(AssertionError, match='no endpoint specified'):
        add_request_task(behave, method=RequestMethod.POST, source='{}')

    assert len(tasks) == 0

    with pytest.raises(AssertionError, match='endpoints should only contain path relative to'):
        add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='http://test/api/v1/test')

    with pytest.raises(AssertionError, match='"TEST" is not a valid value of RequestMethod'):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', endpoint='/api/v1/test')

    assert add_request_task(behave, method=RequestMethod.POST, source='{}', endpoint='/api/v1/test') == []

    assert len(tasks) == 1
    assert isinstance(tasks[0], RequestTask)
    assert tasks[0].name == f'{name_prefix}<unknown>'

    with pytest.raises(AssertionError, match='"TEST" is not a valid value of RequestMethod'):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test')

    assert add_request_task(behave, method=RequestMethod.from_string('POST'), source='{}', name='test') == []

    assert len(tasks) == 2
    assert isinstance(tasks[1], RequestTask)
    assert tasks[0].endpoint == tasks[1].endpoint
    assert tasks[1].name == f'{name_prefix}test'

    with pytest.raises(AssertionError, match='"TEST" is not a valid value of RequestMethod'):
        add_request_task(behave, method=RequestMethod.from_string('TEST'), source='{}', name='test', endpoint='/api/v2/test')

    assert add_request_task(behave, method=RequestMethod.POST, source='{}', name='test', endpoint='/api/v2/test') == []

    assert len(tasks) == 3
    assert isinstance(tasks[2], RequestTask)
    assert tasks[1].endpoint != tasks[2].endpoint
    assert tasks[2].name == f'{name_prefix}test'

    template_path = grizzly_fixture.request_task.context_root
    template_name = grizzly_fixture.request_task.relative_path
    template_full_path = Path(template_path) / template_name

    assert add_request_task(behave, method=RequestMethod.SEND, source=str(template_full_path), name='my_blob', endpoint='my_container') == []

    template_source = json.dumps(json.load(template_full_path.open()))

    assert len(tasks) == 4
    assert isinstance(tasks[-1], RequestTask)
    task = tasks[-1]
    assert task.source == template_source
    assert task.endpoint == 'my_container'
    assert task.name == f'{name_prefix}my_blob'
    assert task.response.content_type == TransformerContentType.UNDEFINED

    with pytest.raises(AssertionError, match='cannot use endpoint from previous request, it has a different request method'):
        add_request_task(behave, method=RequestMethod.POST, source='{}', name='test')

    assert add_request_task(behave, method=RequestMethod.SEND, source=str(template_full_path), name='my_blob2') == []
    assert len(tasks) == 5
    assert isinstance(tasks[-1], RequestTask)
    assert isinstance(tasks[-2], RequestTask)
    assert tasks[-1].source == template_source
    assert tasks[-1].endpoint == tasks[-2].endpoint
    assert tasks[-1].name == f'{name_prefix}my_blob2'

    try:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = str(test_context.parent)
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        behave.config.base_dir = test_context_root
        test_template = test_context / 'template.j2.json'
        test_template.write_text('{{ hello_world }}')

        rows: list[Row] = []
        rows.append(Row(['test'], ['-200,400']))
        rows.append(Row(['test'], ['302']))
        behave.table = Table(['test'], rows=rows)

        if not as_async:
            tasks.clear()
            tasks.append(ExplicitWaitTask(time_expression='1.0'))

            with pytest.raises(AssertionError, match='previous task was not a request'):
                add_request_task(behave, method=RequestMethod.PUT, source='template.j2.json')

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
            request = cast('RequestTask', t)
            name, time_of_day, quote = values[i]
            assert request.name == f'{name_prefix}quote={quote}'
            assert request.endpoint == f'/api/test/{time_of_day}'
            assert request.source == f'Hello {name} and good {time_of_day}!'
    finally:
        del os.environ['GRIZZLY_CONTEXT_ROOT']
        rm_rf(test_context_root)

        with suppress(KeyError):
            del FILTERS['uppercase']

    with pytest.raises(ValueError, match='incorrect format in arguments: "world:False"'):
        add_request_task(behave, method=RequestMethod.GET, endpoint='hello | world:False', source=None, name='hello-world')

    with pytest.raises(ValueError, match='"asdf" is an unknown response content type'):
        add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='hello world | content_type=asdf', name='hello-world')

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

    with pytest.raises(AssertionError, match='configuration variable "test.endpoint" is not set'):
        add_request_task(behave, method=RequestMethod.GET, source=None, endpoint='$conf::test.endpoint$', name='foo-bar')

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


@pytest.mark.parametrize(('as_async', 'default_value'), product([False, True], [None, 'foobar']))
def test_add_save_handler(grizzly_fixture: GrizzlyFixture, *, as_async: bool, default_value: str | None) -> None:  # noqa: PLR0915
    parent = grizzly_fixture()
    behave = grizzly_fixture.behave.context
    grizzly = grizzly_fixture.grizzly

    if as_async:
        grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='async-test-2')

    tasks = grizzly.scenario.tasks()
    tasks.clear()

    assert len(tasks) == 0
    assert parent.user.variables == {}

    # not preceeded by a request source
    with pytest.raises(AssertionError, match='variable "test-variable" has not been declared'):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', 'test-variable', default_value=default_value)

    assert parent.user.variables == {}

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    task = cast('RequestTask', tasks[0])

    with pytest.raises(AssertionError, match='variable "test-variable" has not been declared'):
        add_save_handler(grizzly, ResponseTarget.METADATA, '', 'test', 'test-variable', default_value=default_value)

    with pytest.raises(AssertionError, match='variable "test-variable-metadata" has not been declared'):
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata', default_value=default_value)

    try:
        grizzly.scenario.variables['test-variable-metadata'] = 'none'
        task.response.content_type = TransformerContentType.JSON
        add_save_handler(grizzly, ResponseTarget.METADATA, '$.test.value', '.*', 'test-variable-metadata', default_value=default_value)
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 0
    finally:
        del grizzly.scenario.variables['test-variable-metadata']

    with pytest.raises(AssertionError, match='variable "test-variable-payload" has not been declared'):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload', default_value=default_value)

    try:
        grizzly.scenario.variables['test-variable-payload'] = 'none'

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test-variable-payload', default_value=default_value)
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 1
    finally:
        del grizzly.scenario.variables['test-variable-payload']

    metadata_handler = next(iter(task.response.handlers.metadata))
    payload_handler = next(iter(task.response.handlers.payload))

    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'metadata'}}), parent.user)
    assert parent.user.variables.get('test-variable-metadata', None) == 'metadata'

    del parent.user.variables['test-variable-metadata']

    if default_value is None:
        with pytest.raises(ResponseHandlerError, match=r'"\$\.test.value" did not match value'):
            metadata_handler((TransformerContentType.JSON, {'test': {'attribute': 'metadata'}}), parent.user)
    else:
        metadata_handler((TransformerContentType.JSON, {'test': {'attribute': 'metadata'}}), parent.user)
        assert parent.user.variables.get('test-variable-metadata', None) == default_value

    payload_handler((TransformerContentType.JSON, {'test': {'value': 'payload'}}), parent.user)
    assert parent.user.variables.get('test-variable-payload', None) == 'payload'

    if default_value is None:
        with pytest.raises(ResponseHandlerError, match='did not match value'):
            metadata_handler((TransformerContentType.JSON, {'test': {'name': 'metadata'}}), parent.user)
        assert parent.user.variables.get('test-variable-metadata', 'metadata') is None

        with pytest.raises(ResponseHandlerError, match='did not match value'):
            payload_handler((TransformerContentType.JSON, {'test': {'name': 'payload'}}), parent.user)
        assert parent.user.variables.get('test-variable-payload', 'payload') is None

    else:
        metadata_handler((TransformerContentType.JSON, {'test': {'name': 'metadata'}}), parent.user)
        assert parent.user.variables.get('test-variable-metadata', 'metadata') == default_value

        payload_handler((TransformerContentType.JSON, {'test': {'name': 'payload'}}), parent.user)
        assert parent.user.variables.get('test-variable-payload', 'payload') == default_value

    # previous non RequestTask task
    tasks.append(ExplicitWaitTask(time_expression='1.0'))

    grizzly.scenario.variables['test'] = 'none'
    with pytest.raises(AssertionError, match='latest task was not a request'):
        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', '.*', 'test', default_value=default_value)

    # remove non RequestTask task
    tasks.pop()

    # add_save_handler calling _add_response_handler incorrectly
    with pytest.raises(AssertionError, match='variable is not set'):
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.SAVE, '$test.value', '.*', variable=None)

    try:
        grizzly.scenario.variables['test']

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100', '.*', 'test', default_value=default_value)
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 2

        handler = task.response.handlers.payload[-1]

        assert handler.expression == '$.test.value'
        assert handler.expected_matches == '100'
        assert not handler.as_json

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=-1, as_json=True', '.*', 'test', default_value=default_value)
        assert len(task.response.handlers.metadata) == 1
        assert len(task.response.handlers.payload) == 3

        handler = task.response.handlers.payload[-1]

        assert handler.expression == '$.test.value'
        assert handler.expected_matches == '-1'
        assert handler.as_json

        with pytest.raises(AssertionError, match='unsupported arguments foobar, hello'):
            add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100, foobar=False, hello=world', '.*', 'test', default_value=default_value)

        cast('RequestTask', tasks[-1]).response.content_type = TransformerContentType.UNDEFINED

        with pytest.raises(AssertionError, match='content type is not set for latest request'):
            add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value | expected_matches=100', '.*', 'test', default_value=default_value)

    finally:
        del grizzly.scenario.variables['test']


@pytest.mark.parametrize('as_async', [False, True])
def test_add_validation_handler(grizzly_fixture: GrizzlyFixture, *, as_async: bool) -> None:
    parent = grizzly_fixture()
    grizzly = grizzly_fixture.grizzly
    behave = grizzly_fixture.behave.context

    if as_async:
        grizzly.scenario.tasks.tmp.async_group = AsyncRequestGroupTask(name='test-async-3')

    tasks = grizzly.scenario.tasks()
    tasks.clear()
    assert len(tasks) == 0

    # not preceeded by a request source
    with pytest.raises(AssertionError, match='no request source has been added'):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', condition=False)

    # add request source
    add_request_task(behave, method=RequestMethod.GET, source='{}', name='test', endpoint='/api/v2/test')

    assert len(tasks) == 1

    # empty expression, fail
    with pytest.raises(AssertionError, match='expression is empty'):
        add_validation_handler(grizzly, ResponseTarget.METADATA, '', 'test', condition=False)

    # add metadata response handler
    task = cast('RequestTask', tasks[0])
    task.response.content_type = TransformerContentType.JSON
    add_validation_handler(grizzly, ResponseTarget.METADATA, '$.test.value', 'test', condition=False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 0

    # add payload response handler
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.value', 'test', condition=False)
    assert len(task.response.handlers.metadata) == 1
    assert len(task.response.handlers.payload) == 1

    metadata_handler = next(iter(task.response.handlers.metadata))
    payload_handler = next(iter(task.response.handlers.payload))

    # test that they validates
    metadata_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)
    payload_handler((TransformerContentType.JSON, {'test': {'value': 'test'}}), parent.user)

    # test that they validates, negative
    with pytest.raises(ResponseHandlerError, match='"test" was None'):
        metadata_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), parent.user)

    with pytest.raises(ResponseHandlerError, match='"test" was None'):
        payload_handler((TransformerContentType.JSON, {'test': {'value': 'no-test'}}), parent.user)

    # add a second payload response handler
    parent.user.add_context({'variables': {'property': 'name', 'name': 'bob'}})
    add_validation_handler(grizzly, ResponseTarget.PAYLOAD, '$.test.{{ property }}', '{{ name }}', condition=False)
    assert len(task.response.handlers.payload) == 2

    # test that they validates
    for handler in task.response.handlers.payload:
        handler((TransformerContentType.JSON, {'test': {'value': 'test', 'name': 'bob'}}), parent.user)

    # add_validation_handler calling _add_response_handler incorrectly
    with pytest.raises(AssertionError, match='condition is not set'):
        _add_response_handler(grizzly, ResponseTarget.PAYLOAD, ResponseAction.VALIDATE, '$.test', 'value', condition=None)


def test_normalize_step_name() -> None:
    expected = 'this is just a "" of text with quoted ""'
    actual = normalize_step_name('this is just a "string" of text with quoted "words"')

    assert expected == actual


def test_get_task_client_error(grizzly_fixture: GrizzlyFixture) -> None:
    with pytest.raises(AssertionError, match='could not find scheme in ""'):
        get_task_client(grizzly_fixture.grizzly, '')

    with pytest.raises(AssertionError, match='no client task registered for obscure'):
        get_task_client(grizzly_fixture.grizzly, 'obscure://obscure.example.io')


@pytest.mark.parametrize('test_scheme', client.available.keys())
def test_get_task_client(grizzly_fixture: GrizzlyFixture, test_scheme: str) -> None:
    task_client = get_task_client(grizzly_fixture.grizzly, f'{test_scheme}://example.net')

    assert task_client is not None
    assert issubclass(task_client, ClientTask)

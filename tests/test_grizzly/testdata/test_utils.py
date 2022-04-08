import logging
import shutil

from os import path, environ, mkdir, sep
from typing import Dict, Any, Optional, cast
from json import dumps as jsondumps, loads as jsonloads

import pytest
import zmq

from _pytest.tmpdir import TempPathFactory

from jinja2 import Template
from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.exception import StopUser

from grizzly.context import GrizzlyContext
from grizzly.tasks import PrintTask, DateTask, TransformerTask, UntilRequestTask
from grizzly.testdata.utils import (
    _get_variable_value,
    initialize_testdata,
    create_context_variable,
    resolve_variable,
    _objectify,
    transform,
)
from grizzly.testdata.variables import AtomicCsvRow, AtomicIntegerIncrementer, AtomicMessageQueue, AtomicServiceBus
from grizzly_extras.async_message import AsyncMessageResponse
from grizzly_extras.transformer import TransformerContentType

from ..fixtures import BehaveFixture, AtomicVariableCleanupFixture, GrizzlyFixture, RequestTaskFixture, NoopZmqFixture

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


def test__get_variable_value_static(cleanup: AtomicVariableCleanupFixture) -> None:
    try:
        grizzly = GrizzlyContext()
        variable_name = 'test'

        grizzly.state.variables[variable_name] = 1337
        value, _ = _get_variable_value(variable_name)
        assert value == 1337

        grizzly.state.variables[variable_name] = '1337'
        value, _ = _get_variable_value(variable_name)
        assert value == 1337

        grizzly.state.variables[variable_name] = "'1337'"
        value, _ = _get_variable_value(variable_name)
        assert value == '1337'
    finally:
        cleanup()


def test__get_variable_value_AtomicIntegerIncrementer(cleanup: AtomicVariableCleanupFixture) -> None:
    try:
        grizzly = GrizzlyContext()

        variable_name = 'AtomicIntegerIncrementer.test'
        grizzly.state.variables[variable_name] = 1337
        value, external_dependencies = _get_variable_value(variable_name)
        assert external_dependencies == set()
        assert value['test'] == 1337
        assert value['test'] == 1338
        AtomicIntegerIncrementer.destroy()

        grizzly.state.variables[variable_name] = '1337'
        value, external_dependencies = _get_variable_value(variable_name)
        assert external_dependencies == set()
        assert value['test'] == 1337
        assert value['test'] == 1338
        AtomicIntegerIncrementer.destroy()
    finally:
        cleanup()


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='requires native grizzly-loadtester[mq]')
def test__get_variable_value_AtomicMessageQueue(noop_zmq: NoopZmqFixture, cleanup: AtomicVariableCleanupFixture) -> None:
    noop_zmq('grizzly.testdata.variables.messagequeue')

    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicMessageQueue.test'
        grizzly.state.variables[variable_name] = (
            'queue:TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"'
        )
        value, external_dependencies = _get_variable_value(variable_name)
        assert external_dependencies == set(['async-messaged'])
        assert not isinstance(value, AtomicMessageQueue)
        assert value == '__on_consumer__'

        # this fails because it shouldn't have been initiated here
        with pytest.raises(ValueError) as ve:
            AtomicMessageQueue.destroy()
        assert "'AtomicMessageQueue' is not instantiated" in str(ve)
    finally:
        cleanup()


def test__get_variable_value_AtomicServiceBus(noop_zmq: NoopZmqFixture, cleanup: AtomicVariableCleanupFixture) -> None:
    noop_zmq('grizzly.testdata.variables.servicebus')

    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicServiceBus.test'
        grizzly.state.variables[variable_name] = (
            'queue:documents-in | url="sb://sb.example.com/;SharedAccessKeyName=name;SharedAccessKey=key"'
        )
        value, external_dependencies = _get_variable_value(variable_name)
        assert external_dependencies == set(['async-messaged'])
        assert not isinstance(value, AtomicServiceBus)
        assert value == '__on_consumer__'

        with pytest.raises(ValueError) as ve:
            AtomicServiceBus.destroy()
        assert "'AtomicServiceBus' is not instantiated" in str(ve)
    finally:
        cleanup()


def test__get_variable_value_AtomicCsvRow(cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = path.dirname(test_context)
    environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

    with open(path.join(test_context, 'test.csv'), 'w') as fd:
        fd.write('header1,header2\n')
        fd.write('value1,value2\n')
        fd.write('value3,value4\n')
        fd.flush()
    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicCsvRow.test'
        grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
        value, external_dependencies = _get_variable_value(variable_name)

        assert isinstance(value, AtomicCsvRow)
        assert external_dependencies == set()
        assert 'test' in value._values
        assert 'test' in value._rows
        assert value['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert value['test'] == {'header1': 'value3', 'header2': 'value4'}
        assert value['test'] is None
    finally:
        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except:
            pass

        shutil.rmtree(test_context_root)
        cleanup()


def test_initialize_testdata_no_tasks() -> None:
    testdata, external_dependencies = initialize_testdata([])
    assert testdata == {}
    assert external_dependencies == set()


def test_initialize_testdata_with_tasks(
    request_task: RequestTaskFixture,
    cleanup: AtomicVariableCleanupFixture,
) -> None:
    try:
        grizzly = GrizzlyContext()
        grizzly.state.variables.update({
            'AtomicIntegerIncrementer.messageID': 1337,
            'AtomicDate.now': 'now',
            'transformer_task': 'none',
        })
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 1337
        grizzly.state.variables['AtomicDate.now'] = 'now'
        request = request_task.request
        request.name = '{{ request_name }}'
        request.endpoint = '/api/{{ endpoint_part }}/test'
        request.response.content_type = TransformerContentType.JSON
        scenario = request.scenario
        scenario.add_task(request)
        scenario.add_task(PrintTask(message='{{ message }}'))
        scenario.add_task(DateTask(variable='date_task', value='{{ date_task_date }} | timezone="{{ timezone }}", offset="-{{ days }}D"'))
        scenario.add_task(TransformerTask(
            expression='$.expression',
            variable='transformer_task',
            content='hello this is the {{ content }}!',
            content_type=TransformerContentType.JSON,
        ))
        scenario.add_task(UntilRequestTask(request=request, condition='{{ condition }}'))
        scenario.orphan_templates.append('hello {{ orphan }} template')
        testdata, external_dependencies = initialize_testdata(scenario.tasks)

        scenario_name = scenario.get_name()

        assert external_dependencies == set()
        assert scenario_name in testdata
        variables = testdata[scenario_name]
        assert len(variables) == 12
        assert sorted(variables.keys()) == sorted([
            'messageID',
            'AtomicIntegerIncrementer.messageID',
            'AtomicDate.now',
            'request_name',
            'endpoint_part',
            'message',
            'date_task_date',
            'timezone',
            'days',
            'content',
            'condition',
            'orphan',
        ])
    finally:
        cleanup()


def test_initialize_testdata_with_payload_context(grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture, noop_zmq: NoopZmqFixture) -> None:
    behave = grizzly_fixture.behave
    noop_zmq('grizzly.testdata.variables.messagequeue')

    try:
        _, _, scenario = grizzly_fixture()
        assert scenario is not None
        context_root = grizzly_fixture.request_task.context_root
        request = grizzly_fixture.request_task.request
        mkdir(path.join(context_root, 'adirectory'))
        for index in range(1, 3):
            with open(path.join(context_root, 'adirectory', f'file{index}.txt'), 'w') as fd:
                fd.write(f'file{index}.txt\n')
                fd.flush()

        with open(path.join(context_root, 'test.csv'), 'w') as fd:
            fd.write('header1,header2\n')
            fd.write('value1,value2\n')
            fd.write('value3,value4\n')
            fd.flush()

        assert request.source is not None
        source = jsonloads(request.source)
        source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
        source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
        source['result']['File'] = '{{ AtomicDirectoryContents.test }}'

        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.add_scenario(scenario.__class__.__name__)
        grizzly.state.variables['messageID'] = 123
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
        grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
        grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
        grizzly.state.variables['AtomicDate.now'] = 'now'
        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            grizzly.state.variables['AtomicMessageQueue.document_id'] = (
                'queue:TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"'
            )
            source['result']['DocumentID'] = '{{ AtomicMessageQueue.document_id }}'
        grizzly.state.variables['AtomicServiceBus.event'] = (
            'topic:events, subscription:grizzly | url="Endpoint=sb://sb.example.com?SharedAccessKey=asdfasdfasdf=&SharedAccessKeyName=test"'
        )
        source['result']['Event'] = '{{ AtomicServiceBus.event }}'
        grizzly.scenario.user.class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test.nu'
        grizzly.scenario.iterations = 2

        request.source = jsondumps(source)
        request.template = Template(request.source)

        grizzly.scenario.add_task(request)

        testdata, external_dependencies = initialize_testdata(grizzly.scenario.tasks)

        scenario_name = grizzly.scenario.get_name()

        assert scenario_name in testdata
        assert external_dependencies == set(['async-messaged'])

        data = testdata[scenario_name]

        assert data['messageID'] == 123

        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 456
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 457
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 458
        assert data['AtomicIntegerIncrementer.messageID']['messageID'] == 459

        assert data['AtomicCsvRow.test.header1']['test'] == {'header1': 'value1', 'header2': 'value2'}
        assert data['AtomicCsvRow.test.header2']['test']['header2'] == 'value4'
        with pytest.raises(TypeError):
            assert data['AtomicCsvRow.test.header1']['test']['header1'] is None
        assert data['AtomicCsvRow.test.header2']['test'] is None
        assert data['AtomicCsvRow.test.header1']['test'] is None

        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file1.txt'
        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory{sep}file2.txt'
        assert data['AtomicDirectoryContents.test']['test'] is None
        assert data['AtomicServiceBus.event'] == '__on_consumer__'
        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            assert data['AtomicMessageQueue.document_id'] == '__on_consumer__'
    finally:
        cleanup()


def test_create_context_variable() -> None:
    grizzly = GrizzlyContext()

    try:
        assert create_context_variable(grizzly, 'test.value', '1') == {
            'test': {
                'value': 1,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'trUe') == {
            'test': {
                'value': True,
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'AZURE') == {
            'test': {
                'value': 'AZURE',
            }
        }

        assert create_context_variable(grizzly, 'test.value', 'HOST') == {
            'test': {
                'value': 'HOST'
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD')

        environ['HELLO_WORLD'] = 'environment variable value'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 'environment variable value',
            }
        }

        environ['HELLO_WORLD'] = 'true'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': True,
            }
        }

        environ['HELLO_WORLD'] = '1337'
        assert create_context_variable(grizzly, 'test.value', '$env::HELLO_WORLD') == {
            'test': {
                'value': 1337,
            }
        }

        with pytest.raises(AssertionError):
            create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username')

        grizzly.state.configuration['test.auth.user.username'] = 'username'
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.user.username') == {
            'test': {
                'value': 'username',
            }
        }

        grizzly.state.configuration['test.auth.refresh_time'] = 3000
        assert create_context_variable(grizzly, 'test.value', '$conf::test.auth.refresh_time') == {
            'test': {
                'value': 3000,
            }
        }
    finally:
        GrizzlyContext.destroy()
        del environ['HELLO_WORLD']


def test_resolve_variable() -> None:
    grizzly = GrizzlyContext()

    try:
        assert 'test' not in grizzly.state.variables
        with pytest.raises(AssertionError):
            resolve_variable(grizzly, '{{ test }}')

        grizzly.state.variables['test'] = 'some value'
        assert resolve_variable(grizzly, '{{ test }}') == 'some value'

        assert resolve_variable(grizzly, "now | format='%Y-%m-%d %H'") == "now | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, "{{ test }} | format='%Y-%m-%d %H'") == "some value | format='%Y-%m-%d %H'"

        assert resolve_variable(grizzly, 'static value') == 'static value'
        assert resolve_variable(grizzly, '"static value"') == 'static value'
        assert resolve_variable(grizzly, "'static value'") == 'static value'
        assert resolve_variable(grizzly, "'static' value") == "'static' value"
        assert resolve_variable(grizzly, "static 'value'") == "static 'value'"

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "static 'value\"")

        with pytest.raises(ValueError):
            resolve_variable(grizzly, "'static\" value")

        grizzly.state.variables['number'] = 100
        assert resolve_variable(grizzly, '{{ (number * 0.25) | int }}') == 25

        assert resolve_variable(grizzly, '{{ (number * 0.25 * 0.2) | int }}') == 5

        try:
            with pytest.raises(AssertionError):
                resolve_variable(grizzly, '$env::HELLO_WORLD')

            environ['HELLO_WORLD'] = 'first environment variable!'

            assert resolve_variable(grizzly, '$env::HELLO_WORLD') == 'first environment variable!'

            environ['HELLO_WORLD'] = 'first "environment" variable!'
            assert resolve_variable(grizzly, '$env::HELLO_WORLD') == 'first "environment" variable!'
        finally:
            del environ['HELLO_WORLD']

        with pytest.raises(AssertionError):
            resolve_variable(grizzly, '$conf::sut.host')

        grizzly.state.configuration['sut.host'] = 'http://host.docker.internal:8003'

        assert resolve_variable(grizzly, '$conf::sut.host')

        grizzly.state.configuration['sut.greeting'] = 'hello "{{ test }}"!'
        assert resolve_variable(grizzly, '$conf::sut.greeting') == 'hello "{{ test }}"!'

        with pytest.raises(ValueError):
            resolve_variable(grizzly, '$test::hello')

        assert resolve_variable(grizzly, '') == ''
    finally:
        GrizzlyContext.destroy()


def test__objectify() -> None:
    testdata: Dict[str, Any] = {
        'AtomicIntegerIncrementer': {
            'test': 1337,
        },
        'test': 1338,
        'AtomicCsvRow': {
            'input': {
                'test1': 'hello',
                'test2': 'world!',
            }
        },
        'Test': {
            'test1': {
                'test2': {
                    'test3': 'value',
                }
            }
        }
    }

    obj = _objectify(testdata)

    assert (
        obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
        and obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
    )
    assert getattr(obj['AtomicIntegerIncrementer'], 'test') == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert (
        obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils'
        and obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
    )
    atomiccsvrow_input = getattr(obj['AtomicCsvRow'], 'input', None)
    assert atomiccsvrow_input is not None
    assert (
        atomiccsvrow_input.__module__ == 'grizzly.testdata.utils'
        and atomiccsvrow_input.__class__.__name__ == 'Testdata'
    )
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert (
        obj['Test'].__module__ == 'grizzly.testdata.utils'
        and obj['Test'].__class__.__name__ == 'Testdata'
    )
    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils'
        and test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test2', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils'
        and test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test3', None)
    assert test is not None
    assert isinstance(test, str)
    assert test == 'value'


def test_transform_no_objectify() -> None:
    data = {
        'test.number.value': 1337,
        'test.number.description': 'simple description',
        'test.string.value': 'hello world!',
        'test.bool.value': True,
    }

    actual = transform(data, objectify=False)

    assert actual == {
        'test': {
            'number': {
                'value': 1337,
                'description': 'simple description',
            },
            'string': {
                'value': 'hello world!',
            },
            'bool': {
                'value': True,
            }
        }
    }


def test_transform(behave_fixture: BehaveFixture, noop_zmq: NoopZmqFixture, cleanup: AtomicVariableCleanupFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    behave = behave_fixture.context
    noop_zmq('grizzly.testdata.variables.servicebus')

    def mock_response(response: Optional[AsyncMessageResponse], repeat: int = 1) -> None:
        mocker.patch(
            'grizzly.testdata.variables.servicebus.zmq.Socket.recv_json',
            side_effect=[zmq.Again(), response] * repeat
        )
    try:
        grizzly = cast(GrizzlyContext, behave.grizzly)
        data: Dict[str, Any] = {
            'AtomicIntegerIncrementer.test': 1337,
            'test': 1338,
            'AtomicCsvRow.input.test1': 'hello',
            'AtomicCsvRow.input.test2': 'world!',
            'Test.test1.test2.test3': 'value',
            'Test.test1.test2.test4': 'value',
            'Test.test2.test3': 'value',
            'AtomicServiceBus.document_id': '__on_consumer__',
        }
        grizzly.state.variables['AtomicServiceBus.document_id'] = (
            'queue:messages | url="Endpoint=sb://sb.example.com?SharedAccessKey=asdfasdfasdf=&SharedAccessKeyName=test", repeat=True'
        )

        mock_response({
            'success': True,
            'worker': '1337-aaaabbbb-beef',
        }, 2)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                transform(data)
        assert 'AtomicServiceBus.document_id: payload in response was None' in caplog.text
        caplog.clear()

        mock_response({
            'success': True,
            'worker': '1337-aaaabbbb-beef',
            'payload': jsondumps({
                'document': {
                    'id': 'DOCUMENT_1337-1',
                    'name': 'Boring presentation',
                    'author': 'Drew Ackerman',
                }
            }),
        })

        obj = transform(data)

        assert (
            obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils'
            and obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicIntegerIncrementer'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert (
            obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils'
            and obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
        )
        assert (
            obj['AtomicCsvRow'].input.__module__ == 'grizzly.testdata.utils'
            and obj['AtomicCsvRow'].input.__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicCsvRow'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvRow'].input, 'test2', None) == 'world!'
        assert (
            obj['Test'].__module__ == 'grizzly.testdata.utils'
            and obj['Test'].__class__.__name__ == 'Testdata'
        )
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils'
            and test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test2', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils'
            and test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test3', None)
        assert test is not None
        assert isinstance(test, str)
        assert test == 'value'

        assert getattr(obj['AtomicServiceBus'], 'document_id', None) == jsondumps({
            'document': {
                'id': 'DOCUMENT_1337-1',
                'name': 'Boring presentation',
                'author': 'Drew Ackerman',
            }
        })

        # AtomicMessageQueue.document_id should repeat old values when there is no
        # new message on queue since repeat=True
        mock_response({
            'success': False,
            'worker': '1337-aaaabbbb-beef',
            'message': 'no message on queue:messages',
        })

        obj = transform({
            'AtomicServiceBus.document_id': '__on_consumer__',
        })

        assert getattr(obj['AtomicServiceBus'], 'document_id', None) == jsondumps({
            'document': {
                'id': 'DOCUMENT_1337-1',
                'name': 'Boring presentation',
                'author': 'Drew Ackerman',
            }
        })

        caplog.clear()
    finally:
        cleanup()

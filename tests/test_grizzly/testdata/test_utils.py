import logging
import shutil

from os import path, environ
from typing import Callable, List, Tuple, Dict, Any, Optional, cast
from json import dumps as jsondumps, loads as jsonloads
from os import mkdir, path

import pytest
import zmq

from _pytest.tmpdir import TempdirFactory

from jinja2 import Template
from behave.runner import Context
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.exception import StopUser

from grizzly.context import GrizzlyContext
from grizzly.task import RequestTask
from grizzly.testdata.utils import (
    _get_variable_value,
    initialize_testdata,
    create_context_variable,
    resolve_variable,
    _objectify,
    transform,
)
from grizzly.testdata.variables import AtomicCsvRow, AtomicIntegerIncrementer, AtomicMessageQueue
from grizzly_extras.async_message import AsyncMessageResponse

from ..fixtures import grizzly_context, request_task, behave_context, locust_environment, noop_zmq  # pylint: disable=unused-import
from .fixtures import cleanup  # pylint: disable=unused-import

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

# pylint: disable=redefined-outer-name


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_static(cleanup: Callable) -> None:
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


@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicIntegerIncrementer(cleanup: Callable) -> None:
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
@pytest.mark.usefixtures('cleanup')
def test__get_variable_value_AtomicMessageQueue(noop_zmq: Callable[[str], None], cleanup: Callable) -> None:
    noop_zmq('grizzly.testdata.variables.messagequeue')

    try:
        grizzly = GrizzlyContext()
        variable_name = 'AtomicMessageQueue.test'
        grizzly.state.variables[variable_name] = (
            'TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN", expression="$.test.result", content_type=json'
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


@pytest.mark.usefixtures('cleanup', 'tmpdir_factory')
def test__get_variable_value_AtomicCsvRow(cleanup: Callable, tmpdir_factory: TempdirFactory) -> None:
    test_context = str(tmpdir_factory.mktemp('test_context').mkdir('requests'))
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
        shutil.rmtree(test_context_root)
        cleanup()


def test_initialize_testdata_no_tasks() -> None:
    testdata, external_dependencies = initialize_testdata(None)
    assert testdata == {}
    assert external_dependencies == set()

    testdata, external_dependencies = initialize_testdata([])
    assert testdata == {}
    assert external_dependencies == set()


@pytest.mark.usefixtures('request_task', 'cleanup')
def test_initialize_testdata_with_tasks(
    request_task: Tuple[str, str, RequestTask],
    cleanup: Callable,
) -> None:
    try:
        grizzly = GrizzlyContext()
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 1337
        grizzly.state.variables['AtomicDate.now'] = 'now'
        _, _, request = request_task
        scenario = request.scenario
        scenario.add_task(request)
        testdata, external_dependencies = initialize_testdata(cast(List[RequestTask], scenario.tasks))

        scenario_name = scenario.get_name()

        assert external_dependencies == set()
        assert scenario_name in testdata
        assert len(testdata[scenario_name]) == 3
        assert 'messageID' in testdata[scenario_name]
        assert 'AtomicIntegerIncrementer.messageID' in testdata[scenario_name]
        assert 'AtomicDate.now' in testdata[scenario_name]
    finally:
        cleanup()


@pytest.mark.usefixtures('behave_context', 'grizzly_context', 'cleanup', 'noop_zmq')
def test_initialize_testdata_with_payload_context(behave_context: Context, grizzly_context: Callable, cleanup: Callable, noop_zmq: Callable[[str], None]) -> None:
    noop_zmq('grizzly.testdata.variables.messagequeue')
    try:
        _, _, task, [context_root, _, request] = grizzly_context()
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

        source = jsonloads(request.source)
        source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
        source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
        source['result']['File'] = '{{ AtomicDirectoryContents.test }}'

        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        grizzly.add_scenario(task.__class__.__name__)
        grizzly.state.variables['messageID'] = 123
        grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
        grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
        grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
        grizzly.state.variables['AtomicDate.now'] = 'now'
        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            grizzly.state.variables['AtomicMessageQueue.document_id'] = (
                'TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN", expression="$.test.result", content_type=json'
            )
            source['result']['DocumentID'] = '{{ AtomicMessageQueue.document_id }}'
        grizzly.scenario.user.class_name = 'TestUser'
        grizzly.scenario.context['host'] = 'http://test.nu'
        grizzly.scenario.iterations = 2

        request.source = jsondumps(source)
        request.template = Template(request.source)

        grizzly.scenario.add_task(request)

        testdata, external_dependencies = initialize_testdata(cast(List[RequestTask], grizzly.scenario.tasks))

        scenario_name = grizzly.scenario.get_name()

        assert scenario_name in testdata
        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            assert external_dependencies == set(['async-messaged'])
        else:
            assert external_dependencies == set()

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

        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory/file1.txt'
        assert data['AtomicDirectoryContents.test']['test'] == f'adirectory/file2.txt'
        assert data['AtomicDirectoryContents.test']['test'] is None
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
        obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils' and
        obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
    )
    assert getattr(obj['AtomicIntegerIncrementer'], 'test') == 1337
    assert isinstance(obj['test'], int)
    assert obj['test'] == 1338
    assert (
        obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils' and
        obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
    )
    atomiccsvrow_input = getattr(obj['AtomicCsvRow'], 'input', None)
    assert atomiccsvrow_input is not None
    assert (
        atomiccsvrow_input.__module__ == 'grizzly.testdata.utils' and
        atomiccsvrow_input.__class__.__name__ == 'Testdata'
    )
    assert getattr(atomiccsvrow_input, 'test1', None) == 'hello'
    assert getattr(atomiccsvrow_input, 'test2', None) == 'world!'

    assert (
        obj['Test'].__module__ == 'grizzly.testdata.utils' and
        obj['Test'].__class__.__name__ == 'Testdata'
    )
    test = getattr(obj['Test'], 'test1', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils' and
        test.__class__.__name__ == 'Testdata'
    )
    test = getattr(test, 'test2', None)
    assert test is not None
    assert (
        test.__module__ == 'grizzly.testdata.utils' and
        test.__class__.__name__ == 'Testdata'
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


@pytest.mark.usefixtures('behave_context', 'noop_zmq', 'cleanup', 'mocker', 'noop_zmq')
def test_transform(behave_context: Context, noop_zmq: Callable[[str], None], cleanup: Callable, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    noop_zmq('grizzly.testdata.variables.messagequeue')

    def mock_response(response: Optional[AsyncMessageResponse], repeat: int = 1) -> None:
        mocker.patch(
            'grizzly.testdata.variables.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[zmq.Again(), response] * repeat
        )
    try:
        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        data: Dict[str, Any] = {
            'AtomicIntegerIncrementer.test': 1337,
            'test': 1338,
            'AtomicCsvRow.input.test1': 'hello',
            'AtomicCsvRow.input.test2': 'world!',
            'Test.test1.test2.test3': 'value',
            'Test.test1.test2.test4': 'value',
            'Test.test2.test3': 'value',
        }

        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            grizzly.state.variables['AtomicMessageQueue.document_id'] = (
                'TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN", expression="$.document.id", content_type=json, repeat=True'
            )
            data['AtomicMessageQueue.document_id'] = '__on_consumer__'

            mock_response({
                'success': True,
                'worker': '1337-aaaabbbb-beef',
            }, 2)

            with caplog.at_level(logging.ERROR):
                with pytest.raises(StopUser):
                    transform(data)
            assert 'AtomicMessageQueue.document_id: payload in response was None' in caplog.text
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
                })
            })

            obj = transform(data)
        else:
            obj = transform(data)

        assert (
            obj['AtomicIntegerIncrementer'].__module__ == 'grizzly.testdata.utils' and
            obj['AtomicIntegerIncrementer'].__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicIntegerIncrementer'], 'test', None) == 1337
        assert isinstance(obj['test'], int)
        assert obj['test'] == 1338
        assert (
            obj['AtomicCsvRow'].__module__ == 'grizzly.testdata.utils' and
            obj['AtomicCsvRow'].__class__.__name__ == 'Testdata'
        )
        assert (
            obj['AtomicCsvRow'].input.__module__ == 'grizzly.testdata.utils' and
            obj['AtomicCsvRow'].input.__class__.__name__ == 'Testdata'
        )
        assert getattr(obj['AtomicCsvRow'].input, 'test1', None) == 'hello'
        assert getattr(obj['AtomicCsvRow'].input, 'test2', None) == 'world!'
        assert (
            obj['Test'].__module__ == 'grizzly.testdata.utils' and
            obj['Test'].__class__.__name__ == 'Testdata'
        )
        test = getattr(obj['Test'], 'test1', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test2', None)
        assert test is not None
        assert (
            test.__module__ == 'grizzly.testdata.utils' and
            test.__class__.__name__ == 'Testdata'
        )
        test = getattr(test, 'test3', None)
        assert test is not None
        assert isinstance(test, str)
        assert test == 'value'

        if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
            assert getattr(obj['AtomicMessageQueue'], 'document_id', None) == 'DOCUMENT_1337-1'

            # AtomicMessageQueue.document_id should repeat old values when there is no
            # new message on queue since repeat=True
            mock_response({
                'success': False,
                'worker': '1337-aaaabbbb-beef',
                'message': 'MQRC_NO_MSG_AVAILABLE',
            })

            obj = transform({
                'AtomicMessageQueue.document_id': '__on_consumer__',
            })

            assert getattr(obj['AtomicMessageQueue'], 'document_id', None) == 'DOCUMENT_1337-1'

            caplog.clear()

            grizzly.state.variables['AtomicMessageQueue.wrong_content_type'] = (
                'TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN", expression="$.document.id", content_type=json, repeat=True'
            )

            data = {
                'AtomicMessageQueue.wrong_content_type': '__on_consumer__',
            }

            noop_zmq('grizzly.testdata.variables.messagequeue')

            mock_response({
                'success': True,
                'worker': '1337-aaaabbbb-beef',
            }, 2)

            with caplog.at_level(logging.ERROR):
                with pytest.raises(StopUser):
                    transform(data)
            assert 'AtomicMessageQueue.wrong_content_type: payload in response was None' in caplog.text
            caplog.clear()

            mock_response({
                'success': True,
                'worker': '1337-aaaabbbb-beef',
                'payload': '<?xml encoding="utf-8" version="1.0"?><documents><document id="DOCUMENT_1337-1"></document></documents>',
            })

            with caplog.at_level(logging.ERROR):
                with pytest.raises(StopUser):
                    transform({
                        'AtomicMessageQueue.wrong_content_type': '__on_consumer__',
                    })

            assert 'AtomicMessageQueue.wrong_content_type: failed to transform input as JSON' in caplog.text
            caplog.clear()
    finally:
        cleanup()

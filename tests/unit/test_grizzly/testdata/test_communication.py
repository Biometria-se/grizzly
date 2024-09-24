"""Unit tests of grizzly.testdata.communication."""
from __future__ import annotations

import json
import logging
from contextlib import suppress
from os import environ, sep
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Optional, cast

import gevent
import pytest
import zmq.green as zmq
from zmq.error import Again as ZMQAgain
from zmq.error import ZMQError

from grizzly.tasks import LogMessageTask
from grizzly.testdata.communication import TestdataConsumer, TestdataProducer
from grizzly.testdata.utils import initialize_testdata, transform
from grizzly.testdata.variables import AtomicIntegerIncrementer
from grizzly.testdata.variables.csv_writer import atomiccsvwriter_message_handler
from grizzly.types.locust import StopUser
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from tests.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, NoopZmqFixture


def echo(value: dict[str, Any]) -> dict[str, Any]:
    return {'data': None, **value}

def echo_add_data(return_value: Any | list[Any]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    if not isinstance(return_value, list):
        return_value = [return_value]

    def wrapped(request: dict[str, Any]) -> dict[str, Any]:
        return {'data': return_value.pop(0), **request}

    return wrapped


class TestTestdataProducer:
    def test_run(  # noqa: PLR0915
        self,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
        caplog: LogCaptureFixture,
        mocker: MockerFixture,
    ) -> None:
        producer: Optional[TestdataProducer] = None
        context: Optional[zmq.Context] = None
        request = grizzly_fixture.request_task.request

        success = False

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run.feature'
        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        try:
            parent = grizzly_fixture()
            address = 'tcp://127.0.0.1:5555'

            (context_root / 'adirectory').mkdir()

            for index in range(1, 3):
                value = f'file{index}.txt'
                (context_root / 'adirectory' / value).write_text(f'{value}\n')

            (context_root / 'test.csv').write_text("""header1,header2
value1,value2
value3,value4
""")

            with (context_root / 'test.json').open('w') as fd:
                json.dump([{'header1': 'value1', 'header2': 'value2'}, {'header1': 'value3', 'header2': 'value4'}], fd)

            assert request.source is not None

            source = json.loads(request.source)
            source['result'].update({
                'File': '{{ AtomicDirectoryContents.test }}',
                'CsvRowValue1': '{{ AtomicCsvReader.test.header1 }}',
                'CsvRowValue2': '{{ AtomicCsvReader.test.header2 }}',
                'JsonRowValue1': '{{ AtomicJsonReader.test.header1 }}',
                'JsonRowValue2': '{{ AtomicJsonReader.test.header2 }}',
                'JsonRowValue': '{{ AtomicJsonReader.test2 }}',
                'IntWithStep': '{{ AtomicIntegerIncrementer.value }}',
                'UtcDate': '{{ AtomicDate.utc }}',
                'CustomVariable': '{{ tests.helpers.AtomicCustomVariable.foo }}',
            })

            grizzly = grizzly_fixture.grizzly

            testdata_request_spy = mocker.spy(grizzly.events.testdata_request, 'fire')
            keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

            grizzly.scenarios.clear()
            grizzly.scenarios.create(grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
            grizzly.scenario.orphan_templates.append('{{ AtomicCsvWriter.output }}')
            grizzly.scenario.variables.update({
                'messageID': 123,
                'AtomicIntegerIncrementer.messageID': 456,
                'AtomicDirectoryContents.test': 'adirectory',
                'AtomicCsvReader.test': 'test.csv',
                'AtomicJsonReader.test': 'test.json',
                'AtomicJsonReader.test2': 'test.json',
                'AtomicCsvWriter.output': 'output.csv | headers="foo,bar"',
                'AtomicIntegerIncrementer.value': '1 | step=5, persist=True',
                'AtomicDate.utc': "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC",
                'AtomicDate.now': 'now',
                'world': 'hello!',
                'tests.helpers.AtomicCustomVariable.foo': 'bar',
            })
            grizzly.scenario.variables.alias.update({
                'AtomicCsvReader.test.header1': 'auth.user.username',
                'AtomicCsvReader.test.header2': 'auth.user.password',
            })
            grizzly.scenario.iterations = 2
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'

            request.source = json.dumps(source)

            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='hello {{ world }}'))

            testdata, external_dependencies, message_handlers = initialize_testdata(grizzly)

            assert external_dependencies == set()
            assert message_handlers == {'atomiccsvwriter': atomiccsvwriter_message_handler}

            producer = TestdataProducer(
                grizzly=grizzly,
                address=address,
                testdata=testdata,
            )
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            assert producer.keystore == {}

            context = zmq.Context()
            with context.socket(zmq.REQ) as socket:
                socket.connect(address)

                def request_testdata() -> dict[str, Any]:
                    socket.send_json({
                        'message': 'testdata',
                        'identifier': grizzly.scenario.class_name,
                    })

                    gevent.sleep(0.1)

                    return cast(dict[str, Any], socket.recv_json())

                def request_keystore(action: str, key: str, value: Optional[Any] = None) -> dict[str, Any]:
                    request = {
                        'message': 'keystore',
                        'identifier': grizzly.scenario.class_name,
                        'action': action,
                        'key': key,
                    }

                    if value is not None:
                        request.update({'data': value})

                    socket.send_json(request)

                    gevent.sleep(0.1)

                    return cast(dict[str, Any], socket.recv_json())

                message: dict[str, Any] = request_testdata()
                testdata_request_spy.assert_called_once_with(
                    reverse=False,
                    timestamp=ANY(str),
                    tags={
                        'action': 'consume',
                        'type': 'producer',
                        'identifier': grizzly.scenario.class_name,
                    },
                    measurement='request_testdata',
                    metrics={
                        'response_time': ANY(float),
                        'error': None,
                    },
                )
                testdata_request_spy.reset_mock()
                keystore_request_spy.assert_not_called()
                assert message['action'] == 'consume'
                data = message['data']
                assert 'variables' in data
                variables = data['variables']
                assert variables == {
                    'AtomicIntegerIncrementer.messageID': 456,
                    'AtomicDate.now': ANY(str),
                    'messageID': 123,
                    'AtomicDirectoryContents.test': f'adirectory{sep}file1.txt',
                    'AtomicDate.utc': ANY(str),
                    'AtomicCsvReader.test.header1': 'value1',
                    'AtomicCsvReader.test.header2': 'value2',
                    'AtomicJsonReader.test.header1': 'value1',
                    'AtomicJsonReader.test.header2': 'value2',
                    'AtomicJsonReader.test2': {'header1': 'value1', 'header2': 'value2'},
                    'AtomicIntegerIncrementer.value': 1,
                    'tests.helpers.AtomicCustomVariable.foo': 'bar',
                    'world': 'hello!',
                }
                assert data == {
                    'variables': variables,
                    'auth.user.username': 'value1',
                    'auth.user.password': 'value2',
                }
                assert producer.keystore == {}

                message = request_keystore('set', 'foobar', {'hello': 'world'})
                testdata_request_spy.assert_not_called()
                keystore_request_spy.assert_called_once_with(
                    reverse=False,
                    timestamp=ANY(str),
                    tags={
                        'identifier': grizzly.scenario.class_name,
                        'action': 'set',
                        'key': 'foobar',
                        'type': 'producer',
                    },
                    measurement='request_keystore',
                    metrics={
                        'response_time': ANY(float),
                        'error': None,
                    },
                )
                assert message == {
                    'message': 'keystore',
                    'action': 'set',
                    'identifier': grizzly.scenario.class_name,
                    'key': 'foobar',
                    'data': {'hello': 'world'},
                }

                message = request_testdata()
                assert message['action'] == 'consume'
                data = message['data']
                assert 'variables' in data
                variables = data['variables']
                assert 'AtomicIntegerIncrementer.messageID' in variables
                assert 'AtomicDate.now' in variables
                assert 'messageID' in variables
                assert variables['AtomicIntegerIncrementer.messageID'] == 457
                assert variables['messageID'] == 123
                assert variables['AtomicDirectoryContents.test'] == f'adirectory{sep}file2.txt'
                assert variables['AtomicCsvReader.test.header1'] == 'value3'
                assert variables['AtomicCsvReader.test.header2'] == 'value4'
                assert variables['AtomicJsonReader.test.header1'] == 'value3'
                assert variables['AtomicJsonReader.test.header2'] == 'value4'
                assert variables['AtomicJsonReader.test2'] == {'header1': 'value3', 'header2': 'value4'}
                assert variables['AtomicIntegerIncrementer.value'] == 6
                assert data['auth.user.username'] == 'value3'
                assert data['auth.user.password'] == 'value4'

                message = request_keystore('get', 'foobar')
                assert message == {
                    'message': 'keystore',
                    'action': 'get',
                    'identifier': grizzly.scenario.class_name,
                    'key': 'foobar',
                    'data': {'hello': 'world'},
                }

                caplog.clear()

                with caplog.at_level(logging.ERROR):
                    message = request_keystore('inc', 'counter', 1)
                    assert message == {
                        'message': 'keystore',
                        'action': 'inc',
                        'identifier': grizzly.scenario.class_name,
                        'key': 'counter',
                        'data': 1,
                    }

                assert caplog.messages == []

                caplog.clear()
                producer.keystore.update({'counter': 'asdf'})

                with caplog.at_level(logging.ERROR):
                    message = request_keystore('inc', 'counter', 1)
                    assert message == {
                        'message': 'keystore',
                        'action': 'inc',
                        'identifier': grizzly.scenario.class_name,
                        'key': 'counter',
                        'data': None,
                        'error': 'value asdf for key "counter" cannot be incremented',
                    }

                assert caplog.messages == ['value asdf for key "counter" cannot be incremented']

                caplog.clear()
                producer.keystore.update({'counter': 1})

                with caplog.at_level(logging.ERROR):
                    message = request_keystore('inc', 'counter', 1)
                    assert message == {
                        'message': 'keystore',
                        'action': 'inc',
                        'identifier': grizzly.scenario.class_name,
                        'key': 'counter',
                        'data': 2,
                    }

                assert caplog.messages == []

                with caplog.at_level(logging.ERROR):
                    message = request_keystore('inc', 'counter', 10)
                    assert message == {
                        'message': 'keystore',
                        'action': 'inc',
                        'identifier': grizzly.scenario.class_name,
                        'key': 'counter',
                        'data': 12,
                    }

                assert caplog.messages == []

                message = request_testdata()
                assert message['action'] == 'stop'
                assert 'data' not in message

                producer_thread.join(timeout=1)
                success = True
        finally:
            if producer is not None:
                producer.stop()
                assert producer.context._instance is None

                persist_file = Path(context_root).parent / 'persistent' / 'test_run.json'
                assert persist_file.exists()

                if success:
                    actual_initial_values = json.loads(persist_file.read_text())
                    assert actual_initial_values == {
                        'IteratorScenario_001': {
                            'AtomicIntegerIncrementer.value': '11 | step=5, persist=True',
                        },
                    }

            if context is not None:
                context.destroy()

            cleanup()

    def test_run_variable_none(
        self,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
    ) -> None:
        producer: Optional[TestdataProducer] = None
        context: Optional[zmq.Context] = None

        try:
            grizzly = grizzly_fixture.grizzly
            parent = grizzly_fixture()
            context_root = grizzly_fixture.test_context / 'requests'
            context_root.mkdir(exist_ok=True)
            request = grizzly_fixture.request_task.request
            address = 'tcp://127.0.0.1:5555'
            environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'

            (context_root / 'adirectory').mkdir()

            assert request.source is not None

            source = json.loads(request.source)
            source['result'].update({'File': '{{ AtomicDirectoryContents.file }}'})

            request.source = json.dumps(source)

            grizzly.scenarios.clear()
            grizzly.scenarios.create(grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
            grizzly.scenario.variables.update({
                'messageID': 123,
                'AtomicIntegerIncrementer.messageID': 456,
                'AtomicDirectoryContents.file': 'adirectory',
                'AtomicDate.now': 'now',
                'sure': 'no',
            })
            grizzly.scenario.iterations = 0
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.example.com'
            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='are you {{ sure }}'))

            testdata, external_dependencies, message_handlers = initialize_testdata(grizzly)

            assert external_dependencies == set()
            assert message_handlers == {}

            producer = TestdataProducer(
                grizzly=grizzly,
                address=address,
                testdata=testdata,
            )
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            context = zmq.Context()
            with context.socket(zmq.REQ) as socket:
                socket.connect(address)

                def get_message_from_producer() -> dict[str, Any]:
                    socket.send_json({
                        'message': 'testdata',
                        'scenario': grizzly.scenario.class_name,
                    })

                    gevent.sleep(0.1)

                    return cast(dict[str, Any], socket.recv_json())

                message: dict[str, Any] = get_message_from_producer()
                assert message['action'] == 'stop'

                producer_thread.join(timeout=1)
        finally:
            if producer is not None:
                producer.stop()
                assert producer.context._instance is None

                persist_file = Path(context_root).parent / 'persistent' / 'test_run_with_none.json'
                assert not persist_file.exists()

            if context is not None:
                context.destroy()

            cleanup()

    def test_on_stop(self, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        try:
            environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'
            producer = TestdataProducer(grizzly_fixture.grizzly, {})
            producer.scenarios_iteration = {
                'test-scenario-1': 10,
                'test-scenario-2': 5,
            }

            producer.on_test_stop()

            for scenario, count in producer.scenarios_iteration.items():
                assert count == 0, f'iteration count for {scenario} was not reset'
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            cleanup()

    def test_stop_exception(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, noop_zmq: NoopZmqFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')
        mocker.patch('grizzly.testdata.communication.zmq.Context.destroy', side_effect=[RuntimeError('zmq.Context.destroy failed')] * 3)

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        persistent_file = grizzly_fixture.test_context / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(grizzly_fixture.grizzly, {}).stop()
            assert 'failed to stop' in caplog.messages[-1]
            assert not persistent_file.exists()

            i = AtomicIntegerIncrementer(scenario=scenario1, variable='foobar', value='1 | step=1, persist=True')
            j = AtomicIntegerIncrementer(scenario=scenario2, variable='foobar', value='10 | step=10, persist=True')

            for v in [i, j]:
                v['foobar']
                v['foobar']

            actual_keystore = {'foo': ['hello', 'world'], 'bar': {'hello': 'world', 'foo': 'bar'}, 'hello': 'world'}

            with caplog.at_level(logging.DEBUG):
                producer = TestdataProducer(grizzly_fixture.grizzly, {
                    scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                    scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                })
                producer.keystore.update(actual_keystore)
                producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            assert caplog.messages[-2] == 'failed to stop'

            assert persistent_file.exists()

            actual_persist_values = json.loads(persistent_file.read_text())
            assert actual_persist_values == {
                'IteratorScenario_001': {
                    'AtomicIntegerIncrementer.foobar': '3 | step=1, persist=True',
                },
                'IteratorScenario_002': {
                    'AtomicIntegerIncrementer.foobar': '30 | step=10, persist=True',
                },
            }

            i['foobar']
            j['foobar']

            with caplog.at_level(logging.DEBUG):
                producer = TestdataProducer(grizzly_fixture.grizzly, {
                    scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                    scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                })
                producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            assert caplog.messages[-2] == 'failed to stop'

            assert persistent_file.exists()

            actual_persist_values = json.loads(persistent_file.read_text())
            assert actual_persist_values == {
                'IteratorScenario_001': {
                    'AtomicIntegerIncrementer.foobar': '5 | step=1, persist=True',
                },
                'IteratorScenario_002': {
                    'AtomicIntegerIncrementer.foobar': '50 | step=10, persist=True',
                },
            }
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']
            cleanup()

    def test_run_type_error(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch('grizzly.testdata.communication.zmq.Socket.recv_json', return_value={
            'message': 'testdata',
            'identifier': 'test-consumer',
            'scenario': 'test',
        })
        mocker.patch(
            'grizzly.context.GrizzlyContextScenarios.find_by_class_name',
            side_effect=[TypeError('TypeError raised'), ZMQError],
        )

        send_json_mock = noop_zmq.get_mock('send_json')
        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_run_type_error.feature'

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(grizzly_fixture.grizzly, {}).run()

            assert caplog.messages[-3] == "producing {'action': 'stop'} for consumer test-consumer"
            assert 'test data error, stop consumer test-consumer' in caplog.messages[-4]
            send_json_mock.assert_called_once_with({'action': 'stop'})
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            cleanup()

    def test_run_keystore(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.testdata.communication')

        context_root = Path(grizzly_fixture.request_task.context_root).parent

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_keystore.feature'
        environ['GRIZZLY_CONTEXT_ROOT'] = context_root.as_posix()

        send_json_mock = noop_zmq.get_mock('send_json')
        send_json_mock.side_effect = ZMQError

        recv_json_mock = noop_zmq.get_mock('recv_json')

        try:
            producer = TestdataProducer(grizzly_fixture.grizzly, {})
            producer._stopping = True  # to mask error used for breaking out of loop
            producer.keystore.update({'hello': 'world'})

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'get', 'key': 'hello'}

            with caplog.at_level(logging.ERROR):
                producer.run()
            assert caplog.messages == []

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'get',
                'key': 'hello',
                'data': 'world',
            })
            send_json_mock.reset_mock()

            producer.keystore.clear()

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'set', 'key': 'world', 'data': {'foo': 'bar'}}

            with caplog.at_level(logging.ERROR):
                producer.run()
            assert caplog.messages == []

            assert producer.keystore == {'world': {'foo': 'bar'}}

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'set',
                'key': 'world',
                'data': {'foo': 'bar'},
            })
            send_json_mock.reset_mock()

            # <!-- push
            recv_json_mock.return_value = {'message': 'keystore', 'action': 'push', 'key': 'foobar', 'data': 'foobar'}

            assert 'foobar' not in producer.keystore
            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'push',
                'key': 'foobar',
                'data': 'foobar',
            })
            send_json_mock.reset_mock()

            assert producer.keystore['foobar'] == ['foobar']

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'push', 'key': 'foobar', 'data': 'foobaz'}

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'push',
                'key': 'foobar',
                'data': 'foobaz',
            })
            send_json_mock.reset_mock()

            assert producer.keystore['foobar'] == ['foobar', 'foobaz']
            # // push -->

            # <!-- pop
            recv_json_mock.return_value = {'message': 'keystore', 'action': 'pop', 'key': 'world', 'data': 'foobar'}
            assert 'world' in producer.keystore

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'pop',
                'key': 'world',
                'data': None,
                'error': 'key "world" is not a list, it has not been pushed to',
            })
            send_json_mock.reset_mock()

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'pop', 'key': 'foobaz', 'data': 'foobar'}

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'pop',
                'key': 'foobaz',
                'data': None,
            })
            send_json_mock.reset_mock()

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'pop', 'key': 'foobar'}

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'pop',
                'key': 'foobar',
                'data': 'foobar',
            })
            send_json_mock.reset_mock()

            assert producer.keystore['foobar'] == ['foobaz']

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'pop',
                'key': 'foobar',
                'data': 'foobaz',
            })
            send_json_mock.reset_mock()

            assert producer.keystore['foobar'] == []

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'pop',
                'key': 'foobar',
                'data': None,
            })
            send_json_mock.reset_mock()
            # // pop -->

            # <!-- del
            assert 'foobar' in producer.keystore

            recv_json_mock.return_value = {'message': 'keystore', 'action': 'del', 'key': 'foobar', 'data': 'dummy'}

            producer.run()

            assert 'foobar' not in producer.keystore

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'del',
                'key': 'foobar',
                'data': None,
            })
            send_json_mock.reset_mock()

            producer.run()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'del',
                'key': 'foobar',
                'data': None,
                'error': 'failed to remove key "foobar"',
            })
            send_json_mock.reset_mock()
            # // del -->

            caplog.clear()

            recv_json_mock.return_value = {'message': 'keystore', 'key': 'asdf', 'action': 'unknown'}

            with caplog.at_level(logging.ERROR):
                producer.run()
            assert caplog.messages == ['received unknown keystore action "unknown"']
            caplog.clear()

            send_json_mock.assert_called_once_with({
                'message': 'keystore',
                'action': 'unknown',
                'key': 'asdf',
                'data': None,
                'error': 'received unknown keystore action "unknown"',
            })
            send_json_mock.reset_mock()

            recv_json_mock.return_value = {'message': 'unknown'}

            with caplog.at_level(logging.ERROR):
                producer.run()
            assert caplog.messages == ['received unknown message "unknown"']
            send_json_mock.assert_called_once_with({})
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

    def test_run_zmq_error(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Socket.recv_json',
            side_effect=[ZMQError],
        )

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_persist_data_edge_cases.feature'

        try:
            with caplog.at_level(logging.ERROR):
                TestdataProducer(grizzly_fixture.grizzly, {}).run()
            assert caplog.messages[-1] == 'failed when waiting for consumers'
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']
            cleanup()

    def test_persist_data_edge_cases(
        self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture, cleanup: AtomicVariableCleanupFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')

        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)
        grizzly = grizzly_fixture.grizzly

        persistent_file = context_root / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_persist_data_edge_cases.feature'

        try:
            assert not persistent_file.exists()
            i = AtomicIntegerIncrementer(scenario=grizzly.scenario, variable='foobar', value='1 | step=1, persist=True')

            producer = TestdataProducer(grizzly_fixture.grizzly, {grizzly.scenario.class_name: {'AtomicIntegerIncrementer.foobar': i}})
            producer.has_persisted = True

            with caplog.at_level(logging.DEBUG):
                producer.persist_data()

            assert caplog.messages == []
            assert not persistent_file.exists()

            producer.has_persisted = False
            producer.keystore = {'hello': 'world'}

            mocker.patch('grizzly.testdata.communication.jsondumps', side_effect=[json.JSONDecodeError])

            with caplog.at_level(logging.ERROR):
                producer.persist_data()

            assert caplog.messages == ['failed to persist feature file data']
            assert not persistent_file.exists()

            caplog.clear()
        finally:
            cleanup()
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']


class TestTestdataConsumer:
    def test_testdata(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        def mock_recv_json(data: dict[str, Any], action: Optional[str] = 'consume') -> None:
            mocker.patch(
                'grizzly.testdata.communication.zmq.Socket.recv_json',
                side_effect=[
                    {
                        'action': action,
                        'data': data,
                    },
                    {
                        'success': True,
                        'worker': 'asdf-asdf-asdf',
                    },
                    {
                        'success': True,
                        'payload': json.dumps({
                            'document': {
                                'id': 'DOCUMENT_1337-2',
                                'name': 'Very important memo about the TPM report',
                            },
                        }),
                    },
                ],
            )

        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        testdata_request_spy = mocker.spy(grizzly.events.testdata_request, 'fire')

        consumer = TestdataConsumer(parent)

        try:
            mock_recv_json({})

            mock_recv_json({
                'auth.user.username': 'username',
                'auth.user.password': 'password',
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            })

            assert consumer.testdata() == {
                'auth': {
                    'user': {
                        'username': 'username',
                        'password': 'password',
                    },
                },
                'variables': transform(grizzly.scenario, {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }),
            }

            testdata_request_spy.assert_called_once_with(
                reverse=False,
                timestamp=ANY(str),
                tags={
                    'type': 'consumer',
                    'action': 'consume',
                    'identifier': consumer.identifier,
                },
                measurement='request_testdata',
                metrics={
                    'error': None,
                    'response_time': ANY(float),
                },
            )
            testdata_request_spy.reset_mock()

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            }, 'stop')

            with caplog.at_level(logging.DEBUG):
                assert consumer.testdata() is None
            assert caplog.messages[-1] == 'received stop command'

            caplog.clear()

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                },
            }, 'asdf')

            with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
                consumer.testdata()
            assert 'unknown action "asdf" received, stopping user' in caplog.text

            caplog.clear()

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                },
            }, 'consume')

            assert consumer.testdata() == {
                'variables': transform(grizzly.scenario, {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                }),
            }
        finally:
            consumer.stop()
            assert consumer.context._instance is None

    def test_stop_exception(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Context.destroy',
            side_effect=[RuntimeError('zmq.Context.destroy failed')],
        )

        parent = grizzly_fixture()

        consumer = TestdataConsumer(parent)

        with caplog.at_level(logging.DEBUG):
            consumer.stop()
        assert caplog.messages[-1] == 'failed to stop'

        assert consumer.stopped

        caplog.clear()

        with caplog.at_level(logging.DEBUG):
            consumer.stop()

        assert caplog.messages == []

    def test_testdata_exception(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Socket.recv_json',
            side_effect=[ZMQAgain],
        )

        gsleep_mock = mocker.patch(
            'grizzly.testdata.communication.gsleep',
            side_effect=[ZMQAgain],
        )

        parent = grizzly_fixture()

        consumer = TestdataConsumer(parent)

        with pytest.raises(ZMQAgain):
            consumer.testdata()

        gsleep_mock.assert_called_once_with(0.1)

        mocker.patch.object(consumer, '_request', return_value=None)

        with caplog.at_level(logging.ERROR):
            assert consumer.testdata() is None

        assert caplog.messages == ['no testdata received']

    def test_keystore_get(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)

        assert consumer.keystore_get('hello') is None
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'get',
                'key': 'hello',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

        request_spy.assert_called_once_with({
            'action': 'get',
            'key': 'hello',
            'message': 'keystore',
            'identifier': consumer.identifier,
        })

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo_add_data({'hello': 'world'}))

        assert consumer.keystore_get('hello') == {'hello': 'world'}
        request_spy.assert_called_once_with({
            'action': 'get',
            'key': 'hello',
            'message': 'keystore',
            'identifier': consumer.identifier,
        })

    def test_keystore_set(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_set('world', {'hello': 'world'})

        request_spy.assert_called_once_with({
            'message': 'keystore',
            'action': 'set',
            'key': 'world',
            'identifier': consumer.identifier,
            'data': {'hello': 'world'},
        })
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'set',
                'key': 'world',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

    def test_keystore_inc(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_inc('counter') == 1

        request_spy.assert_called_once_with({
            'action': 'inc',
            'key': 'counter',
            'message': 'keystore',
            'identifier': consumer.identifier,
            'data': 1,
        })
        request_spy.reset_mock()
        keystore_request_spy.reset_mock()

        assert consumer.keystore_inc('counter', step=10) == 10

        request_spy.assert_called_once_with({
            'action': 'inc',
            'key': 'counter',
            'message': 'keystore',
            'identifier': consumer.identifier,
            'data': 10,
        })
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'inc',
                'key': 'counter',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )
        keystore_request_spy.reset_mock()

    def test_keystore_push(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_push('foobar', 'hello')

        request_spy.assert_called_once_with({
            'action': 'push',
            'key': 'foobar',
            'message': 'keystore',
            'identifier': consumer.identifier,
            'data': 'hello',
        })
        request_spy.reset_mock()
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'push',
                'key': 'foobar',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )

    def test_keystore_pop(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo_add_data([None, None, 'hello']))
        gsleep_mock = mocker.patch('grizzly.testdata.communication.gsleep', return_value=None)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_pop('foobar') == 'hello'

        assert gsleep_mock.call_count == 2
        assert request_spy.call_count == 3
        assert keystore_request_spy.call_count == 3

    def test_keystore_del(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        consumer.keystore_del('foobar')

        request_spy.assert_called_once_with({
            'action': 'del',
            'key': 'foobar',
            'message': 'keystore',
            'identifier': consumer.identifier,
        })
        request_spy.reset_mock()
        keystore_request_spy.assert_called_once_with(
            reverse=False,
            timestamp=ANY(str),
            tags={
                'action': 'del',
                'key': 'foobar',
                'identifier': consumer.identifier,
                'type': 'consumer',
            },
            measurement='request_keystore',
            metrics={
                'response_time': ANY(float),
                'error': None,
            },
        )

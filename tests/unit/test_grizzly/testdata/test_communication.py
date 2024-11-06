"""Unit tests of grizzly.testdata.communication."""
from __future__ import annotations

import json
import logging
from contextlib import suppress
from os import environ, sep
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, cast

import pytest
from gevent.event import AsyncResult

from grizzly.tasks import LogMessageTask
from grizzly.testdata.communication import TestdataConsumer, TestdataProducer
from grizzly.testdata.utils import initialize_testdata, transform
from grizzly.testdata.variables import AtomicIntegerIncrementer
from grizzly.testdata.variables.csv_writer import atomiccsvwriter_message_handler
from grizzly.types.locust import Environment, LocalRunner, Message, StopUser
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture

    from tests.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


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
        request = grizzly_fixture.request_task.request

        success = False

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run.feature'
        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        try:
            parent = grizzly_fixture()

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

            grizzly.state.producer = TestdataProducer(
                runner=cast(LocalRunner, grizzly.state.locust),
                testdata=testdata,
            )

            assert grizzly.state.producer.keystore == {}

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)
            grizzly.state.locust.register_message('produce_testdata', grizzly.state.producer.handle_request)

            def request_testdata() -> dict[str, Any] | None:
                uid = id(parent.user)

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message(
                    'produce_testdata',
                    {'uid': uid, 'cid': cast(LocalRunner, grizzly.state.locust).client_id, 'request': {'message': 'testdata', 'identifier': grizzly.scenario.class_name}},
                )

                response = cast(dict[str, Any] | None, responses[uid].get())

                del responses[uid]

                return response

            def request_keystore(action: str, key: str, value: Any | None = None) -> dict[str, Any] | None:
                uid = id(parent.user)

                request = {
                    'message': 'keystore',
                    'identifier': grizzly.scenario.class_name,
                    'action': action,
                    'key': key,
                }

                if value is not None:
                    request.update({'data': value})

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message('produce_testdata', {'uid': uid, 'cid': cast(LocalRunner, grizzly.state.locust).client_id, 'request': request})

                response = cast(dict[str, Any] | None, responses[uid].get())

                del responses[uid]

                return response

            response = request_testdata()
            assert response is not None
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
            assert response['action'] == 'consume'
            data = response['data']
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
            assert grizzly.state.producer is not None
            assert grizzly.state.producer.keystore == {}


            response = request_keystore('set', 'foobar', {'hello': 'world'})
            assert response is not None
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
            assert response == {
                'message': 'keystore',
                'action': 'set',
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'data': {'hello': 'world'},
            }

            response = request_testdata()
            assert response is not None
            assert response['action'] == 'consume'
            data = response['data']
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

            response = request_keystore('get', 'foobar')
            assert response == {
                'message': 'keystore',
                'action': 'get',
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'data': {'hello': 'world'},
            }

            caplog.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 1,
            }

            assert caplog.messages == []

            caplog.clear()

            grizzly.state.producer.keystore.update({'counter': 'asdf'})

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': None,
                'error': 'value asdf for key "counter" cannot be incremented',
            }

            assert caplog.messages == ['value asdf for key "counter" cannot be incremented']

            caplog.clear()
            grizzly.state.producer.keystore.update({'counter': 1})

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 1)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 2,
            }

            assert caplog.messages == []

            with caplog.at_level(logging.ERROR):
                response = request_keystore('inc', 'counter', 10)

            assert response == {
                'message': 'keystore',
                'action': 'inc',
                'identifier': grizzly.scenario.class_name,
                'key': 'counter',
                'data': 12,
            }

            assert caplog.messages == []

            response = request_testdata()
            assert response is not None
            assert response['action'] == 'stop'
            assert 'data' not in response

            success = True
        finally:
            if grizzly.state.producer is not None:
                grizzly.state.producer.stop()

                persist_file = Path(context_root).parent / 'persistent' / 'test_run.json'
                assert persist_file.exists()

                if success:
                    actual_initial_values = json.loads(persist_file.read_text())
                    assert actual_initial_values == {
                        'IteratorScenario_001': {
                            'AtomicIntegerIncrementer.value': '11 | step=5, persist=True',
                        },
                    }

            cleanup()

    def test_run_variable_none(
        self,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        try:
            grizzly = grizzly_fixture.grizzly
            parent = grizzly_fixture()
            context_root = grizzly_fixture.test_context / 'requests'
            context_root.mkdir(exist_ok=True)
            request = grizzly_fixture.request_task.request
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

            grizzly.state.producer = TestdataProducer(
                runner=cast(LocalRunner, grizzly.state.locust),
                testdata=testdata,
            )

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)
            grizzly.state.locust.register_message('produce_testdata', grizzly.state.producer.handle_request)

            def request_testdata() -> dict[str, Any] | None:
                uid = id(parent.user)

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message(
                    'produce_testdata',
                    {'uid': uid, 'cid': cast(LocalRunner, grizzly.state.locust).client_id, 'request': {'message': 'testdata', 'identifier': grizzly.scenario.class_name}},
                )

                response = cast(dict[str, Any] | None, responses[uid].get())

                del responses[uid]

                return response

            with caplog.at_level(logging.DEBUG):
                response = request_testdata()
                assert response is not None
                assert response['action'] == 'stop'

        finally:
            if grizzly.state.producer is not None:
                grizzly.state.producer.stop()

                persist_file = Path(context_root).parent / 'persistent' / 'test_run_with_none.json'
                assert not persist_file.exists()

            cleanup()

    def test_on_stop(self, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly
            environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'
            grizzly.state.producer = TestdataProducer(runner=cast(LocalRunner, grizzly.state.locust), testdata={})
            grizzly.state.producer.scenarios_iteration = {
                'test-scenario-1': 10,
                'test-scenario-2': 5,
            }

            grizzly.state.producer.on_test_stop()

            for scenario, count in grizzly.state.producer.scenarios_iteration.items():
                assert count == 0, f'iteration count for {scenario} was not reset'
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            cleanup()

    def test_stop_exception(
        self, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture,
    ) -> None:
        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)

        persistent_file = grizzly_fixture.test_context / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_with_variable_none.feature'

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(cast(LocalRunner, grizzly.state.locust), {}).stop()
            assert caplog.messages == ['serving:\n{}']
            assert not persistent_file.exists()

            i = AtomicIntegerIncrementer(scenario=scenario1, variable='foobar', value='1 | step=1, persist=True')
            j = AtomicIntegerIncrementer(scenario=scenario2, variable='foobar', value='10 | step=10, persist=True')

            for v in [i, j]:
                v['foobar']
                v['foobar']

            actual_keystore = {'foo': ['hello', 'world'], 'bar': {'hello': 'world', 'foo': 'bar'}, 'hello': 'world'}

            with caplog.at_level(logging.DEBUG):
                grizzly.state.producer = TestdataProducer(cast(LocalRunner, grizzly.state.locust), {
                    scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                    scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                })
                grizzly.state.producer.keystore.update(actual_keystore)
                grizzly.state.producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            caplog.clear()

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
                grizzly.state.producer = TestdataProducer(cast(LocalRunner, grizzly.state.locust), {
                    scenario1.class_name: {'AtomicIntegerIncrementer.foobar': i},
                    scenario2.class_name: {'AtomicIntegerIncrementer.foobar': j},
                })
                grizzly.state.producer.stop()

            assert caplog.messages[-1] == f'feature file data persisted in {persistent_file}'
            caplog.clear()

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

    def test_run_keystore(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly
        context_root = Path(grizzly_fixture.request_task.context_root).parent

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_run_keystore.feature'
        environ['GRIZZLY_CONTEXT_ROOT'] = context_root.as_posix()

        try:
            grizzly.state.producer = TestdataProducer(cast(LocalRunner, grizzly.state.locust), {})
            grizzly.state.producer.keystore.update({'hello': 'world'})

            responses: dict[int, AsyncResult] = {}

            def handle_consume_data(*, environment: Environment, msg: Message) -> None:  # noqa: ARG001
                uid = msg.data['uid']
                response = msg.data['response']
                responses[uid].set(response)

            grizzly.state.locust.register_message('consume_testdata', handle_consume_data)
            grizzly.state.locust.register_message('produce_testdata', grizzly.state.producer.handle_request)

            def request_keystore(action: str, key: str, value: Any | None = None, message: str = 'keystore') -> dict[str, Any] | None:
                uid = id(parent.user)

                request = {
                    'message': message,
                    'identifier': grizzly.scenario.class_name,
                    'action': action,
                    'key': key,
                }

                if value is not None:
                    request.update({'data': value})

                responses[uid] = AsyncResult()
                grizzly.state.locust.send_message('produce_testdata', {'uid': uid, 'cid': cast(LocalRunner, grizzly.state.locust).client_id, 'request': request})

                response = cast(dict[str, Any] | None, responses[uid].get())

                del responses[uid]

                return response

            with caplog.at_level(logging.ERROR):
                response = request_keystore('get', 'hello', 'world')

            assert response == {'message': 'keystore', 'action': 'get', 'data': 'world', 'identifier': grizzly.scenario.class_name, 'key': 'hello'}
            assert caplog.messages == []

            grizzly.state.producer.keystore.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('set', 'world', {'foo': 'bar'})

            assert response == {'message': 'keystore', 'action': 'set', 'data': {'foo': 'bar'}, 'identifier': grizzly.scenario.class_name, 'key': 'world'}
            assert caplog.messages == []
            assert grizzly.state.producer.keystore == {'world': {'foo': 'bar'}}

            # <!-- push
            assert 'foobar' not in grizzly.state.producer.keystore

            response = request_keystore('push', 'foobar', 'foobar')

            assert response == {'message': 'keystore', 'action': 'push', 'data': 'foobar', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobar']

            response = request_keystore('push', 'foobar', 'foobaz')

            assert response == {'message': 'keystore', 'action': 'push', 'data': 'foobaz', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobar', 'foobaz']
            # // push -->

            # <!-- pop
            assert 'world' in grizzly.state.producer.keystore

            response = request_keystore('pop', 'world', 'foobar')

            assert response == {
                'message': 'keystore',
                'action': 'pop',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'world',
                'error': 'key "world" is not a list, it has not been pushed to',
            }

            response = request_keystore('pop', 'foobaz', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobaz'}

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': 'foobar', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == ['foobaz']

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': 'foobaz', 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert grizzly.state.producer.keystore['foobar'] == []

            response = request_keystore('pop', 'foobar')

            assert response == {'message': 'keystore', 'action': 'pop', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            # // pop -->

            # <!-- del
            assert 'foobar' in grizzly.state.producer.keystore

            response = request_keystore('del', 'foobar', 'dummy')

            assert response == {'message': 'keystore', 'action': 'del', 'data': None, 'identifier': grizzly.scenario.class_name, 'key': 'foobar'}
            assert 'foobar' not in grizzly.state.producer.keystore

            response = request_keystore('del', 'foobar', 'dummy')
            assert response == {
                'message': 'keystore',
                'action': 'del',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'foobar',
                'error': 'failed to remove key "foobar"',
            }
            # // del -->

            caplog.clear()

            response = request_keystore('unknown', 'asdf')
            assert response == {
                'message': 'keystore',
                'action': 'unknown',
                'data': None,
                'identifier': grizzly.scenario.class_name,
                'key': 'asdf',
                'error': 'received unknown keystore action "unknown"',
            }
            assert caplog.messages == ['received unknown keystore action "unknown"']
            caplog.clear()

            with caplog.at_level(logging.ERROR):
                response = request_keystore('get', 'foobar', None, message='unknown')

            assert response == {}
            assert caplog.messages == ['received unknown message "unknown"']
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

    def test_persist_data_edge_cases(
        self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, cleanup: AtomicVariableCleanupFixture,
    ) -> None:
        context_root = grizzly_fixture.test_context / 'requests'
        context_root.mkdir(exist_ok=True)
        grizzly = grizzly_fixture.grizzly

        persistent_file = context_root / 'persistent' / 'test_run_with_variable_none.json'

        environ['GRIZZLY_FEATURE_FILE'] = 'features/test_persist_data_edge_cases.feature'

        try:
            assert not persistent_file.exists()
            i = AtomicIntegerIncrementer(scenario=grizzly.scenario, variable='foobar', value='1 | step=1, persist=True')

            grizzly.state.producer = TestdataProducer(
                runner=cast(LocalRunner, grizzly.state.locust),
                testdata={grizzly.scenario.class_name: {'AtomicIntegerIncrementer.foobar': i}},
            )
            grizzly.state.producer.has_persisted = True

            with caplog.at_level(logging.DEBUG):
                grizzly.state.producer.persist_data()

            assert caplog.messages == []
            assert not persistent_file.exists()

            grizzly.state.producer.has_persisted = False
            grizzly.state.producer.keystore = {'hello': 'world'}

            mocker.patch('grizzly.testdata.communication.jsondumps', side_effect=[json.JSONDecodeError])

            with caplog.at_level(logging.ERROR):
                grizzly.state.producer.persist_data()

            assert caplog.messages == ['failed to persist feature file data']
            assert not persistent_file.exists()

            caplog.clear()
        finally:
            cleanup()
            with suppress(KeyError):
                del environ['GRIZZLY_FEATURE_FILE']


class TestTestdataConsumer:
    def test_testdata(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        def mock_testdata(consumer: TestdataConsumer, data: dict[str, Any], action: str | None = 'consume') -> MagicMock:
            def send_message_mock(*_args: Any, **_kwargs: Any) -> None:
                message = Message(
                    'consume_testdata',
                    {
                        'uid': id(parent.user),
                        'cid': cast(LocalRunner, grizzly.state.locust).client_id,
                        'response': {'action': action, 'data': data},
                    },
                    node_id=None,
                )
                TestdataConsumer.handle_response(environment=consumer.runner.environment, msg=message)

            return mocker.patch.object(grizzly.state.locust, 'send_message', side_effect=send_message_mock)

        testdata_request_spy = mocker.spy(grizzly.events.testdata_request, 'fire')

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

        send_message = mock_testdata(consumer, {
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

        send_message.assert_called_once_with('produce_testdata', {
            'uid': id(parent.user),
            'cid': cast(LocalRunner, grizzly.state.locust).client_id,
            'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
        })
        send_message.reset_mock()

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

        send_message = mock_testdata(consumer, {
            'variables': {
                'AtomicIntegerIncrementer.messageID': 100,
                'test': 1,
            },
        }, 'stop')

        with caplog.at_level(logging.DEBUG):
            assert consumer.testdata() is None
        assert caplog.messages[-1] == 'received stop command'

        send_message.assert_called_once_with('produce_testdata', {
            'uid': id(parent.user),
            'cid': cast(LocalRunner, grizzly.state.locust).client_id,
            'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
        })
        send_message.reset_mock()

        caplog.clear()

        send_message = mock_testdata(consumer, {
            'variables': {
                'AtomicIntegerIncrementer.messageID': 100,
                'test': 1,
            },
        }, 'asdf')

        with caplog.at_level(logging.DEBUG), pytest.raises(StopUser):
            consumer.testdata()
        assert 'unknown action "asdf" received, stopping user' in caplog.text

        send_message.assert_called_once_with('produce_testdata', {
            'uid': id(parent.user),
            'cid': cast(LocalRunner, grizzly.state.locust).client_id,
            'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
        })
        send_message.reset_mock()

        caplog.clear()

        send_message = mock_testdata(consumer, {
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

        send_message.assert_called_once_with('produce_testdata', {
            'uid': id(parent.user),
            'cid': cast(LocalRunner, grizzly.state.locust).client_id,
            'request': {'message': 'testdata', 'identifier': 'TestScenario_001'},
        })
        send_message.reset_mock()

    def test_keystore_get(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

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

    def test_keystore_set(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

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

    def test_keystore_inc(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

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

    def test_keystore_push(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

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

    def test_keystore_pop(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

        request_spy = mocker.patch.object(consumer, '_request', side_effect=echo_add_data([None, None, 'hello']))
        gsleep_mock = mocker.patch('grizzly.testdata.communication.gsleep', return_value=None)
        keystore_request_spy = mocker.spy(grizzly.events.keystore_request, 'fire')

        assert consumer.keystore_pop('foobar') == 'hello'

        assert gsleep_mock.call_count == 2
        assert request_spy.call_count == 3
        assert keystore_request_spy.call_count == 3

    def test_keystore_del(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()
        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(cast(LocalRunner, grizzly.state.locust), parent)

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

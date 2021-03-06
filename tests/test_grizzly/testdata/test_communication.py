import json
import logging

from os import path, mkdir, sep
from typing import Dict, Optional, Any, cast

import pytest

import zmq.green as zmq
import gevent

from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture
from zmq.sugar.constants import REQ as ZMQ_REQ
from zmq.error import ZMQError, Again as ZMQAgain
from locust.exception import StopUser

from grizzly.testdata.communication import TestdataConsumer, TestdataProducer
from grizzly.testdata.utils import initialize_testdata, transform
from grizzly.context import GrizzlyContext
from grizzly.tasks import LogMessageTask

from ...fixtures import AtomicVariableCleanupFixture, BehaveFixture, GrizzlyFixture, NoopZmqFixture

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


class TestTestdataProducer:
    def test_run_with_behave(
        self,
        behave_fixture: BehaveFixture,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
        mocker: MockerFixture,
    ) -> None:
        mocker.patch(
            'grizzly.testdata.variables.messagequeue.AtomicMessageQueue.create_client',
            return_value=None,
        )
        producer: Optional[TestdataProducer] = None
        context: Optional[zmq.Context] = None
        context_root = grizzly_fixture.request_task.context_root
        request = grizzly_fixture.request_task.request

        try:
            _, _, scenario = grizzly_fixture()
            address = 'tcp://127.0.0.1:5555'

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

            source = json.loads(request.source)
            source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
            source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
            source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
            source['result']['IntWithStep'] = '{{ AtomicIntegerIncrementer.value }}'
            source['result']['UtcDate'] = '{{ AtomicDate.utc }}'
            source['result']['CustomVariable'] = '{{ tests.helpers.AtomicCustomVariable.foo }}'

            grizzly = cast(GrizzlyContext, behave_fixture.context.grizzly)
            grizzly.scenarios.clear()
            grizzly.scenarios.create(behave_fixture.create_scenario(scenario.__class__.__name__))
            grizzly.state.variables['messageID'] = 123
            grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
            grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
            grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
            grizzly.state.alias['AtomicCsvRow.test.header1'] = 'auth.user.username'
            grizzly.state.alias['AtomicCsvRow.test.header2'] = 'auth.user.password'
            grizzly.state.variables['AtomicIntegerIncrementer.value'] = '1 | step=5'
            grizzly.state.variables['AtomicDate.utc'] = "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC"
            grizzly.state.variables['AtomicDate.now'] = 'now'
            grizzly.state.variables['tests.helpers.AtomicCustomVariable.foo'] = 'bar'
            grizzly.scenario.iterations = 2
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'

            if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
                source['result']['DocumentID'] = '{{ AtomicMessageQueue.document_id }}'
                grizzly.state.variables['AtomicMessageQueue.document_id'] = (
                    'queue:TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"'
                )

            request.source = json.dumps(source)

            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='hello {{ world }}'))

            testdata, external_dependencies = initialize_testdata(grizzly, grizzly.scenario.tasks)

            print(testdata)

            if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
                assert external_dependencies == set(['async-messaged'])
            else:
                assert external_dependencies == set()

            producer = TestdataProducer(
                grizzly=grizzly,
                address=address,
                testdata=testdata,
            )
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            context = zmq.Context()
            with context.socket(ZMQ_REQ) as socket:
                socket.connect(address)

                def get_message_from_producer() -> Dict[str, Any]:
                    message: Dict[str, Any] = {}
                    socket.send_json({
                        'message': 'available',
                        'scenario': grizzly.scenario.class_name,
                    })

                    gevent.sleep(0.1)

                    message = socket.recv_json()
                    return message

                message: Dict[str, Any] = get_message_from_producer()
                assert message['action'] == 'consume'
                data = message['data']
                assert 'variables' in data
                variables = data['variables']
                assert 'AtomicIntegerIncrementer.messageID' in variables
                assert 'AtomicDate.now' in variables
                assert 'messageID' in variables
                assert 'AtomicDate.utc' in variables
                assert variables['AtomicIntegerIncrementer.messageID'] == 456
                assert variables['messageID'] == 123
                assert variables['AtomicDirectoryContents.test'] == f'adirectory{sep}file1.txt'
                assert 'AtomicCsvRow.test.header1' not in variables
                assert 'AtomicCsvRow.test.header2' not in variables
                assert variables['AtomicIntegerIncrementer.value'] == 1
                utc_date = variables['AtomicDate.utc']
                assert 'T' in utc_date and utc_date.endswith('Z')
                assert data['auth.user.username'] == 'value1'
                assert data['auth.user.password'] == 'value2'
                assert variables['tests.helpers.AtomicCustomVariable.foo'] == 'bar'

                message = get_message_from_producer()
                assert message['action'] == 'consume'
                data = message['data']
                assert 'variables' in data
                variables = data['variables']
                if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
                    assert variables['AtomicMessageQueue.document_id'] == '__on_consumer__'
                assert 'AtomicIntegerIncrementer.messageID' in variables
                assert 'AtomicDate.now' in variables
                assert 'messageID' in variables
                assert variables['AtomicIntegerIncrementer.messageID'] == 457
                assert variables['messageID'] == 123
                assert variables['AtomicDirectoryContents.test'] == f'adirectory{sep}file2.txt'
                assert 'AtomicCsvRow.test.header1' not in variables
                assert 'AtomicCsvRow.test.header2' not in variables
                assert variables['AtomicIntegerIncrementer.value'] == 6
                assert data['auth.user.username'] == 'value3'
                assert data['auth.user.password'] == 'value4'
                if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
                    assert variables['AtomicMessageQueue.document_id'] == '__on_consumer__'

                message = get_message_from_producer()
                assert message['action'] == 'stop'
                assert 'data' not in message

                producer_thread.join(timeout=1)
        finally:
            if producer is not None:
                producer.stop()
                assert producer.context._instance is None

            if context is not None:
                context.destroy()

            cleanup()

    def test_run_variable_none(
        self,
        behave_fixture: BehaveFixture,
        grizzly_fixture: GrizzlyFixture,
        cleanup: AtomicVariableCleanupFixture,
    ) -> None:
        producer: Optional[TestdataProducer] = None
        context: Optional[zmq.Context] = None

        try:
            _, _, scenario = grizzly_fixture()
            context_root = grizzly_fixture.request_task.context_root
            request = grizzly_fixture.request_task.request
            address = 'tcp://127.0.0.1:5555'

            mkdir(path.join(context_root, 'adirectory'))

            assert request.source is not None

            source = json.loads(request.source)
            source['result']['File'] = '{{ AtomicDirectoryContents.file }}'

            request.source = json.dumps(source)

            grizzly = cast(GrizzlyContext, behave_fixture.context.grizzly)
            grizzly.scenarios.create(behave_fixture.create_scenario(scenario.__class__.__name__))
            grizzly.state.variables['messageID'] = 123
            grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
            grizzly.state.variables['AtomicDirectoryContents.file'] = 'adirectory'
            grizzly.state.variables['AtomicDate.now'] = 'now'
            grizzly.scenario.iterations = 0
            grizzly.scenario.user.class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'
            grizzly.scenario.tasks.add(request)
            grizzly.scenario.tasks.add(LogMessageTask(message='are you {{ sure }}'))

            testdata, external_dependencies = initialize_testdata(grizzly, grizzly.scenario.tasks)

            assert external_dependencies == set()

            producer = TestdataProducer(
                grizzly=grizzly,
                address=address,
                testdata=testdata,
            )
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            context = zmq.Context()
            with context.socket(ZMQ_REQ) as socket:
                socket.connect(address)

                def get_message_from_producer() -> Dict[str, Any]:
                    message: Dict[str, Any] = {}
                    socket.send_json({
                        'message': 'available',
                        'scenario': grizzly.scenario.class_name,
                    })

                    gevent.sleep(0.1)

                    message = socket.recv_json()
                    return message

                message: Dict[str, Any] = get_message_from_producer()
                assert message['action'] == 'stop'

                producer_thread.join(timeout=1)
        finally:
            if producer is not None:
                producer.stop()
                assert producer.context._instance is None

            if context is not None:
                context.destroy()

            cleanup()

    def test_reset(self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        try:
            producer = TestdataProducer(grizzly_fixture.grizzly, {})
            producer.scenarios_iteration = {
                'test-scenario-1': 10,
                'test-scenario-2': 5,
            }

            producer.reset()

            for scenario, count in producer.scenarios_iteration.items():
                assert count == 0, f'iteration count for {scenario} was not reset'
        finally:
            cleanup()

    def test_stop_exception(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture, noop_zmq: NoopZmqFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')
        mocker.patch('grizzly.testdata.communication.zmq.Context.destroy', side_effect=[RuntimeError('zmq.Context.destroy failed')])

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(grizzly_fixture.grizzly, {}).stop()
            assert 'failed to stop' in caplog.messages[-1]
        finally:
            cleanup()

    def test_run_type_error(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch('grizzly.testdata.communication.zmq.Socket.recv_json', return_value={
            'message': 'available',
            'identifier': 'test-consumer',
            'scenario': 'test',
        })
        mocker.patch(
            'grizzly.context.GrizzlyContextScenarios.find_by_class_name',
            side_effect=[TypeError('TypeError raised'), ZMQError],
        )

        send_json_mock = noop_zmq.get_mock('send_json')

        try:
            with caplog.at_level(logging.DEBUG):
                TestdataProducer(grizzly_fixture.grizzly, {}).run()

            print(caplog.text)
            assert caplog.messages[-3] == "producing {'action': 'stop'} for consumer test-consumer"
            assert 'test data error, stop consumer test-consumer' in caplog.messages[-4]
            assert send_json_mock.call_count == 1
            args, _ = send_json_mock.call_args_list[-1]
            # send_json was autospec'ed, meaning args[0] == self
            assert args[1].get('action', None) == 'stop'
        finally:
            cleanup()

    def test_run_zmq_error(
        self, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture,
    ) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Socket.recv_json',
            side_effect=[ZMQError]
        )

        try:
            with caplog.at_level(logging.ERROR):
                TestdataProducer(grizzly_fixture.grizzly, {}).run()
            print(caplog.text)
            assert caplog.messages[-1] == 'failed when waiting for consumers'
        finally:
            cleanup()


class TestTestdataConsumer:
    def test_request(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        def mock_recv_json(data: Dict[str, Any], action: Optional[str] = 'consume') -> None:
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
                ]
            )

        grizzly = grizzly_fixture.grizzly

        consumer = TestdataConsumer(grizzly, identifier='test')

        try:
            # this will no longer throw StopUser, but rather go into an infinite loop
            # with pytest.raises(StopUser):
            #    consumer.request('test')
            mock_recv_json({})

            # this will no longer throw StopUser, but rather go into an infinite loop
            # with pytest.raises(StopUser):
            #    consumer.request('test')

            mock_recv_json({
                'auth.user.username': 'username',
                'auth.user.password': 'password',
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }
            })

            assert consumer.request('test') == {
                'auth': {
                    'user': {
                        'username': 'username',
                        'password': 'password',
                    },
                },
                'variables': transform(grizzly, {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }),
            }

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }
            }, 'stop')

            with caplog.at_level(logging.DEBUG):
                assert consumer.request('test') is None
            assert caplog.messages[-1] == 'received stop command'

            caplog.clear()

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }
            }, 'asdf')

            with caplog.at_level(logging.DEBUG):
                with pytest.raises(StopUser):
                    consumer.request('test')
            assert 'unknown action "asdf" received, stopping user' in caplog.text

            caplog.clear()

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                }
            }, 'consume')

            assert consumer.request('test') == {
                'variables': transform(grizzly, {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                })
            }

            if pymqi.__name__ != 'grizzly_extras.dummy_pymqi':
                mock_recv_json({
                    'variables': {
                        'AtomicMessageQueue.document_id': '__on_consumer__',
                        'AtomicIntegerIncrementer.messageID': 100,
                        'test': None,
                    }
                })

                grizzly.state.variables['AtomicMessageQueue.document_id'] = (
                    'queue:TEST.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"'
                )

                assert consumer.request('test') == {
                    'variables': transform(grizzly, {
                        'AtomicMessageQueue.document_id': json.dumps({
                            'document': {
                                'id': 'DOCUMENT_1337-2',
                                'name': 'Very important memo about the TPM report',
                            },
                        }),
                        'AtomicIntegerIncrementer.messageID': 100,
                        'test': None,
                    })
                }
        finally:
            consumer.stop()
            assert consumer.context._instance is None

    def test_request_stop_exception(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, caplog: LogCaptureFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Context.destroy',
            side_effect=[RuntimeError('zmq.Context.destroy failed')],
        )

        with caplog.at_level(logging.DEBUG):
            TestdataConsumer(grizzly_fixture.grizzly, identifier='test').stop()
        assert caplog.messages[-1] == 'failed to stop'

    def test_request_exception(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.testdata.communication')

        mocker.patch(
            'grizzly.testdata.communication.zmq.Socket.recv_json',
            side_effect=[ZMQAgain],
        )

        gsleep_mock = mocker.patch(
            'grizzly.testdata.communication.gsleep',
            side_effect=[ZMQAgain]
        )

        with pytest.raises(ZMQAgain):
            TestdataConsumer(grizzly_fixture.grizzly, identifier='test').request('test')

        assert gsleep_mock.call_count == 1
        args, _ = gsleep_mock.call_args_list[-1]
        assert args[0] == 0.1

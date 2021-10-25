import json

from os import path, mkdir
from typing import Dict, Callable, List, Optional, Any, Tuple, cast
from logging import Logger

import pytest
import zmq
import gevent

from jinja2 import Template
from behave.runner import Context
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from zmq.sugar.context import Context
from zmq.sugar.socket import Socket
from locust.exception import StopUser
from locust.env import Environment

from grizzly.testdata.communication import TestdataConsumer, TestdataProducer
from grizzly.testdata.utils import initialize_testdata
from grizzly.utils import transform
from grizzly.context import GrizzlyContext
from grizzly.task import RequestTask

from ..fixtures import grizzly_context, locust_environment, request_task, behave_context  # pylint: disable=unused-import
from .fixtures import cleanup  # pylint: disable=unused-import


class TestTestdataProducer:
    @pytest.mark.usefixtures('grizzly_context', 'behave_context', 'cleanup')
    def test_run_with_behave(
        self,
        behave_context: Context,
        grizzly_context: Callable,
        cleanup: Callable,
    ) -> None:
        producer: Optional[TestdataProducer] = None
        try:
            environment, _, task, [context_root, _, request] = grizzly_context()
            request = cast(RequestTask, request)
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

            source = json.loads(request.source)
            source['result']['File'] = '{{ AtomicDirectoryContents.test }}'
            source['result']['CsvRowValue1'] = '{{ AtomicCsvRow.test.header1 }}'
            source['result']['CsvRowValue2'] = '{{ AtomicCsvRow.test.header2 }}'
            source['result']['IntWithStep'] = '{{ AtomicIntegerIncrementer.value }}'
            source['result']['UtcDate'] = '{{ AtomicDate.utc }}'

            request.source = json.dumps(source)
            request.template = Template(request.source)

            grizzly = cast(GrizzlyContext, behave_context.grizzly)
            grizzly.add_scenario(task.__class__.__name__)
            grizzly.state.variables['messageID'] = 123
            grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
            grizzly.state.variables['AtomicDirectoryContents.test'] = 'adirectory'
            grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
            grizzly.state.alias['AtomicCsvRow.test.header1'] = 'auth.user.username'
            grizzly.state.alias['AtomicCsvRow.test.header2'] = 'auth.user.password'
            grizzly.state.variables['AtomicIntegerIncrementer.value'] = '1 | step=5'
            grizzly.state.variables['AtomicDate.utc'] = "now | format='%Y-%m-%dT%H:%M:%S.000Z', timezone=UTC"
            grizzly.state.variables['AtomicDate.now'] = 'now'
            grizzly.scenario.iterations = 2
            grizzly.scenario.user_class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'
            grizzly.scenario.add_task(request)

            testdata = initialize_testdata(cast(List[RequestTask], grizzly.scenario.tasks))

            producer = TestdataProducer(address=address, testdata=testdata, environment=environment)
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            context = zmq.Context()
            with context.socket(zmq.REQ) as socket:
                socket.connect(address)

                def get_message_from_producer() -> Dict[str, Any]:
                    message: Dict[str, Any] = {}
                    socket.send_json({
                        'message': 'available',
                        'scenario': grizzly.scenario.get_name(),
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
                assert variables['AtomicDirectoryContents.test'] == f'adirectory/file1.txt'
                assert 'AtomicCsvRow.test.header1' not in variables
                assert 'AtomicCsvRow.test.header2' not in variables
                assert variables['AtomicIntegerIncrementer.value'] == 1
                utc_date = variables['AtomicDate.utc']
                assert 'T' in utc_date and utc_date.endswith('Z')
                assert data['auth.user.username'] == 'value1'
                assert data['auth.user.password'] == 'value2'

                message = get_message_from_producer()
                assert message['action'] == 'consume'
                data = message['data']
                assert 'variables' in data
                variables = data['variables']
                assert 'AtomicIntegerIncrementer.messageID' in variables
                assert 'AtomicDate.now' in variables
                assert 'messageID' in variables
                assert variables['AtomicIntegerIncrementer.messageID'] == 457
                assert variables['messageID'] == 123
                assert variables['AtomicDirectoryContents.test'] == f'adirectory/file2.txt'
                assert 'AtomicCsvRow.test.header1' not in variables
                assert 'AtomicCsvRow.test.header2' not in variables
                assert variables['AtomicIntegerIncrementer.value'] == 6
                assert data['auth.user.username'] == 'value3'
                assert data['auth.user.password'] == 'value4'

                message = get_message_from_producer()
                assert message['action'] == 'stop'
                assert 'data' not in message

                producer_thread.join(timeout=1)
        finally:
            if producer is not None:
                producer.stop()
                assert producer.context._instance is None

            cleanup()

    @pytest.mark.usefixtures('grizzly_context', 'behave_context', 'cleanup')
    def test_run_variable_none(
        self,
        behave_context: Context,
        grizzly_context: Callable,
        cleanup: Callable,
    ) -> None:
        producer: Optional[TestdataProducer] = None

        try:
            environment, _, task, [context_root, _, request] = grizzly_context()
            request = cast(RequestTask, request)
            address = 'tcp://127.0.0.1:5555'

            mkdir(path.join(context_root, 'adirectory'))

            source = json.loads(request.source)
            source['result']['File'] = '{{ AtomicDirectoryContents.file }}'

            request.source = json.dumps(source)
            request.template = Template(request.source)

            grizzly = cast(GrizzlyContext, behave_context.grizzly)
            grizzly.add_scenario(task.__class__.__name__)
            grizzly.state.variables['messageID'] = 123
            grizzly.state.variables['AtomicIntegerIncrementer.messageID'] = 456
            grizzly.state.variables['AtomicDirectoryContents.file'] = 'adirectory'
            grizzly.state.variables['AtomicDate.now'] = 'now'
            grizzly.scenario.iterations = 0
            grizzly.scenario.user_class_name = 'TestUser'
            grizzly.scenario.context['host'] = 'http://test.nu'
            grizzly.scenario.add_task(request)

            testdata = initialize_testdata(cast(List[RequestTask], grizzly.scenario.tasks))

            producer = TestdataProducer(address=address, testdata=testdata, environment=environment)
            producer_thread = gevent.spawn(producer.run)
            producer_thread.start()

            context = zmq.Context()
            with context.socket(zmq.REQ) as socket:
                socket.connect(address)

                def get_message_from_producer() -> Dict[str, Any]:
                    message: Dict[str, Any] = {}
                    socket.send_json({
                        'message': 'available',
                        'scenario': grizzly.scenario.get_name(),
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

            cleanup()

    @pytest.mark.usefixtures('cleanup', 'locust_environment')
    def test_reset(self, mocker: MockerFixture, cleanup: Callable, locust_environment: Environment) -> None:
        def socket_bind(instance: Socket, address: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.bind',
            socket_bind,
        )

        try:
            producer = TestdataProducer({}, environment=locust_environment)
            producer.scenarios_iteration = {
                'test-scenario-1': 10,
                'test-scenario-2': 5,
            }

            producer.reset()

            for scenario, count in producer.scenarios_iteration.items():
                assert count == 0, f'iteration count for {scenario} was not reset'
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup', 'locust_environment')
    def test_stop_exception(self, mocker: MockerFixture, cleanup: Callable, locust_environment: Environment) -> None:
        def mocked_destroy(instance: zmq.Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            raise RuntimeError('zmq.Context.destroy failed')

        mocker.patch(
            'zmq.Context.destroy',
            mocked_destroy,
        )

        def mocked_gsleep(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            pass

        mocker.patch(
            'gevent.sleep',
            mocked_gsleep,
        )

        def mocked_logger_error(instance: Logger, msg: str, exc_info: bool) -> None:
            assert exc_info
            assert msg == 'failed to stop producer'

        mocker.patch(
            'logging.Logger.error',
            mocked_logger_error,
        )

        def socket_bind(instance: Socket, address: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.bind',
            socket_bind,
        )

        try:
            TestdataProducer({}, environment=locust_environment).stop()
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup', 'locust_environment')
    def test_run_type_error(self, mocker: MockerFixture, cleanup: Callable, locust_environment: Environment) -> None:
        def socket_bind(instance: Socket, address: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.bind',
            socket_bind,
        )

        def socket_recv_json(self: 'Socket', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Dict[str, Any]:
            return {
                'message': 'available',
                'scenario': 'test',
            }


        mocker.patch(
            'zmq.sugar.socket.Socket.recv_json',
            socket_recv_json,
        )

        def socket_send_json(self: 'Socket', obj: Any, flags: Optional[int] = 0, **_kwargs: Dict[str, Any]) -> None:
            assert obj.get('action', None) == 'stop'
            raise RuntimeError()

        mocker.patch(
            'zmq.sugar.socket.Socket.send_json',
            socket_send_json,
        )

        def logger_error(l: Logger, msg: str, exc_info: bool) -> None:
            assert exc_info
            assert msg == 'test data error, stop consumer'

        mocker.patch(
            'logging.Logger.error',
            logger_error,
        )

        def mocked_get_scenario(c: GrizzlyContext, name: str) -> Any:
            raise TypeError('TypeError raised')

        mocker.patch(
            'grizzly.context.GrizzlyContext.get_scenario',
            mocked_get_scenario,
        )

        try:
            with pytest.raises(RuntimeError):
                TestdataProducer({}, environment=locust_environment).run()
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup', 'locust_environment')
    def test_run_zmq_error(self, mocker: MockerFixture, cleanup: Callable, locust_environment: Environment) -> None:
        def socket_bind(instance: Socket, address: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.bind',
            socket_bind,
        )

        def socket_recv_json(self: 'Socket', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            raise zmq.error.ZMQError()

        mocker.patch(
            'zmq.sugar.socket.Socket.recv_json',
            socket_recv_json,
        )

        def logger_error(l: Logger, msg: str, exc_info: bool) -> None:
            assert exc_info
            assert msg == 'failed when waiting for consumers'

        mocker.patch(
            'logging.Logger.error',
            logger_error,
        )


        try:
            TestdataProducer({}, environment=locust_environment).run()
        finally:
            cleanup()


class TestTestdataConsumer:
    def test(self, mocker: MockerFixture) -> None:
        def socket_connect(self: 'Socket', addr: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.connect',
            socket_connect,
        )

        def socket_send_json(self: 'Socket', obj: Any, flags: Optional[int] = 0, **_kwargs: Dict[str, Any]) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.send_json',
            socket_send_json,
        )

        def mock_recv_json(data: Dict[str, Any], action: Optional[str] = 'consume') -> None:
            def socket_recv_json(self: 'Socket', flags: int, **_kwargs: Dict[str, Any]) -> Any:
                return {
                    'action': action,
                    'data': data,
                }

            mocker.patch(
                'zmq.sugar.socket.Socket.recv_json',
                socket_recv_json,
            )

        consumer = TestdataConsumer()

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
                'variables': transform({
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

            with pytest.raises(StopUser) as e:
                consumer.request('test')
            assert 'stop command received' in str(e)

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': 1,
                }
            }, 'asdf')

            with pytest.raises(StopUser) as e:
                consumer.request('test')
            assert 'unknown action' in str(e)

            mock_recv_json({
                'variables': {
                    'AtomicIntegerIncrementer.messageID': 100,
                    'test': None,
                }
            }, 'consume')
        finally:
            consumer.stop()
            assert consumer.context._instance is None

    def test_stop_exception(self, mocker: MockerFixture) -> None:
        def mocked_destroy(instance: zmq.Context, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            raise RuntimeError('zmq.Context.destroy failed')

        mocker.patch(
            'zmq.Context.destroy',
            mocked_destroy,
        )

        def mocked_gsleep(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            pass

        mocker.patch(
            'gevent.sleep',
            mocked_gsleep,
        )

        def mocked_logger_error(instance: Logger, msg: str, exc_info: bool) -> None:
            assert exc_info
            assert msg == 'failed to stop consumer'

        mocker.patch(
            'logging.Logger.error',
            mocked_logger_error,
        )

        def socket_connect(self: 'Socket', addr: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.connect',
            socket_connect,
        )

        TestdataConsumer().stop()

    def test_request_exception(self, mocker: MockerFixture) -> None:
        def socket_connect(self: 'Socket', addr: str) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.connect',
            socket_connect,
        )

        def socket_send_json(self: 'Socket', obj: Any, flags: Optional[int] = 0, **_kwargs: Dict[str, Any]) -> None:
            pass

        mocker.patch(
            'zmq.sugar.socket.Socket.send_json',
            socket_send_json,
        )

        def socket_recv_json(self: 'Socket', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            raise zmq.error.Again()

        mocker.patch(
            'zmq.sugar.socket.Socket.recv_json',
            socket_recv_json,
        )

        def gsleep(time: float, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            assert time == 0.1
            raise zmq.error.Again()

        mocker.patch(
            'gevent.sleep',
            gsleep,
        )

        with pytest.raises(zmq.error.Again):
            TestdataConsumer().request('test')

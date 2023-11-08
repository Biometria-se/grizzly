from __future__ import annotations

import subprocess
import sys
from os import environ
from typing import Any, Dict, Optional, Tuple, cast
from unittest.mock import ANY

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

import pytest
from pytest_mock import MockerFixture
from zmq.error import Again as ZMQAgain
from zmq.error import ZMQError

from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.exceptions import ResponseHandlerError, RestartScenario, StopScenario
from grizzly.scenarios import GrizzlyScenario, IteratorScenario
from grizzly.steps._helpers import add_save_handler
from grizzly.tasks import RequestTask
from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.utils import transform
from grizzly.types import RequestMethod, ResponseTarget
from grizzly.types.locust import Environment, StopUser
from grizzly.users.base import RequestLogger, ResponseHandler
from grizzly.users.messagequeue import MessageQueueUser
from grizzly_extras.async_message import AsyncMessageResponse
from grizzly_extras.transformer import TransformerContentType
from tests.fixtures import BehaveFixture, GrizzlyFixture, NoopZmqFixture

MqScenarioFixture = Tuple[MessageQueueUser, GrizzlyContextScenario, Environment]


@pytest.mark.usefixtures('grizzly_fixture')
@pytest.fixture
def mq_parent(grizzly_fixture: GrizzlyFixture) -> GrizzlyScenario:
    parent = grizzly_fixture(
        host='mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1',
        user_type=MessageQueueUser,
    )

    request = grizzly_fixture.request_task.request

    scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario(parent.__class__.__name__))
    scenario.user.class_name = 'MessageQueueUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.tasks.add(request)
    grizzly_fixture.grizzly.scenarios.clear()
    grizzly_fixture.grizzly.scenarios.append(scenario)
    parent.user._scenario = scenario

    return parent


class TestMessageQueueUserNoPymqi:
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        try:
            del env['LD_LIBRARY_PATH']
        except KeyError:
            pass

        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            [
                sys.executable,
                '-c',
                'import grizzly.users.messagequeue as mq; from grizzly.types.locust import Environment; print(f"{mq.pymqi.__name__=}"); mq.MessageQueueUser(Environment())'
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, _ = process.communicate()
        output = out.decode()

        assert process.returncode == 1
        assert "mq.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
        assert 'NotImplementedError: MessageQueueUser could not import pymqi, have you installed IBM MQ dependencies?' in output


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestMessageQueueUser:
    real_stuff = {
        'username': '',
        'password': '',
        'key_file': '',
        'endpoint': '',
        'host': '',
        'queue_manager': '',
        'channel': '',
    }

    def test_on_start(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        MessageQueueUser.__scenario__ = behave_fixture.grizzly.scenario
        MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1'
        user = MessageQueueUser(behave_fixture.locust.environment)
        connect_mock = noop_zmq.get_mock('zmq.Socket.connect')

        assert not hasattr(user, 'zmq_client')

        request_context_spy = mocker.patch.object(user, '_request_context', return_value=mocker.MagicMock())

        user.on_start()

        request_context_spy.assert_called_once_with({'action': 'CONN', 'client': id(user), 'context': user.am_context})
        request_context_spy.return_value.__enter__.assert_called_once_with()
        request_context_spy.return_value.__enter__.return_value.update.assert_not_called()
        connect_mock.assert_called_once_with(user.zmq_url)

    def test_on_stop(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')
        disconnect_mock = noop_zmq.get_mock('zmq.Socket.disconnect')

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        MessageQueueUser.__scenario__ = behave_fixture.grizzly.scenario
        MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1'
        user = MessageQueueUser(behave_fixture.locust.environment)
        request_context_spy = mocker.patch.object(user, '_request_context', return_value=mocker.MagicMock())

        user.on_start()

        request_context_spy.reset_mock()

        user.on_stop()

        request_context_spy.assert_not_called()

        user.worker_id = 'foobar'

        user.on_stop()

        request_context_spy.assert_called_once_with({'action': 'DISC', 'worker': 'foobar', 'client': id(user), 'context': user.am_context})
        request_context_spy.return_value.__enter__.assert_called_once_with()
        request_context_spy.return_value.__enter__.return_value.update.assert_not_called()
        disconnect_mock.assert_called_once_with(user.zmq_url)

    def test_create(self, behave_fixture: BehaveFixture) -> None:
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        MessageQueueUser.__scenario__ = behave_fixture.grizzly.scenario

        try:
            MessageQueueUser.host = 'http://mq.example.com:1337'
            with pytest.raises(ValueError, match='is not a supported scheme for MessageQueueUser'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            MessageQueueUser.host = 'mq://mq.example.com:1337'
            with pytest.raises(ValueError, match='needs QueueManager and Channel in the query string'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            MessageQueueUser.host = 'mq://:1337'
            with pytest.raises(ValueError, match='hostname is not specified'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            MessageQueueUser.host = 'mq://mq.example.com:1337/?Channel=Kanal1'
            with pytest.raises(ValueError, match='needs QueueManager in the query string'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01'
            with pytest.raises(ValueError, match='needs Channel in the query string'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            MessageQueueUser.host = 'mq://username:password@mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
            with pytest.raises(ValueError, match='username and password should be set via context'):
                MessageQueueUser(environment=behave_fixture.locust.environment)

            # Test default port and ssl_cipher
            MessageQueueUser.host = 'mq://mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
            user = MessageQueueUser(environment=behave_fixture.locust.environment)
            assert user.am_context.get('connection', None) == 'mq.example.com(1414)'
            assert user.am_context.get('ssl_cipher', None) == 'ECDHE_RSA_AES_256_GCM_SHA384'

            MessageQueueUser.__context__['auth'] = {
                'username': 'syrsa',
                'password': 'hemligaarne',
                'key_file': '/my/key',
                'ssl_cipher': 'rot13',
                'cert_label': 'some_label',
            }

            MessageQueueUser.host = 'mq://mq.example.com:1415?Channel=Kanal1&QueueManager=QMGR01'
            user = MessageQueueUser(environment=behave_fixture.locust.environment)

            assert user.am_context.get('connection', None) == 'mq.example.com(1415)'
            assert user.am_context.get('queue_manager', None) == 'QMGR01'
            assert user.am_context.get('channel', None) == 'Kanal1'
            assert user.am_context.get('key_file', None) == '/my/key'
            assert user.am_context.get('ssl_cipher', None) == 'rot13'
            assert user.am_context.get('cert_label', None) == 'some_label'

            MessageQueueUser.__context__['auth']['cert_label'] = None

            user = MessageQueueUser(environment=behave_fixture.locust.environment)

            assert user.am_context.get('cert_label', None) == 'syrsa'

            MessageQueueUser.__context__['message']['wait'] = 5

            user = MessageQueueUser(environment=behave_fixture.locust.environment)
            assert user.am_context.get('message_wait', None) == 5
            assert issubclass(user.__class__, (RequestLogger, ResponseHandler))

            MessageQueueUser.__context__['message']['header_type'] = 'RFH2'

            user = MessageQueueUser(environment=behave_fixture.locust.environment)
            assert user.am_context.get('header_type', None) == 'rfh2'
            assert issubclass(user.__class__, (RequestLogger, ResponseHandler))

            MessageQueueUser.__context__['message']['header_type'] = 'None'

            user = MessageQueueUser(environment=behave_fixture.locust.environment)
            assert user.am_context.get('header_type', None) is None
            assert issubclass(user.__class__, (RequestLogger, ResponseHandler,))

            MessageQueueUser.__context__['message']['header_type'] = 'wrong'
            with pytest.raises(ValueError, match='unsupported value for header_type: "wrong"') as e:
                MessageQueueUser(environment=behave_fixture.locust.environment)

        finally:
            MessageQueueUser.__context__ = {
                'auth': {
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None
                }
            }

    def test_on_start__action_conn_error(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        def mocked_zmq_connect(*_args: Any, **_kwargs: Any) -> Any:
            raise ZMQError(msg='error connecting')

        noop_zmq('grizzly.users.messagequeue')

        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.connect',
            mocked_zmq_connect,
        )

        scenario = GrizzlyContextScenario(3, behave=behave_fixture.create_scenario('test scenario'))

        def mocked_request_fire(*_args: Any, **kwargs: Any) -> None:
            properties = list(kwargs.keys())
            # self.environment.events.request.fire
            if properties == ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']:
                assert kwargs['request_type'] == 'CONN'
                assert kwargs['name'] == f'{scenario.identifier} {user.am_context.get("connection", None)}'
                assert kwargs['response_time'] >= 0
                assert kwargs['response_length'] == 0
                assert isinstance(kwargs['exception'], ZMQError)
            elif properties == ['name', 'request', 'context', 'user', 'exception']:  # self.response_event.fire
                pytest.fail(f'what should we do with {kwargs=}')
            else:
                pytest.fail(f'unknown event fired: {properties}')

        mocker.patch(
            'locust.event.EventHook.fire',
            mocked_request_fire,
        )

        MessageQueueUser.__scenario__ = scenario
        MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1'
        user = MessageQueueUser(behave_fixture.locust.environment)

        request = RequestTask(RequestMethod.PUT, name='test-put', endpoint='EXAMPLE.QUEUE')
        scenario.name = 'test'
        scenario.failure_exception = StopUser
        scenario.tasks.add(request)
        user._scenario = scenario

        with pytest.raises(StopScenario):
            user.on_start()

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    def test_get_tls_real(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=MessageQueueUser, scenario_type=IteratorScenario)
        assert isinstance(parent, IteratorScenario)
        assert isinstance(parent.user, MessageQueueUser)

        process: Optional[subprocess.Popen] = None
        try:
            process = subprocess.Popen(
                ['async-messaged'],
                env=environ.copy(),
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            from gevent import sleep as gsleep
            gsleep(2)

            MessageQueueUser._context = {
                'auth': {
                    'username': self.real_stuff['username'],
                    'password': self.real_stuff['password'],
                    'key_file': self.real_stuff['key_file'],
                    'cert_label': None,
                    'ssl_cipher': None
                },
                'message': {
                    'wait': 0,
                }
            }

            request = RequestTask(RequestMethod.GET, name='test-get', endpoint=self.real_stuff['endpoint'])
            scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('get tls real'))
            scenario.failure_exception = StopUser
            scenario.tasks.add(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(parent.user.environment)
            parent.user = user

            parent.user.request(request)
            assert 0
        finally:
            if process is not None:
                try:
                    process.terminate()
                    out, _ = process.communicate()
                    print(out)
                except Exception as e:
                    print(e)
            MessageQueueUser._context = {
                'auth': {
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None,
                },
            }

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    def test_put_tls_real(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=MessageQueueUser, scenario_type=IteratorScenario)
        assert isinstance(parent, IteratorScenario)
        assert isinstance(parent.user, MessageQueueUser)

        process: Optional[subprocess.Popen] = None
        try:
            process = subprocess.Popen(
                ['async-messaged'],
                env=environ.copy(),
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            from gevent import sleep as gsleep
            gsleep(2)

            MessageQueueUser._context = {
                'auth': {
                    'username': self.real_stuff['username'],
                    'password': self.real_stuff['password'],
                    'key_file': self.real_stuff['key_file'],
                    'cert_label': None,
                    'ssl_cipher': None,
                },
            }

            request = RequestTask(RequestMethod.PUT, name='test-put', endpoint=self.real_stuff['endpoint'])
            request.source = 'we <3 IBM MQ'
            scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('put tls real'))
            scenario.failure_exception = StopUser
            scenario.tasks.add(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(parent.user.environment)
            parent.user = user

            parent.user.request(request)
        finally:
            if process is not None:
                try:
                    process.terminate()
                    out, _ = process.communicate()
                    print(out)
                except Exception as e:
                    print(e)
            MessageQueueUser.__context__ = {
                'auth': {
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None,
                },
            }

    def test_get(self, grizzly_fixture: GrizzlyFixture, mq_parent: GrizzlyScenario, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')

        assert isinstance(mq_parent.user, MessageQueueUser)

        grizzly = grizzly_fixture.grizzly

        response_connected: AsyncMessageResponse = {
            'worker': '0000-1337',
            'success': True,
            'message': 'connected',
        }

        test_payload = '<?xml encoding="utf-8"?>'

        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            response_connected,
            {
                'success': True,
                'worker': '0000-1337',
                'response_length': 24,
                'response_time': -1337,  # fake so message queue daemon response time is a huge chunk
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
            },
        ]

        mq_parent.user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            },
        }

        remote_variables = {
            'variables': transform(grizzly, {
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
                'payload_variable': '',
                'metadata_variable': '',
            }),
        }

        grizzly.state.variables = cast(GrizzlyVariables, {
            'payload_variable': '',
            'metadata_variable': '',
        })

        request_event_spy = mocker.spy(mq_parent.user.environment.events.request, 'fire')
        response_event_spy = mocker.spy(mq_parent.user.response_event, 'fire')

        request = cast(RequestTask, mq_parent.user._scenario.tasks()[-1])
        request.endpoint = 'queue:test-queue'
        request.method = RequestMethod.GET
        request.source = None
        mq_parent.user._scenario.tasks.add(request)

        mq_parent.user.add_context(remote_variables)
        mq_parent.user.on_start()

        metadata, payload = mq_parent.user.request(request)

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=None,
            response_length=len(test_payload.encode()),
            context=mq_parent.user._context,
            name='001 TestScenario',
            response_time=ANY,
        )

        response_event_spy.assert_called_once_with(
            request=ANY,
            context=(pymqi.MD().get(), test_payload),
            user=mq_parent.user,
            exception=None,
            name='001 TestScenario',
        )

        assert payload == test_payload
        assert metadata == pymqi.MD().get()

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        noop_zmq.get_mock('zmq.Socket.disconnect').return_value = ZMQError
        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            ZMQAgain,
            {
                'success': True,
                'worker': '0000-1337',
                'response_length': 24,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
            },
        ]

        request.response.content_type = TransformerContentType.JSON

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test', '.*', 'payload_variable')
        mq_parent.user.request(request)

        assert mq_parent.user.context_variables['payload_variable'] == ''
        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=ANY,
            response_length=len(test_payload.encode()),
            context=mq_parent.user._context,
            name='001 TestScenario',
            response_time=ANY,
        )
        _, kwargs = request_event_spy.call_args_list[0]
        assert isinstance(kwargs['exception'], ResponseHandlerError)

        response_event_spy.assert_called_once_with(
            request=ANY,
            user=mq_parent.user,
            context=(pymqi.MD().get(), test_payload),
            name='001 TestScenario',
            exception=None,
        )
        _, kwargs = response_event_spy.call_args_list[0]
        actual_request = kwargs.get('request', None)
        assert actual_request.name == f'{mq_parent.user._scenario.identifier} {request.name}'
        assert actual_request.endpoint == request.endpoint
        assert actual_request.source == request.source

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        test_payload = """{
            "test": "payload_variable value"
        }"""

        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            ZMQAgain,
            {
                'success': True,
                'worker': '0000-1337',
                'response_length': 24,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
            },
        ]

        request.response.content_type = TransformerContentType.JSON
        mq_parent.user.request(request)

        assert mq_parent.user.context_variables['payload_variable'] == 'payload_variable value'

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=None,
            response_length=len(test_payload.encode()),
            context=mq_parent.user._context,
            name='001 TestScenario',
            response_time=ANY,
        )

        response_event_spy.assert_called_once()

        request.response.handlers.payload.clear()

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        mq_parent.user.request(request)

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=ANY,
            response_length=0,
            context=mq_parent.user._context,
            name='001 TestScenario',
            response_time=ANY,
        )

        request_event_spy.reset_mock()

        request.method = RequestMethod.POST

        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            {
                'success': False,
                'worker': '0000-1337',
                'response_length': 0,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
                'message': 'no implementation for POST'
            },
        ]
        mq_parent.user.request(request)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            exception=ANY,
            response_length=0,
            context=mq_parent.user._context,
            name='001 TestScenario',
            response_time=ANY,
        )

        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert 'no implementation for POST' in str(kwargs['exception'])
        request_event_spy.reset_mock()

        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            {
                'success': False,
                'worker': '0000-1337',
                'response_length': 0,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
                'message': 'no implementation for POST'
            } for _ in range(3)
        ]

        mq_parent.user._scenario.failure_exception = None
        mq_parent.user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            mq_parent.user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = RestartScenario
        with pytest.raises(RestartScenario):
            mq_parent.user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None

        # Test queue / expression START
        response_event_spy.reset_mock()

        the_side_effect = [
            {
                'success': True,
                'worker': '0000-1337',
                'response_length': 24,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': '',
            }
        ]

        send_json_spy = noop_zmq.get_mock('zmq.Socket.send_json')
        send_json_spy.side_effect = None
        send_json_spy.return_value = None
        send_json_spy.reset_mock()

        recv_json = noop_zmq.get_mock('zmq.Socket.recv_json')
        recv_json.side_effect = the_side_effect * 7

        request.method = RequestMethod.GET
        request.endpoint = 'queue:IFKTEST'

        mq_parent.user.request(request)

        assert send_json_spy.call_count == 1
        args, kwargs = send_json_spy.call_args_list[0]
        assert len(args) == 1
        assert kwargs == {}
        ctx: Dict[str, str] = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        # Test with specifying queue: prefix as endpoint
        request.endpoint = 'queue:IFKTEST'
        mq_parent.user.request(request)
        assert send_json_spy.call_count == 2
        args, kwargs = send_json_spy.call_args_list[-1]
        assert len(args) == 1
        assert kwargs == {}
        ctx = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        # Test specifying queue: prefix with expression
        request.endpoint = 'queue:IFKTEST2, expression:/class/student[marks>85]'
        mq_parent.user.request(request)
        assert send_json_spy.call_count == 3
        args, kwargs = send_json_spy.call_args_list[-1]
        assert len(args) == 1
        assert kwargs == {}
        ctx = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        # Test specifying queue: prefix with expression, and spacing
        request.endpoint = 'queue: IFKTEST2  , expression: /class/student[marks>85]'
        mq_parent.user.request(request)
        assert send_json_spy.call_count == 4
        args, kwargs = send_json_spy.call_args_list[-1]
        assert len(args) == 1
        assert kwargs == {}
        ctx = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        # Test specifying queue without prefix, with expression
        request.endpoint = 'queue:IFKTEST3, expression:/class/student[marks<55], max_message_size:13337'
        mq_parent.user.request(request)
        assert send_json_spy.call_count == 5
        args, kwargs = send_json_spy.call_args_list[-1]
        assert len(args) == 1
        assert kwargs == {}
        ctx = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        request.endpoint = 'queue:IFKTEST3, max_message_size:444'
        mq_parent.user.request(request)
        assert send_json_spy.call_count == 6
        args, kwargs = send_json_spy.call_args_list[-1]
        assert len(args) == 1
        assert kwargs == {}
        ctx = args[0]['context']
        assert ctx['endpoint'] == request.endpoint

        # Test error when missing expression: prefix
        request.endpoint = 'queue:IFKTEST3, /class/student[marks<55]'
        mq_parent.user._scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            mq_parent.user.request(request)

        # Test with expression argument but wrong method
        request.endpoint = 'queue:IFKTEST3, expression:/class/student[marks<55]'
        request.method = RequestMethod.PUT
        mq_parent.user._scenario.failure_exception = RestartScenario

        with pytest.raises(RestartScenario):
            mq_parent.user.request(request)

        assert response_event_spy.call_count == 8
        assert send_json_spy.call_count == 6
        _, kwargs = response_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'argument "expression" is not allowed when sending to an endpoint'

        # Test with empty queue name
        request.endpoint = 'queue:, expression:/class/student[marks<55]'
        request.method = RequestMethod.GET
        mq_parent.user._scenario.failure_exception = StopUser

        with pytest.raises(StopUser):
            mq_parent.user.request(request)

        assert response_event_spy.call_count == 9
        assert send_json_spy.call_count == 6
        _, kwargs = response_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == 'invalid value for argument "queue"'

        send_json_spy.reset_mock()
        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        # Test queue / expression END

    def test_send(self, mq_parent: GrizzlyScenario, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')

        response_connected: AsyncMessageResponse = {
            'worker': '0000-1337',
            'success': True,
            'message': 'connected',
        }

        mq_parent.user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            }
        }

        remote_variables = {
            'variables': transform(GrizzlyContext(), {
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        # always throw error when disconnecting, it is ignored
        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.disconnect',
            side_effect=[ZMQError] * 10,
        )

        request_event_spy = mocker.spy(mq_parent.user.environment.events.request, 'fire')

        template = cast(RequestTask, mq_parent.user._scenario.tasks()[-1])
        template.endpoint = 'queue:TEST.QUEUE'

        mq_parent.user.add_context(remote_variables)

        request = mq_parent.user.render(template)

        assert request.source is not None

        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.recv_json',
            side_effect=[
                response_connected,
                {
                    'success': True,
                    'worker': '0000-1337',
                    'response_length': 182,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': request.source,
                }
            ],
        )

        mq_parent.user.on_start()

        mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['request_type'] == 'SEND'
        assert kwargs['exception'] is None
        assert kwargs['response_length'] == len(request.source.encode())

        request_event_spy.reset_mock()

        mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        template.method = RequestMethod.POST

        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.recv_json',
            side_effect=[
                {
                    'success': False,
                    'worker': '0000-1337',
                    'response_length': 0,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': request.source,
                    'message': 'no implementation for POST'
                } for _ in range(3)
            ],
        )

        mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        assert 'no implementation for POST' in str(kwargs['exception'])
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = None
        mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = StopUser
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = RestartScenario
        with pytest.raises(RestartScenario):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_exception = StopUser
        template.endpoint = 'sub:TEST.QUEUE'

        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert 'queue name must be prefixed with queue:' in str(exception)

        template.endpoint = 'queue:TEST.QUEUE, argument:False'
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 2
        _, kwargs = request_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert 'arguments argument is not supported' in str(exception)

        template.endpoint = 'queue:TEST.QUEUE, expression:$.test.result'
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 3
        _, kwargs = request_event_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert 'argument "expression" is not allowed when sending to an endpoint' in str(exception)

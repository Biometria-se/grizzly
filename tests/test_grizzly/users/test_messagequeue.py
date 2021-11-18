import subprocess

from typing import Callable, Dict, Tuple, Any, cast, Optional
from os import environ

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

import zmq
import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.exception import StopUser
from jinja2 import Template

from grizzly.users.messagequeue import MessageQueueUser
from grizzly.users.meta import RequestLogger, ResponseHandler
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContext, GrizzlyContextScenario
from grizzly.task import RequestTask
from grizzly.types import ResponseTarget, GrizzlyDict
from grizzly.testdata.utils import transform
from grizzly.exceptions import ResponseHandlerError
from grizzly.steps.helpers import add_save_handler
from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse

from ..fixtures import grizzly_context, request_task, locust_environment, noop_zmq  # pylint: disable=unused-import
from ..helpers import clone_request

import logging

# we are not interested in misleading log messages when unit testing
logging.getLogger().setLevel(logging.CRITICAL)

@pytest.fixture
def mq_user(grizzly_context: Callable) -> Tuple[MessageQueueUser, GrizzlyContextScenario, Environment]:
    environment, user, task, [_, _, request] = grizzly_context(
        'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1', MessageQueueUser)

    request = cast(RequestTask, request)

    scenario = GrizzlyContextScenario()
    scenario.name = task.__class__.__name__
    scenario.user.class_name = 'MessageQueueUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND
    request.scenario = scenario

    scenario.add_task(request)

    return user, scenario, environment

class TestMessageQueueUserNoPymqi:
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        del env['LD_LIBRARY_PATH']
        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            [
                '/usr/bin/env',
                'python3',
                '-c',
                'import grizzly.users.messagequeue as mq; print(f"{mq.pymqi.__name__=}"); mq.MessageQueueUser()'
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

    @pytest.mark.usefixtures('locust_environment')
    def test_create(self, locust_environment: Environment) -> None:
        try:
            MessageQueueUser.host = 'http://mq.example.com:1337'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'is not a supported scheme for MessageQueueUser' in str(e)

            MessageQueueUser.host = 'mq://mq.example.com:1337'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'needs QueueManager and Channel in the query string' in str(e)

            MessageQueueUser.host = 'mq://:1337'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'hostname is not specified in' in str(e)

            MessageQueueUser.host = 'mq://mq.example.com:1337/?Channel=Kanal1'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'needs QueueManager in the query string' in str(e)

            MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'needs Channel in the query string' in str(e)

            MessageQueueUser.host = 'mq://username:password@mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
            with pytest.raises(ValueError) as e:
                MessageQueueUser(environment=locust_environment)
            assert 'username and password should be set via context' in str(e)

            # Test default port and ssl_cipher
            MessageQueueUser.host = 'mq://mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
            user = MessageQueueUser(environment=locust_environment)
            assert user.am_context.get('connection', None) == f'mq.example.com(1414)'
            assert user.am_context.get('ssl_cipher', None) == 'ECDHE_RSA_AES_256_GCM_SHA384'

            MessageQueueUser._context['auth'] = {
                'username': 'syrsa',
                'password': 'hemligaarne',
                'key_file': '/my/key',
                'ssl_cipher': 'rot13',
                'cert_label': 'some_label',
            }

            MessageQueueUser.host = 'mq://mq.example.com:1415?Channel=Kanal1&QueueManager=QMGR01'
            user = MessageQueueUser(environment=locust_environment)

            assert user.am_context.get('connection', None) == 'mq.example.com(1415)'
            assert user.am_context.get('queue_manager', None) == 'QMGR01'
            assert user.am_context.get('channel', None) == 'Kanal1'
            assert user.am_context.get('key_file', None) == '/my/key'
            assert user.am_context.get('ssl_cipher', None) == 'rot13'
            assert user.am_context.get('cert_label', None) == 'some_label'

            MessageQueueUser._context['auth']['cert_label'] = None

            user = MessageQueueUser(environment=locust_environment)

            assert user.am_context.get('cert_label', None) == 'syrsa'

            MessageQueueUser._context['message']['wait'] = 5

            user = MessageQueueUser(environment=locust_environment)
            assert user.am_context.get('message_wait', None) == 5
            assert issubclass(user.__class__, (RequestLogger, ResponseHandler,))
        finally:
            MessageQueueUser._context = {
                'auth': {
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None
                }
            }

    @pytest.mark.usefixtures('locust_environment', 'noop_zmq')
    def test_request__action_conn_error(self, locust_environment: Environment, mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        def mocked_zmq_connect(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            raise zmq.error.ZMQError(msg='error connecting')

        noop_zmq('grizzly.users.messagequeue')

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.connect',
            mocked_zmq_connect,
        )

        def mocked_request_fire(*args: Tuple[Any, ...], **_kwargs: Dict[str, Any]) -> None:
            # ehm, mypy thinks that _kwargs has type dict[str, Dict[str, Any]]
            kwargs = cast(Dict[str, Any], _kwargs)
            properties = list(kwargs.keys())
            # self.environment.events.request.fire
            if properties == ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']:
                assert kwargs['request_type'] == 'mq:CONN'
                assert kwargs['name'] == user.am_context.get('connection', None)
                assert kwargs['response_time'] >= 0
                assert kwargs['response_length'] == 0
                assert isinstance(kwargs['exception'], zmq.error.ZMQError)
            elif properties == ['name', 'request', 'context', 'user', 'exception']:  # self.response_event.fire
                pytest.fail(f'what should we do with {kwargs=}')
            else:
                pytest.fail(f'unknown event fired: {properties}')

        mocker.patch(
            'locust.event.EventHook.fire',
            mocked_request_fire,
        )

        MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1'
        user = MessageQueueUser(locust_environment)

        request = RequestTask(RequestMethod.PUT, name='test-put', endpoint='EXAMPLE.QUEUE')
        scenario = GrizzlyContextScenario()
        scenario.name = 'test'
        scenario.add_task(request)

        with pytest.raises(StopUser):
            user.request(request)

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    @pytest.mark.usefixtures('locust_environment')
    def test_get_tls_real(self, locust_environment: Environment) -> None:
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
            scenario = GrizzlyContextScenario()
            scenario.name = 'test'
            scenario.stop_on_failure = True
            scenario.add_task(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(locust_environment)

            user.request(request)
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
                    'ssl_cipher': None
                }
            }

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    @pytest.mark.usefixtures('locust_environment')
    def test_put_tls_real(self, locust_environment: Environment) -> None:
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
                }
            }

            request = RequestTask(RequestMethod.PUT, name='test-put', endpoint=self.real_stuff['endpoint'])
            request.source = 'we <3 IBM MQ'
            request.template = Template(request.source)
            scenario = GrizzlyContextScenario()
            scenario.name = 'test'
            scenario.stop_on_failure = True
            scenario.add_task(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(locust_environment)

            user.request(request)
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
                    'ssl_cipher': None
                }
            }

    @pytest.mark.usefixtures('noop_zmq')
    def test_get(self, mq_user: Tuple[MessageQueueUser, GrizzlyContextScenario, Environment], mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        [user, scenario, _] = mq_user

        noop_zmq('grizzly.users.messagequeue')

        response_connected: AsyncMessageResponse = {
            'worker': '0000-1337',
            'success': True,
            'message': 'connected',
        }

        payload = '<?xml encoding="utf-8"?>'

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                response_connected,
                {
                    'success': True,
                    'worker': '0000-1337',
                    'response_length': 24,
                    'response_time': -1337,  # fake so message queue daemon response time is a huge chunk
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                }
            ],
        )

        grizzly = GrizzlyContext()
        grizzly._scenarios = [scenario]

        user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            }
        }


        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
                'payload_variable': '',
                'metadata_variable': '',
            }),
        }

        grizzly.state.variables = cast(GrizzlyDict, {
            'payload_variable': '',
            'metadata_variable': '',
        })

        request_event_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_spy = mocker.spy(user.response_event, 'fire')

        request = cast(RequestTask, scenario.tasks[-1])
        request.method = RequestMethod.GET
        request.source = None
        request.template = None
        scenario.add_task(request)

        user.add_context(remote_variables)

        user.request(request)

        assert request_event_spy.call_count == 2
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['request_type'] == 'mq:CONN'
        assert kwargs['exception'] is None
        assert kwargs['response_length'] == 0

        _, kwargs = request_event_spy.call_args_list[1]
        assert kwargs['request_type'] == 'mq:GET'
        assert kwargs['exception'] is None
        assert kwargs['response_length'] == len(payload)

        assert response_event_spy.call_count == 1
        _, kwargs = response_event_spy.call_args_list[0]
        assert kwargs['request'] is request
        assert kwargs['context'] == (pymqi.MD().get(), payload)
        assert kwargs['user'] is user

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                {
                    'success': True,
                    'worker': '0000-1337',
                    'response_length': 24,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                }
            ],
        )

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test', '.*', 'payload_variable')

        user.request(request)

        assert user.context_variables['payload_variable'] == ''
        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['request_type'] == 'mq:GET'
        assert isinstance(kwargs['exception'], ResponseHandlerError)

        assert response_event_spy.call_count == 1
        _, kwargs = response_event_spy.call_args_list[0]
        assert kwargs['request'] is request
        assert kwargs['user'] is user
        assert kwargs['context'] == (pymqi.MD().get(), payload)

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        payload = '''{
            "test": "payload_variable value"
        }'''

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                {
                    'success': True,
                    'worker': '0000-1337',
                    'response_length': 24,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                }
            ],
        )

        user.request(request)

        assert user.context_variables['payload_variable'] == 'payload_variable value'

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is None
        assert response_event_spy.call_count == 1

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        request_error = clone_request('POST', request)

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                {
                    'success': False,
                    'worker': '0000-1337',
                    'response_length': 0,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                    'message': 'no implementation for POST'
                }
            ],
        )

        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert 'no implementation for POST' in str(kwargs['exception'])
        request_event_spy.reset_mock()

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                {
                    'success': False,
                    'worker': '0000-1337',
                    'response_length': 0,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                    'message': 'no implementation for POST'
                } for _ in range(3)
            ],
        )

        scenario.stop_on_failure = False
        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        scenario.stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

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

        # Setup mock to capture json sent to async_messaged
        class JsonMocker(object):
            sent_request : Dict[str, Any] = {}

        def mocked_send_json(foo: Any, am_request: Dict[str, Any]) -> None:
            JsonMocker.sent_request = am_request

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.send_json',
            mocked_send_json,
        )

        # Test with only queue name as endpoint
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'IFKTEST'
        user.request(request)
        ctx : Dict[str, str] = JsonMocker.sent_request['context']
        assert ctx['endpoint'] == 'IFKTEST'
        assert ctx['expression'] == None

        # Test with specifying queue: prefix as endpoint
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'queue:IFKTEST'
        user.request(request)
        ctx = JsonMocker.sent_request['context']
        assert ctx['endpoint'] == 'IFKTEST'
        assert ctx['expression'] == None

        # Test specifying queue: prefix with expression
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'queue:IFKTEST2, expression:/class/student[marks>85]'
        user.request(request)
        ctx = JsonMocker.sent_request['context']
        assert ctx['endpoint'] == 'IFKTEST2'
        assert ctx['expression'] == '/class/student[marks>85]'

        # Test specifying queue: prefix with expression, and spacing
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'queue: IFKTEST2  , expression: /class/student[marks>85]'
        user.request(request)
        ctx = JsonMocker.sent_request['context']
        assert ctx['endpoint'] == 'IFKTEST2'
        assert ctx['expression'] == '/class/student[marks>85]'

        # Test specifying queue without prefix, with expression
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'IFKTEST3, expression:/class/student[marks<55]'
        user.request(request)
        ctx = JsonMocker.sent_request['context']
        assert ctx['endpoint'] == 'IFKTEST3'
        assert ctx['expression'] == '/class/student[marks<55]'

        # Test error when missing expression: prefix
        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=the_side_effect,
        )
        request.endpoint = 'IFKTEST3, /class/student[marks<55]'
        with pytest.raises(StopUser):
            user.request(request)

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        # Test queue / expression END


    @pytest.mark.usefixtures('noop_zmq')
    def test_send(self, mq_user: Tuple[MessageQueueUser, GrizzlyContextScenario, Environment], mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        [user, scenario, _] = mq_user

        noop_zmq('grizzly.users.messagequeue')

        response_connected: AsyncMessageResponse = {
            'worker': '0000-1337',
            'success': True,
            'message': 'connected',
        }

        user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            }
        }

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        request_event_spy = mocker.spy(user.environment.events.request, 'fire')

        request = cast(RequestTask, scenario.tasks[-1])

        user.add_context(remote_variables)

        _, _, payload = user.render(request)

        assert payload is not None

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                response_connected,
                {
                    'success': True,
                    'worker': '0000-1337',
                    'response_length': 182,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                }
            ],
        )

        user.request(request)

        assert request_event_spy.call_count == 2
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['request_type'] == 'mq:CONN'
        assert kwargs['exception'] is None
        assert kwargs['response_length'] == 0

        _, kwargs = request_event_spy.call_args_list[1]
        assert kwargs['request_type'] == 'mq:SEND'
        assert kwargs['exception'] is None
        assert kwargs['response_length'] == len(payload)

        request_event_spy.reset_mock()

        user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        request_error = clone_request('POST', request)

        mocker.patch(
            'grizzly.users.messagequeue.zmq.sugar.socket.Socket.recv_json',
            side_effect=[
                {
                    'success': False,
                    'worker': '0000-1337',
                    'response_length': 0,
                    'response_time': 1337,
                    'metadata': pymqi.MD().get(),
                    'payload': payload,
                    'message': 'no implementation for POST'
                } for _ in range(3)
            ],
        )

        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert 'no implementation for POST' in str(kwargs['exception'])
        request_event_spy.reset_mock()

        scenario.stop_on_failure = False
        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        scenario.stop_on_failure = True
        with pytest.raises(StopUser):
            user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

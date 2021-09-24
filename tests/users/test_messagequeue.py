import gevent.monkey

gevent.monkey.patch_all()

import subprocess

from typing import Callable, Dict, Tuple, Type, Any, cast
from json import loads as jsonloads
from os import environ

import pymqi
import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.exception import StopUser
from jinja2 import Template

from grizzly.users.messagequeue import MessageQueueUser
from grizzly.users.meta import RequestLogger, ResponseHandler
from grizzly.types import RequestMethod
from grizzly.context import LocustContext, LocustContextScenario, RequestContext, ResponseTarget
from grizzly.testdata.utils import transform
from grizzly.testdata.models import TemplateData
from grizzly.exceptions import ResponseHandlerError
from grizzly.utils import add_save_handler

from ..fixtures import locust_context, request_context, locust_environment  # pylint: disable=unused-import
from ..helpers import clone_request

import logging

# we are not interested in misleading log messages when unit testing
logging.getLogger().setLevel(logging.CRITICAL)

@pytest.fixture
def mq_user(locust_context: Callable, mocker: MockerFixture) -> Tuple[MessageQueueUser, LocustContextScenario, Environment]:
    def mq_connect(queue_manager: str, channel: str, conn_info: str, username: str, password: str) -> Dict[str, str]:
        return {'queue_manager': queue_manager, 'channel': channel, 'conn_info': conn_info, 'username': username, 'password': password}

    mocker.patch(
        'pymqi.connect',
        mq_connect,
    )

    environment, user, task, [_, _, request] = locust_context(
        'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1', MessageQueueUser)

    request = cast(RequestContext, request)

    scenario = LocustContextScenario()
    scenario.name = task.__class__.__name__
    scenario.user_class_name = 'MessageQueueUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND
    request.scenario = scenario

    scenario.add_task(request)

    return user, scenario, environment

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

    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        del env['LD_LIBRARY_PATH']
        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            [
                '/usr/bin/env',
                'python3',
                '-c',
                'from gevent.monkey import patch_all; patch_all(); import grizzly.users.messagequeue as mq; print(f"{mq.has_dependency=}"); mq.MessageQueueUser()'
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, _ = process.communicate()
        output = out.decode('utf-8')
        print(output)
        assert process.returncode == 1
        assert 'mq.has_dependency=False' in output
        assert 'could not import pymqi, have you installed IBM MQ dependencies?' in output

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
            assert user.port == 1414
            assert user.ssl_cipher == 'ECDHE_RSA_AES_256_GCM_SHA384'

            MessageQueueUser._context['auth'] = {
                'username': 'syrsa',
                'password': 'hemligaarne',
                'key_file': '/my/key',
                'ssl_cipher': 'rot13',
                'cert_label': 'some_label',
            }

            MessageQueueUser.host = 'mq://mq.example.com:1415?Channel=Kanal1&QueueManager=QMGR01'
            user = MessageQueueUser(environment=locust_environment)

            assert user.hostname == 'mq.example.com'
            assert user.port == 1415
            assert user.queue_manager == 'QMGR01'
            assert user.channel == 'Kanal1'
            assert user.key_file == '/my/key'
            assert user.ssl_cipher == 'rot13'
            assert user.cert_label == 'some_label'

            MessageQueueUser._context['auth']['cert_label'] = None

            user = MessageQueueUser(environment=locust_environment)

            assert user.cert_label == 'syrsa'
            assert hasattr(user, 'md')
            assert not hasattr(user, 'gmo')
            assert user._get_arguments == (None, user.md)

            MessageQueueUser._context['message']['wait'] = 5

            user = MessageQueueUser(environment=locust_environment)
            assert getattr(user, 'md', None) is not None
            assert getattr(user, 'gmo', None) is not None
            # shut up pylint!
            assert cast(Any, user.gmo).WaitInterval == 5000  # pylint: disable=no-member
            assert cast(Any, user.gmo).Options == pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
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

    @pytest.mark.usefixtures('locust_environment')
    def test_request__action_conn_error(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        def mocked_pymqi_connect(queue_manager: str, channel: str, conn_info: str, username: str, password: str) -> Any:
            raise pymqi.MQMIError(comp=2, reason=2538)

        mocker.patch(
            'pymqi.connect',
            mocked_pymqi_connect,
        )

        def mocked_request_fire(*args: Tuple[Any, ...], **_kwargs: Dict[str, Any]) -> None:
            # ehm, mypy thinks that _kwargs has type dict[str, Dict[str, Any]]
            kwargs = cast(Dict[str, Any], _kwargs)
            properties = list(kwargs.keys())
            # self.environment.events.request.fire
            if properties == ['request_type', 'name', 'response_time', 'response_length', 'context', 'exception']:
                assert kwargs['request_type'] == 'mq:CONN'
                assert kwargs['name'] == f'{user.hostname}({user.port})'
                assert kwargs['response_time'] >= 0
                assert kwargs['response_length'] == 0
                assert isinstance(kwargs['exception'], pymqi.MQMIError)
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

        request = RequestContext(RequestMethod.PUT, name='test-put', endpoint='EXAMPLE.QUEUE')
        scenario = LocustContextScenario()
        scenario.name = 'test'
        scenario.add_task(request)

        with pytest.raises(StopUser):
            user.request(request)

    @pytest.mark.usefixtures('locust_environment')
    def test_request_tls(self, locust_environment: Environment, mocker: MockerFixture) -> None:
        def mocked_connect_with_options(i: pymqi.QueueManager, user: bytes, password: bytes, cd: pymqi.CD, sco: pymqi.SCO) -> None:
            assert user == 'test_username'.encode('utf-8')
            assert password == 'test_password'.encode('utf-8')

            assert cd.ChannelName == 'Kanal1'.encode('utf-8')
            assert cd.ConnectionName == 'mq.example.com(1337)'.encode('utf-8')
            assert cd.ChannelType == pymqi.CMQC.MQCHT_CLNTCONN
            assert cd.TransportType == pymqi.CMQC.MQXPT_TCP
            assert cd.SSLCipherSpec == 'ECDHE_RSA_AES_256_GCM_SHA384'

            assert sco.KeyRepository == '/home/test/key_file'.encode('utf-8')
            assert sco.CertificateLabel == 'test_cert_label'.encode('utf-8')

            raise RuntimeError('skip rest of the method')

        mocker.patch(
            'pymqi.QueueManager.connect_with_options',
            mocked_connect_with_options,
        )

        request = RequestContext(RequestMethod.PUT, name='test-put', endpoint='EXAMPLE.QUEUE')
        scenario = LocustContextScenario()
        scenario.name = 'test'
        scenario.stop_on_failure = True
        scenario.add_task(request)

        try:
            MessageQueueUser.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1'
            MessageQueueUser._context['auth'] = {
                'username': 'test_username',
                'password': 'test_password',
                'key_file': '/home/test/key_file',
                'cert_label': 'test_cert_label',
            }
            user = MessageQueueUser(locust_environment)

            with pytest.raises(StopUser):
                user.request(request)
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

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    @pytest.mark.usefixtures('locust_environment')
    def test_get_tls_real(self, locust_environment: Environment) -> None:
        try:
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


            request = RequestContext(RequestMethod.GET, name='test-get', endpoint=self.real_stuff['endpoint'])
            scenario = LocustContextScenario()
            scenario.name = 'test'
            scenario.stop_on_failure = True
            scenario.add_task(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(locust_environment)

            user.request(request)
            assert 0
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

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    @pytest.mark.usefixtures('locust_environment')
    def test_put_tls_real(self, locust_environment: Environment) -> None:
        try:

            MessageQueueUser._context = {
                'auth': {
                    'username': self.real_stuff['username'],
                    'password': self.real_stuff['password'],
                    'key_file': self.real_stuff['key_file'],
                    'cert_label': None,
                    'ssl_cipher': None
                }
            }

            request = RequestContext(RequestMethod.PUT, name='test-put', endpoint=self.real_stuff['endpoint'])
            request.source = 'we <3 IBM MQ'
            request.template = Template(request.source)
            scenario = LocustContextScenario()
            scenario.name = 'test'
            scenario.stop_on_failure = True
            scenario.add_task(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(locust_environment)

            user.request(request)
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

    def test_get(self, mq_user: Tuple[MessageQueueUser, LocustContextScenario, Environment], mocker: MockerFixture) -> None:
        [user, scenario, _] = mq_user

        context_locust = LocustContext()
        context_locust._scenarios = [scenario]

        user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            }
        }

        payload = '<?xml encoding="utf-8"?>'

        class DummyQueue:
            name: str
            closed: bool

            def __init__(self) -> None:
                self.closed = False

            def get(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> bytes:
                return payload.encode('utf-8')

            def close(self) -> None:
                self.closed = True

        class DummyErrorQueue(DummyQueue):
            def get(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
                raise Exception('get failed')


        def mock_queue(queue_class: Type[DummyQueue]) -> DummyQueue:
            queue = queue_class()

            def mq_queue(qmgr: pymqi.QueueManager, qname: str) -> DummyQueue:
                queue.name = qname
                return queue

            mocker.patch(
                'pymqi.Queue',
                mq_queue,
            )

            return queue

        queue = mock_queue(DummyQueue)

        assert queue.closed == False

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
                'payload_variable': '',
                'metadata_variable': '',
            }),
        }

        context_locust.state.variables = cast(TemplateData, {
            'payload_variable': '',
            'metadata_variable': '',
        })

        request_event_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_spy = mocker.spy(user.response_event, 'fire')

        request = cast(RequestContext, scenario.tasks[-1])
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

        add_save_handler(context_locust, ResponseTarget.PAYLOAD, '$.test', '.*', 'payload_variable')

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

        user.request(request)

        assert user.context_variables['payload_variable'] == 'payload_variable value'

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is None
        assert response_event_spy.call_count == 1

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        queue = mock_queue(DummyErrorQueue)

        assert queue.closed == False

        user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert queue.closed == True
        request_event_spy.reset_mock()

        request_error = clone_request('POST', request)
        queue = mock_queue(DummyErrorQueue)

        assert queue.closed == False

        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert 'has not implemented POST' in str(kwargs['exception'])
        assert queue.closed == True
        request_event_spy.reset_mock()

        queue = mock_queue(DummyErrorQueue)

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


    def test_send(self, mq_user: Tuple[MessageQueueUser, LocustContextScenario, Environment], mocker: MockerFixture) -> None:
        [user, scenario, _] = mq_user

        user._context = {
            'auth': {
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
            }
        }

        class DummyQueue:
            name: str
            payload: str
            closed: bool
            md: pymqi.MD

            def __init__(self) -> None:
                self.closed = False

            def put(self, payload: str, *opts: Tuple[Any, ...]) -> None:
                self.payload = payload
                self.md = opts[0]

            def close(self) -> None:
                self.closed = True

        class DummyErrorQueue(DummyQueue):
            def put(self, payload: str, *opts: Tuple[Any, ...]) -> None:
                raise Exception('put failed')


        def mock_queue(queue_class: Type[DummyQueue]) -> DummyQueue:
            queue = queue_class()

            def mq_queue(qmgr: pymqi.QueueManager, qname: str) -> DummyQueue:
                queue.name = qname
                return queue

            mocker.patch(
                'pymqi.Queue',
                mq_queue,
            )

            return queue

        queue = mock_queue(DummyQueue)

        assert queue.closed == False

        remote_variables = {
            'variables': transform({
                'AtomicIntegerIncrementer.messageID': 31337,
                'AtomicDate.now': '',
                'messageID': 137,
            }),
        }

        request_event_spy = mocker.spy(user.environment.events.request, 'fire')

        request = cast(RequestContext, scenario.tasks[-1])

        user.add_context(remote_variables)

        _, _, payload = user.render(request)

        assert payload is not None

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

        data = jsonloads(queue.payload)

        assert '31337' in queue.payload
        assert data['result']['id'] == 'ID-31337'
        assert queue.closed == True
        assert queue.name == request.endpoint

        queue = mock_queue(DummyErrorQueue)

        assert queue.closed == False

        user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert queue.closed == True
        request_event_spy.reset_mock()

        request_error = clone_request('POST', request)
        queue = mock_queue(DummyErrorQueue)

        assert queue.closed == False

        user.request(request_error)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[0]
        assert kwargs['exception'] is not None
        assert 'has not implemented POST' in str(kwargs['exception'])
        assert queue.closed == True
        request_event_spy.reset_mock()

        queue = mock_queue(DummyErrorQueue)

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

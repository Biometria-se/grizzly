"""Unit tests for grizzly.users.messagequeue."""

from __future__ import annotations

import subprocess
import sys
from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, Any, ClassVar, cast

import pytest
from async_messaged import AsyncMessageError
from grizzly.context import GrizzlyContextScenario
from grizzly.exceptions import ResponseHandlerError, RestartScenario, StopScenario
from grizzly.scenarios import GrizzlyScenario, IteratorScenario
from grizzly.steps._helpers import add_save_handler
from grizzly.tasks import RequestTask
from grizzly.testdata.utils import transform
from grizzly.types import RequestMethod, ResponseTarget, pymqi
from grizzly.types.locust import Environment, StopUser
from grizzly.users.messagequeue import MessageQueueUser
from grizzly_common.transformer import TransformerContentType
from zmq.error import Again as ZMQAgain
from zmq.error import ZMQError

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from async_messaged import AsyncMessageResponse

    from test_framework.fixtures import GrizzlyFixture, MockerFixture, NoopZmqFixture

MqScenarioFixture = tuple[MessageQueueUser, GrizzlyContextScenario, Environment]


@pytest.fixture
def mq_parent(grizzly_fixture: GrizzlyFixture) -> GrizzlyScenario:
    parent = grizzly_fixture(
        host='mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1',
        user_type=MessageQueueUser,
    )

    request = grizzly_fixture.request_task.request

    scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario(parent.__class__.__name__), grizzly=grizzly_fixture.grizzly)
    scenario.user.class_name = 'MessageQueueUser'
    scenario.context['host'] = 'test'

    request.method = RequestMethod.SEND

    scenario.tasks.add(request)
    grizzly_fixture.grizzly.scenarios.clear()
    grizzly_fixture.grizzly.scenarios.append(scenario)
    parent.user._scenario = scenario

    return parent


class TestMessageQueueUserNoPymqi:
    @pytest.mark.timeout(40)
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        with suppress(KeyError):
            del env['LD_LIBRARY_PATH']

        env['PYTHONPATH'] = '.:framework/src:common/src'

        with pytest.raises(subprocess.CalledProcessError) as e:
            subprocess.check_output(
                [
                    sys.executable,
                    '-c',
                    (
                        'from grizzly.types import RequestDirection; import grizzly.tasks.clients.messagequeue as mq; '
                        'print(f"{mq.pymqi.__name__=}"); mq.MessageQueueClientTask(RequestDirection.FROM, "mqs://localhost:1");'
                    ),
                ],
                env=env,
                stderr=subprocess.STDOUT,
            )
        assert e.value.returncode == 1
        output = e.value.output.decode()
        assert "mq.pymqi.__name__='grizzly_common.dummy_pymqi'" in output
        assert 'NotImplementedError: MessageQueueClientTask could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?' in output

        """
        process = subprocess.Popen(
            [
                sys.executable,
                '-c',
                'import grizzly.users.messagequeue as mq; from grizzly.types.locust import Environment; print(f"{mq.pymqi.__name__=}"); mq.MessageQueueUser(Environment())',
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, _ = process.communicate()
        output = out.decode()

        assert process.returncode == 1
        assert "mq.pymqi.__name__='grizzly_common.dummy_pymqi'" in output
        assert 'NotImplementedError: MessageQueueUser could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?' in output
        """


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_common.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestMessageQueueUser:
    real_stuff: ClassVar[dict[str, str]] = {
        'username': '',
        'password': '',
        'key_file': '',
        'endpoint': '',
        'host': '',
        'queue_manager': '',
        'channel': '',
    }

    def test_on_start(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')

        parent = grizzly_fixture(host='mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1', user_type=MessageQueueUser)

        assert isinstance(parent.user, MessageQueueUser)

        connect_mock = noop_zmq.get_mock('zmq.Socket.connect')

        assert not hasattr(parent.user, 'zmq_client')

        request_context_spy = mocker.patch.object(parent.user, '_request_context', return_value=mocker.MagicMock())

        parent.user.on_start()

        request_context_spy.assert_called_once_with({'action': 'CONN', 'client': id(parent.user), 'context': parent.user.am_context})
        request_context_spy.return_value.__enter__.assert_called_once_with()
        request_context_spy.return_value.__enter__.return_value.update.assert_not_called()
        connect_mock.assert_called_once_with(parent.user.zmq_url)

    def test_on_stop(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.messagequeue')
        zmq_disconnect_mock = mocker.patch('grizzly.users.messagequeue.zmq_disconnect')

        parent = grizzly_fixture(host='mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1', user_type=MessageQueueUser)

        assert isinstance(parent.user, MessageQueueUser)

        request_context_spy = mocker.patch.object(parent.user, '_request_context', return_value=mocker.MagicMock())

        parent.user.on_start()

        request_context_spy.reset_mock()

        parent.user.on_stop()

        request_context_spy.assert_not_called()
        zmq_disconnect_mock.assert_called_once_with(parent.user.zmq_client, destroy_context=False)
        zmq_disconnect_mock.reset_mock()

        parent.user.worker_id = 'foobar'

        parent.user.on_stop()

        request_context_spy.assert_called_once_with({'action': 'DISC', 'worker': 'foobar', 'client': id(parent.user), 'context': parent.user.am_context})
        request_context_spy.return_value.__enter__.assert_called_once_with()
        request_context_spy.return_value.__enter__.return_value.update.assert_not_called()
        zmq_disconnect_mock.assert_called_once_with(parent.user.zmq_client, destroy_context=False)

    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:  # noqa: PLR0915
        parent = grizzly_fixture(host='mq://mq.example.com:1415?Channel=Kanal1&QueueManager=QMGR01', user_type=MessageQueueUser)
        environment = grizzly_fixture.behave.locust.environment
        test_cls = parent.user.__class__

        assert issubclass(test_cls, MessageQueueUser)

        test_cls.host = 'http://mq.example.com:1337'
        with pytest.raises(ValueError, match='is not a supported scheme for MessageQueueUser'):
            test_cls(environment=environment)

        test_cls.host = 'mq://mq.example.com:1337'
        with pytest.raises(ValueError, match='needs QueueManager and Channel in the query string'):
            test_cls(environment=environment)

        test_cls.host = 'mq://:1337'
        with pytest.raises(ValueError, match='hostname is not specified'):
            test_cls(environment=environment)

        test_cls.host = 'mq://mq.example.com:1337/?Channel=Kanal1'
        with pytest.raises(ValueError, match='needs QueueManager in the query string'):
            test_cls(environment=environment)

        test_cls.host = 'mq://mq.example.com:1337/?QueueManager=QMGR01'
        with pytest.raises(ValueError, match='needs Channel in the query string'):
            test_cls(environment=environment)

        test_cls.host = 'mq://username:password@mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
        with pytest.raises(ValueError, match='username and password should be set via context'):
            test_cls(environment=environment)

        # Test default port and ssl_cipher
        test_cls.host = 'mq://mq.example.com?Channel=Kanal1&QueueManager=QMGR01'
        user = test_cls(environment=environment)
        assert user.am_context.get('connection', None) == 'mq.example.com(1414)'
        assert user.am_context.get('ssl_cipher', None) == 'ECDHE_RSA_AES_256_GCM_SHA384'

        test_cls.__context__['auth'] = {
            'username': 'syrsa',
            'password': 'hemligaarne',
            'key_file': '/my/key',
            'ssl_cipher': 'rot13',
            'cert_label': 'some_label',
        }

        test_cls.host = 'mq://mq.example.com:1415?Channel=Kanal1&QueueManager=QMGR01'

        with pytest.raises(ValueError, match='MessageQueueUser_001 key file /my/key does not exist'):
            user = test_cls(environment=environment)

        kdb_file = grizzly_fixture.test_context / 'requests' / 'key.kdb'
        kdb_file.parent.mkdir(exist_ok=True)
        kdb_file.touch()
        test_cls.__context__['auth'].update({'key_file': 'requests/key'})
        user = test_cls(environment=environment)

        assert user.am_context.get('connection', None) == 'mq.example.com(1415)'
        assert user.am_context.get('queue_manager', None) == 'QMGR01'
        assert user.am_context.get('channel', None) == 'Kanal1'
        assert user.am_context.get('key_file', None) == kdb_file.with_suffix('').as_posix()
        assert user.am_context.get('ssl_cipher', None) == 'rot13'
        assert user.am_context.get('cert_label', None) == 'some_label'

        test_cls.__context__['auth']['cert_label'] = None

        user = test_cls(environment=environment)

        assert user.am_context.get('cert_label', None) == 'syrsa'

        test_cls.__context__['message']['wait'] = 5

        user = test_cls(environment=environment)
        assert user.am_context.get('message_wait', None) == 5

        test_cls.__context__['message']['header_type'] = 'RFH2'

        user = test_cls(environment=environment)
        assert user.am_context.get('header_type', None) == 'rfh2'

        test_cls.__context__['message']['header_type'] = 'None'

        user = test_cls(environment=environment)
        assert user.am_context.get('header_type', None) is None

        test_cls.__context__['message']['header_type'] = 'wrong'
        with pytest.raises(ValueError, match='unsupported value for header_type: "wrong"'):
            test_cls(environment=environment)

    def test_on_start__action_conn_error(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        def mocked_zmq_connect(*_args: Any, **_kwargs: Any) -> Any:
            raise ZMQError(msg='error connecting')

        noop_zmq('grizzly.users.messagequeue')

        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.connect',
            mocked_zmq_connect,
        )

        parent = grizzly_fixture(host='mq://mq.example.com:1337/?QueueManager=QMGR01&Channel=Kanal1', user_type=MessageQueueUser)
        user = parent.user
        assert isinstance(user, MessageQueueUser)

        scenario = user._scenario

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

        request = RequestTask(RequestMethod.PUT, name='test-put', endpoint='EXAMPLE.QUEUE')
        scenario.name = 'test'
        scenario.failure_handling.update({None: StopUser})
        scenario.tasks.add(request)
        user._scenario = scenario

        with pytest.raises(StopScenario):
            user.on_start()

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    def test_get_tls_real(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(user_type=MessageQueueUser, scenario_type=IteratorScenario)
        assert isinstance(parent, IteratorScenario)
        assert isinstance(parent.user, MessageQueueUser)

        process: subprocess.Popen | None = None
        try:
            process = subprocess.Popen(
                ['async-messaged'],  # noqa: S607
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
                'message': {
                    'wait': 0,
                },
            }

            request = RequestTask(RequestMethod.GET, name='test-get', endpoint=self.real_stuff['endpoint'])
            scenario = GrizzlyContextScenario(1, behave=grizzly_fixture.behave.create_scenario('get tls real'), grizzly=grizzly_fixture.grizzly)
            scenario.failure_handling.update({None: StopUser})
            scenario.tasks.add(request)

            MessageQueueUser.host = f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}'
            user = MessageQueueUser(parent.user.environment)
            parent.user = user

            parent.user.request(request)
            assert 0  # noqa: PT015
        finally:
            if process is not None:
                try:
                    process.terminate()
                    out, _ = process.communicate()
                    print(out)
                except Exception as e:
                    print(e)

    @pytest.mark.skip(reason='needs real credentials and host etc.')
    def test_put_tls_real(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture(
            host=f'mq://{self.real_stuff["host"]}/?QueueManager={self.real_stuff["queue_manager"]}&Channel={self.real_stuff["channel"]}',
            user_type=MessageQueueUser,
            scenario_type=IteratorScenario,
        )
        assert isinstance(parent, IteratorScenario)
        assert isinstance(parent.user, MessageQueueUser)
        grizzly = grizzly_fixture.grizzly

        process: subprocess.Popen | None = None
        try:
            process = subprocess.Popen(
                ['async-messaged'],  # noqa: S607
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
            grizzly.scenario.tasks.clear()
            grizzly.scenario.tasks.add(request)

            parent.user.request(request)
        finally:
            if process is not None:
                try:
                    process.terminate()
                    out, _ = process.communicate()
                    print(out)
                except Exception as e:
                    print(e)

    def test_get(self, grizzly_fixture: GrizzlyFixture, mq_parent: GrizzlyScenario, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:  # noqa: PLR0915
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
            'variables': transform(
                grizzly.scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 31337,
                    'AtomicDate.now': '',
                    'messageID': 137,
                    'payload_variable': '',
                    'metadata_variable': '',
                },
            ),
        }

        grizzly.scenario.variables.update(
            {
                'payload_variable': '',
                'metadata_variable': '',
            },
        )

        request_event_spy = mocker.spy(mq_parent.user.environment.events.request, 'fire')
        response_event_spy = mocker.spy(mq_parent.user.events.request, 'fire')

        request = cast('RequestTask', mq_parent.user._scenario.tasks()[-1])
        request.endpoint = 'queue:test-queue'
        request.method = RequestMethod.GET
        request.source = None
        mq_parent.user._scenario.tasks.add(request)

        mq_parent.user.add_context(remote_variables)
        mq_parent.user.on_start()

        assert mq_parent.user.worker_id == '0000-1337'

        metadata, payload = mq_parent.user.request(request)

        assert mq_parent.user.worker_id == '0000-1337'

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=None,
            response_length=len(test_payload.encode()),
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            name='001 TestScenario',
            response_time=ANY(int),
        )

        response_event_spy.assert_called_once_with(
            request=ANY(RequestTask),
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

        add_save_handler(grizzly, ResponseTarget.PAYLOAD, '$.test', '.*', 'payload_variable', default_value=None)
        mq_parent.user.request(request)

        assert mq_parent.user.variables['payload_variable'] == ''
        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=ANY(ResponseHandlerError, message='failed to transform input as JSON:'),
            response_length=len(test_payload.encode()),
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            name='001 TestScenario',
            response_time=ANY(int),
        )

        response_event_spy.assert_called_once_with(
            request=ANY(RequestTask),
            user=mq_parent.user,
            context=(pymqi.MD().get(), test_payload),
            name='001 TestScenario',
            exception=None,
        )

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

        assert mq_parent.user.variables['payload_variable'] == 'payload_variable value'

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=None,
            response_length=len(test_payload.encode()),
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            name='001 TestScenario',
            response_time=ANY(int),
        )

        response_event_spy.assert_called_once()

        request.response.handlers.payload.clear()

        request_event_spy.reset_mock()
        response_event_spy.reset_mock()

        mq_parent.user.request(request)

        request_event_spy.assert_called_once_with(
            request_type='GET',
            exception=ANY(RuntimeError, message='generator raised StopIteration'),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            name='001 TestScenario',
            response_time=ANY(int),
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
                'message': 'no implementation for POST',
            },
        ]
        mq_parent.user.request(request)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            exception=ANY(AsyncMessageError, message='no implementation for POST'),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            name='001 TestScenario',
            response_time=ANY(int),
        )
        request_event_spy.reset_mock()

        noop_zmq.get_mock('zmq.Socket.recv_json').side_effect = [
            {
                'success': False,
                'worker': '0000-1337',
                'response_length': 0,
                'response_time': 1337,
                'metadata': pymqi.MD().get(),
                'payload': test_payload,
                'message': 'no implementation for POST',
            }
            for _ in range(3)
        ]

        with suppress(KeyError):
            del mq_parent.user._scenario.failure_handling[None]
        mq_parent.user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_handling.update({None: StopUser})
        with pytest.raises(StopUser):
            mq_parent.user.request(request)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_handling.update({None: RestartScenario})
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
            },
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
        ctx: dict[str, str] = args[0]['context']
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
        mq_parent.user._scenario.failure_handling.update({None: StopUser})
        with pytest.raises(StopUser):
            mq_parent.user.request(request)

        # Test with expression argument but wrong method
        request.endpoint = 'queue:IFKTEST3, expression:/class/student[marks<55]'
        request.method = RequestMethod.PUT
        mq_parent.user._scenario.failure_handling.update({None: RestartScenario})

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
        mq_parent.user._scenario.failure_handling.update({None: StopUser})

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

    def test_send(self, mq_parent: GrizzlyScenario, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.users.messagequeue')

        assert isinstance(mq_parent.user, MessageQueueUser)

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
            },
        }

        remote_variables = {
            'variables': transform(
                mq_parent.user._scenario,
                {
                    'AtomicIntegerIncrementer.messageID': 31337,
                    'AtomicDate.now': '',
                    'messageID': 137,
                },
            ),
        }

        # always throw error when disconnecting, it is ignored
        mocker.patch(
            'grizzly.users.messagequeue.zmq.Socket.disconnect',
            side_effect=[ZMQError] * 10,
        )

        request_event_spy = mocker.spy(mq_parent.user.environment.events.request, 'fire')

        template = cast('RequestTask', mq_parent.user._scenario.tasks()[-1])
        template.endpoint = 'queue:TEST.QUEUE'

        mq_parent.user.add_context(remote_variables)

        request = mq_parent.user.render_request(template)

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
                },
            ],
        )

        assert getattr(mq_parent.user, 'worker_id', '0000-1337') is None

        mq_parent.user.on_start()

        assert mq_parent.user.worker_id == '0000-1337'

        mq_parent.user.request(template)

        assert mq_parent.user.worker_id == '0000-1337'

        request_event_spy.assert_called_once_with(
            request_type='SEND',
            name='001 TestScenario',
            exception=None,
            response_time=ANY(int),
            response_length=len(request.source.encode()),
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='SEND',
            name='001 TestScenario',
            exception=ANY(RuntimeError, message='generator raised StopIteration'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
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
                    'message': 'no implementation for POST',
                }
                for _ in range(3)
            ],
        )

        mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(AsyncMessageError, message='no implementation for POST'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        with suppress(KeyError):
            del mq_parent.user._scenario.failure_handling[None]
        mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(AsyncMessageError, message='no implementation for POST'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_handling.update({None: StopUser})
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(AsyncMessageError, message='no implementation for POST'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(RestartScenario):
            mq_parent.user.request(template)

        assert request_event_spy.call_count == 1
        _, kwargs = request_event_spy.call_args_list[-1]
        assert kwargs['exception'] is not None
        request_event_spy.reset_mock()

        mq_parent.user._scenario.failure_handling.update({None: StopUser})
        template.endpoint = 'sub:TEST.QUEUE'

        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(RuntimeError, message='queue name must be prefixed with queue:'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        template.endpoint = 'queue:TEST.QUEUE, argument:False'
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(RuntimeError, message='arguments argument is not supported'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

        template.endpoint = 'queue:TEST.QUEUE, expression:$.test.result'
        with pytest.raises(StopUser):
            mq_parent.user.request(template)

        request_event_spy.assert_called_once_with(
            request_type='POST',
            name='001 TestScenario',
            exception=ANY(RuntimeError, message='argument "expression" is not allowed when sending to an endpoint'),
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(mq_parent.user),
                **mq_parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
        )
        request_event_spy.reset_mock()

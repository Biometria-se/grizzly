import logging

from typing import Callable, cast

import pytest
import zmq

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from locust.env import Environment
from locust.exception import StopUser
from jinja2 import Template

from grizzly.users.meta import ContextVariables, RequestLogger, ResponseHandler
from grizzly.users.servicebus import ServiceBusUser
from grizzly.types import RequestMethod
from grizzly.task import RequestTask, SleepTask
from grizzly.context import GrizzlyContextScenario
from grizzly_extras.async_message import AsyncMessageResponse, AsyncMessageError

from ..fixtures import behave_context, request_task, locust_environment, noop_zmq  # pylint: disable=unused-import


class TestServiceBusUser:
    @pytest.mark.usefixtures('locust_environment', 'noop_zmq')
    def test_create(self, locust_environment: Environment, mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.users.servicebus')

        try:
            zmq_client_connect_spy = mocker.patch('grizzly.users.servicebus.zmq.sugar.socket.Socket.connect', side_effect=[None] * 10)
            say_hello_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.say_hello', side_effect=[None] * 10)

            ServiceBusUser.host = 'Endpoint=mq://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: "mq" is not a supported scheme' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKeyName and SharedAccessKey must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKeyName must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_environment)
            assert 'ServiceBusUser: SharedAccessKey must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            user = ServiceBusUser(environment=locust_environment)
            assert issubclass(user.__class__, ContextVariables)
            assert issubclass(user.__class__, ResponseHandler)
            assert issubclass(user.__class__, RequestLogger)

            assert zmq_client_connect_spy.call_count == 1
            args, _ = zmq_client_connect_spy.call_args_list[0]
            assert args[0] == ServiceBusUser.zmq_url
            assert user.zmq_client.type == zmq.REQ
            assert say_hello_spy.call_count == 0

            scenario = GrizzlyContextScenario()
            scenario.name = 'test'
            scenario.user.class_name = 'ServiceBusUser'

            scenario.add_task(SleepTask(sleep=1.54))
            scenario.add_task(RequestTask(RequestMethod.SEND, name='test-send', endpoint='{{ endpoint }}'))
            scenario.add_task(RequestTask(RequestMethod.RECEIVE, name='test-receive', endpoint='queue:test-queue'))
            scenario.add_task(RequestTask(RequestMethod.SEND, name='test-send', endpoint='topic:test-topic'))

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            ServiceBusUser._scenario = scenario
            user = ServiceBusUser(environment=locust_environment)
            assert say_hello_spy.call_count == 3

            for index, (args, _) in enumerate(say_hello_spy.call_args_list):
                assert args[0] is scenario.tasks[index+1]
                assert args[1] == cast(RequestTask, scenario.tasks[index+1]).endpoint

        finally:
            ServiceBusUser._scenario = None
            ServiceBusUser._context = {
                'message': {
                    'wait': None,
                }
            }

    @pytest.mark.usefixtures('noop_zmq', 'locust_environment')
    def test_say_hello(self, noop_zmq: Callable[[str], None], locust_environment: Environment, mocker: MockerFixture, caplog: pytest.LogCaptureFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='

        user = ServiceBusUser(environment=locust_environment)
        assert user.hellos == set()

        async_action_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.async_action', autospec=True)

        user.hellos = set(['sender=queue:test-queue'])

        task = RequestTask(RequestMethod.SEND, name='test-send', endpoint='queue:{{ queue_name }}')

        with caplog.at_level(logging.WARNING):
            user.say_hello(task, task.endpoint)
        assert 'ServiceBusUser: cannot say hello for test-send when endpoint (queue:{{ queue_name }}) is a template' in caplog.text
        assert user.hellos == set(['sender=queue:test-queue'])
        assert async_action_spy.call_count == 0
        caplog.clear()

        user.say_hello(task, 'queue:test-queue')

        assert user.hellos == set(['sender=queue:test-queue'])
        assert async_action_spy.call_count == 0

        user.say_hello(task, 'topic:test-topic')

        assert user.hellos == set(['sender=queue:test-queue', 'sender=topic:test-topic'])
        assert async_action_spy.call_count == 1
        args, kwargs = async_action_spy.call_args_list[0]

        assert len(args) == 4
        assert len(kwargs) == 1
        assert kwargs.get('meta', False)
        assert args[1] is task
        assert args[2] == {
            'worker': None,
            'action': 'HELLO',
            'context': {
                'endpoint': 'topic:test-topic',
                'url': user.am_context['url'],
                'message_wait': None,
            }
        }
        assert args[3] == 'sender=topic:test-topic'

        task = RequestTask(RequestMethod.RECEIVE, name='test-recv', endpoint='topic:test-topic, subscription:test-subscription')

        user.say_hello(task, task.endpoint)

        assert user.hellos == set(['sender=queue:test-queue', 'sender=topic:test-topic', 'receiver=topic:test-topic, subscription:test-subscription'])
        assert async_action_spy.call_count == 2
        args, kwargs = async_action_spy.call_args_list[1]

        assert len(args) == 4
        assert len(kwargs) == 1
        assert kwargs.get('meta', False)
        assert args[1] is task
        assert args[2] == {
            'worker': None,
            'action': 'HELLO',
            'context': {
                'endpoint': 'topic:test-topic, subscription:test-subscription',
                'url': user.am_context['url'],
                'message_wait': None,
            }
        }
        assert args[3] == 'receiver=topic:test-topic'

    @pytest.mark.usefixtures('locust_environment', 'noop_zmq')
    def test_request(self, locust_environment: Environment, noop_zmq: Callable[[str], None], mocker: MockerFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        ServiceBusUser.host = 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='

        user = ServiceBusUser(environment=locust_environment)
        user.worker_id = 'asdf-asdf-asdf'

        send_json_spy = mocker.spy(user.zmq_client, 'send_json')
        say_hello_spy = mocker.patch.object(user, 'say_hello', side_effect=[None]*10)
        request_fire_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(user.response_event, 'fire')

        def mock_recv_json(response: AsyncMessageResponse) -> None:
            mocker.patch.object(user.zmq_client,
                'recv_json',
                side_effect=[zmq.Again(), response],
            )

        mock_recv_json({
            'worker': 'asdf-asdf-asdf',
            'success': False,
            'message': 'unknown error',
        })

        scenario = GrizzlyContextScenario()
        scenario.name = 'test'

        # unsupported request method
        task = RequestTask(RequestMethod.PUT, name='test-send', endpoint='queue:test-queue')
        task.source = 'hello'
        task.template = Template(task.source)
        scenario.add_task(task)
        scenario.stop_on_failure = True
        mocker.patch.object(user.zmq_client, 'disconnect', side_effect=[TypeError])

        with pytest.raises(StopUser):
            user.request(task)

        assert say_hello_spy.call_count == 1
        assert send_json_spy.call_count == 0
        assert request_fire_spy.call_count == 1
        assert response_event_fire_spy.call_count == 1

        _, kwargs = response_event_fire_spy.call_args_list[0]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', ({'meta': True}, 'hello',))
        assert metadata is None
        assert payload is None
        assert kwargs.get('user', None) is user
        assert isinstance(kwargs.get('exception', None), NotImplementedError)

        _, kwargs = request_fire_spy.call_args_list[0]
        assert kwargs.get('request_type', None) == 'sb:PUT'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'no implementation for PUT requests' in str(exception)

        task.method = RequestMethod.SEND

        # unsuccessful response from async-messaged
        scenario.stop_on_failure = False

        user.request(task)
        assert say_hello_spy.call_count == 2
        assert send_json_spy.call_count == 1
        assert request_fire_spy.call_count == 2
        assert response_event_fire_spy.call_count == 2

        _, kwargs = response_event_fire_spy.call_args_list[1]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert metadata is None
        assert payload is None
        assert kwargs.get('user', None) is user
        assert isinstance(kwargs.get('exception', None), AsyncMessageError)

        _, kwargs = request_fire_spy.call_args_list[1]
        assert kwargs.get('request_type', None) == 'sb:SEND'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert 'unknown error' in str(exception)
        args, _ = send_json_spy.call_args_list[0]
        assert args[0] == {
            'worker': 'asdf-asdf-asdf',
            'action': 'SEND',
            'payload': 'hello',
            'context': {
                'endpoint': 'queue:test-queue',
                'connection': 'sender',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'message_wait': None,
            }
        }

        # successful request
        task.method = RequestMethod.RECEIVE
        task.template = None
        task.source = None

        mock_recv_json({
            'worker': 'asdf-asdf-asdf',
            'success': True,
            'payload': 'hello',
            'metadata': {'meta': True},
            'response_length': 133,
        })

        user.request(task)
        assert say_hello_spy.call_count == 3
        assert send_json_spy.call_count == 2
        assert request_fire_spy.call_count == 3
        assert response_event_fire_spy.call_count == 3

        _, kwargs = response_event_fire_spy.call_args_list[2]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task
        metadata, payload = kwargs.get('context', (None, None,))
        assert metadata == {'meta': True}
        assert payload is 'hello'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', '') is None

        _, kwargs = request_fire_spy.call_args_list[2]
        assert kwargs.get('request_type', None) == 'sb:RECV'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 133
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', '') is None
        args, _ = send_json_spy.call_args_list[1]
        print(args[0])
        assert args[0] == {
            'worker': 'asdf-asdf-asdf',
            'action': 'RECEIVE',
            'payload': None,
            'context': {
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'message_wait': None,
            }
        }

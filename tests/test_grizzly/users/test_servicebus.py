import logging

from typing import cast

import pytest

from pytest_mock import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.exception import StopUser
from zmq.sugar.constants import REQ as ZMQ_REQ
from zmq.error import Again as ZMQAgain

from grizzly.users.base import GrizzlyUser, RequestLogger, ResponseHandler
from grizzly.users.servicebus import ServiceBusUser
from grizzly.types import RequestMethod
from grizzly.tasks import RequestTask, WaitTask
from grizzly.context import GrizzlyContextScenario
from grizzly_extras.async_message import AsyncMessageResponse, AsyncMessageError
from grizzly_extras.transformer import TransformerContentType

from ...fixtures import LocustFixture, NoopZmqFixture


class TestServiceBusUser:
    def test_create(self, locust_fixture: LocustFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        try:
            zmq_client_connect_spy = mocker.patch('grizzly.users.servicebus.zmq.Socket.connect', side_effect=[None] * 10)
            say_hello_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.say_hello', side_effect=[None] * 10)

            ServiceBusUser.host = 'Endpoint=mq://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_fixture.env)
            assert 'ServiceBusUser: "mq" is not a supported scheme' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_fixture.env)
            assert 'ServiceBusUser: SharedAccessKeyName and SharedAccessKey must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKey=secret='
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_fixture.env)
            assert 'ServiceBusUser: SharedAccessKeyName must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey'
            with pytest.raises(ValueError) as e:
                user = ServiceBusUser(environment=locust_fixture.env)
            assert 'ServiceBusUser: SharedAccessKey must be in the query string' in str(e)

            assert zmq_client_connect_spy.call_count == 0

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            user = ServiceBusUser(environment=locust_fixture.env)
            assert issubclass(user.__class__, GrizzlyUser)
            assert issubclass(user.__class__, ResponseHandler)
            assert issubclass(user.__class__, RequestLogger)

            assert zmq_client_connect_spy.call_count == 1
            args, _ = zmq_client_connect_spy.call_args_list[0]
            assert args[0] == ServiceBusUser.zmq_url
            assert user.zmq_client.type == ZMQ_REQ
            assert say_hello_spy.call_count == 0

            scenario = GrizzlyContextScenario(2)
            scenario.name = 'test'
            scenario.user.class_name = 'ServiceBusUser'

            scenario.tasks.add(WaitTask(time_expression='1.54'))
            scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='{{ endpoint }}'))
            scenario.tasks.add(RequestTask(RequestMethod.RECEIVE, name='test-receive', endpoint='queue:test-queue'))
            scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='topic:test-topic'))

            ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
            ServiceBusUser._scenario = scenario
            user = ServiceBusUser(environment=locust_fixture.env)
            assert say_hello_spy.call_count == 3

            for index, (args, _) in enumerate(say_hello_spy.call_args_list):
                assert args[0] is scenario.tasks[index + 1]
                assert args[1] == cast(RequestTask, scenario.tasks[index + 1]).endpoint

        finally:
            setattr(ServiceBusUser, '_scenario', None)
            ServiceBusUser._context = {
                'message': {
                    'wait': None,
                }
            }

    def test_say_hello(self, noop_zmq: NoopZmqFixture, locust_fixture: LocustFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        ServiceBusUser.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='

        user = ServiceBusUser(environment=locust_fixture.env)
        assert user.hellos == set()

        async_action_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.async_action', autospec=True)

        user.hellos = set(['sender=queue:test-queue'])

        task = RequestTask(RequestMethod.SEND, name='test-send', endpoint='queue:"{{ queue_name }}"')
        scenario = GrizzlyContextScenario(1)
        scenario.name = 'test'
        scenario.tasks.add(task)

        with caplog.at_level(logging.WARNING):
            user.say_hello(task, task.endpoint)
        assert 'ServiceBusUser: cannot say hello for test-send when endpoint is a template' in caplog.text
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
        assert len(kwargs) == 0
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
        scenario.tasks.add(task)

        user.say_hello(task, task.endpoint)

        assert user.hellos == set(['sender=queue:test-queue', 'sender=topic:test-topic', 'receiver=topic:test-topic, subscription:test-subscription'])
        assert async_action_spy.call_count == 2
        args, kwargs = async_action_spy.call_args_list[1]

        assert len(args) == 4
        assert len(kwargs) == 0
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
        assert args[3] == 'receiver=topic:test-topic, subscription:test-subscription'

        # error handling
        task.endpoint = 'test-topic'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'incorrect format in arguments: "test-topic"' in str(re)

        task.endpoint = 'subscription:test-subscription'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'endpoint needs to be prefixed with queue: or topic:' in str(re)

        task.endpoint = 'topic:test-topic, queue:test-queue'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'cannot specify both topic: and queue: in endpoint' in str(re)

        task.endpoint = 'queue:test-queue, subscription:test-subscription'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'argument subscription is only allowed if endpoint is a topic' in str(re)

        task.endpoint = 'topic:test-topic, subscription:test-subscription, argument:False'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'arguments argument is not supported' in str(re)

        task.endpoint = 'topic:test-topic'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'endpoint needs to include subscription when receiving messages from a topic' in str(re)

        task.method = RequestMethod.SEND
        task.endpoint = 'topic:test-topic2, expression:$.test.result'
        with pytest.raises(RuntimeError) as re:
            user.say_hello(task, task.endpoint)
        assert 'argument expression is only allowed when receiving messages' in str(re)

    def test_request(self, locust_fixture: LocustFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        ServiceBusUser.host = 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='

        user = ServiceBusUser(environment=locust_fixture.env)
        user.worker_id = 'asdf-asdf-asdf'

        send_json_spy = noop_zmq.get_mock('send_json')
        say_hello_spy = mocker.patch.object(user, 'say_hello', side_effect=[None] * 10)
        request_fire_spy = mocker.spy(user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(user.response_event, 'fire')

        def mock_recv_json(response: AsyncMessageResponse) -> None:
            mocker.patch.object(
                user.zmq_client,
                'recv_json',
                side_effect=[ZMQAgain(), response],
            )

        mock_recv_json({
            'worker': 'asdf-asdf-asdf',
            'success': False,
            'message': 'unknown error',
        })

        scenario = GrizzlyContextScenario(1)
        scenario.name = 'test'
        user._scenario = scenario

        # unsupported request method
        task = RequestTask(RequestMethod.PUT, name='test-send', endpoint='queue:test-queue')
        task.source = 'hello'
        scenario.tasks.add(task)
        scenario.failure_exception = StopUser
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
        assert kwargs.get('request_type', None) == 'PUT'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, NotImplementedError)
        assert 'no implementation for PUT requests' in str(exception)

        task.method = RequestMethod.SEND

        # unsuccessful response from async-messaged
        scenario.failure_exception = None

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
        assert kwargs.get('request_type', None) == 'SEND'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == user._context
        exception = kwargs.get('exception', None)
        assert 'unknown error' in str(exception)

        args, _ = send_json_spy.call_args_list[0]
        assert args[1] == {
            'worker': 'asdf-asdf-asdf',
            'action': 'SEND',
            'payload': 'hello',
            'context': {
                'endpoint': 'queue:test-queue',
                'connection': 'sender',
                'content_type': 'undefined',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'message_wait': None,
            }
        }

        # successful request
        task.method = RequestMethod.RECEIVE
        task.source = None

        mock_recv_json({
            'worker': 'asdf-asdf-asdf',
            'success': True,
            'payload': 'hello',
            'metadata': {'meta': True},
            'response_length': 133,
        })

        metadata, payload = user.request(task)
        assert say_hello_spy.call_count == 3
        assert send_json_spy.call_count == 2
        assert request_fire_spy.call_count == 3
        assert response_event_fire_spy.call_count == 3

        assert metadata == {'meta': True}
        assert payload == 'hello'

        _, kwargs = response_event_fire_spy.call_args_list[2]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task

        metadata, payload = kwargs.get('context', (None, None,))
        assert metadata == {'meta': True}
        assert payload == 'hello'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', '') is None

        _, kwargs = request_fire_spy.call_args_list[2]
        assert kwargs.get('request_type', None) == 'RECV'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 133
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', '') is None

        args, _ = send_json_spy.call_args_list[1]
        assert args[1] == {
            'worker': 'asdf-asdf-asdf',
            'action': 'RECEIVE',
            'payload': None,
            'context': {
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
                'content_type': 'undefined',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'message_wait': None,
            }
        }

        task.method = RequestMethod.RECEIVE
        task.source = None
        task.response.content_type = TransformerContentType.JSON
        task.endpoint = f'{task.endpoint}, expression:"$.document[?(@.name=="TPM Report")]'

        mock_recv_json({
            'worker': 'asdf-asdf-asdf',
            'success': True,
            'payload': 'hello',
            'metadata': {'meta': True},
            'response_length': 133,
        })

        metadata, payload = user.request(task)
        assert say_hello_spy.call_count == 4
        assert send_json_spy.call_count == 3
        assert request_fire_spy.call_count == 4
        assert response_event_fire_spy.call_count == 4

        assert metadata == {'meta': True}
        assert payload == 'hello'

        _, kwargs = response_event_fire_spy.call_args_list[3]
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('request', None) is task

        metadata, payload = kwargs.get('context', (None, None,))
        assert metadata == {'meta': True}
        assert payload == 'hello'
        assert kwargs.get('user', None) is user
        assert kwargs.get('exception', '') is None

        _, kwargs = request_fire_spy.call_args_list[3]
        assert kwargs.get('request_type', None) == 'RECV'
        assert kwargs.get('name', None) == f'{scenario.identifier} {task.name}'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 133
        assert kwargs.get('context', None) == user._context
        assert kwargs.get('exception', '') is None

        args, _ = send_json_spy.call_args_list[2]
        assert args[1] == {
            'worker': 'asdf-asdf-asdf',
            'action': 'RECEIVE',
            'payload': None,
            'context': {
                'endpoint': 'queue:test-queue, expression:"$.document[?(@.name=="TPM Report")]',
                'connection': 'receiver',
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'message_wait': None,
                'content_type': 'json',
            }
        }

"""Tests of grizzly.users.servicebus."""

from __future__ import annotations

import logging
from contextlib import suppress
from typing import TYPE_CHECKING, cast

import pytest
import zmq.green as zmq
from async_messaged import AsyncMessageError, AsyncMessageResponse
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import ExplicitWaitTask, RequestTask
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users import GrizzlyUser, ServiceBusUser
from grizzly_common.transformer import TransformerContentType
from zmq.error import Again as ZMQAgain

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture, NoopZmqFixture


class TestServiceBusUser:
    def test_on_start(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.servicebus')
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('ServiceBusTestUser', (ServiceBusUser,), {'__scenario__': behave_fixture.grizzly.scenario, 'host': None})

        assert issubclass(test_cls, ServiceBusUser)

        zmq_client_connect_spy = mocker.patch('grizzly.users.servicebus.zmq.Socket.connect', return_value=None)
        say_hello_spy = mocker.patch.object(test_cls, 'say_hello', return_value=None)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = test_cls(environment=behave_fixture.locust.environment)
        assert issubclass(user.__class__, GrizzlyUser | ServiceBusUser)

        user.on_start()

        assert zmq_client_connect_spy.call_count == 1
        args, _ = zmq_client_connect_spy.call_args_list[0]
        assert args[0] == ServiceBusUser.zmq_url
        assert user.zmq_client.type == zmq.REQ
        assert say_hello_spy.call_count == 0

        scenario = GrizzlyContextScenario(2, behave=behave_fixture.create_scenario('test'), grizzly=behave_fixture.grizzly)
        scenario.user.class_name = 'ServiceBusUser'

        scenario.tasks.add(ExplicitWaitTask(time_expression='1.54'))
        scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='{{ endpoint }}'))
        scenario.tasks.add(RequestTask(RequestMethod.RECEIVE, name='test-receive', endpoint='queue:test-queue'))
        scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='topic:test-topic'))

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        test_cls.__scenario__ = scenario
        user = test_cls(environment=behave_fixture.locust.environment)
        user.on_start()
        assert say_hello_spy.call_count == 3

        for index, (args, _) in enumerate(say_hello_spy.call_args_list):
            assert args == (scenario.tasks()[index + 1],)

    def test_on_stop(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('ServiceBusTestUser', (ServiceBusUser,), {'__scenario__': behave_fixture.grizzly.scenario, 'host': None})

        assert issubclass(test_cls, ServiceBusUser)

        zmq_client_disconnect_spy = mocker.patch('grizzly.users.servicebus.zmq_disconnect', return_value=None)
        disconnect_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.disconnect', return_value=None)
        mocker.patch('grizzly.users.servicebus.ServiceBusUser.say_hello', return_value=None)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = test_cls(environment=behave_fixture.locust.environment)
        assert issubclass(user.__class__, GrizzlyUser)

        user.on_start()

        user.on_stop()

        zmq_client_disconnect_spy.assert_called_once_with(user.zmq_client, destroy_context=False)
        assert user.zmq_client.type == zmq.REQ
        assert disconnect_spy.call_count == 0

        scenario = GrizzlyContextScenario(2, behave=behave_fixture.create_scenario('test'), grizzly=behave_fixture.grizzly)
        scenario.user.class_name = 'ServiceBusUser'

        scenario.tasks.add(ExplicitWaitTask(time_expression='1.54'))
        scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='{{ endpoint }}'))
        scenario.tasks.add(RequestTask(RequestMethod.RECEIVE, name='test-receive', endpoint='queue:test-queue'))
        scenario.tasks.add(RequestTask(RequestMethod.SEND, name='test-send', endpoint='topic:test-topic'))

        test_cls.__scenario__ = scenario
        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = test_cls(environment=behave_fixture.locust.environment)
        user.on_start()
        user.on_stop()
        assert disconnect_spy.call_count == 3

        for index, (args, _) in enumerate(disconnect_spy.call_args_list):
            assert len(args) == 1
            assert args[0] is scenario.tasks()[index + 1]

    def test___init__(self, behave_fixture: BehaveFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.servicebus')

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('ServiceBusTestUser', (ServiceBusUser,), {'__scenario__': behave_fixture.grizzly.scenario, 'host': None})

        assert issubclass(test_cls, ServiceBusUser)

        # <!-- connection string
        test_cls.host = 'Endpoint=mq://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
        with pytest.raises(ValueError, match='ServiceBusTestUser: "mq" is not a supported scheme'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls.host = 'Endpoint=sb://sb.example.org'
        with pytest.raises(ValueError, match='ServiceBusTestUser: SharedAccessKeyName and SharedAccessKey must be in the query string'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKey=secret='
        with pytest.raises(ValueError, match='ServiceBusTestUser: SharedAccessKeyName must be in the query string'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey'
        with pytest.raises(ValueError, match='ServiceBusTestUser: SharedAccessKey must be in the query string'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret='
        user = test_cls(environment=behave_fixture.locust.environment)

        assert user.hellos == set()
        assert user.worker_id is None
        assert user.am_context == {
            'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=secret=',
            'message_wait': None,
            'tenant': None,
            'username': None,
            'password': None,
        }
        # // -->

        # <!-- credential
        test_cls.host = 'mq://my-sbns'
        with pytest.raises(ValueError, match='ServiceBusTestUser: "mq" is not a supported scheme'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls = type(
            'ServiceBusTestUser',
            (ServiceBusUser,),
            {
                '__scenario__': behave_fixture.grizzly.scenario,
                'host': 'sb://my-sbns',
                '__context__': {
                    'auth': {
                        'tenant': None,
                        'user': {
                            'username': 'bob@example.com',
                            'password': 'secret',
                        },
                    },
                },
            },
        )

        assert issubclass(test_cls, ServiceBusUser)

        with pytest.raises(ValueError, match='ServiceBusTestUser: does not have context variable auth.tenant set while auth.user is'):
            test_cls(environment=behave_fixture.locust.environment)

        test_cls.__context__['auth']['tenant'] = 'example.com'
        user = test_cls(environment=behave_fixture.locust.environment)

        assert user.hellos == set()
        assert user.worker_id is None
        assert user.am_context == {
            'url': 'sb://my-sbns',
            'message_wait': None,
            'tenant': 'example.com',
            'username': 'bob@example.com',
            'password': 'secret',
        }
        # // -->

    def test_disconnect(self, behave_fixture: BehaveFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.users.servicebus')
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('ServiceBusTestUser', (ServiceBusUser,), {'__scenario__': behave_fixture.grizzly.scenario, 'host': None})

        assert issubclass(test_cls, ServiceBusUser)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        user = test_cls(environment=behave_fixture.locust.environment)
        request_context_spy = mocker.patch.object(user, 'request_context')

        task = RequestTask(RequestMethod.SEND, name='test-send', endpoint='queue:test-queue')
        scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('test'), grizzly=behave_fixture.grizzly)
        scenario.tasks.add(task)
        user._scenario = scenario

        user.disconnect(task)

        request_context_spy.assert_not_called()

        user.hellos = {'sender=queue:test-queue'}
        user.disconnect(task)

        request_context_spy.assert_called_once_with(
            task,
            {
                'action': 'DISCONNECT',
                'context': {
                    'endpoint': 'queue:test-queue',
                    'url': user.am_context['url'],
                    'message_wait': None,
                    'username': None,
                    'password': None,
                    'tenant': None,
                },
            },
        )

        request_context_spy.return_value.__enter__.assert_called_once_with()
        assert user.hellos == set()

    def test_say_hello(self, noop_zmq: NoopZmqFixture, behave_fixture: BehaveFixture, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.users.servicebus')
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('ServiceBusTestUser', (ServiceBusUser,), {'__scenario__': behave_fixture.grizzly.scenario, 'host': None})

        assert issubclass(test_cls, ServiceBusUser)

        test_cls.host = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='

        user = test_cls(environment=behave_fixture.locust.environment)
        user.on_start()
        assert user.hellos == set()

        request_context_spy = mocker.patch('grizzly.users.servicebus.ServiceBusUser.request_context', autospec=True)

        user.hellos = {'sender=queue:test-queue'}

        task = RequestTask(RequestMethod.SEND, name='test-send', endpoint='queue:"{{ queue_name }}"')
        scenario = GrizzlyContextScenario(1, behave=behave_fixture.create_scenario('test'), grizzly=behave_fixture.grizzly)
        scenario.tasks.add(task)
        user._scenario = scenario

        with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
            user.say_hello(task)

        assert 'cannot say hello for test-send when endpoint is a template' in caplog.text
        assert user.hellos == {'sender=queue:test-queue'}
        request_context_spy.assert_not_called()
        caplog.clear()

        task.endpoint = 'queue:test-queue'
        user.say_hello(task)

        assert user.hellos == {'sender=queue:test-queue'}
        request_context_spy.assert_not_called()

        task.endpoint = 'topic:test-topic'
        user.say_hello(task)

        assert user.hellos == {'sender=queue:test-queue', 'sender=topic:test-topic'}

        request_context_spy.assert_called_once_with(
            user,
            task,
            {
                'action': 'HELLO',
                'context': {
                    'endpoint': 'topic:test-topic',
                    'url': user.am_context['url'],
                    'message_wait': None,
                    'username': None,
                    'password': None,
                    'tenant': None,
                },
            },
        )
        request_context_spy.return_value.__enter__.assert_called_once_with()
        request_context_spy.reset_mock()

        task = RequestTask(RequestMethod.RECEIVE, name='test-recv', endpoint='topic:test-topic, subscription:test-subscription')
        scenario.tasks.add(task)

        user.say_hello(task)

        assert user.hellos == {'sender=queue:test-queue', 'sender=topic:test-topic', 'receiver=topic:test-topic, subscription:test-subscription'}
        request_context_spy.assert_called_once_with(
            user,
            task,
            {
                'action': 'HELLO',
                'context': {
                    'endpoint': 'topic:test-topic, subscription:test-subscription',
                    'url': user.am_context['url'],
                    'message_wait': None,
                    'username': None,
                    'password': None,
                    'tenant': None,
                },
            },
        )

        # error handling
        task.endpoint = 'test-topic'
        with pytest.raises(RuntimeError, match='incorrect format in arguments: "test-topic"'):
            user.say_hello(task)

        task.endpoint = 'subscription:test-subscription'
        with pytest.raises(RuntimeError, match='endpoint needs to be prefixed with queue: or topic:'):
            user.say_hello(task)

        task.endpoint = 'topic:test-topic, queue:test-queue'
        with pytest.raises(RuntimeError, match='cannot specify both topic: and queue: in endpoint'):
            user.say_hello(task)

        task.endpoint = 'queue:test-queue, subscription:test-subscription'
        with pytest.raises(RuntimeError, match='argument subscription is only allowed if endpoint is a topic'):
            user.say_hello(task)

        task.endpoint = 'topic:test-topic, subscription:test-subscription, argument:False'
        with pytest.raises(RuntimeError, match='arguments argument is not supported'):
            user.say_hello(task)

        task.endpoint = 'topic:test-topic'
        with pytest.raises(RuntimeError, match='endpoint needs to include subscription when receiving messages from a topic'):
            user.say_hello(task)

        task.method = RequestMethod.SEND
        task.endpoint = 'topic:test-topic2, expression:$.test.result'
        with pytest.raises(RuntimeError, match='argument expression is only allowed when receiving messages'):
            user.say_hello(task)

    def test_request(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.users.servicebus')
        mocker.patch('async_messaged.utils.uuid4', return_value='foobar')

        grizzly_fixture.grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test scenario'))
        grizzly = grizzly_fixture.grizzly

        parent = grizzly_fixture()

        cls_service_bus_user = type(
            'ServiceBusUser_002',
            (ServiceBusUser,),
            {
                '__scenario__': grizzly.scenario,
                'host': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
            },
        )

        parent._user = cls_service_bus_user(grizzly.state.locust.environment)

        assert isinstance(parent.user, ServiceBusUser)

        grizzly.scenario.tasks.clear()

        parent.user.on_start()
        parent.user.worker_id = 'asdf-asdf-asdf'

        send_json_spy = noop_zmq.get_mock('send_json')
        say_hello_spy = mocker.patch.object(parent.user, 'say_hello', side_effect=[None] * 10)
        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')
        response_event_fire_spy = mocker.spy(parent.user.events.request, 'fire')

        def mock_recv_json(response: AsyncMessageResponse) -> None:
            mocker.patch.object(
                cast('ServiceBusUser', parent.user).zmq_client,
                'recv_json',
                side_effect=[ZMQAgain(), response],
            )

        mock_recv_json(
            {
                'worker': 'asdf-asdf-asdf',
                'success': False,
                'message': 'unknown error',
            },
        )

        parent.user._scenario.tasks.clear()

        # unsupported request method
        task = RequestTask(RequestMethod.PUT, name='test-send', endpoint='queue:test-queue')
        task.source = 'hello'
        parent.user._scenario.tasks.add(task)
        parent.user._scenario.failure_handling.update({None: StopUser})
        mocker.patch.object(parent.user.zmq_client, 'disconnect', side_effect=[TypeError])

        with pytest.raises(StopUser):
            parent.user.request(task)

        say_hello_spy.assert_called_once()
        say_hello_spy.reset_mock()
        send_json_spy.assert_not_called()
        response_event_fire_spy.assert_called_once_with(
            name=f'{parent.user._scenario.identifier} {task.name}',
            request=ANY(RequestTask),
            context=(None, None),
            user=parent.user,
            exception=ANY(NotImplementedError, message='ServiceBusUser_002: no implementation for PUT requests'),
        )
        response_event_fire_spy.reset_mock()
        request_fire_spy.assert_called_once_with(
            request_type='PUT',
            name=f'{parent.user._scenario.identifier} {task.name}',
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(parent.user),
                **parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(NotImplementedError, message='ServiceBusUser_002: no implementation for PUT requests'),
        )
        request_fire_spy.reset_mock()

        task.method = RequestMethod.SEND

        # unsuccessful response from async-messaged
        with suppress(KeyError):
            del parent.user._scenario.failure_handling[None]

        parent.user.request(task)

        say_hello_spy.assert_called_once()
        say_hello_spy.reset_mock()
        send_json_spy.assert_called_once_with(
            {
                'worker': 'asdf-asdf-asdf',
                'client': id(parent.user),
                'action': 'SEND',
                'payload': 'hello',
                'context': {
                    'endpoint': 'queue:test-queue',
                    'connection': 'sender',
                    'content_type': 'undefined',
                    'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                    'message_wait': None,
                    'consume': False,
                    'username': None,
                    'password': None,
                    'tenant': None,
                    'verbose': False,
                    'forward': False,
                },
                'request_id': 'foobar',
            },
        )

        send_json_spy.reset_mock()
        request_fire_spy.assert_called_once_with(
            request_type='SEND',
            name=f'{parent.user._scenario.identifier} {task.name}',
            response_time=ANY(int),
            response_length=0,
            context={
                'user': id(parent.user),
                **parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=ANY(AsyncMessageError, message='unknown error'),
        )
        request_fire_spy.reset_mock()

        response_event_fire_spy.assert_not_called()
        response_event_fire_spy.reset_mock()

        # successful request
        task = RequestTask(RequestMethod.RECEIVE, name='test-send', endpoint='queue:test-queue | verbose=True, forward=True', source=None)

        mock_recv_json(
            {
                'worker': 'asdf-asdf-asdf',
                'success': True,
                'payload': 'hello',
                'metadata': {'meta': True},
                'response_length': 133,
            },
        )

        metadata, payload = parent.user.request(task)
        assert metadata == {'meta': True}
        assert payload == 'hello'

        say_hello_spy.assert_called_once()
        say_hello_spy.reset_mock()

        response_event_fire_spy.assert_called_once_with(
            name=f'{parent.user._scenario.identifier} {task.name}',
            request=ANY(RequestTask),
            context=({'meta': True}, 'hello'),
            user=parent.user,
            exception=None,
        )
        response_event_fire_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='RECV',
            name=f'{parent.user._scenario.identifier} {task.name}',
            response_time=ANY(int),
            response_length=5,
            context={
                'user': id(parent.user),
                **parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=None,
        )
        request_fire_spy.reset_mock()

        send_json_spy.assert_called_once_with(
            {
                'worker': 'asdf-asdf-asdf',
                'client': id(parent.user),
                'action': 'RECEIVE',
                'payload': None,
                'context': {
                    'endpoint': 'queue:test-queue',
                    'connection': 'receiver',
                    'content_type': 'undefined',
                    'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                    'message_wait': None,
                    'consume': False,
                    'username': None,
                    'password': None,
                    'tenant': None,
                    'verbose': True,
                    'forward': True,
                },
                'request_id': 'foobar',
            },
        )
        send_json_spy.reset_mock()

        consume_task = RequestTask(RequestMethod.RECEIVE, name=task.name, endpoint=f'{task.endpoint}, expression:"$.document[?(@.name=="TPM Report")] | consume=True', source=None)
        consume_task.response.content_type = TransformerContentType.JSON

        mock_recv_json(
            {
                'worker': 'asdf-asdf-asdf',
                'success': True,
                'payload': 'hello',
                'metadata': {'meta': True},
                'response_length': 133,
            },
        )

        metadata, payload = parent.user.request(consume_task)
        assert metadata == {'meta': True}
        assert payload == 'hello'

        say_hello_spy.assert_called_once()
        say_hello_spy.reset_mock()

        response_event_fire_spy.assert_called_once_with(
            name=f'{parent.user._scenario.identifier} {consume_task.name}',
            request=ANY(RequestTask),
            context=({'meta': True}, 'hello'),
            user=parent.user,
            exception=None,
        )
        response_event_fire_spy.reset_mock()

        request_fire_spy.assert_called_once_with(
            request_type='RECV',
            name=f'{parent.user._scenario.identifier} {consume_task.name}',
            response_time=ANY(int),
            response_length=5,
            context={
                'user': id(parent.user),
                **parent.user._context,
                '__time__': ANY(str),
                '__fields_request_started__': ANY(str),
                '__fields_request_finished__': ANY(str),
            },
            exception=None,
        )
        request_fire_spy.reset_mock()

        send_json_spy.assert_called_once_with(
            {
                'worker': 'asdf-asdf-asdf',
                'client': id(parent.user),
                'action': 'RECEIVE',
                'payload': None,
                'context': {
                    'endpoint': 'queue:test-queue, expression:"$.document[?(@.name=="TPM Report")]',
                    'connection': 'receiver',
                    'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                    'message_wait': None,
                    'content_type': 'json',
                    'consume': True,
                    'username': None,
                    'password': None,
                    'tenant': None,
                    'verbose': False,
                    'forward': False,
                },
                'request_id': 'foobar',
            },
        )
        send_json_spy.reset_mock()

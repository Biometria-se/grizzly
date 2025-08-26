"""Unit tests for async_messaged.sb."""

from __future__ import annotations

import logging
from contextlib import suppress
from itertools import cycle
from json import dumps as jsondumps
from typing import TYPE_CHECKING, cast

import pytest
from async_messaged import AsyncMessageContext, AsyncMessageError, AsyncMessageRequest
from async_messaged.sb import AsyncServiceBusHandler
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver, ServiceBusSender, TransportType
from azure.servicebus.exceptions import ServiceBusError
from grizzly_common.arguments import parse_arguments
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential

from test_async_messaged.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock import MockerFixture


class TestAsyncServiceBusHandler:
    def test___init__(self) -> None:
        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        assert handler.worker == 'asdf-asdf-asdf'
        assert handler.message_wait is None
        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

    def test__prepare_clients_conn_str(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        service_bus_client_mock = mocker.patch('async_messaged.sb.ServiceBusClient')
        service_bus_mgmt_client_mock = mocker.patch('async_messaged.sb.ServiceBusAdministrationClient')

        # <!-- connection string
        context: AsyncMessageContext = {
            'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
            'username': None,
            'password': None,
            'tenant': None,
        }

        # only_mgmt = False, logging level = DEBUG
        with caplog.at_level(logging.DEBUG):
            handler._prepare_clients(context, only_mgmt=False)

        service_bus_client_mock.assert_not_called()
        service_bus_client_mock.from_connection_string.assert_called_once_with(
            conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
            transport_type=TransportType.AmqpOverWebsocket,
        )
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.assert_not_called()
        service_bus_mgmt_client_mock.from_connection_string.assert_called_once_with(
            conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
        )
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None

        # only_mgmt = False, logging level = INFO
        with caplog.at_level(logging.INFO):
            handler._prepare_clients(context, only_mgmt=False)

        service_bus_client_mock.assert_not_called()
        service_bus_client_mock.from_connection_string.assert_called_once_with(
            conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
            transport_type=TransportType.AmqpOverWebsocket,
        )
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.assert_not_called()
        service_bus_mgmt_client_mock.from_connection_string.assert_not_called()
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None

        # only_mgmt = True, logging level does not matter
        with caplog.at_level(logging.INFO):
            handler._prepare_clients(context, only_mgmt=True)

        service_bus_client_mock.assert_not_called()
        service_bus_client_mock.from_connection_string.assert_not_called()
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.assert_not_called()
        service_bus_mgmt_client_mock.from_connection_string.assert_called_once_with(
            conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
        )
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None
        # // -->

    def test__prepare_clients_credential(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        service_bus_client_mock = mocker.patch('async_messaged.sb.ServiceBusClient')
        service_bus_mgmt_client_mock = mocker.patch('async_messaged.sb.ServiceBusAdministrationClient')

        # <!-- credential
        context: AsyncMessageContext = {
            'url': 'sb://my-sbns',
            'username': 'bob@example.com',
            'password': 'secret',
            'tenant': None,
        }

        with pytest.raises(AsyncMessageError, match='no tenant in context'):
            handler._prepare_clients(context)

        context.update({'tenant': 'example.com'})

        # only_mgmt = False, logging level = DEBUG
        with caplog.at_level(logging.DEBUG):
            handler._prepare_clients(context, only_mgmt=False)

        service_bus_client_mock.assert_called_once_with(
            'my-sbns.servicebus.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='bob@example.com',
                password='secret',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='sb://my-sbns',
            ),
            transport_type=TransportType.AmqpOverWebsocket,
        )
        service_bus_client_mock.from_connection_string.assert_not_called()
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.from_connection_string.assert_not_called()
        service_bus_mgmt_client_mock.assert_called_once_with(
            'my-sbns.servicebus.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='bob@example.com',
                password='secret',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='sb://my-sbns',
            ),
        )
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None

        # only_mgmt = False, logging level = INFO
        with caplog.at_level(logging.INFO):
            handler._prepare_clients(context, only_mgmt=False)

        service_bus_client_mock.from_connection_string.assert_not_called()
        service_bus_client_mock.assert_called_once_with(
            'my-sbns.servicebus.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='bob@example.com',
                password='secret',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='sb://my-sbns',
            ),
            transport_type=TransportType.AmqpOverWebsocket,
        )
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.assert_not_called()
        service_bus_mgmt_client_mock.from_connection_string.assert_not_called()
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None

        # only_mgmt = True, logging level does not matter
        with caplog.at_level(logging.INFO):
            handler._prepare_clients(context, only_mgmt=True)

        service_bus_client_mock.assert_not_called()
        service_bus_client_mock.from_connection_string.assert_not_called()
        service_bus_client_mock.reset_mock()

        service_bus_mgmt_client_mock.from_connection_string.assert_not_called()
        service_bus_mgmt_client_mock.assert_called_once_with(
            'my-sbns.servicebus.windows.net',
            credential=SOME(
                AzureAadCredential,
                username='bob@example.com',
                password='secret',
                tenant='example.com',
                auth_method=AuthMethod.USER,
                host='sb://my-sbns',
            ),
        )
        service_bus_mgmt_client_mock.reset_mock()

        handler._client = None
        handler.mgmt_client = None
        # // -->

    def test_close(self, mocker: MockerFixture) -> None:
        handler = AsyncServiceBusHandler('asdf-asdf-asdf')

        receiver = mocker.MagicMock()
        receiver.return_value.close = mocker.MagicMock()
        sender = mocker.MagicMock()
        sender.return_value.close = mocker.MagicMock()
        client = mocker.MagicMock()
        mgmt_client = mocker.MagicMock()

        handler._sender_cache.update({'foo-sender': sender})
        handler._receiver_cache.update({'bar-receiver': receiver})
        handler.client = client
        handler.mgmt_client = mgmt_client

        handler.close()

        client.close.assert_called_once_with()
        mgmt_client.close.assert_called_once_with()
        sender.close.assert_called_once_with()
        receiver.close.assert_called_once_with()

    def test_disconnect(self, mocker: MockerFixture) -> None:
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        handler._client = mocker.MagicMock()
        request: AsyncMessageRequest = {
            'action': 'DISCONNECT',
        }

        with pytest.raises(AsyncMessageError, match='no context in request'):
            handlers[request['action']](handler, request)

        request = {
            'action': 'DISCONNECT',
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'asdf',
            },
        }

        with pytest.raises(AsyncMessageError, match='"asdf" is not a valid value for context.connection'):
            handlers[request['action']](handler, request)

        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

        # successful disconnect, sender
        sender_instance = mocker.MagicMock()

        handler._sender_cache.update({'queue:test-queue': sender_instance})

        request['context']['connection'] = 'sender'

        assert handlers[request['action']](handler, request) == {
            'message': 'thanks for all the fish',
        }

        assert handler._sender_cache == {}
        sender_instance.__exit__.assert_called_once_with()

        # successful disconnect, receiver
        receiver_instance = mocker.MagicMock()
        handler._receiver_cache.update({'queue:test-queue': receiver_instance})

        request['context']['connection'] = 'receiver'

        assert handlers[request['action']](handler, request) == {
            'message': 'thanks for all the fish',
        }

        assert handler._receiver_cache == {}
        receiver_instance.__exit__.assert_called_once_with()

        # error disconnecting
        receiver_instance = mocker.MagicMock()
        receiver_instance.__exit__.side_effect = [RuntimeError]
        handler._receiver_cache.update({'queue:test-queue': receiver_instance})

        request['context']['connection'] = 'receiver'

        assert handlers[request['action']](handler, request) == {
            'message': 'thanks for all the fish',
        }

        assert handler._receiver_cache == {}
        receiver_instance.__exit__.assert_called_once_with()

    def test_subscribe(self, mocker: MockerFixture) -> None:  # noqa: PLR0915
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        # malformed request, no context
        request: AsyncMessageRequest = {
            'action': 'SUBSCRIBE',
        }

        with pytest.raises(AsyncMessageError, match='no context in request'):
            handlers[request['action']](handler, request)

        # malformed request, subscribe on queue
        request = {
            'action': 'SUBSCRIBE',
            'context': {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
            },
        }

        with pytest.raises(AsyncMessageError, match='subscriptions is only allowed on topics'):
            handlers[request['action']](handler, request)

        # malformed request, no subscription specified
        request['context'].update({'endpoint': 'topic:my-topic'})

        with pytest.raises(ValueError, match='endpoint needs to include subscription when receiving messages from a topic'):
            handlers[request['action']](handler, request)

        # malformed request, no rule text in payload
        request['context'].update({'endpoint': 'topic:my-topic, subscription:my-subscription'})

        with pytest.raises(AsyncMessageError, match='no rule text in request'):
            handlers[request['action']](handler, request)

        # pre: valid request
        mgmt_client_mock = mocker.MagicMock()
        create_client_mock = mocker.patch('async_messaged.sb.ServiceBusAdministrationClient.from_connection_string', return_value=mgmt_client_mock)

        request['payload'] = '1=1'

        # specified topic does not exist
        mgmt_client_mock.get_topic.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError, match='topic "my-topic" does not exist'):
            handlers[request['action']](handler, request)

        create_client_mock.assert_called_once_with(conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=')
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')

        mgmt_client_mock.get_topic.side_effect = None
        mgmt_client_mock.get_topic.reset_mock()

        # subscription already exist, default rule does not exist, rule exists
        rule_mock = mocker.MagicMock()
        mgmt_client_mock.delete_rule.side_effect = [ResourceNotFoundError]
        mgmt_client_mock.create_rule.side_effect = [ResourceExistsError]
        mgmt_client_mock.get_rule.return_value = rule_mock

        assert handlers[request['action']](handler, request) == {'message': 'created subscription "my-subscription" on topic "my-topic"'}

        mgmt_client_mock.create_subscription.assert_not_called()
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='$Default')
        mgmt_client_mock.create_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='grizzly')
        mgmt_client_mock.get_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='grizzly')
        assert rule_mock.filter.sql_expression == '1=1'
        mgmt_client_mock.update_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule=rule_mock)

        mgmt_client_mock.reset_mock()
        rule_mock.reset_mock()

        # subscription does not exist, default rule exists, rule does not exist
        request['payload'] = 'foo=bar AND foo=baz'
        mgmt_client_mock.get_rule.return_value = None
        mgmt_client_mock.create_rule.return_value = rule_mock
        mgmt_client_mock.delete_rule.side_effect = None
        mgmt_client_mock.create_rule.side_effect = None
        mgmt_client_mock.get_subscription.side_effect = [ResourceNotFoundError]

        assert handlers[request['action']](handler, request) == {'message': 'created subscription "my-subscription" on topic "my-topic"'}

        mgmt_client_mock.delete_queue.assert_not_called()
        mgmt_client_mock.create_queue.assert_not_called()
        mgmt_client_mock.create_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='$Default')
        mgmt_client_mock.create_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='grizzly')
        mgmt_client_mock.get_rule.assert_not_called()
        assert rule_mock.filter.sql_expression == 'foo=bar AND foo=baz'
        mgmt_client_mock.update_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule=rule_mock)
        mgmt_client_mock.reset_mock()

        # non-unique subscription, already exist
        request['context']['unique'] = False
        mgmt_client_mock.get_subscription.side_effect = None

        assert handlers[request['action']](handler, request) == {'message': 'non-unique subscription "my-subscription" on topic "my-topic" already created'}

        mgmt_client_mock.create_subscription.assert_not_called()
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.reset_mock()

        del request['context']['unique']

        # forwarded subscription, failed to create forward queue
        request['context']['forward'] = True
        mgmt_client_mock.get_rule.return_value = None
        mgmt_client_mock.create_rule.return_value = rule_mock
        mgmt_client_mock.delete_rule.side_effect = None
        mgmt_client_mock.create_rule.side_effect = None
        mgmt_client_mock.get_subscription.side_effect = [ResourceNotFoundError]
        mgmt_client_mock.create_queue.side_effect = [ResourceExistsError]

        with pytest.raises(AsyncMessageError, match='failed to create forward queue for subscription "my-subscription"'):
            handlers[request['action']](handler, request)

        mgmt_client_mock.delete_queue.assert_called_once_with(queue_name='my-subscription')
        mgmt_client_mock.create_queue.assert_called_once_with(queue_name='my-subscription')
        mgmt_client_mock.reset_mock()

        # forwarded subscription
        mgmt_client_mock.create_queue.side_effect = None

        assert handlers[request['action']](handler, request) == {'message': 'created forward queue and subscription "my-subscription" on topic "my-topic"'}

        mgmt_client_mock.delete_queue.assert_called_once_with(queue_name='my-subscription')
        mgmt_client_mock.create_queue.assert_called_once_with(queue_name='my-subscription')
        mgmt_client_mock.create_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', forward_to='my-subscription')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='$Default')
        mgmt_client_mock.create_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='grizzly')
        mgmt_client_mock.get_rule.assert_not_called()
        assert rule_mock.filter.sql_expression == 'foo=bar AND foo=baz'
        mgmt_client_mock.update_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule=rule_mock)
        mgmt_client_mock.reset_mock()

    def test_unsubscribe(self, mocker: MockerFixture) -> None:  # noqa: PLR0915
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        # malformed request, no context
        request: AsyncMessageRequest = {
            'action': 'UNSUBSCRIBE',
        }

        with pytest.raises(AsyncMessageError, match='no context in request'):
            handlers[request['action']](handler, request)

        # malformed request, subscribe on queue
        request = {
            'action': 'UNSUBSCRIBE',
            'context': {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
            },
        }

        with pytest.raises(AsyncMessageError, match='subscriptions is only allowed on topics'):
            handlers[request['action']](handler, request)

        # malformed request, no subscription specified
        request['context'].update({'endpoint': 'topic:my-topic'})

        with pytest.raises(ValueError, match='endpoint needs to include subscription when receiving messages from a topic'):
            handlers[request['action']](handler, request)

        # pre: valid request
        mgmt_client_mock = mocker.MagicMock()
        create_client_mock = mocker.patch('async_messaged.sb.ServiceBusAdministrationClient.from_connection_string', return_value=mgmt_client_mock)
        request['context'].update({'endpoint': 'topic:my-topic, subscription:my-subscription'})

        # topic does not exist
        mgmt_client_mock.get_topic.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError, match='topic "my-topic" does not exist'):
            handlers[request['action']](handler, request)

        create_client_mock.assert_called_once_with(conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=')
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.delete_subscription.assert_not_called()

        mgmt_client_mock.reset_mock()

        # subscription does not exist
        mgmt_client_mock.get_topic.side_effect = None
        mgmt_client_mock.get_subscription.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError, match='subscription "my-subscription" does not exist on topic "my-topic"'):
            handlers[request['action']](handler, request)

        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_subscription.assert_not_called()

        mgmt_client_mock.reset_mock()

        # all good
        mgmt_client_mock.get_subscription.side_effect = None

        actual_response = handlers[request['action']](handler, request)
        assert list(actual_response.keys()) == ['message']
        assert (actual_response.get('message', None) or '').startswith('removed subscription "my-subscription" on topic "my-topic" (stats: active_message_count=')

        mgmt_client_mock.delete_queue.assert_not_called()
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.get_subscription_runtime_properties.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.reset_mock()

        # all good, forwarded subscription
        request['context']['forward'] = True

        actual_response = handlers[request['action']](handler, request)
        assert list(actual_response.keys()) == ['message']
        assert (actual_response.get('message', None) or '').startswith('removed forward queue and subscription "my-subscription" on topic "my-topic" (stats: active_message_count=')

        mgmt_client_mock.delete_queue.assert_called_once_with(queue_name='my-subscription')
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.get_subscription_runtime_properties.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.reset_mock()

        del request['context']['forward']

        # non-unique subscription does not exist
        request['context']['unique'] = False
        mgmt_client_mock.get_subscription.side_effect = [ResourceNotFoundError]

        assert handlers[request['action']](handler, request) == {'message': 'non-unique subscription "my-subscription" on topic "my-topic" already removed'}
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.get_subscription_runtime_properties.assert_not_called()

    def test_from_message(self) -> None:
        assert AsyncServiceBusHandler.from_message(None) == (None, None)

        message = ServiceBusMessage('a message')
        message.raw_amqp_message._properties = None
        message.raw_amqp_message._header = None
        assert AsyncServiceBusHandler.from_message(message) == ({}, 'a message')

        message = ServiceBusMessage(b'a message')
        metadata, payload = AsyncServiceBusHandler.from_message(message)
        assert payload == 'a message'
        assert isinstance(metadata, dict)
        assert len(metadata) > 0

    def test_get_arguments(self) -> None:
        with pytest.raises(ValueError, match='incorrect format in arguments: "test"'):
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'test')

        with pytest.raises(ValueError, match='endpoint needs to be prefixed with queue: or topic:'):
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'asdf:test')

        with pytest.raises(ValueError, match='arguments dummy is not supported'):
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'topic:test, dummy:test')

        with pytest.raises(ValueError, match='arguments dummy is not supported'):
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test, dummy:test')

        with pytest.raises(ValueError, match='endpoint needs to include subscription when receiving messages from a topic'):
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test')

        with pytest.raises(ValueError, match='cannot specify both topic: and queue: in endpoint'):
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test, queue:test')

        with pytest.raises(ValueError, match='argument subscription is only allowed if endpoint is a topic'):
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'queue:test, subscription:test')

        with pytest.raises(ValueError, match='argument expression is only allowed when receiving messages'):
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'queue:test, expression:test')

        assert AsyncServiceBusHandler.get_endpoint_arguments('sender', 'queue:test') == {
            'endpoint': 'test',
            'endpoint_type': 'queue',
        }

        assert AsyncServiceBusHandler.get_endpoint_arguments('sender', 'topic:test') == {
            'endpoint': 'test',
            'endpoint_type': 'topic',
        }

        assert AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'queue:test') == {
            'endpoint': 'test',
            'endpoint_type': 'queue',
        }

        assert AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test, subscription:test') == {
            'endpoint': 'test',
            'endpoint_type': 'topic',
            'subscription': 'test',
        }

        assert AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:incoming, subscription:all, expression:"$.hello.world"') == {
            'endpoint': 'incoming',
            'endpoint_type': 'topic',
            'subscription': 'all',
            'expression': '$.hello.world',
        }

    def test_get_sender_instance(self, mocker: MockerFixture) -> None:
        url = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        client = ServiceBusClient.from_connection_string(
            conn_str=url,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        queue_spy = mocker.spy(client, 'get_queue_sender')
        topic_spy = mocker.spy(client, 'get_topic_sender')

        handler = AsyncServiceBusHandler('asdf-asdf-asdf')

        handler._client = client

        sender = handler.get_sender_instance(handler.get_endpoint_arguments('sender', 'queue:test-queue'))
        assert isinstance(sender, ServiceBusSender)
        topic_spy.assert_not_called()
        queue_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', queue_name='test-queue')
        queue_spy.reset_mock()

        sender = handler.get_sender_instance(handler.get_endpoint_arguments('sender', 'topic:test-topic'))
        assert isinstance(sender, ServiceBusSender)
        queue_spy.assert_not_called()
        topic_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', topic_name='test-topic')

    def test_get_receiver_instance(self, mocker: MockerFixture) -> None:
        url = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        client = ServiceBusClient.from_connection_string(
            conn_str=url,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        queue_spy = mocker.spy(client, 'get_queue_receiver')
        topic_spy = mocker.spy(client, 'get_subscription_receiver')

        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        handler._client = client

        receiver = handler.get_receiver_instance(handler.get_endpoint_arguments('receiver', 'queue:test-queue'))
        assert isinstance(receiver, ServiceBusReceiver)
        topic_spy.assert_not_called()
        queue_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', queue_name='test-queue')
        queue_spy.reset_mock()

        handler.get_receiver_instance(dict({'wait': '100'}, **handler.get_endpoint_arguments('receiver', 'queue:test-queue')))
        topic_spy.assert_not_called()
        queue_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', queue_name='test-queue', max_wait_time=100)
        queue_spy.reset_mock()

        receiver = handler.get_receiver_instance(handler.get_endpoint_arguments('receiver', 'topic:test-topic, subscription: test-subscription'))
        queue_spy.assert_not_called()
        topic_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', topic_name='test-topic', subscription_name='test-subscription')
        topic_spy.reset_mock()

        receiver = handler.get_receiver_instance(
            dict(
                {'wait': '100'},
                **handler.get_endpoint_arguments(
                    'receiver',
                    'topic:test-topic, subscription:test-subscription, expression:$.foo.bar',
                ),
            ),
        )

        queue_spy.assert_not_called()
        topic_spy.assert_called_once_with(client_identifier='asdf-asdf-asdf', topic_name='test-topic', subscription_name='test-subscription', max_wait_time=100)
        topic_spy.reset_mock()

    def test_hello(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        request: AsyncMessageRequest = {
            'action': 'HELLO',
        }

        with pytest.raises(AsyncMessageError, match='no context in request'):
            handlers[request['action']](handler, request)

        assert handler._client is None

        servicebusclient_connect_spy = mocker.spy(ServiceBusClient, 'from_connection_string')

        request = {
            'action': 'HELLO',
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'asdf',
            },
        }

        with pytest.raises(AsyncMessageError, match='"asdf" is not a valid value for context.connection'):
            handlers[request['action']](handler, request)

        servicebusclient_connect_spy.assert_called_once_with(conn_str=f'Endpoint={request["context"]["url"]}', transport_type=TransportType.AmqpOverWebsocket)
        assert isinstance(getattr(handler, 'client', None), ServiceBusClient)

        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

        sender_instance_spy = mocker.patch.object(handler, 'get_sender_instance', autospec=True)
        receiver_instance_spy = mocker.patch.object(handler, 'get_receiver_instance', autospec=True)
        mocker.patch('async_messaged.sb.sleep', return_value=None)

        request['context']['connection'] = 'sender'

        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        receiver_instance_spy.assert_not_called()
        sender_instance_spy.assert_called_once_with({'endpoint_type': 'queue', 'endpoint': 'test-queue'})
        sender_instance_spy.return_value.__enter__.assert_called_once_with()
        sender_instance_spy.reset_mock()

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache == {}

        # read from cache
        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        receiver_instance_spy.assert_not_called()

        request['context'].update(
            {
                'connection': 'receiver',
                'endpoint': 'topic:test-topic, subscription:test-subscription',
            },
        )

        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        receiver_instance_spy.assert_called_once_with({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

        # read from cache, not new instance needed
        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        receiver_instance_spy.assert_not_called()
        receiver_instance_spy.return_value.__enter__.assert_not_called()

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

        # unexpected exception raised when trying to get instance
        handler._sender_cache.clear()
        handler._receiver_cache.clear()

        receiver_instance_spy.side_effect = [RuntimeError('foo')]
        with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError, match='foo'):
            handlers[request['action']](handler, request)

        assert caplog.messages == []
        receiver_instance_spy.assert_called_once_with({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()

        # expected exception raised, different error message
        receiver_instance_spy.side_effect = [TypeError('foo')]
        with caplog.at_level(logging.WARNING), pytest.raises(TypeError, match='foo'):
            handlers[request['action']](handler, request)

        assert caplog.messages == []
        receiver_instance_spy.assert_called_once_with({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()

        # expected exception raised, expected error message, should retry, but will fail
        receiver_instance_spy.side_effect = cycle([TypeError("'NoneType' is not subscriptable")])
        with caplog.at_level(logging.WARNING), pytest.raises(AsyncMessageError, match='hello failed, creating service bus connection timed out 3 times'):
            handlers[request['action']](handler, request)

        assert caplog.messages == [
            'hello failed: service bus connection timed out, retry 1 in 0.50 seconds',
            'hello failed: service bus connection timed out, retry 2 in 0.85 seconds',
            'hello failed: service bus connection timed out, retry 3 in 1.44 seconds',
        ]
        caplog.clear()
        assert receiver_instance_spy.call_count == 3
        receiver_instance_spy.assert_called_with({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()

        # expected exception raised, expected error message, last re-try succeeds
        receiver_instance_spy.side_effect = [TypeError("'NoneType' is not subscriptable"), TypeError("'NoneType' is not subscriptable"), mocker.MagicMock()]
        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == [
            'hello failed: service bus connection timed out, retry 1 in 0.50 seconds',
            'hello failed: service bus connection timed out, retry 2 in 0.85 seconds',
        ]
        caplog.clear()
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        assert receiver_instance_spy.call_count == 3
        receiver_instance_spy.assert_called_with({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()

        # forwarded subscription, create receiver instance
        handler._sender_cache.clear()
        handler._receiver_cache.clear()
        receiver_instance_spy.side_effect = None
        request['context'].update(
            {
                'connection': 'receiver',
                'endpoint': 'topic:test-topic, subscription:test-subscription',
                'forward': True,
            },
        )

        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        receiver_instance_spy.assert_called_once_with({'endpoint_type': 'queue', 'endpoint': 'test-subscription', 'wait': '10'})
        receiver_instance_spy.reset_mock()
        assert handler._sender_cache == {}
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

        # forwarded subscription, read from cache, not new instance needed
        with caplog.at_level(logging.WARNING):
            assert handlers[request['action']](handler, request) == {
                'message': 'there general kenobi',
            }

        assert caplog.messages == []
        sender_instance_spy.assert_not_called()
        sender_instance_spy.return_value.__enter__.assert_not_called()
        receiver_instance_spy.assert_not_called()
        receiver_instance_spy.return_value.__enter__.assert_not_called()

        assert handler._sender_cache == {}
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

    def test_request(self, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        sender_instance_mock = mocker.patch.object(handler, 'get_sender_instance')
        receiver_instance_mock = mocker.patch.object(handler, 'get_receiver_instance')
        mocker.patch('async_messaged.sb.perf_counter', side_effect=cycle([0, 11]))

        request: AsyncMessageRequest = {
            'action': 'SEND',
        }

        def setup_handler(handler: AsyncServiceBusHandler, request: AsyncMessageRequest) -> None:
            handler._arguments.update(
                {
                    f'{request["context"]["connection"]}={request["context"]["endpoint"]}': handler.get_endpoint_arguments(
                        request['context']['connection'],
                        request['context']['endpoint'],
                    ),
                },
            )

            endpoint = request['context']['endpoint']

            if request['context']['connection'] == 'sender':
                handler._sender_cache[endpoint] = sender_instance_mock.return_value
            else:
                handler._receiver_cache[endpoint] = receiver_instance_mock.return_value

        with pytest.raises(AsyncMessageError, match='no context in request'):
            handlers[request['action']](handler, request)

        assert handler._client is None

        # sender request
        request = {
            'action': 'SEND',
            'client': id(handler),
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'asdf',
            },
        }

        with pytest.raises(AsyncMessageError, match='"asdf" is not a valid value for context.connection'):
            handlers[request['action']](handler, request)

        request['context']['connection'] = 'sender'

        with pytest.raises(AsyncMessageError, match='no HELLO received for queue:test-queue'):
            handlers[request['action']](handler, request)

        setup_handler(handler, request)

        with pytest.raises(AsyncMessageError, match='no payload'):
            handlers[request['action']](handler, request)

        request['payload'] = 'grizzly <3 service bus'

        sender_instance_mock.return_value.send_messages.side_effect = [RuntimeError('unknown error')]

        with pytest.raises(AsyncMessageError, match='failed to send message: unknown error'):
            handlers[request['action']](handler, request)

        sender_instance_mock.reset_mock(return_value=True, side_effect=True)
        setup_handler(handler, request)

        response = handlers[request['action']](handler, request)

        assert len(response) == 3
        expected_metadata, expected_payload = handler.from_message(ServiceBusMessage('grizzly <3 service bus'))
        actual_metadata = response.get('metadata', None)
        assert actual_metadata is not None
        assert expected_metadata is not None
        actual_metadata['message_id'] = None
        expected_metadata['message_id'] = None

        assert response.get('payload', None) == expected_payload
        assert actual_metadata == expected_metadata
        assert response.get('response_length', 0) == len('grizzly <3 service bus')

        # receiver request
        request['action'] = 'RECEIVE'
        request['context'].update(
            {
                'connection': 'receiver',
                'endpoint': 'topic:test-topic, subscription:test-subscription',
            },
        )

        setup_handler(handler, request)

        with pytest.raises(AsyncMessageError, match='payload not allowed'):
            handlers[request['action']](handler, request)

        del request['payload']

        received_message = ServiceBusMessage('grizzly >3 service bus')
        receiver_instance_mock.return_value.__iter__.side_effect = [StopIteration, iter([received_message])]

        with pytest.raises(AsyncMessageError, match='no messages on "topic:test-topic, subscription:test-subscription" within 10 seconds'):
            handlers[request['action']](handler, request)
        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        receiver_instance_mock.return_value.complete_message.assert_not_called()
        receiver_instance_mock.reset_mock()

        response = handlers[request['action']](handler, request)

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        receiver_instance_mock.return_value.complete_message.assert_called_once_with(received_message)
        receiver_instance_mock.reset_mock()

        assert len(response) == 3
        expected_metadata, expected_payload = handler.from_message(received_message)
        actual_metadata = response.get('metadata', None)
        assert actual_metadata is not None
        assert expected_metadata is not None
        assert expected_payload is not None
        actual_metadata['message_id'] = None
        expected_metadata['message_id'] = None

        assert response.get('payload', None) == expected_payload
        assert actual_metadata == expected_metadata
        assert response.get('response_length', 0) == len(expected_payload)

        # receive request, forwarded subscription
        request['context']['forward'] = True
        receiver_instance_mock.return_value.__iter__.side_effect = [iter([received_message])]

        response = handlers[request['action']](handler, request)

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        receiver_instance_mock.return_value.complete_message.assert_called_once_with(received_message)
        receiver_instance_mock.reset_mock()

        assert len(response) == 3
        expected_metadata, expected_payload = handler.from_message(received_message)
        actual_metadata = response.get('metadata', None)
        assert actual_metadata is not None
        assert expected_metadata is not None
        assert expected_payload is not None
        actual_metadata['message_id'] = None
        expected_metadata['message_id'] = None

        assert response.get('payload', None) == expected_payload
        assert actual_metadata == expected_metadata
        assert response.get('response_length', 0) == len(expected_payload)

        del request['context']['forward']

        _hello_mock = mocker.patch.object(handler, '_hello', return_value=None)
        receiver_instance_mock.return_value.__iter__.side_effect = [ServiceBusError('Connection to remote host was lost'), iter([received_message])]

        with caplog.at_level(logging.WARNING):
            response = handlers[request['action']](handler, request)
        assert receiver_instance_mock.return_value.__iter__.call_count == 2
        _hello_mock.assert_called_once_with(request, force=True)
        assert caplog.messages[-1] == 'connection unexpectedly closed, reconnecting'

    def test_request_expression(self, mocker: MockerFixture) -> None:  # noqa: PLR0915
        from async_messaged.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        receiver_instance_mock = mocker.patch.object(handler, 'get_receiver_instance')

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue, expression:"$.`this`[?(@.name="test")]"',
                'connection': 'receiver',
                'content_type': 'json',
            },
        }

        def setup_handler(handler: AsyncServiceBusHandler, request: AsyncMessageRequest) -> None:
            endpoint_arguments = parse_arguments(request['context']['endpoint'], ':')
            with suppress(Exception):
                del endpoint_arguments['expression']
            cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

            key = f'{request["context"]["connection"]}={cache_endpoint}'
            handler._arguments.update(
                {
                    key: handler.get_endpoint_arguments(
                        request['context']['connection'],
                        request['context']['endpoint'],
                    ),
                },
            )

            handler._arguments[key]['content_type'] = cast('str', request['context']['content_type'])
            handler._arguments[key]['consume'] = f'{request["context"].get("consume", False)}'
            handler._receiver_cache[cache_endpoint] = receiver_instance_mock.return_value

        setup_handler(handler, request)
        message1 = ServiceBusMessage(
            jsondumps(
                {
                    'document': {
                        'name': 'not-test',
                        'id': 10,
                    },
                },
            ),
        )
        message2 = ServiceBusMessage(
            jsondumps(
                {
                    'document': {
                        'name': 'test',
                        'id': 13,
                    },
                },
            ),
        )
        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2]),
        ]

        response = handlers[request['action']](handler, request)

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        receiver_instance_mock.return_value.complete_message.assert_called_once_with(message2)
        receiver_instance_mock.return_value.abandon_message.assert_called_once_with(message1)
        receiver_instance_mock.reset_mock()

        assert len(response) == 3
        expected_metadata, expected_payload = handler.from_message(message2)
        actual_metadata = response.get('metadata', None)
        assert actual_metadata is not None
        assert expected_metadata is not None
        assert expected_payload is not None
        actual_metadata['message_id'] = None
        expected_metadata['message_id'] = None

        assert response.get('payload', None) == expected_payload
        assert actual_metadata == expected_metadata
        assert response.get('response_length', 0) == len(expected_payload)

        message_error = ServiceBusMessage('<?xml version="1.0" encoding="UTF-8"?><document/>')

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message_error]),
        ] * 2

        with pytest.raises(AsyncMessageError, match=r'failed to transform input as JSON: Expecting value: line 1 column 1 \(char 0\)'):
            handlers[request['action']](handler, request)
        receiver_instance_mock.return_value.abandon_message.assert_called_once_with(message_error)
        receiver_instance_mock.reset_mock()

        endpoint_backup = request['context']['endpoint']
        request['context']['endpoint'] = 'queue:test-queue, expression:"//document[@name="test-document"]"'
        with pytest.raises(AsyncMessageError, match=r'JsonTransformer: unable to parse with ".*": not a valid expression'):
            handlers[request['action']](handler, request)

        request['context']['endpoint'] = endpoint_backup

        from_message = handler.from_message
        mocker.patch.object(handler, 'from_message', side_effect=[(None, None)])
        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message2]),
        ]

        with pytest.raises(AsyncMessageError, match='no payload in message'):
            handlers[request['action']](handler, request)

        receiver_instance_mock.return_value.abandon_message.assert_called_once_with(message2)
        receiver_instance_mock.reset_mock()

        setattr(handler, 'from_message', from_message)  # noqa: B010

        message3 = ServiceBusMessage(
            jsondumps(
                {
                    'document': {
                        'name': 'not-test',
                        'id': 14,
                    },
                },
            ),
        )

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message3]),
        ]

        mocker.patch(
            'async_messaged.sb.perf_counter',
            side_effect=[0.0, 5.0, 0.1, 0.5, 0, 11.0],
        )

        with pytest.raises(AsyncMessageError, match=r'no messages on "queue:test-queue" matching expression "\$.`this`\[\?\(@.name="test"\)\]"'):
            handlers[request['action']](handler, request)

        assert receiver_instance_mock.return_value.abandon_message.call_count == 2
        receiver_instance_mock.reset_mock()

        mocker.patch(
            'async_messaged.sb.perf_counter',
            return_value=0.0,
        )

        request['context'].update({'consume': True})

        setup_handler(handler, request)

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2]),
        ]

        response = handlers[request['action']](handler, request)

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        assert receiver_instance_mock.return_value.complete_message.call_count == 2
        receiver_instance_mock.return_value.abandon_message.assert_not_called()
        receiver_instance_mock.reset_mock()

        assert response.get('payload', None) == jsondumps({'document': {'name': 'test', 'id': 13}})

        message1 = ServiceBusMessage(
            jsondumps(
                {
                    'name': 'bob',
                },
            ),
        )

        message2 = ServiceBusMessage(
            jsondumps(
                {
                    'name': 'alice',
                },
            ),
        )

        message3 = ServiceBusMessage(
            jsondumps(
                {
                    'name': 'mallory',
                },
            ),
        )

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2, message3]),
        ]

        request['context'].update({'endpoint': 'topic:events, subscription:my-subscription, expression:$.name=="mallory"'})

        setup_handler(handler, request)

        response = handlers[request['action']](handler, request)

        assert response.get('payload', None) == jsondumps({'name': 'mallory'})

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        assert receiver_instance_mock.return_value.complete_message.call_count == 3
        receiver_instance_mock.return_value.abandon_message.assert_not_called()
        receiver_instance_mock.reset_mock()

        request['context'].update({'endpoint': 'topic:events, subscription:my-subscription, expression:$.name|=\'["mallory", "alice"]\''})

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2, message3]),
        ]

        setup_handler(handler, request)

        response = handlers[request['action']](handler, request)

        assert response.get('payload', None) == jsondumps({'name': 'alice'})

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        assert receiver_instance_mock.return_value.complete_message.call_count == 2
        receiver_instance_mock.return_value.abandon_message.assert_not_called()
        receiver_instance_mock.reset_mock()

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message3]),
        ]

        response = handlers[request['action']](handler, request)

        assert response.get('payload', None) == jsondumps({'name': 'mallory'})

        receiver_instance_mock.return_value.__iter__.assert_called_once_with()
        assert receiver_instance_mock.return_value.complete_message.call_count == 1
        receiver_instance_mock.return_value.abandon_message.assert_not_called()
        receiver_instance_mock.reset_mock()

    def test_get_handler(self) -> None:
        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        assert handler.get_handler('NONE') is None
        assert handler.get_handler('HELLO') is AsyncServiceBusHandler.hello
        assert handler.get_handler('RECEIVE') is AsyncServiceBusHandler.request
        assert handler.get_handler('SEND') is AsyncServiceBusHandler.request
        assert handler.get_handler('GET') is None
        assert handler.get_handler('PUT') is None

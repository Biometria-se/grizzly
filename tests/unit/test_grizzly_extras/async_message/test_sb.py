from typing import cast
from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture

from azure.servicebus import ServiceBusMessage, TransportType, ServiceBusClient, ServiceBusSender, ServiceBusReceiver
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from grizzly_extras.arguments import parse_arguments
from grizzly_extras.async_message import AsyncMessageError, AsyncMessageRequest
from grizzly_extras.async_message.sb import AsyncServiceBusHandler


class TestAsyncServiceBusHandler:
    def test___init__(self) -> None:
        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        assert handler.worker == 'asdf-asdf-asdf'
        assert handler.message_wait is None
        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

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

        assert client.close.call_count == 1
        assert mgmt_client.close.call_count == 1
        assert sender.close.call_count == 1
        assert receiver.close.call_count == 1

    def test_disconnect(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        handler._client = mocker.MagicMock()
        request: AsyncMessageRequest = {
            'action': 'DISCONNECT',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no context in request' in str(ame)

        request = {
            'action': 'DISCONNECT',
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'asdf',
            },
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert '"asdf" is not a valid value for context.connection' in str(ame)

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
        assert sender_instance.__exit__.call_count == 1

        # successful disconnect, receiver
        receiver_instance = mocker.MagicMock()
        handler._receiver_cache.update({'queue:test-queue': receiver_instance})

        request['context']['connection'] = 'receiver'

        assert handlers[request['action']](handler, request) == {
            'message': 'thanks for all the fish',
        }

        assert handler._receiver_cache == {}
        assert receiver_instance.__exit__.call_count == 1

        # error disconnecting
        receiver_instance = mocker.MagicMock()
        receiver_instance.__exit__.side_effect = [RuntimeError]
        handler._receiver_cache.update({'queue:test-queue': receiver_instance})

        request['context']['connection'] = 'receiver'

        assert handlers[request['action']](handler, request) == {
            'message': 'thanks for all the fish',
        }

        assert handler._receiver_cache == {}
        assert receiver_instance.__exit__.call_count == 1

    def test_subscribe(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        # malformed request, no context
        request: AsyncMessageRequest = {
            'action': 'SUBSCRIBE',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'no context in request'

        # malformed request, subscribe on queue
        request = {
            'action': 'SUBSCRIBE',
            'context': {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
            },
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'subscriptions is only allowed on topics'

        # malformed request, no subscription specified
        request['context'].update({'endpoint': 'topic:my-topic'})

        with pytest.raises(ValueError) as ve:
            handlers[request['action']](handler, request)
        assert str(ve.value) == 'endpoint needs to include subscription when receiving messages from a topic'

        # malformed request, no rule text in payload
        request['context'].update({'endpoint': 'topic:my-topic, subscription:my-subscription'})

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'no rule text in request'

        # pre: valid request
        mgmt_client_mock = mocker.MagicMock()
        create_client_mock = mocker.patch('grizzly_extras.async_message.sb.ServiceBusAdministrationClient.from_connection_string', return_value=mgmt_client_mock)

        request['payload'] = '1=1'

        # specified topic does not exist
        mgmt_client_mock.get_topic.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'topic "my-topic" does not exist'

        create_client_mock.assert_called_once_with(conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=')
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')

        mgmt_client_mock.get_topic.side_effect = None
        mgmt_client_mock.get_topic.reset_mock()

        # subscription already exist, default rule does not exist, rule exists
        rule_mock = mocker.MagicMock()
        mgmt_client_mock.delete_rule.side_effect = [ResourceNotFoundError]
        mgmt_client_mock.create_rule.side_effect = [ResourceExistsError]
        mgmt_client_mock.get_rule.return_value = rule_mock

        assert handlers[request['action']](handler, request) == {'message': 'created subscription my-subscription on topic my-topic'}

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

        assert handlers[request['action']](handler, request) == {'message': 'created subscription my-subscription on topic my-topic'}

        mgmt_client_mock.create_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='$Default')
        mgmt_client_mock.create_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule_name='grizzly')
        mgmt_client_mock.get_rule.assert_not_called()
        assert rule_mock.filter.sql_expression == 'foo=bar AND foo=baz'
        mgmt_client_mock.update_rule.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription', rule=rule_mock)

    def test_unsubscribe(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        # malformed request, no context
        request: AsyncMessageRequest = {
            'action': 'UNSUBSCRIBE',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'no context in request'

        # malformed request, subscribe on queue
        request = {
            'action': 'UNSUBSCRIBE',
            'context': {
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'receiver',
            },
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'subscriptions is only allowed on topics'

        # malformed request, no subscription specified
        request['context'].update({'endpoint': 'topic:my-topic'})

        with pytest.raises(ValueError) as ve:
            handlers[request['action']](handler, request)
        assert str(ve.value) == 'endpoint needs to include subscription when receiving messages from a topic'

        # pre: valid request
        mgmt_client_mock = mocker.MagicMock()
        create_client_mock = mocker.patch('grizzly_extras.async_message.sb.ServiceBusAdministrationClient.from_connection_string', return_value=mgmt_client_mock)
        request['context'].update({'endpoint': 'topic:my-topic, subscription:my-subscription'})

        # topic does not exist
        mgmt_client_mock.get_topic.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'topic "my-topic" does not exist'

        create_client_mock.assert_called_once_with(conn_str='Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=')
        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.delete_subscription.assert_not_called()

        mgmt_client_mock.reset_mock()

        # subscription does not exist
        mgmt_client_mock.get_topic.side_effect = None
        mgmt_client_mock.get_subscription.side_effect = [ResourceNotFoundError]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'subscription "my-subscription" does not exist on topic "my-topic"'

        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_subscription.assert_not_called()

        mgmt_client_mock.reset_mock()

        # all good
        mgmt_client_mock.get_subscription.side_effect = None

        assert handlers[request['action']](handler, request) == {'message': 'removed subscription my-subscription on topic my-topic'}

        mgmt_client_mock.get_topic.assert_called_once_with(topic_name='my-topic')
        mgmt_client_mock.get_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')
        mgmt_client_mock.delete_subscription.assert_called_once_with(topic_name='my-topic', subscription_name='my-subscription')

    def test_from_message(self) -> None:
        assert AsyncServiceBusHandler.from_message(None) == (None, None,)

        message = ServiceBusMessage('a message')
        message.raw_amqp_message.properties = None
        message.raw_amqp_message.header = None
        assert AsyncServiceBusHandler.from_message(message) == ({}, 'a message',)

        message = ServiceBusMessage('a message'.encode('utf-8'))
        metadata, payload = AsyncServiceBusHandler.from_message(message)
        assert payload == 'a message'
        assert isinstance(metadata, dict)
        assert len(metadata) > 0

    def test_get_arguments(self) -> None:
        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'test')
        assert 'incorrect format in arguments: "test"' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'asdf:test')
        assert 'endpoint needs to be prefixed with queue: or topic:' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'topic:test, dummy:test')
        assert 'arguments dummy is not supported' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test, dummy:test')
        assert 'arguments dummy is not supported' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test')
        assert 'endpoint needs to include subscription when receiving messages from a topic' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'topic:test, queue:test')
        assert 'cannot specify both topic: and queue: in endpoint' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('receiver', 'queue:test, subscription:test')
        assert 'argument subscription is only allowed if endpoint is a topic' in str(ve)

        with pytest.raises(ValueError) as ve:
            AsyncServiceBusHandler.get_endpoint_arguments('sender', 'queue:test, expression:test')
        assert 'argument expression is only allowed when receiving messages' in str(ve)

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
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        args, kwargs = queue_spy.call_args_list[0]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'queue_name': 'test-queue'}

        sender = handler.get_sender_instance(handler.get_endpoint_arguments('sender', 'topic:test-topic'))
        assert isinstance(sender, ServiceBusSender)
        assert queue_spy.call_count == 1
        assert topic_spy.call_count == 1
        args, kwargs = topic_spy.call_args_list[0]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'topic_name': 'test-topic'}

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
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        args, kwargs = queue_spy.call_args_list[-1]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'queue_name': 'test-queue'}

        handler.get_receiver_instance(dict({'wait': '100'}, **handler.get_endpoint_arguments('receiver', 'queue:test-queue')))
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 2
        args, kwargs = queue_spy.call_args_list[-1]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'queue_name': 'test-queue', 'max_wait_time': 100}

        receiver = handler.get_receiver_instance(handler.get_endpoint_arguments('receiver', 'topic:test-topic, subscription: test-subscription'))
        assert topic_spy.call_count == 1
        assert queue_spy.call_count == 2
        args, kwargs = topic_spy.call_args_list[-1]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'topic_name': 'test-topic', 'subscription_name': 'test-subscription'}

        receiver = handler.get_receiver_instance(dict({'wait': '100'}, **handler.get_endpoint_arguments(
            'receiver', 'topic:test-topic, subscription:test-subscription, expression:$.foo.bar',
        )))
        assert topic_spy.call_count == 2
        assert queue_spy.call_count == 2
        args, kwargs = topic_spy.call_args_list[-1]
        assert args == ()
        assert kwargs == {'client_identifier': 'asdf-asdf-asdf', 'topic_name': 'test-topic', 'subscription_name': 'test-subscription', 'max_wait_time': 100}

    def test_hello(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        request: AsyncMessageRequest = {
            'action': 'HELLO',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no context in request' in str(ame)

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

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert '"asdf" is not a valid value for context.connection' in str(ame)

        assert servicebusclient_connect_spy.call_count == 1
        _, kwargs = servicebusclient_connect_spy.call_args_list[0]
        assert kwargs.get('conn_str', None) == f'Endpoint={request["context"]["url"]}'
        assert kwargs.get('transport_type', None) == TransportType.AmqpOverWebsocket
        assert isinstance(getattr(handler, 'client', None), ServiceBusClient)

        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

        sender_instance_spy = mocker.patch.object(handler, 'get_sender_instance', autospec=True)
        receiver_instance_spy = mocker.patch.object(handler, 'get_receiver_instance', autospec=True)

        request['context']['connection'] = 'sender'

        assert handlers[request['action']](handler, request) == {
            'message': 'there general kenobi',
        }

        assert sender_instance_spy.call_count == 1
        assert sender_instance_spy.return_value.__enter__.call_count == 1
        assert receiver_instance_spy.call_count == 0

        args, kwargs = sender_instance_spy.call_args_list[0]
        assert args == ({'endpoint_type': 'queue', 'endpoint': 'test-queue'},)
        assert kwargs == {}

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache == {}

        # read from cache
        assert handlers[request['action']](handler, request) == {
            'message': 'there general kenobi',
        }

        assert sender_instance_spy.call_count == 1
        assert sender_instance_spy.return_value.__enter__.call_count == 1
        assert receiver_instance_spy.call_count == 0

        request['context'].update({
            'connection': 'receiver',
            'endpoint': 'topic:test-topic, subscription:test-subscription',
        })

        assert handlers[request['action']](handler, request) == {
            'message': 'there general kenobi',
        }

        assert sender_instance_spy.call_count == 1
        assert sender_instance_spy.return_value.__enter__.call_count == 1
        assert receiver_instance_spy.call_count == 1
        assert receiver_instance_spy.return_value.__enter__.call_count == 1

        args, kwargs = receiver_instance_spy.call_args_list[0]
        assert kwargs == {}
        assert args == ({'endpoint_type': 'topic', 'endpoint': 'test-topic', 'subscription': 'test-subscription', 'wait': '10'},)

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

        # read from cache, not new instance needed
        assert handlers[request['action']](handler, request) == {
            'message': 'there general kenobi',
        }

        assert sender_instance_spy.call_count == 1
        assert sender_instance_spy.return_value.__enter__.call_count == 1
        assert receiver_instance_spy.call_count == 1
        assert receiver_instance_spy.return_value.__enter__.call_count == 1

        assert handler._sender_cache.get('queue:test-queue', None) is not None
        assert handler._receiver_cache.get('topic:test-topic, subscription:test-subscription', None) is not None

    def test_request(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        sender_instance_mock = mocker.patch.object(handler, 'get_sender_instance')
        receiver_instance_mock = mocker.patch.object(handler, 'get_receiver_instance')
        mocker.patch('grizzly_extras.async_message.sb.perf_counter', side_effect=[0, 11, 0, 11, 0, 11, 0, 11, 0, 11])

        request: AsyncMessageRequest = {
            'action': 'SEND',
        }

        def setup_handler(handler: AsyncServiceBusHandler, request: AsyncMessageRequest) -> None:
            handler._arguments.update({
                f'{request["context"]["connection"]}={request["context"]["endpoint"]}': handler.get_endpoint_arguments(
                    request['context']['connection'],
                    request['context']['endpoint'],
                )
            })

            endpoint = request['context']['endpoint']

            if request['context']['connection'] == 'sender':
                handler._sender_cache[endpoint] = sender_instance_mock.return_value
            else:
                handler._receiver_cache[endpoint] = receiver_instance_mock.return_value

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no context in request' in str(ame)

        assert handler._client is None

        # sender request
        request = {
            'action': 'SEND',
            'context': {
                'message_wait': 10,
                'url': 'sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789=',
                'endpoint': 'queue:test-queue',
                'connection': 'asdf',
            },
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert '"asdf" is not a valid value for context.connection'

        request['context']['connection'] = 'sender'

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no HELLO received for queue:test-queue' in str(ame)

        setup_handler(handler, request)

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no payload' in str(ame)

        request['payload'] = 'grizzly <3 service bus'

        sender_instance_mock.return_value.send_messages.side_effect = [RuntimeError('unknown error')]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'failed to send message: unknown error' in str(ame)

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
        request['context'].update({
            'connection': 'receiver',
            'endpoint': 'topic:test-topic, subscription:test-subscription',
        })

        setup_handler(handler, request)

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'payload not allowed' in str(ame)

        del request['payload']

        received_message = ServiceBusMessage('grizzly >3 service bus')
        receiver_instance_mock.return_value.__iter__.side_effect = [StopIteration, iter([received_message])]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert str(ame.value) == 'no messages on topic:test-topic, subscription:test-subscription within 10 seconds'
        assert receiver_instance_mock.return_value.__iter__.call_count == 1
        assert receiver_instance_mock.return_value.complete_message.call_count == 0

        response = handlers[request['action']](handler, request)

        assert receiver_instance_mock.return_value.__iter__.call_count == 2
        assert receiver_instance_mock.return_value.complete_message.call_count == 1
        args, _ = receiver_instance_mock.return_value.complete_message.call_args_list[0]
        assert args[0] is received_message

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

    def test_request_expression(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

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
            try:
                del endpoint_arguments['expression']
            except:
                pass
            cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

            key = f'{request["context"]["connection"]}={cache_endpoint}'
            handler._arguments.update({
                key: handler.get_endpoint_arguments(
                    request['context']['connection'],
                    request['context']['endpoint'],
                )
            })

            handler._arguments[key]['content_type'] = cast(str, request['context']['content_type'])
            handler._arguments[key]['consume'] = f'{request["context"].get("consume", False)}'
            handler._receiver_cache[cache_endpoint] = receiver_instance_mock.return_value

        setup_handler(handler, request)
        message1 = ServiceBusMessage(jsondumps({
            'document': {
                'name': 'not-test',
                'id': 10,
            }
        }))
        message2 = ServiceBusMessage(jsondumps({
            'document': {
                'name': 'test',
                'id': 13,
            }
        }))
        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2]),
        ]

        response = handlers[request['action']](handler, request)

        assert receiver_instance_mock.return_value.__iter__.call_count == 1
        assert receiver_instance_mock.return_value.complete_message.call_count == 1
        assert receiver_instance_mock.return_value.abandon_message.call_count == 1

        args, _ = receiver_instance_mock.return_value.complete_message.call_args_list[-1]
        assert args[0] is message2

        args, _ = receiver_instance_mock.return_value.abandon_message.call_args_list[-1]
        assert args[0] is message1

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

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'failed to transform input as JSON: Expecting value: line 1 column 1 (char 0)' in str(ame)
        assert receiver_instance_mock.return_value.abandon_message.call_count == 2

        endpoint_backup = request['context']['endpoint']
        request['context']['endpoint'] = 'queue:test-queue, expression:"//document[@name="test-document"]"'
        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'JsonTransformer: unable to parse "//document[@name="test-document"]": JsonTransformer: not a valid expression' in str(ame)

        request['context']['endpoint'] = endpoint_backup

        from_message = handler.from_message
        mocker.patch.object(handler, 'from_message', side_effect=[(None, None,)])
        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message2]),
        ]

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no payload in message' in str(ame)

        assert receiver_instance_mock.return_value.abandon_message.call_count == 3

        setattr(handler, 'from_message', from_message)

        message3 = ServiceBusMessage(jsondumps({
            'document': {
                'name': 'not-test',
                'id': 14,
            }
        }))

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message3]),
        ]

        mocker.patch(
            'grizzly_extras.async_message.sb.perf_counter',
            side_effect=[0.0, 5.0, 0.1, 0.5, 0, 11.0],
        )

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no messages on queue:test-queue, expression:"$.`this`[?(@.name="test")]"' in str(ame)

        assert receiver_instance_mock.return_value.abandon_message.call_count == 5

        mocker.patch(
            'grizzly_extras.async_message.sb.perf_counter',
            return_value=0.0,
        )

        request['context'].update({'consume': True})

        setup_handler(handler, request)
        receiver_instance_mock.reset_mock()

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2]),
        ]

        response = handlers[request['action']](handler, request)

        assert receiver_instance_mock.return_value.__iter__.call_count == 1
        assert receiver_instance_mock.return_value.complete_message.call_count == 2
        assert receiver_instance_mock.return_value.abandon_message.call_count == 0

        assert response.get('payload', None) == jsondumps({'document': {'name': 'test', 'id': 13}})

        message1 = ServiceBusMessage(jsondumps({
            'name': 'bob',
        }))

        message2 = ServiceBusMessage(jsondumps({
            'name': 'alice',
        }))

        message3 = ServiceBusMessage(jsondumps({
            'name': 'mallory',
        }))

        receiver_instance_mock.return_value.__iter__.side_effect = [
            iter([message1, message2, message3]),
        ]
        receiver_instance_mock.reset_mock()

        request['context'].update({'endpoint': 'queue:test-queue, expression:$.name=="mallory"'})

        setup_handler(handler, request)

        response = handlers[request['action']](handler, request)

        assert response.get('payload', None) == jsondumps({'name': 'mallory'})

        assert receiver_instance_mock.return_value.__iter__.call_count == 1
        assert receiver_instance_mock.return_value.complete_message.call_count == 3
        assert receiver_instance_mock.return_value.abandon_message.call_count == 0

    def test_get_handler(self) -> None:
        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        assert handler.get_handler('NONE') is None
        assert handler.get_handler('HELLO') is AsyncServiceBusHandler.hello
        assert handler.get_handler('RECEIVE') is AsyncServiceBusHandler.request
        assert handler.get_handler('SEND') is AsyncServiceBusHandler.request
        assert handler.get_handler('GET') is None
        assert handler.get_handler('PUT') is None

import logging

import pytest

from pytest_mock import MockerFixture, mocker  # pylint: disable=unused-import

from azure.servicebus import ServiceBusMessage, TransportType, ServiceBusClient, ServiceBusSender, ServiceBusReceiver
from grizzly_extras.async_message import AsyncMessageError, AsyncMessageRequest
from grizzly_extras.async_message.sb import AsyncServiceBusHandler

class TestAsyncServiceBusHandler:
    def test___init__(self, mocker: MockerFixture) -> None:
        spy = mocker.patch(
            'grizzly_extras.async_message.sb.logging.Logger.setLevel',
            side_effect=[None],
        )

        handler = AsyncServiceBusHandler('asdf-asdf-asdf')
        assert handler.worker == 'asdf-asdf-asdf'
        assert handler.message_wait is None
        assert handler._sender_cache == {}
        assert handler._receiver_cache == {}

        assert spy.call_count == 1
        args, _ = spy.call_args_list[0]
        assert args[0] == logging.ERROR

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

    def test_get_endpoint_details(self) -> None:
        with pytest.raises(AsyncMessageError) as ame:
            AsyncServiceBusHandler.get_endpoint_details('receiver', 'test')
        assert '"test" is not prefixed with queue: or topic:' in str(ame)

        with pytest.raises(AsyncMessageError) as ame:
            AsyncServiceBusHandler.get_endpoint_details('sender', 'asdf:test')
        assert 'only support for endpoint types queue and topic, not asdf' in str(ame)

        with pytest.raises(AsyncMessageError) as ame:
            AsyncServiceBusHandler.get_endpoint_details('sender', 'topic:test, dummy:test')
        assert 'additional arguments in endpoint is not supported for sender' in str(ame)

        with pytest.raises(AsyncMessageError) as ame:
            AsyncServiceBusHandler.get_endpoint_details('receiver', 'topic:test, dummy:test')
        assert 'argument dummy is not supported' in str(ame)

        with pytest.raises(AsyncMessageError) as ame:
            AsyncServiceBusHandler.get_endpoint_details('receiver', 'topic:test')
        assert 'endpoint needs to include subscription when receiving messages from a topic' in str(ame)

        assert AsyncServiceBusHandler.get_endpoint_details('sender', 'queue:test') == ('queue', 'test', None, )

        assert AsyncServiceBusHandler.get_endpoint_details('sender', 'topic:test') == ('topic', 'test', None, )

        assert AsyncServiceBusHandler.get_endpoint_details('receiver', 'queue:test') == ('queue', 'test', None, )

        assert AsyncServiceBusHandler.get_endpoint_details('receiver', 'topic:test, subscription:test') == ('topic', 'test', 'test', )

    def test_get_sender_instance(self, mocker: MockerFixture) -> None:
        url = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        client = ServiceBusClient.from_connection_string(
            conn_str=url,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        queue_spy = mocker.spy(client, 'get_queue_sender')
        topic_spy = mocker.spy(client, 'get_topic_sender')

        handler = AsyncServiceBusHandler('asdf-asdf-asdf')

        sender = handler.get_sender_instance(client, 'queue:test-queue')
        assert isinstance(sender, ServiceBusSender)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        _, kwargs = queue_spy.call_args_list[0]
        assert len(kwargs) == 1
        assert kwargs.get('queue_name', None) == 'test-queue'

        sender = handler.get_sender_instance(client, 'topic:test-topic')
        assert isinstance(sender, ServiceBusSender)
        assert queue_spy.call_count == 1
        assert topic_spy.call_count == 1
        _, kwargs = topic_spy.call_args_list[0]
        assert len(kwargs) == 1
        assert kwargs.get('topic_name', None) == 'test-topic'

    def test_get_receiver_instance(self, mocker: MockerFixture) -> None:
        url = 'Endpoint=sb://sb.example.org/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=abc123def456ghi789='
        client = ServiceBusClient.from_connection_string(
            conn_str=url,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        queue_spy = mocker.spy(client, 'get_queue_receiver')
        topic_spy = mocker.spy(client, 'get_subscription_receiver')

        handler = AsyncServiceBusHandler('asdf-asdf-asdf')

        receiver = handler.get_receiver_instance(client, 'queue:test-queue')
        assert isinstance(receiver, ServiceBusReceiver)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 1
        _, kwargs = queue_spy.call_args_list[-1]
        assert len(kwargs) == 1
        assert kwargs.get('queue_name', None) == 'test-queue'

        handler.get_receiver_instance(client, 'queue:test-queue', 100)
        assert topic_spy.call_count == 0
        assert queue_spy.call_count == 2
        _, kwargs = queue_spy.call_args_list[-1]
        assert len(kwargs) == 2
        assert kwargs.get('queue_name', None) == 'test-queue'
        assert kwargs.get('max_wait_time', None) == 100

        receiver = handler.get_receiver_instance(client, 'topic:test-topic, subscription:test-subscription')
        assert topic_spy.call_count == 1
        assert queue_spy.call_count == 2
        _, kwargs = topic_spy.call_args_list[-1]
        assert len(kwargs) == 2
        assert kwargs.get('topic_name', None) == 'test-topic'
        assert kwargs.get('subscription_name', None) == 'test-subscription'

        receiver = handler.get_receiver_instance(client, 'topic:test-topic, subscription:test-subscription', 100)
        assert topic_spy.call_count == 2
        assert queue_spy.call_count == 2
        _, kwargs = topic_spy.call_args_list[-1]
        assert len(kwargs) == 3
        assert kwargs.get('topic_name', None) == 'test-topic'
        assert kwargs.get('subscription_name', None) == 'test-subscription'
        assert kwargs.get('max_wait_time', None) == 100

    def test_hello(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.sb import handlers

        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')
        request: AsyncMessageRequest = {
            'action': 'HELLO',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no context in request' in str(ame)

        assert handler.client is None

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

        args, _ = sender_instance_spy.call_args_list[0]
        assert len(args) == 2
        assert args[0] is handler.client
        assert args[1] == 'queue:test-queue'

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

        args, _ = receiver_instance_spy.call_args_list[0]
        assert len(args) == 3
        assert args[0] is handler.client
        assert args[1] == 'topic:test-topic, subscription:test-subscription'
        assert args[2] == 10

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
        sender_instance_spy = mocker.patch.object(handler, 'get_sender_instance', autospec=True)
        receiver_instance_spy = mocker.patch.object(handler, 'get_receiver_instance', autospec=True)

        request: AsyncMessageRequest = {
            'action': 'SEND',
        }

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no context in request' in str(ame)

        assert handler.client is None

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
        assert 'no payload' in str(ame)

        request['payload'] = 'grizzly <3 service bus'

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no HELLO sent for queue:test-queue' in str(ame)

        handler._sender_cache[request['context']['endpoint']] = sender_instance_spy

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'failed to send message' in str(ame)

        handler._sender_cache[request['context']['endpoint']] = sender_instance_spy.return_value

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

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'payload not allowed' in str(ame)

        del request['payload']

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no HELLO sent for topic:test-topic, subscription:test-subscription' in str(ame)

        received_message = ServiceBusMessage('grizzly >3 service bus')

        receiver_instance_spy.return_value.next.side_effect = [StopIteration, received_message]
        handler._receiver_cache[request['context']['endpoint']] = receiver_instance_spy.return_value

        with pytest.raises(AsyncMessageError) as ame:
            handlers[request['action']](handler, request)
        assert 'no messages on topic:test-topic, subscription:test-subscription' in str(ame)
        assert receiver_instance_spy.return_value.next.call_count == 1
        assert receiver_instance_spy.return_value.complete_message.call_count == 0

        response = handlers[request['action']](handler, request)

        assert receiver_instance_spy.return_value.next.call_count == 2
        assert receiver_instance_spy.return_value.complete_message.call_count == 1
        args, _ = receiver_instance_spy.return_value.complete_message.call_args_list[0]
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

    def test_get_handler(self) -> None:
        handler = AsyncServiceBusHandler(worker='asdf-asdf-asdf')

        assert handler.get_handler('NONE') is None
        assert handler.get_handler('HELLO') is AsyncServiceBusHandler.hello
        assert handler.get_handler('RECEIVE') is AsyncServiceBusHandler.request
        assert handler.get_handler('SEND') is AsyncServiceBusHandler.request
        assert handler.get_handler('GET') is None
        assert handler.get_handler('PUT') is None

import logging

from typing import Any, Callable, Dict, Optional, Union, Tuple, Iterable, cast
from mypy_extensions import VarArg, KwArg

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType, ServiceBusSender, ServiceBusReceiver
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.amqp._amqp_message import DictMixin

from . import (
    AsyncMessageHandler,
    AsyncMessageRequestHandler,
    AsyncMessageRequest,
    AsyncMessageResponse,
    AsyncMessageError,
    register,
)

__all__ = [
    'AsyncServiceBusHandler',
]


handlers: Dict[str, AsyncMessageRequestHandler] = {}

GenericCacheValue = Union[ServiceBusSender, ServiceBusReceiver]
GenericCache = Dict[str, GenericCacheValue]
GenericInstance = Callable[[VarArg(Any)], GenericCacheValue]


class AsyncServiceBusHandler(AsyncMessageHandler):
    _sender_cache: Dict[str, ServiceBusSender]
    _receiver_cache: Dict[str, ServiceBusReceiver]

    client: Optional[ServiceBusClient] = None

    def __init__(self, worker: str) -> None:
        super().__init__(worker)

        self._sender_cache = {}
        self._receiver_cache = {}

        # silence uamqp loggers
        logging.getLogger('uamqp').setLevel(logging.ERROR)

    @classmethod
    def get_endpoint_details(cls, instance_type: str, endpoint: str) -> Tuple[str, str, Optional[str]]:
        if ':' not in endpoint:
            raise AsyncMessageError(f'"{endpoint}" is not prefixed with queue: or topic:')

        endpoint_type: str
        endpoint_name: str
        subscription_name: Optional[str] = None

        endpoint_type, endpoint_details = [v.strip() for v in endpoint.split(':', 1)]

        if endpoint_type not in ['queue', 'topic']:
            raise AsyncMessageError(f'only support for endpoint types queue and topic, not {endpoint_type}')

        if ',' in endpoint_details:
            if instance_type != 'receiver':
                raise AsyncMessageError(f'additional arguments in endpoint is not supported for {instance_type}')

            endpoint_name, endpoint_details = [v.strip() for v in endpoint_details.split(',', 1)]

            detail_type, detail_value = [v.strip() for v in endpoint_details.split(':', 1)]

            if detail_type != 'subscription':
                raise AsyncMessageError(f'argument {detail_type} is not supported')

            if len(detail_value) > 0:
                subscription_name = detail_value
        else:
            endpoint_name = endpoint_details

        if endpoint_type == 'topic' and subscription_name is None and instance_type == 'receiver':
            raise AsyncMessageError('endpoint needs to include subscription when receiving messages from a topic')

        return endpoint_type, endpoint_name, subscription_name

    @classmethod
    def get_sender_instance(cls, client: ServiceBusClient, endpoint: str) -> ServiceBusSender:
        arguments: Dict[str, Any] = {}
        endpoint_type, endpoint_name, _ = cls.get_endpoint_details('sender', endpoint)

        sender_type: Callable[[KwArg(Any)], ServiceBusSender]

        if endpoint_type == 'queue':
            arguments.update({'queue_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                client.get_queue_sender,
            )
        else:
            arguments.update({'topic_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                client.get_topic_sender,
            )

        return sender_type(**arguments)

    @classmethod
    def get_receiver_instance(cls, client: ServiceBusClient, endpoint: str, message_wait: Optional[int] = None) -> ServiceBusReceiver:
        arguments: Dict[str, Any] = {}
        endpoint_type, endpoint_name, subscription_name = cls.get_endpoint_details('receiver', endpoint)

        receiver_type: Callable[[KwArg(Any)], ServiceBusReceiver]

        if message_wait is not None:
            arguments.update({'max_wait_time': message_wait})

        if endpoint_type == 'queue':
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], client.get_queue_receiver)
            arguments.update({'queue_name': endpoint_name})
        else:
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], client.get_subscription_receiver)
            arguments.update({
                'topic_name': endpoint_name,
                'subscription_name': subscription_name,
            })

        return receiver_type(**arguments)

    @classmethod
    def from_message(cls, message: Optional[ServiceBusMessage]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        def to_dict(obj: Optional[DictMixin]) -> Dict[str, Any]:
            if obj is None:
                return {}

            result: Dict[str, Any] = {}

            for key, value in obj.items():
                result[key] = value

            return result

        if message is None:
            return None, None

        raw_amqp_message = message.raw_amqp_message
        metadata = to_dict(raw_amqp_message.properties)
        metadata.update(to_dict(raw_amqp_message.header))

        body = raw_amqp_message.body

        if raw_amqp_message.body_type == AmqpMessageBodyType.DATA:
            if isinstance(body, Iterable):
                payload = ''
                for buffer in body:
                    payload += buffer.decode('utf-8')
            else:
                payload = body.encode('utf-8')
        elif raw_amqp_message.body_type == AmqpMessageBodyType.SEQUENCE:
            payload = ''.join([v.encode('utf-8') for v in body])
        else:
            payload = str(body)

        return metadata, payload

    @register(handlers, 'HELLO')
    def hello(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        self.message_wait = context.get('message_wait', None)
        url = context['url']

        if self.client is None:
            if not url.startswith('Endpoint='):
                url = f'Endpoint={url}'

            self.client = ServiceBusClient.from_connection_string(
                conn_str=url,
                transport_type=TransportType.AmqpOverWebsocket,
            )

        instance_type = context['connection']

        cache: GenericCache
        endpoint = context['endpoint']
        arguments: Tuple[Any, ...] = (self.client, endpoint, )
        get_instance: GenericInstance

        if instance_type == 'sender':
            cache = cast(GenericCache, self._sender_cache)
            get_instance = cast(GenericInstance, self.get_sender_instance)
        elif instance_type == 'receiver':
            cache = cast(GenericCache, self._receiver_cache)
            arguments += (self.message_wait, )
            get_instance = cast(GenericInstance, self.get_receiver_instance)
        else:
            raise AsyncMessageError(f'"{instance_type}" is not a valid value for context.connection')

        if endpoint not in cache:
            cache.update({endpoint: get_instance(*arguments).__enter__()})

        return {
            'message': 'there general kenobi',
        }

    @register(handlers, 'SEND', 'RECEIVE')
    def request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        instance_type = context.get('connection', None)
        endpoint = context['endpoint']

        message: ServiceBusMessage
        metadata: Optional[Dict[str, Any]] = None
        payload = request.get('payload', None)

        if instance_type == 'sender':
            if payload is None:
                raise AsyncMessageError('no payload')
            sender = self._sender_cache.get(endpoint, None)
            if sender is None:
                raise AsyncMessageError(f'no HELLO sent for {endpoint}')

            message = ServiceBusMessage(payload)
            try:
                sender.send_messages(message)
            except Exception as e:
                raise AsyncMessageError('failed to send message') from e
        elif instance_type == 'receiver':
            if payload is not None:
                raise AsyncMessageError('payload not allowed')

            receiver = self._receiver_cache.get(endpoint, None)
            if receiver is None:
                raise AsyncMessageError(f'no HELLO sent for {endpoint}')
            try:
                message = cast(ServiceBusMessage, receiver.next())
                receiver.complete_message(message)
            except StopIteration:
                raise AsyncMessageError(f'no messages on {endpoint}')
        else:
            raise AsyncMessageError(f'"{instance_type}" is not a valid value for context.connection')

        metadata, payload = self.from_message(message)
        response_length = len(payload or '')

        return {
            'payload': payload,
            'metadata': metadata,
            'response_length': response_length,
        }

    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        return handlers.get(action, None)



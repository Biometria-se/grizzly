from typing import Any, Callable, Dict, Optional, Union, Tuple, Iterable, cast
from time import monotonic as time, sleep
from mypy_extensions import VarArg, KwArg

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType, ServiceBusSender, ServiceBusReceiver
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.amqp._amqp_message import DictMixin

from grizzly_extras.transformer import TransformerError, transformer, TransformerContentType

from ..arguments import parse_arguments, get_unsupported_arguments

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
    _arguments: Dict[str, Dict[str, str]]

    client: Optional[ServiceBusClient] = None

    def __init__(self, worker: str) -> None:
        super().__init__(worker)

        self._sender_cache = {}
        self._receiver_cache = {}
        self._arguments = {}

    @classmethod
    def get_sender_instance(cls, client: ServiceBusClient, arguments: Dict[str, str]) -> ServiceBusSender:
        endpoint_type = arguments['endpoint_type']
        endpoint_name = arguments['endpoint']

        sender_arguments: Dict[str, str] = {}

        sender_type: Callable[[KwArg(Any)], ServiceBusSender]

        if endpoint_type == 'queue':
            sender_arguments.update({'queue_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                client.get_queue_sender,
            )
        else:
            sender_arguments.update({'topic_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                client.get_topic_sender,
            )

        return sender_type(**sender_arguments)

    @classmethod
    def get_receiver_instance(cls, client: ServiceBusClient, arguments: Dict[str, str]) -> ServiceBusReceiver:
        endpoint_type = arguments['endpoint_type']
        endpoint_name = arguments['endpoint']
        subscription_name = arguments.get('subscription', None)
        message_wait = arguments.get('wait', None)

        receiver_arguments: Dict[str, Any] = {}
        receiver_type: Callable[[KwArg(Any)], ServiceBusReceiver]

        if message_wait is not None:
            receiver_arguments.update({'max_wait_time': int(message_wait)})

        if endpoint_type == 'queue':
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], client.get_queue_receiver)
            receiver_arguments.update({'queue_name': endpoint_name})
        else:
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], client.get_subscription_receiver)
            receiver_arguments.update({
                'topic_name': endpoint_name,
                'subscription_name': subscription_name,
            })

        return receiver_type(**receiver_arguments)

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

    @classmethod
    def get_endpoint_arguments(cls, instance_type: str, endpoint: str) -> Dict[str, str]:
        arguments = parse_arguments(endpoint, ':')

        if 'queue' not in arguments and 'topic' not in arguments:
            raise ValueError('endpoint needs to be prefixed with queue: or topic:')

        if 'queue' in arguments and 'topic' in arguments:
            raise ValueError('cannot specify both topic: and queue: in endpoint')

        endpoint_type = 'topic' if 'topic' in arguments else 'queue'

        if len(arguments) > 1:
            if endpoint_type != 'topic' and 'subscription' in arguments:
                raise ValueError('argument subscription is only allowed if endpoint is a topic')

            unsupported_arguments = get_unsupported_arguments(['topic', 'queue', 'subscription', 'expression'], arguments)

            if len(unsupported_arguments) > 0:
                raise ValueError(f'arguments {", ".join(unsupported_arguments)} is not supported')

        if endpoint_type == 'topic' and arguments.get('subscription', None) is None and instance_type == 'receiver':
            raise ValueError('endpoint needs to include subscription when receiving messages from a topic')

        if instance_type == 'sender' and arguments.get('expression', None) is not None:
            raise ValueError('argument expression is only allowed when receiving messages')

        arguments['endpoint_type'] = endpoint_type
        arguments['endpoint'] = arguments[endpoint_type]

        del arguments[endpoint_type]

        return arguments

    @register(handlers, 'HELLO')
    def hello(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        url = context['url']

        if self.client is None:
            if not url.startswith('Endpoint='):
                url = f'Endpoint={url}'

            self.client = ServiceBusClient.from_connection_string(
                conn_str=url,
                transport_type=TransportType.AmqpOverWebsocket,
            )

        endpoint = context['endpoint']
        instance_type = context['connection']
        message_wait = context.get('message_wait', None)

        arguments = self.get_endpoint_arguments(instance_type, endpoint)
        if message_wait is not None and instance_type == 'receiver':
            arguments['wait'] = str(message_wait)

        cache: GenericCache

        get_instance: GenericInstance

        if instance_type == 'sender':
            cache = cast(GenericCache, self._sender_cache)
            get_instance = cast(GenericInstance, self.get_sender_instance)
        elif instance_type == 'receiver':
            cache = cast(GenericCache, self._receiver_cache)
            get_instance = cast(GenericInstance, self.get_receiver_instance)
        else:
            raise AsyncMessageError(f'"{instance_type}" is not a valid value for context.connection')

        if endpoint not in cache:
            self._arguments[f'{instance_type}={endpoint}'] = arguments
            cache.update({endpoint: get_instance(self.client, arguments).__enter__()})

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
        endpoint_arguments = parse_arguments(endpoint, ':')
        request_arguments = dict(endpoint_arguments)

        try:
            del endpoint_arguments['expression']
        except:
            pass

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        message: Optional[ServiceBusMessage] = None
        metadata: Optional[Dict[str, Any]] = None
        payload = request.get('payload', None)

        if instance_type not in ['receiver', 'sender']:
            raise AsyncMessageError(f'"{instance_type}" is not a valid value for context.connection')

        arguments = self._arguments.get(f'{instance_type}={cache_endpoint}', None)

        if arguments is None:
            raise AsyncMessageError(f'no HELLO received for {cache_endpoint}')

        expression = request_arguments.get('expression', None)

        if instance_type == 'sender':
            if payload is None:
                raise AsyncMessageError('no payload')
            sender = self._sender_cache[cache_endpoint]
            message = ServiceBusMessage(payload)

            try:
                sender.send_messages(message)
            except Exception as e:
                raise AsyncMessageError(f'failed to send message: {str(e)}') from e
        elif instance_type == 'receiver':
            if payload is not None:
                raise AsyncMessageError('payload not allowed')

            receiver = self._receiver_cache[cache_endpoint]
            message_wait = int(request_arguments.get('message_wait', str(context.get('message_wait', 0))))

            try:
                wait_start = time()
                if expression is not None:
                    try:
                        content_type = TransformerContentType.from_string(cast(str, request.get('context', {})['content_type']))
                        transform = transformer.available[content_type]
                        get_values = transform.parser(request_arguments['expression'])
                    except Exception as e:
                        raise AsyncMessageError(str(e)) from e

                for received_message in receiver:
                    message = cast(ServiceBusMessage, received_message)

                    self.logger.debug(f'got message id: {message.message_id}')

                    if expression is None:
                        self.logger.debug(f'completing message id: {message.message_id}')
                        receiver.complete_message(message)
                        break

                    had_error = True
                    try:
                        metadata, payload = self.from_message(message)

                        if payload is None:
                            raise AsyncMessageError('no payload in message')

                        try:
                            _, transformed_payload = transform.transform(content_type, payload)
                        except TransformerError as e:
                            self.logger.error(payload)
                            raise AsyncMessageError(e.message)

                        values = get_values(transformed_payload)

                        self.logger.debug(f'expression={request_arguments["expression"]}, matches={values}, payload={transformed_payload}')

                        if len(values) > 0:
                            self.logger.debug(f'completing message id: {message.message_id}, with expression "{request_arguments["expression"]}"')
                            receiver.complete_message(message)
                            had_error = False
                            break
                    except:
                        raise
                    finally:
                        if had_error:
                            if message is not None:
                                self.logger.debug(f'abandoning message id: {message.message_id}, {message._raw_amqp_message.header.delivery_count}')
                                receiver.abandon_message(message)
                                message = None

                            wait_now = time()
                            if message_wait > 0 and wait_now - wait_start >= message_wait:
                                raise StopIteration()

                            sleep(0.2)

                if message is None:
                    raise StopIteration()

            except StopIteration:
                error_message = f'no messages on {endpoint}'
                message = None
                if message_wait > 0:
                    error_message = f'{error_message} within {message_wait} seconds'
                raise AsyncMessageError(error_message)

        if expression is None:
            metadata, payload = self.from_message(message)

        response_length = len(payload or '')

        return {
            'payload': payload,
            'metadata': metadata,
            'response_length': response_length,
        }

    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        return handlers.get(action, None)



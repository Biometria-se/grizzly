import logging

from typing import Any, Callable, Dict, Optional, Union, Tuple, Iterable, cast
from time import perf_counter, sleep

from mypy_extensions import VarArg, KwArg

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType, ServiceBusSender, ServiceBusReceiver, ServiceBusReceivedMessage
from azure.servicebus.management import ServiceBusAdministrationClient, TopicProperties, SqlRuleFilter
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.amqp._amqp_message import DictMixin
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError

from grizzly_extras.transformer import TransformerError, transformer, TransformerContentType
from grizzly_extras.arguments import parse_arguments, get_unsupported_arguments

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

    _client: Optional[ServiceBusClient] = None
    mgmt_client: Optional[ServiceBusAdministrationClient] = None

    def __init__(self, worker: str) -> None:
        super().__init__(worker)

        self._sender_cache = {}
        self._receiver_cache = {}
        self._arguments = {}

    @property
    def client(self) -> ServiceBusClient:
        if self._client is None:
            raise AttributeError('no client')

        return self._client

    @client.setter
    def client(self, value: ServiceBusClient) -> None:
        self._client = value

    def close(self, soft: bool = False) -> None:
        self.logger.debug(f'close: {soft=}')
        if not soft:
            for key, sender in self._sender_cache.items():
                self.logger.debug(f'closing sender {key}')
                sender.close()

            self._sender_cache.clear()

            for key, receiver in self._receiver_cache.items():
                self.logger.debug(f'closing receiver {key}')
                receiver.close()

            self._receiver_cache.clear()

        if len(self._sender_cache) + len(self._receiver_cache) == 0:
            self.logger.debug('no senders or receivers left, close ServiceBus clients')
            if self._client is not None:
                self.logger.debug('closing client')
                self.client.close()

            if self.mgmt_client is not None:
                self.logger.debug('closing management client')
                self.mgmt_client.close()

    def get_sender_instance(self, arguments: Dict[str, str]) -> ServiceBusSender:
        endpoint_type = arguments['endpoint_type']
        endpoint_name = arguments['endpoint']

        sender_arguments: Dict[str, str] = {'client_identifier': self.worker}

        sender_type: Callable[[KwArg(Any)], ServiceBusSender]

        if endpoint_type == 'queue':
            sender_arguments.update({'queue_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                self.client.get_queue_sender,
            )
        else:
            sender_arguments.update({'topic_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                self.client.get_topic_sender,
            )

        return sender_type(**sender_arguments)

    def get_receiver_instance(self, arguments: Dict[str, str]) -> ServiceBusReceiver:
        endpoint_type = arguments['endpoint_type']
        endpoint_name = arguments['endpoint']
        subscription_name = arguments.get('subscription', None)
        message_wait = arguments.get('wait', None)

        receiver_arguments: Dict[str, Any] = {
            'client_identifier': self.worker,
        }
        receiver_type: Callable[[KwArg(Any)], ServiceBusReceiver]

        if message_wait is not None:
            receiver_arguments.update({'max_wait_time': int(message_wait)})

        if endpoint_type == 'queue':
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], self.client.get_queue_receiver)
            receiver_arguments.update({'queue_name': endpoint_name})
        else:
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], self.client.get_subscription_receiver)
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
        return self._hello(request, force=False)

    @register(handlers, 'DISCONNECT')
    def disconnect(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        endpoint = context['endpoint']
        instance_type = context['connection']
        message_wait = context.get('message_wait', None)
        arguments = self.get_endpoint_arguments(instance_type, endpoint)
        endpoint_arguments = parse_arguments(endpoint, ':')
        try:
            del endpoint_arguments['expression']
        except:
            pass

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        if message_wait is not None and instance_type == 'receiver':
            arguments['wait'] = str(message_wait)

        cache: GenericCache

        if instance_type == 'sender':
            cache = cast(GenericCache, self._sender_cache)
        elif instance_type == 'receiver':
            cache = cast(GenericCache, self._receiver_cache)
        else:
            raise AsyncMessageError(f'"{instance_type}" is not a valid value for context.connection')

        if cache_endpoint in cache:
            instance = cache.get(cache_endpoint, None)
            if instance is not None:
                try:
                    self.logger.info(f'disconnecting {instance_type} instance for {cache_endpoint}')
                    instance.__exit__()
                except:
                    pass

            try:
                del cache[cache_endpoint]
            except:  # pragma: no cover
                pass

            self.close(soft=True)

        return {
            'message': 'thanks for all the fish',
        }

    @register(handlers, 'SUBSCRIBE')
    def subscribe(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        endpoint = context['endpoint']
        instance_type = context['connection']
        arguments = self.get_endpoint_arguments(instance_type, endpoint)

        if arguments.get('endpoint_type', None) != 'topic':
            raise AsyncMessageError('subscriptions is only allowed on topics')

        topic_name = arguments['endpoint']
        subscription_name = arguments['subscription']
        rule_name = 'grizzly'
        rule_text = request.get('payload', None)

        if rule_text is None:
            raise AsyncMessageError('no rule text in request')

        if self.mgmt_client is None:
            url = context['url']
            if not url.startswith('Endpoint='):
                url = f'Endpoint={url}'

            self.mgmt_client = ServiceBusAdministrationClient.from_connection_string(conn_str=url)

        topic: Optional[TopicProperties] = None

        try:
            topic = self.mgmt_client.get_topic(topic_name=topic_name)
        except ResourceNotFoundError:
            topic = None

        if topic is None:
            raise AsyncMessageError(f'topic "{topic_name}" does not exist')

        try:
            self.mgmt_client.get_subscription(topic_name=topic_name, subscription_name=subscription_name)
        except ResourceNotFoundError:
            self.mgmt_client.create_subscription(topic_name=topic_name, subscription_name=subscription_name)

        try:
            self.mgmt_client.delete_rule(
                topic_name=topic_name,
                subscription_name=subscription_name,
                rule_name='$Default',
            )
        except ResourceNotFoundError:
            pass

        try:
            rule = self.mgmt_client.create_rule(
                topic_name=topic_name,
                subscription_name=subscription_name,
                rule_name=rule_name,
            )
        except ResourceExistsError:
            rule = self.mgmt_client.get_rule(
                topic_name=topic_name,
                subscription_name=subscription_name,
                rule_name=rule_name,
            )

        rule.action = None
        rule.filter = SqlRuleFilter(rule_text)

        self.mgmt_client.update_rule(
            topic_name=topic_name,
            subscription_name=subscription_name,
            rule=rule,
        )

        return {
            'message': f'created subscription {subscription_name} on topic {topic_name}'
        }

    @register(handlers, 'UNSUBSCRIBE')
    def unsubscribe(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        endpoint = context['endpoint']
        instance_type = context['connection']
        arguments = self.get_endpoint_arguments(instance_type, endpoint)

        if arguments['endpoint_type'] != 'topic':
            raise AsyncMessageError('subscriptions is only allowed on topics')

        topic_name = arguments['endpoint']
        subscription_name = arguments['subscription']

        if self.mgmt_client is None:
            url = context['url']
            if not url.startswith('Endpoint='):
                url = f'Endpoint={url}'

            self.mgmt_client = ServiceBusAdministrationClient.from_connection_string(conn_str=url)

        topic: Optional[TopicProperties] = None

        try:
            topic = self.mgmt_client.get_topic(topic_name=topic_name)
        except ResourceNotFoundError:
            topic = None

        if topic is None:
            raise AsyncMessageError(f'topic "{topic_name}" does not exist')

        try:
            self.mgmt_client.get_subscription(topic_name=topic_name, subscription_name=subscription_name)
        except ResourceNotFoundError:
            raise AsyncMessageError(f'subscription "{subscription_name}" does not exist on topic "{topic_name}"')

        self.mgmt_client.delete_subscription(
            topic_name=topic_name,
            subscription_name=subscription_name,
        )

        return {
            'message': f'removed subscription {subscription_name} on topic {topic_name}'
        }

    def _hello(self, request: AsyncMessageRequest, force: bool) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            raise AsyncMessageError('no context in request')

        url = context['url']
        if not url.startswith('Endpoint='):
            url = f'Endpoint={url}'

        if self._client is None:
            self.client = ServiceBusClient.from_connection_string(
                conn_str=url,
                transport_type=TransportType.AmqpOverWebsocket,
            )

        if self.mgmt_client is None and self.logger._logger.level == logging.DEBUG:
            self.mgmt_client = ServiceBusAdministrationClient.from_connection_string(conn_str=url)

        endpoint = context['endpoint']
        instance_type = context['connection']
        message_wait = context.get('message_wait', None)
        arguments = self.get_endpoint_arguments(instance_type, endpoint)
        endpoint_arguments = parse_arguments(endpoint, ':')

        try:
            del endpoint_arguments['expression']
        except:
            pass

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        if instance_type == 'receiver' and message_wait is not None:
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

        if cache_endpoint not in cache or force:
            self._arguments.update({f'{instance_type}={cache_endpoint}': arguments})
            instance = cache.get(cache_endpoint, None)
            # clean up stale instance
            if instance is not None:
                try:
                    self.logger.info(f'cleaning up stale {instance_type} instance for {cache_endpoint}')
                    instance.__exit__()
                except:
                    pass

            cache.update({cache_endpoint: get_instance(arguments).__enter__()})

            self.logger.debug(f'cached {instance_type} instance for {cache_endpoint}')

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

        self.logger.info(f'handling request towards {cache_endpoint}')

        message: Optional[ServiceBusMessage] = None
        metadata: Optional[Dict[str, Any]] = None
        payload = request.get('payload', None)
        client = request.get('client', -1)

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
            consume = context.get('consume', False)

            wait_start = perf_counter()

            # reset last activity timestamp, might be set from previous usage that was more
            # than message_wait ago, which will cause a "timeout" when trying to read it now
            # which means we'll not get any messages, even though the endpoint isn't empty
            if message_wait > 0:
                receiver._handler._last_activity_timestamp = None

            for retry in range(1, 4):
                try:
                    if expression is not None:
                        try:
                            content_type = TransformerContentType.from_string(cast(str, request.get('context', {})['content_type']))
                            transform = transformer.available[content_type]
                            get_values = transform.parser(request_arguments['expression'])
                        except Exception as e:
                            raise AsyncMessageError(str(e)) from e

                    for received_message in receiver:
                        message = cast(ServiceBusReceivedMessage, received_message)

                        self.logger.debug(f'{client}::{cache_endpoint}: got message id {message.message_id}')

                        if expression is None:
                            self.logger.debug(f'{client}::{cache_endpoint}: completing message id {message.message_id}')
                            receiver.complete_message(message)
                            break

                        had_error = True
                        try:
                            metadata, payload = self.from_message(message)

                            if payload is None:
                                raise AsyncMessageError('no payload in message')

                            try:
                                transformed_payload = transform.transform(payload)
                            except TransformerError as e:
                                self.logger.error(payload)
                                raise AsyncMessageError(e.message)

                            values = get_values(transformed_payload)

                            self.logger.debug(f'{client}::{cache_endpoint}: expression={request_arguments["expression"]}, matches={values}, payload={transformed_payload}')

                            if len(values) > 0:
                                self.logger.debug(f'{client}::{cache_endpoint}: completing message id {message.message_id}, with expression "{request_arguments["expression"]}"')
                                receiver.complete_message(message)
                                had_error = False
                                break
                        except:
                            raise
                        finally:
                            if had_error:
                                if message is not None:
                                    if not consume:
                                        self.logger.debug(
                                            f'{client}::{cache_endpoint}: abandoning message id {message.message_id}, {message._raw_amqp_message.header.delivery_count}',
                                        )
                                        receiver.abandon_message(message)
                                        message = None
                                    else:
                                        self.logger.debug(f'{client}::{cache_endpoint}: consuming and ignoring message id {message.message_id}')
                                        receiver.complete_message(message)  # remove message from endpoint, but ignore contents
                                        message = payload = metadata = None

                                if message_wait > 0 and (perf_counter() - wait_start) >= message_wait:
                                    raise StopIteration()

                                sleep(0.2)

                    if message is None:
                        raise StopIteration()

                    break
                except StopIteration:
                    delta = perf_counter() - wait_start

                    if message_wait > 0:
                        if delta >= message_wait:
                            error_message = f'no messages on {endpoint}'
                            message = None
                            if message_wait > 0:
                                error_message = f'{error_message} within {message_wait} seconds'
                        elif consume and expression is not None:
                            self.logger.debug(f'{client}::{cache_endpoint}: waiting for more messages')
                            continue
                        else:
                            # ugly brute-force way of handling no messages on service bus
                            if retry < 3:
                                self.logger.warning(f'receiver for {client}::{cache_endpoint} returned no message without trying, brute-force retry #{retry}')
                                # <!-- useful debugging information, actual message count on message entity
                                if self.logger._logger.level == logging.DEBUG and self.mgmt_client is not None:
                                    if 'topic' in endpoint_arguments:
                                        topic_properties = self.mgmt_client.get_subscription_runtime_properties(
                                            topic_name=endpoint_arguments['topic'],
                                            subscription_name=endpoint_arguments['subscription']
                                        )
                                        self.logger.debug((
                                            f'{cache_endpoint}: {topic_properties.active_message_count=}, '
                                            f'{topic_properties.total_message_count=}, {topic_properties.transfer_dead_letter_message_count=}, '
                                            f'{topic_properties.transfer_message_count=}'
                                        ))
                                    elif 'queue' in endpoint_arguments:
                                        queue_properties = self.mgmt_client.get_queue_runtime_properties(
                                            queue_name=endpoint_arguments['queue'],
                                        )
                                        self.logger.debug((
                                            f'{cache_endpoint}: {queue_properties.active_message_count=}, '
                                            f'{queue_properties.total_message_count=}, {queue_properties.transfer_dead_letter_message_count=}, '
                                            f'{queue_properties.transfer_message_count=}'
                                        ))
                                # // useful debugging information -->

                                self._hello(request, force=True)
                                receiver = self._receiver_cache[cache_endpoint]
                                message = None
                                continue
                    else:
                        error_message = f'{endpoint} receiver returned no messages, without trying'

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

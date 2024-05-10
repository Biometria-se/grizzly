"""ServiceBus handler implementation for async-messaged."""
from __future__ import annotations

import logging
from contextlib import suppress
from time import perf_counter, sleep, time
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, Optional, Tuple, Union, cast
from urllib.parse import urlparse

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiver, ServiceBusSender, TransportType
from azure.servicebus._pyamqp import ReceiveClient
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.management import ServiceBusAdministrationClient, SqlRuleFilter, TopicProperties

from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments
from grizzly_extras.azure.aad import AuthMethod, AzureAadCredential
from grizzly_extras.transformer import TransformerContentType, TransformerError, transformer

from . import (
    AsyncMessageContext,
    AsyncMessageError,
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageRequestHandler,
    AsyncMessageResponse,
    register,
)

if TYPE_CHECKING:  # pragma: no cover
    from azure.servicebus.amqp._amqp_message import DictMixin

__all__ = [
    'AsyncServiceBusHandler',
]


handlers: Dict[str, AsyncMessageRequestHandler] = {}

GenericCacheValue = Union[ServiceBusSender, ServiceBusReceiver]
GenericCache = Dict[str, GenericCacheValue]
GenericInstance = Callable[..., GenericCacheValue]


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
            message = 'no client'
            raise AttributeError(message)

        return self._client

    @client.setter
    def client(self, value: ServiceBusClient) -> None:
        self._client = value

    def close(self, *, soft: bool = False) -> None:
        self.logger.debug('close: soft=%r', soft)
        if not soft:
            for key, sender in self._sender_cache.items():
                self.logger.debug('closing sender %s', key)
                sender.close()

            self._sender_cache.clear()

            for key, receiver in self._receiver_cache.items():
                self.logger.debug('closing receiver %s', key)
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

        sender_type: Callable[..., ServiceBusSender]

        if endpoint_type == 'queue':
            sender_arguments.update({'queue_name': endpoint_name})
            sender_type = cast(
                Callable[..., ServiceBusSender],
                self.client.get_queue_sender,
            )
        else:
            sender_arguments.update({'topic_name': endpoint_name})
            sender_type = cast(
                Callable[..., ServiceBusSender],
                self.client.get_topic_sender,
            )

        return sender_type(**sender_arguments)

    def get_receiver_instance(self, arguments: Dict[str, str]) -> ServiceBusReceiver:
        endpoint_type = arguments['endpoint_type']
        endpoint_name = arguments['endpoint']
        subscription_name = arguments.get('subscription')
        message_wait = arguments.get('wait')

        receiver_arguments: Dict[str, Any] = {
            'client_identifier': self.worker,
        }
        receiver_type: Callable[..., ServiceBusReceiver]

        if message_wait is not None:
            receiver_arguments.update({'max_wait_time': int(message_wait)})

        if endpoint_type == 'queue':
            receiver_type = cast(Callable[..., ServiceBusReceiver], self.client.get_queue_receiver)
            receiver_arguments.update({'queue_name': endpoint_name})
        else:
            receiver_type = cast(Callable[..., ServiceBusReceiver], self.client.get_subscription_receiver)
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
            message = 'endpoint needs to be prefixed with queue: or topic:'
            raise ValueError(message)

        if 'queue' in arguments and 'topic' in arguments:
            message = 'cannot specify both topic: and queue: in endpoint'
            raise ValueError(message)

        endpoint_type = 'topic' if 'topic' in arguments else 'queue'

        if len(arguments) > 1:
            if endpoint_type != 'topic' and 'subscription' in arguments:
                message = 'argument subscription is only allowed if endpoint is a topic'
                raise ValueError(message)

            unsupported_arguments = get_unsupported_arguments(['topic', 'queue', 'subscription', 'expression'], arguments)

            if len(unsupported_arguments) > 0:
                message = f'arguments {", ".join(unsupported_arguments)} is not supported'
                raise ValueError(message)

        if endpoint_type == 'topic' and arguments.get('subscription', None) is None and instance_type == 'receiver':
            message = 'endpoint needs to include subscription when receiving messages from a topic'
            raise ValueError(message)

        if instance_type == 'sender' and arguments.get('expression', None) is not None:
            message = 'argument expression is only allowed when receiving messages'
            raise ValueError(message)

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
            message = 'no context in request'
            raise AsyncMessageError(message)

        endpoint = context['endpoint']
        instance_type = context['connection']
        message_wait = context.get('message_wait', None)
        arguments = self.get_endpoint_arguments(instance_type, endpoint)
        endpoint_arguments = parse_arguments(endpoint, ':')
        with suppress(KeyError):
            del endpoint_arguments['expression']

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        if message_wait is not None and instance_type == 'receiver':
            arguments['wait'] = str(message_wait)

        cache: GenericCache

        if instance_type == 'sender':
            cache = cast(GenericCache, self._sender_cache)
        elif instance_type == 'receiver':
            cache = cast(GenericCache, self._receiver_cache)
        else:
            message = f'"{instance_type}" is not a valid value for context.connection'
            raise AsyncMessageError(message)

        if cache_endpoint in cache:
            instance = cache.get(cache_endpoint, None)
            if instance is not None:
                with suppress(Exception):
                    self.logger.info('disconnecting %s instance for %s', instance_type, cache_endpoint)
                    instance.__exit__()

            with suppress(KeyError):
                del cache[cache_endpoint]

            self.close(soft=True)

        return {
            'message': 'thanks for all the fish',
        }

    @register(handlers, 'SUBSCRIBE')
    def subscribe(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            message = 'no context in request'
            raise AsyncMessageError(message)

        endpoint = context['endpoint']
        instance_type = context['connection']
        arguments = self.get_endpoint_arguments(instance_type, endpoint)

        if arguments.get('endpoint_type', None) != 'topic':
            message = 'subscriptions is only allowed on topics'
            raise AsyncMessageError(message)

        topic_name = arguments['endpoint']
        subscription_name = arguments['subscription']
        rule_name = 'grizzly'
        rule_text = request.get('payload', None)

        if rule_text is None:
            message = 'no rule text in request'
            raise AsyncMessageError(message)

        self._prepare_clients(context, only_mgmt=True)

        if self.mgmt_client is None:
            message = 'no mgmt client found'
            raise AsyncMessageError(message)

        topic: Optional[TopicProperties] = None

        try:
            topic = self.mgmt_client.get_topic(topic_name=topic_name)
        except ResourceNotFoundError:
            topic = None

        if topic is None:
            message = f'topic "{topic_name}" does not exist'
            raise AsyncMessageError(message)

        try:
            self.mgmt_client.get_subscription(topic_name=topic_name, subscription_name=subscription_name)
        except ResourceNotFoundError:
            self.mgmt_client.create_subscription(topic_name=topic_name, subscription_name=subscription_name)

        with suppress(ResourceNotFoundError):
            self.mgmt_client.delete_rule(
                topic_name=topic_name,
                subscription_name=subscription_name,
                rule_name='$Default',
            )

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
            'message': f'created subscription {subscription_name} on topic {topic_name}',
        }

    @register(handlers, 'UNSUBSCRIBE')
    def unsubscribe(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
        context = request.get('context', None)
        if context is None:
            message = 'no context in request'
            raise AsyncMessageError(message)

        endpoint = context['endpoint']
        instance_type = context['connection']
        arguments = self.get_endpoint_arguments(instance_type, endpoint)

        if arguments['endpoint_type'] != 'topic':
            message = 'subscriptions is only allowed on topics'
            raise AsyncMessageError(message)

        topic_name = arguments['endpoint']
        subscription_name = arguments['subscription']

        self._prepare_clients(context, only_mgmt=True)

        if self.mgmt_client is None:
            message = 'no mgmt client found'
            raise AsyncMessageError(message)


        topic: Optional[TopicProperties] = None

        try:
            topic = self.mgmt_client.get_topic(topic_name=topic_name)
        except ResourceNotFoundError:
            topic = None

        if topic is None:
            message = f'topic "{topic_name}" does not exist'
            raise AsyncMessageError(message)

        try:
            self.mgmt_client.get_subscription(topic_name=topic_name, subscription_name=subscription_name)
        except ResourceNotFoundError as e:
            message = f'subscription "{subscription_name}" does not exist on topic "{topic_name}"'
            raise AsyncMessageError(message) from e

        try:
            topic_properties = self.mgmt_client.get_subscription_runtime_properties(
                topic_name=topic_name,
                subscription_name=subscription_name,
            )

            topic_statistics = (
                f'active_message_count={topic_properties.active_message_count}, '
                f'total_message_count={topic_properties.total_message_count}, transfer_dead_letter_message_count={topic_properties.transfer_dead_letter_message_count}, '
                f'transfer_message_count={topic_properties.transfer_message_count}'
            )
        except Exception:
            self.logger.exception('failed to get topic statistics for %s@%s', subscription_name, topic_name)
            topic_statistics = 'unknown'

        self.mgmt_client.delete_subscription(
            topic_name=topic_name,
            subscription_name=subscription_name,
        )

        return {
            'message': f'removed subscription {subscription_name} on topic {topic_name} (stats: {topic_statistics})',
        }

    def _prepare_clients(self, context: AsyncMessageContext, *, only_mgmt: bool = False) -> None:
        username = context.get('username')
        password = context.get('password')
        url = context['url']

        effective_level = self.logger.getEffectiveLevel()

        if username is None and password is None:
            if not url.startswith('Endpoint='):
                url = f'Endpoint={url}'

            if not only_mgmt:
                self.client = self._client or ServiceBusClient.from_connection_string(
                    conn_str=url,
                    transport_type=TransportType.AmqpOverWebsocket,
                )

            if self.mgmt_client is None and (effective_level == logging.DEBUG or only_mgmt):
                self.mgmt_client = ServiceBusAdministrationClient.from_connection_string(conn_str=url)
        else:
            tenant = context.get('tenant')
            if tenant is None:
                message = 'no tenant in context'
                raise AsyncMessageError(message)

            password = cast(str, password)

            url_parsed = urlparse(url)
            fully_qualified_namespace = f'{url_parsed.hostname}'

            if not fully_qualified_namespace.endswith('.servicebus.windows.net'):
                fully_qualified_namespace = f'{fully_qualified_namespace}.servicebus.windows.net'

            credential = AzureAadCredential(username, password, tenant, AuthMethod.USER, host=url)
            if not only_mgmt:
                self.client = self._client or ServiceBusClient(fully_qualified_namespace, credential=credential, transport_type=TransportType.AmqpOverWebsocket)

            if self.mgmt_client is None and (effective_level == logging.DEBUG or only_mgmt):
                credential = AzureAadCredential(username, password, tenant, AuthMethod.USER, host=url)
                self.mgmt_client = ServiceBusAdministrationClient(fully_qualified_namespace, credential=credential)

    def _hello(self, request: AsyncMessageRequest, *, force: bool) -> AsyncMessageResponse:  # noqa: PLR0915
        context = request.get('context', None)
        if context is None:
            message = 'no context in request'
            raise AsyncMessageError(message)

        self._prepare_clients(context)

        endpoint = context['endpoint']
        instance_type = context['connection']
        message_wait = context.get('message_wait', None)
        arguments = self.get_endpoint_arguments(instance_type, endpoint)
        endpoint_arguments = parse_arguments(endpoint, ':')

        with suppress(KeyError):
            del endpoint_arguments['expression']

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
            message = f'"{instance_type}" is not a valid value for context.connection'
            raise AsyncMessageError(message)

        if cache_endpoint not in cache or force:
            self._arguments.update({f'{instance_type}={cache_endpoint}': arguments})
            instance = cache.get(cache_endpoint, None)
            # clean up stale instance
            if instance is not None:
                with suppress(Exception):
                    self.logger.info('cleaning up stale %s instance for %s', instance_type, cache_endpoint)
                    instance.__exit__()

            """
            Timeout when creating an instance in `azure.servicebus` is not handled very well...
            We'll workaround it by re-trying with a backoff delay.
            """
            retries = _retries = 3
            delay = 0.5
            backoff = 1.7

            while retries > 0:
                try:
                    cache.update({cache_endpoint: get_instance(arguments).__enter__()})

                    # all good, exit while-loop
                    retries = 0
                except TypeError as e:
                    if "'NoneType' is not subscriptable" not in str(e):
                        raise

                    self.logger.warning('hello failed: service bus connection timed out, retry %d in %0.2f seconds', (_retries - retries) + 1, delay)
                    sleep(delay)
                    retries -= 1
                    delay *= backoff

                    # bail out
                    if retries == 0:
                        message = f'hello failed, creating service bus connection timed out {_retries} times'
                        raise AsyncMessageError(message) from e

                self.logger.debug('cached %s instance for %s', instance_type, cache_endpoint)

        return {
            'message': 'there general kenobi',
        }

    @register(handlers, 'SEND', 'RECEIVE')
    def request(self, request: AsyncMessageRequest) -> AsyncMessageResponse:  # noqa: C901, PLR0912, PLR0915
        context = request.get('context', None)
        if context is None:
            msg = 'no context in request'
            raise AsyncMessageError(msg)

        instance_type = context.get('connection', None)
        endpoint = context['endpoint']
        endpoint_arguments = parse_arguments(endpoint, ':')
        request_arguments = dict(endpoint_arguments)

        with suppress(KeyError):
            del endpoint_arguments['expression']

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        self.logger.info('handling request towards %s', cache_endpoint)

        message: Optional[ServiceBusMessage] = None
        metadata: Optional[Dict[str, Any]] = None
        payload = request.get('payload', None)
        client = request.get('client', -1)

        if instance_type not in ['receiver', 'sender']:
            msg = f'"{instance_type}" is not a valid value for context.connection'
            raise AsyncMessageError(msg)

        arguments = self._arguments.get(f'{instance_type}={cache_endpoint}', None)

        if arguments is None:
            msg = f'no HELLO received for {cache_endpoint}'
            raise AsyncMessageError(msg)

        expression = request_arguments.get('expression')

        if instance_type == 'sender':
            if payload is None:
                msg = 'no payload'
                raise AsyncMessageError(msg)
            sender = self._sender_cache[cache_endpoint]

            message = ServiceBusMessage(payload)

            try:
                sender.send_messages(message)
            except Exception as e:
                msg = f'failed to send message: {e!s}'
                raise AsyncMessageError(msg) from e
        elif instance_type == 'receiver':
            if payload is not None:
                msg = 'payload not allowed'
                raise AsyncMessageError(msg)

            receiver = self._receiver_cache[cache_endpoint]
            message_wait = int(request_arguments.get('message_wait', str(context.get('message_wait', 0))))
            consume = context.get('consume', False)

            wait_start = perf_counter()

            # make sure receiver or it's handler hasn't gone stale
            if receiver is None or receiver._handler is None:
                self._hello(request, force=True)
                receiver = self._receiver_cache[cache_endpoint]

            # reset last activity timestamp, might be set from previous usage that was more
            # than message_wait ago, which will cause a "timeout" when trying to read it now
            # which means we'll not get any messages, even though the endpoint isn't empty
            if message_wait > 0:
                receiver._handler._last_activity_timestamp = time() if isinstance(receiver._handler, ReceiveClient) else None

            for retry in range(1, 4):
                try:
                    if expression is not None:
                        try:
                            content_type = TransformerContentType.from_string(cast(str, request.get('context', {})['content_type']))
                            transform = transformer.available[content_type]
                            get_values = transform.parser(expression)
                        except Exception as e:
                            raise AsyncMessageError(str(e)) from e

                    for received_message in receiver:
                        message = received_message

                        self.logger.debug('%d::%s: got message id %s', client, cache_endpoint, message.message_id)

                        if expression is None:
                            self.logger.debug('%d::%s: completing message id %s', client, cache_endpoint, message.message_id)
                            receiver.complete_message(message)
                            break

                        had_error = True
                        try:
                            metadata, payload = self.from_message(message)

                            if payload is None:
                                msg = 'no payload in message'
                                raise AsyncMessageError(msg)

                            try:
                                transformed_payload = transform.transform(payload)
                            except TransformerError as e:
                                self.logger.exception(payload)
                                raise AsyncMessageError(e.message) from e

                            values = get_values(transformed_payload)

                            self.logger.debug('%d::%s: expression=%s, matches=%r, payload=%s', client, cache_endpoint, request_arguments['expression'], values, transformed_payload)

                            if len(values) > 0:
                                self.logger.debug(
                                    '%d::%s: completing message id %s, with expression "%s"', client, cache_endpoint, message.message_id, request_arguments['expression'],
                                )
                                receiver.complete_message(message)
                                had_error = False
                                break
                        finally:
                            if had_error:
                                if message is not None:
                                    if not consume:
                                        self.logger.debug(
                                            '%d::%s: abandoning message id %s, %d', client, cache_endpoint, message.message_id, message._raw_amqp_message.header.delivery_count,
                                        )
                                        receiver.abandon_message(message)
                                        message = None
                                    else:
                                        self.logger.debug('%d::%s: consuming and ignoring message id %s', client, cache_endpoint, message.message_id)
                                        receiver.complete_message(message)  # remove message from endpoint, but ignore contents
                                        message = payload = metadata = None

                                # do not wait any longer, give up due to message_wait
                                if message_wait > 0 and (perf_counter() - wait_start) >= message_wait:
                                    raise StopIteration

                                sleep(0.2)

                    if message is None:
                        raise StopIteration

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
                            self.logger.debug('%d::%s: waiting for more messages', client, cache_endpoint)
                            continue
                        else:  # noqa: PLR5501
                            # ugly brute-force way of handling no messages on service bus
                            if retry < 3:
                                self.logger.warning('receiver for %d::%s returned no message without trying, brute-force retry #%d', client, cache_endpoint, retry)
                                # <!-- useful debugging information, actual message count on message entity
                                if self.logger.getEffectiveLevel() == logging.DEBUG and self.mgmt_client is not None:  # pragma: no cover
                                    if 'topic' in endpoint_arguments:
                                        topic_properties = self.mgmt_client.get_subscription_runtime_properties(
                                            topic_name=endpoint_arguments['topic'],
                                            subscription_name=endpoint_arguments['subscription'],
                                        )
                                        msg = (
                                            f'{cache_endpoint}: {topic_properties.active_message_count=}, '
                                            f'{topic_properties.total_message_count=}, {topic_properties.transfer_dead_letter_message_count=}, '
                                            f'{topic_properties.transfer_message_count=}'
                                        )
                                        self.logger.debug(msg)
                                    elif 'queue' in endpoint_arguments:
                                        queue_properties = self.mgmt_client.get_queue_runtime_properties(
                                            queue_name=endpoint_arguments['queue'],
                                        )
                                        msg = (
                                            f'{cache_endpoint}: {queue_properties.active_message_count=}, '
                                            f'{queue_properties.total_message_count=}, {queue_properties.transfer_dead_letter_message_count=}, '
                                            f'{queue_properties.transfer_message_count=}'
                                        )
                                        self.logger.debug(msg)
                                # // useful debugging information -->

                                self._hello(request, force=True)
                                receiver = self._receiver_cache[cache_endpoint]
                                message = None
                                continue
                    else:
                        error_message = f'{endpoint} receiver returned no messages, without trying'

                    raise AsyncMessageError(error_message) from None

        if expression is None:
            metadata, payload = self.from_message(message)

        response_length = len(payload or '')

        return {
            'payload': payload,
            'metadata': metadata,
            'response_length': response_length,
        }

    def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
        return handlers.get(action)

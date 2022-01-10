# pylint: disable=line-too-long
'''Listens for messages on Azure Service Bus queue or topic.

Use [transformer task](/grizzly/usage/tasks/transformer/) to extract specific parts of the message.

## Format

Initial value for a variable must have the prefix `queue:` or `topic:` followed by the name of the targeted
type. When receiving messages from a topic, the argument `subscription:` is mandatory. The format of endpoint is:

```plain
[queue|topic]:<endpoint name>[, subscription:<subscription name>][, expression:<expression>]
```

Where `<expression>` can be a XPath or jsonpath expression, depending on the specified content type. This argument is only allowed when
receiving messages. See example below.

> **Warning**: Do not use `expression` to filter messages unless you do not care about the messages that does not match the expression. If
> you do care about them, you should setup a subscription to do the filtering in Azure.

Arguments support templating for their value, but not the complete endpoint value.

Examples:

```plain
queue:test-queue
topic:test-topic, subscription:test-subscription
queue:"$conf::sb.endpoint.queue"
topic:"$conf::sb.endpoint.topic", subscription:"$conf::sb.endpoint.subscription"
queue:"{{ queue_name }}", expression="$.document[?(@.name=='TPM report')]"
```

## Arguments

* `repeat` _bool_ (optional) - if `True`, values read from the endpoint will be saved in a list and re-used if there are no new messages available
* `url` _str_ - see format of url below.
* `wait` _int_ - number of seconds to wait for a message on the queue
* `content_type` _str_ (optional) - specify the MIME type of the message received on the queue, only mandatory when `expression` is specified in endpoint

### URL format

```plain
[Endpoint=]sb://<hostname>/;SharedAccessKeyName=<shared key name>;SharedAccessKey=<shared key>
```

The complete `url` has templating support, but not parts of it.

```plain
# valid
$conf::sb.url

# not valid
Endpoint=sb://$conf::sb.hostname/;SharedAccessKeyName=$conf::sb.keyname;SharedAccessKey=$conf::sb.key
```

## Example

```gherkin
And value of variable "AtomicServiceBus.document_id" is "queue:documents-in | wait=120, url=$conf::sb.endpoint, repeat=True"
...
Given a user of type "RestApi" load testing "http://example.com"
...
Then get request "fetch-document" from "/api/v1/document/{{ AtomicServiceBus.document_id }}"
```

When the scenario starts `grizzly` will wait up to 120 seconds until `AtomicServiceBus.document_id` has been populated from a message on the queue `documents-in`.

If there are no messages within 120 seconds, and it is the first iteration of the scenario, it will fail. If there has been at least one message on the queue since
the scenario started, it will use the oldest of those values, and then add it back in the end of the list again.

### Get message with expression

When specifying an expression, the messages on the endpoint is first peeked on. If any message matches the expression, it is later consumed from the endpoint.
If no matching messages was found when peeking, it is repeated again up until the specified `wait` seconds has elapsed. To use expression, a content type must
be specified for the endpint, e.g. `application/xml`.

```gherking
And value of variable "AtomicServiceBus.document_id" is "queue:documents-in | wait=120, url=$conf::sb.endpoint, repeat=True, content_type=json, expression='$.document[?(@.name=='TPM Report')'"
```
'''
import logging

from typing import Dict, Any, List, Type, Optional, cast
from urllib.parse import urlparse, parse_qs

import zmq

from gevent import sleep as gsleep
from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.arguments import split_value, parse_arguments, get_unsupported_arguments
from grizzly_extras.transformer import TransformerContentType

from ...types import AtomicVariable, bool_typed
from ...context import GrizzlyContext
from ..utils import resolve_variable


def atomicservicebus__base_type__(value: str) -> str:
    if '|' not in value:
        raise ValueError('AtomicServiceBus: initial value must contain arguments')

    endpoint_name, endpoint_arguments = split_value(value)

    try:
        arguments = parse_arguments(endpoint_arguments)
    except ValueError as e:
        raise ValueError(f'AtomicServiceBus: {str(e)}') from e

    if endpoint_name is None or len(endpoint_name) < 1:
        raise ValueError(f'AtomicServiceBus: endpoint name is not valid: "{endpoint_name}"')

    atomicservicebus_endpoint(endpoint_name)

    for argument in ['url']:
        if argument not in arguments:
            raise ValueError(f'AtomicServiceBus: {argument} parameter must be specified')

    for argument_name, argument_value in arguments.items():
        if argument_name not in AtomicServiceBus.arguments:
            raise ValueError(f'AtomicServiceBus: argument {argument_name} is not allowed')
        else:
            AtomicServiceBus.arguments[argument_name](argument_value)

    return f'{endpoint_name} | {endpoint_arguments}'


def atomicservicebus_url(url: str) -> str:
    grizzly = GrizzlyContext()
    try:
        resolved_url = cast(str, resolve_variable(grizzly, url, False))
    except Exception as e:
        raise ValueError(f'AtomicServiceBus: {str(e)}')

    connection_string = resolved_url

    if connection_string.startswith('Endpoint='):
        connection_string = connection_string[9:]

    connection_string = connection_string.replace(';', '?', 1).replace(';', '&')

    parsed = urlparse(connection_string)

    if parsed.scheme != 'sb':
        raise ValueError(f'AtomicServiceBus: "{parsed.scheme}" is not a supported scheme')

    if parsed.query == '':
        raise ValueError('AtomicServiceBus: SharedAccessKeyName and SharedAccessKey must be in the query string')

    params = parse_qs(parsed.query)

    if 'SharedAccessKeyName' not in params:
        raise ValueError('AtomicServiceBus: SharedAccessKeyName must be in the query string')

    if 'SharedAccessKey' not in params:
        raise ValueError('AtomicServiceBus: SharedAccessKey must be in the query string')

    return resolved_url

def atomicservicebus_endpoint(endpoint: str) -> str:
    if ':' not in endpoint:
        raise ValueError(f'AtomicServiceBus: {endpoint} does not specify queue: or topic:')

    try:
        arguments = parse_arguments(endpoint, ':', unquote=False)
    except ValueError as e:
        raise ValueError(f'AtomicServiceBus: {str(e)}') from e

    if 'topic' not in arguments and 'queue' not in arguments:
        raise ValueError(f'AtomicServiceBus: endpoint needs to be prefixed with queue: or topic:')

    if 'topic' in arguments and 'queue' in arguments:
        raise ValueError('AtomicServiceBus: cannot specify both topic: and queue: in endpoint')

    endpoint_type = 'topic' if 'topic' in arguments else 'queue'

    if len(arguments) > 1:
        if endpoint_type != 'topic' and 'subscription' in arguments:
            raise ValueError(f'AtomicServiceBus: argument subscription is only allowed if endpoint is a topic')

        unsupported_arguments = get_unsupported_arguments(['topic', 'queue', 'subscription', 'expression'], arguments)

        if len(unsupported_arguments) > 0:
            raise ValueError(f'AtomicServiceBus: arguments {", ".join(unsupported_arguments)} is not supported')

    expression = arguments.get('expression', None)
    subscription = arguments.get('subscription', None)
    if endpoint_type == 'topic' and subscription is None:
        raise ValueError(f'AtomicServiceBus: endpoint needs to include subscription when receiving messages from a topic')

    grizzly = GrizzlyContext()

    try:
        resolved_endpoint_name = cast(str, resolve_variable(grizzly, arguments[endpoint_type], guess_datatype=False))
    except Exception as e:
        raise ValueError(f'AtomicServiceBus: {str(e)}') from e

    endpoint = f'{endpoint_type}:{resolved_endpoint_name}'

    if subscription is not None:
        try:
            resolved_subscription_name = cast(str, resolve_variable(grizzly, subscription, guess_datatype=False))
        except Exception as e:
            raise ValueError(f'AtomicServiceBus: {str(e)}') from e

        endpoint = f'{endpoint}, subscription:{resolved_subscription_name}'

    if expression is not None:
        try:
            resolved_expression = cast(str, resolve_variable(grizzly, expression, guess_datatype=False))
        except Exception as e:
            raise ValueError(f'AtomicServiceBus: {str(e)}') from e

        endpoint = f'{endpoint}, expression:{resolved_expression}'

    return endpoint


class AtomicServiceBus(AtomicVariable[str]):
    __base_type__ = atomicservicebus__base_type__
    __dependencies__ = set(['async-messaged'])
    __on_consumer__ = True

    __initialized: bool = False

    _settings: Dict[str, Dict[str, Any]]
    _endpoint_clients: Dict[str, zmq.Socket]
    _endpoint_messages: Dict[str, List[str]]

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    arguments: Dict[str, Any] = {
        'repeat': bool_typed,
        'url': atomicservicebus_url,
        'wait': int,
        'endpoint_name': atomicservicebus_endpoint,
        'content_type': TransformerContentType.from_string,
    }

    def __init__(self, variable: str, value: str) -> None:
        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

        safe_value = self.__class__.__base_type__(value)

        settings = {'repeat': False, 'wait': None, 'url': None, 'worker': None, 'context': None, 'endpoint_name': None, 'content_type': None}

        endpoint_name, endpoint_arguments = split_value(safe_value)

        arguments = parse_arguments(endpoint_arguments)
        endpoint_parameters = parse_arguments(endpoint_name, ':')

        for argument, caster in self.__class__.arguments.items():
            if argument in arguments:
                settings[argument] = caster(arguments[argument])

        if 'expression' in endpoint_parameters and not 'content_type' in arguments:
            raise ValueError(f'{self.__class__.__name__}.{variable}: argument "content_type" is mandatory when "expression" is used in endpoint')

        settings['endpoint_name'] = self.arguments['endpoint_name'](endpoint_name)

        super().__init__(variable, endpoint_name)

        with self._semaphore:
            if self.__initialized:
                if variable not in self._endpoint_messages:
                    self._endpoint_messages[variable] = []

                if variable not in self._settings:
                    self._settings[variable] = settings

                if variable not in self._endpoint_clients:
                    self._endpoint_clients[variable] = self.create_client(variable, settings)

                return

            self._endpoint_messages = {variable: []}
            self._settings = {variable: settings}
            self._zmq_context = zmq.Context()
            self._endpoint_clients = {variable: self.create_client(variable, settings)}
            self.__initialized = True

    @classmethod
    def create_context(cls, settings: Dict[str, Any]) -> AsyncMessageContext:
        url = settings['url']

        if url.startswith('Endpoint='):
            url = url[9:]

        context: AsyncMessageContext = {
            'url': url,
            'connection': 'receiver',
            'endpoint': settings['endpoint_name'],
            'message_wait': settings.get('wait', None),
        }

        content_type = settings.get('content_type', None)
        if content_type is not None:
            context.update({'content_type': content_type.name.lower()})

        return context

    def create_client(self, variable: str, settings: Dict[str, Any]) -> zmq.Socket:
        self._settings[variable].update({'context': self.create_context(settings)})

        zmq_client = cast(zmq.Socket, self._zmq_context.socket(zmq.REQ))
        zmq_client.connect(self._zmq_url)

        self.say_hello(zmq_client, variable)

        return zmq_client

    def say_hello(self, client: zmq.Socket, variable: str) -> None:
        settings = self._settings[variable]
        context = cast(AsyncMessageContext, dict(settings['context']))

        endpoint_arguments = parse_arguments(context['endpoint'], ':')
        try:
            del endpoint_arguments['expression']
        except:
            pass

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])
        context['endpoint'] = cache_endpoint

        if settings.get('worker', None) is not None:
            return

        response: Optional[AsyncMessageResponse] = None
        request: AsyncMessageRequest = {
            'worker': settings['worker'],
            'action': 'HELLO',
            'context': context,
        }

        client.send_json(request)

        while True:
            try:
                response = client.recv_json(flags=zmq.NOBLOCK)
                break
            except zmq.Again:
                gsleep(0.1)

        if response is None:
            raise RuntimeError(f'{self.__class__.__name__}.{variable}: no response when trying to connect')

        message = response.get('message', None)
        if not response['success']:
            raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

        self._settings[variable]['worker'] = response['worker']

    @classmethod
    def destroy(cls: Type['AtomicServiceBus']) -> None:
        try:
            instance = cast(AtomicServiceBus, cls.get())
            clients = getattr(instance, '_settings', None)

            if clients is not None:
                variables = list(clients.keys())[:]
                for variable in variables:
                    try:
                        instance.__delitem__(variable)
                    except:
                        pass
        except:
            pass
        finally:
            try:
                instance._zmq_context.destroy()
            except:
                pass


        super().destroy()

    @classmethod
    def clear(cls: Type['AtomicServiceBus']) -> None:
        super().clear()

        instance = cast(AtomicServiceBus, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            instance.__delitem__(variable)

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            endpoint = cast(str, self._get_value(variable))
            settings = self._settings[variable]

            client = self._endpoint_clients[variable]

            self.say_hello(client, variable)

            request: AsyncMessageRequest = {
                'action': 'RECEIVE',
                'worker': settings['worker'],
                'context': settings['context'],
                'payload': None,
            }
            response: Optional[AsyncMessageResponse] = None

            client.send_json(request)

            while True:
                try:
                    response = cast(AsyncMessageResponse, client.recv_json(flags=zmq.NOBLOCK))
                    break
                except zmq.Again:
                    gsleep(0.1)

            if response is None:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: unknown error, no response')

            payload: Optional[str]
            message = response.get('message', None)

            if not response['success']:
                if message is not None and f'no message on {endpoint}' in message and settings.get('repeat', False) and len(self._endpoint_messages[variable]) > 0:
                    payload = self._endpoint_messages[variable].pop(0)
                    self._endpoint_messages[variable].append(payload)

                    return payload

                raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

            payload = cast(Optional[str], response.get('payload', None))
            if payload is None or len(payload) < 1:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: payload in response was None')

            if settings.get('repeat', False):
                self._endpoint_messages[variable].append(payload)

            return payload

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._settings[variable]
                del self._endpoint_messages[variable]

                try:
                    self._endpoint_clients[variable].disconnect(self._zmq_url)
                except (zmq.ZMQError, AttributeError, ):
                    pass
                finally:
                    del self._endpoint_clients[variable]
            except (KeyError, AttributeError, ):
                pass

        super().__delitem__(variable)

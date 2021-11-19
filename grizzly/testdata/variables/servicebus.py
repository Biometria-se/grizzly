'''Listens for messages on Azure Service Bus queue or topic and extracts values from messages based on a JSON path or XPath expressions.

## Format

Initial value is the name of the queue or topic, prefix with the endpoint type. If the endpoint is a topic the additional value subscription
is mandatory. Arguments support templating for their value, but not the complete endpoint value.

Examples:
```plain
queue:test-queue
topic:test-topic, subscription:test-subscription
queue:$conf::sb.endpoint.queue
topic:$conf::sb.endpoint.topic, subscription:$conf::sb.endpoint.subscription
```

## Arguments

* `repeat` _bool_ (optional) - if `True`, values read for the queue will be saved in a list and re-used if there are no new messages available
* `url` _str_ - see format of url below.
* `expression` _str_ - JSON path or XPath expression for finding _one_ specific value or object in the payload
* `content_type` _str_ - see [`step_response_content_type`](/grizzly/usage/steps/scenario/response/#step_response_content_type)
* `wait` _int_ - number of seconds to wait for a message on the queue

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
And value of variable "AtomicServiceBus.document_id" is "queue:documents-in | wait=120, url=$conf::sb.endpoint, repeat=True, expression='$.document.id', content_type=json"
...
Given a user of type "RestApi" load testing "http://example.com"
...
Then get request "fetch-document" from "/api/v1/document/{{ AtomicServiceBus.document_id }}"
```

When the scenario starts `grizzly` will wait up to 120 seconds until `AtomicServiceBus.document_id` has been populated from a message on the queue `documents-in`.

If there are no messages within 120 seconds, and it is the first iteration of the scenario, it will fail. If there has been at least one message on the queue since
the scenario started, it will use the oldest of those values, and then add it back in the end of the list again.
'''

from typing import Dict, Any, List, Type, Optional, cast
from urllib.parse import urlparse, parse_qs

import zmq

from gevent import sleep as gsleep
from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse

from ...types import AtomicVariable, bool_typed, str_response_content_type
from ...transformer import transformer
from ...exceptions import TransformerError
from ...context import GrizzlyContext
from ..utils import resolve_variable


def atomicservicebus__base_type__(value: str) -> str:
    if '|' not in value:
        raise ValueError('AtomicServiceBus: initial value must contain arguments')

    endpoint_name, endpoint_arguments = AtomicServiceBus.split_value(value)

    arguments = AtomicServiceBus.parse_arguments(endpoint_arguments)

    if endpoint_name is None or len(endpoint_name) < 1:
        raise ValueError(f'AtomicServiceBus: endpoint name is not valid: "{endpoint_name}"')

    atomicservicebus_endpoint(endpoint_name)

    for argument in ['url', 'expression', 'content_type']:
        if argument not in arguments:
            raise ValueError(f'AtomicServiceBus: {argument} parameter must be specified')

    for argument_name, argument_value in arguments.items():
        if argument_name not in AtomicServiceBus.arguments:
            raise ValueError(f'AtomicServiceBus: argument {argument_name} is not allowed')
        else:
            AtomicServiceBus.arguments[argument_name](argument_value)

    content_type = AtomicServiceBus.arguments['content_type'](arguments['content_type'])
    transform = transformer.available.get(content_type, None)

    if transform is None:
        raise ValueError(f'AtomicServiceBus: could not find a transformer for {content_type.name}')

    if not transform.validate(arguments['expression']):
        raise ValueError(f'AtomicServiceBus: expression "{arguments["expression"]}" is not a valid expression for {content_type.name}')

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

    endpoint_type, endpoint_details = endpoint.split(':', 1)
    subscription_name: Optional[str] = None

    if endpoint_type not in ['queue', 'topic']:
        raise ValueError(f'AtomicServiceBus: only support endpoint types queue and topic, not {endpoint_type}')

    if ',' in endpoint_details:
        if endpoint_type != 'topic':
            raise ValueError(f'AtomicServiceBus: additional arguments in endpoint is only supported for topic')

        endpoint_name, endpoint_details = [v.strip() for v in AtomicServiceBus.split_value(endpoint_details, ',')]
        detail_type, subscription_name = [v.strip() for v in AtomicServiceBus.split_value(endpoint_details, ':')]

        if detail_type != 'subscription':
            raise ValueError(f'AtomicServiceBus: argument {detail_type} is not supported')

        if len(subscription_name) < 1:
            subscription_name = None
    else:
        endpoint_name = endpoint_details

    if endpoint_type == 'topic' and subscription_name is None:
        raise ValueError(f'AtomicServiceBus: endpoint needs to include subscription when receiving messages from a topic')

    grizzly = GrizzlyContext()

    try:
        resolved_endpoint_name = resolve_variable(grizzly, endpoint_name)
    except Exception as e:
        raise ValueError(f'AtomicServiceBus: {str(e)}') from e

    endpoint = f'{endpoint_type}:{resolved_endpoint_name}'

    if subscription_name is not None:
        try:
            resolved_subscription_name = resolve_variable(grizzly, subscription_name)
        except Exception as e:
            raise ValueError(f'AtomicServiceBus: {str(e)}') from e

        endpoint = f'{endpoint}, subscription:{resolved_subscription_name}'

    return endpoint


class AtomicServiceBus(AtomicVariable[str]):
    __base_type__ = atomicservicebus__base_type__
    __dependencies__ = set(['async-messaged'])
    __on_consumer__ = True

    __initialized: bool = False

    _settings: Dict[str, Dict[str, Any]]
    _endpoint_clients: Dict[str, zmq.Socket]
    _endpoint_values: Dict[str, List[str]]

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    arguments: Dict[str, Any] = {
        'repeat': bool_typed,
        'url': atomicservicebus_url,
        'expression': str,
        'wait': int,
        'content_type': str_response_content_type,
        'endpoint_name': atomicservicebus_endpoint,
    }

    def __init__(self, variable: str, value: str) -> None:
        safe_value = self.__class__.__base_type__(value)

        settings = {'repeat': False, 'wait': None, 'expression': None, 'url': None, 'worker': None, 'context': None, 'endpoint_name': None}

        endpoint_name, endpoint_arguments = self.split_value(safe_value)

        arguments = self.parse_arguments(endpoint_arguments)

        for argument, caster in self.__class__.arguments.items():
            if argument in arguments:
                settings[argument] = caster(arguments[argument])

        settings['endpoint_name'] = self.arguments['endpoint_name'](endpoint_name)

        super().__init__(variable, endpoint_name)

        if self.__initialized:
            with self._semaphore:
                if variable not in self._endpoint_values:
                    self._endpoint_values[variable] = []

                if variable not in self._settings:
                    self._settings[variable] = settings

                if variable not in self._endpoint_clients:
                    self._endpoint_clients[variable] = self.create_client(variable, settings)

            return

        self._endpoint_values = {variable: []}
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

        return context

    def create_client(self, variable: str, settings: Dict[str, Any]) -> zmq.Socket:
        self._settings[variable].update({'context': self.create_context(settings)})

        zmq_client = cast(zmq.Socket, self._zmq_context.socket(zmq.REQ))
        zmq_client.connect(self._zmq_url)

        self.say_hello(zmq_client, variable)

        return zmq_client

    def say_hello(self, client: zmq.Socket, variable: str) -> None:
        settings = self._settings[variable]
        context = settings['context']

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

            message = response.get('message', None)
            if not response['success']:
                if message is not None and f'no message on {endpoint}' in message and settings.get('repeat', False) and len(self._endpoint_values[variable]) > 0:
                    value = self._endpoint_values[variable].pop(0)
                    self._endpoint_values[variable].append(value)

                    return value

                raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

            raw = response.get('payload', None)
            if raw is None or len(raw) < 1:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: payload in response was None')

            content_type = settings['content_type']
            expression = settings['expression']
            transform = transformer.available.get(content_type, None)

            if transform is None:
                raise TypeError(f'{self.__class__.__name__}.{variable}: could not find a transformer for {content_type.name}')

            try:
                get_values = transform.parser(expression)
                _, payload = transform.transform(content_type, raw)
            except TransformerError as e:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: {str(e.message)}')

            values = get_values(payload)

            number_of_values = len(values)

            if number_of_values != 1:
                if number_of_values < 1:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: "{expression}" returned no values')
                elif number_of_values > 1:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: "{expression}" returned more than one value')

            value = values[0]

            if settings.get('repeat', False):
                self._endpoint_values[variable].append(value)

            return value

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._settings[variable]
                del self._endpoint_values[variable]
                try:
                    self._endpoint_clients[variable].disconnect(self._zmq_url)
                except (zmq.ZMQError, AttributeError, ):
                    pass
                finally:
                    del self._endpoint_clients[variable]
            except (KeyError, AttributeError, ):
                pass

        super().__delitem__(variable)

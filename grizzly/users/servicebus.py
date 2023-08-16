'''Send and receive messages on Azure Service Bus queues and topics.

!!! note
    If `message.wait` is not set, `azure.servicebus` will wait until there is a message available, and hence block the scenario.

!!! attention
    Do not use `expression` to filter messages unless you do not care about the messages that does not match the expression. If
    you do care about them, you should setup a subscription to do the filtering in Azure.

User is based on `azure.servicebus` for communicating with Azure Service Bus. But creating a connection and session towards a queue or a topic
is a costly operation, and caching of the session was causing problems with `gevent` due to the sockets blocking and hence grizzly was
blocking when finished. To get around this, the user implementation communicates with a stand-alone process via `zmq`, which in turn communicates
with Azure Service Bus.

`async-messaged` starts automagically when a scenario uses the `ServiceBusUser`.

## Request methods

Supports the following request methods:

* send
* receive

## Format

Format of `host` is the following:

``` plain
[Endpoint=]sb://<hostname>/;SharedAccessKeyName=<shared key name>;SharedAccessKey=<shared key>
```

`endpoint` in the request must have the prefix `queue:` or `topic:` followed by the name of the targeted
type. When receiving messages from a topic, the argument `subscription:` is mandatory. The format of endpoint is:

``` plain
[queue|topic]:<endpoint name>[, subscription:<subscription name>][, expression:<expression>]
```

Where `<expression>` can be a XPath or jsonpath expression, depending on the specified content type. This argument is only allowed when
receiving messages. See example below.

## Examples

Example of how to use it in a scenario:

``` gherkin
Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm"
And set context variable "message.wait" to "5"
Then send request "queue-send" to endpoint "queue:shared-queue"
Then send request "topic-send" to endpoint "topic:shared-topic"
Then receive request "queue-recv" from endpoint "queue:shared-queue"
Then receive request "topic-recv" from endpoint "topic:shared-topic, subscription:my-subscription"
```

### Get message with expression

When specifying an expression, the messages on the endpoint is first peeked on. If any message matches the expression, it is later consumed from the
endpoint. If no matching messages was found when peeking, it is repeated again after a slight delay, up until the specified `message.wait` seconds has
elapsed. To use expressions, a content type must be specified for the request, e.g. `application/xml`.

``` gherkin
Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm"
And set context variable "message.wait" to "5"
Then receive request "queue-recv" from endpoint "queue:shared-queue, expression:$.document[?(@.name=='TPM report')].id"
And set response content type to "application/json"
Then receive request "topic-recv" from endpoint "topic:shared-topic, subscription:my-subscription, expression:/documents/document[@name='TPM Report']/id/text()"
And set response content type to "application/xml"
```
'''
import logging

from typing import Generator, Dict, Any, Tuple, Optional, Set, cast
from urllib.parse import urlparse, parse_qs
from contextlib import contextmanager

import zmq.green as zmq

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageResponse, AsyncMessageRequest, async_message_request
from grizzly_extras.arguments import parse_arguments, get_unsupported_arguments

from grizzly.types import RequestMethod, RequestDirection, GrizzlyResponse, RequestType
from grizzly.types.locust import StopUser, Environment
from grizzly.tasks import RequestTask
from grizzly.utils import merge_dicts

from .base import GrizzlyUser, ResponseHandler


class ServiceBusUser(ResponseHandler, GrizzlyUser):
    _context: Dict[str, Any] = {
        'message': {
            'wait': None,
        }
    }

    __dependencies__ = set(['async-messaged'])

    am_context: AsyncMessageContext
    worker_id: Optional[str]
    zmq_context = zmq.Context()
    zmq_client: zmq.Socket
    zmq_url = 'tcp://127.0.0.1:5554'
    hellos: Set[str]

    def __init__(self, environment: Environment, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        if not self.host.startswith('Endpoint='):
            conn_str = self.host
            self.host = f'Endpoint={self.host}'
        else:
            conn_str = self.host[9:]

        # Replace semicolon separators between parameters to ? and & to make it "urlparse-compliant"
        # for validation
        conn_str = conn_str.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.scheme != 'sb':
            raise ValueError(f'{self.__class__.__name__}: "{parsed.scheme}" is not a supported scheme')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKeyName and SharedAccessKey must be in the query string')

        params = parse_qs(parsed.query)

        if 'SharedAccessKeyName' not in params:
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKeyName must be in the query string')

        if 'SharedAccessKey' not in params:
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKey must be in the query string')

        self._context = merge_dicts(super().context(), self.__class__._context)

        self.am_context = {
            'url': self.host[9:],
            'message_wait': self._context.get('message', {}).get('wait', None)
        }

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

        self.hellos = set()
        self.worker_id = None

    def on_start(self) -> None:
        super().on_start()

        self.zmq_client = self.zmq_context.socket(zmq.REQ)
        self.zmq_client.connect(self.zmq_url)

        for task in self._scenario.tasks:
            if not isinstance(task, RequestTask):
                continue

            self.say_hello(task)

    def on_stop(self) -> None:
        if getattr(self, '_scenario', None) is not None:
            for task in self._scenario.tasks:
                if not isinstance(task, RequestTask):
                    continue

                endpoint = task.endpoint
                self.disconnect(task, endpoint)

        self.zmq_client.disconnect(self.zmq_url)

        super().on_stop()

    def get_description(self, task: RequestTask) -> str:
        if ('{{' in task.endpoint and '}}' in task.endpoint) or '$conf' in task.endpoint or '$env' in task.endpoint:
            self.logger.error(f'cannot say hello for {task.name} when endpoint is a template')
            raise StopUser()

        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'

        try:
            arguments = parse_arguments(task.endpoint, ':')
        except ValueError as e:
            raise RuntimeError(str(e)) from e
        endpoint_arguments = dict(arguments)

        try:
            del endpoint_arguments['expression']
        except:
            pass

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        return f'{connection}={cache_endpoint}'

    def disconnect(self, task: RequestTask, endpoint: str) -> None:
        description = self.get_description(task)

        if description not in self.hellos:
            return

        _, cache_endpoint = description.split('=', 1)

        context = cast(AsyncMessageContext, dict(self.am_context))
        context.update({
            'endpoint': cache_endpoint,
        })

        request: AsyncMessageRequest = {
            'action': RequestType.DISCONNECT.name,
            'context': context,
        }

        with self.request_context(task, request):
            pass

        self.hellos.remove(description)

    def say_hello(self, task: RequestTask) -> None:
        description = self.get_description(task)

        if description in self.hellos:
            return

        _, cache_endpoint = description.split('=', 1)

        arguments = parse_arguments(task.endpoint, ':')

        request_context = cast(AsyncMessageContext, dict(self.am_context))
        request_context.update({
            'endpoint': cache_endpoint,
        })

        request: AsyncMessageRequest = {
            'action': RequestType.HELLO.name,
            'context': request_context,
        }

        with self.request_context(task, request) as context:
            if 'queue' not in arguments and 'topic' not in arguments:
                raise RuntimeError('endpoint needs to be prefixed with queue: or topic:')

            if 'queue' in arguments and 'topic' in arguments:
                raise RuntimeError('cannot specify both topic: and queue: in endpoint')

            endpoint_type = 'topic' if 'topic' in arguments else 'queue'

            if len(arguments) > 1:
                if endpoint_type != 'topic' and 'subscription' in arguments:
                    raise RuntimeError('argument subscription is only allowed if endpoint is a topic')

                unsupported_arguments = get_unsupported_arguments(['topic', 'queue', 'subscription', 'expression'], arguments)

                if len(unsupported_arguments) > 0:
                    raise RuntimeError(f'arguments {", ".join(unsupported_arguments)} is not supported')

            if endpoint_type == 'topic' and arguments.get('subscription', None) is None and task.method.direction == RequestDirection.FROM:
                raise RuntimeError('endpoint needs to include subscription when receiving messages from a topic')

            if task.method.direction == RequestDirection.TO and arguments.get('expression', None) is not None:
                raise RuntimeError('argument expression is only allowed when receiving messages')

            context['failure_exception'] = self._scenario.failure_exception

        self.hellos.add(description)

    @contextmanager
    def request_context(self, task: RequestTask, request: AsyncMessageRequest) -> Generator[Dict[str, Any], None, None]:
        name = task.name

        if len(name) > 65:
            name = f'{name[:65]}...'

        request.update({
            'worker': self.worker_id,
            'client': id(self),
        })

        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'
        request['context'].update({'connection': connection})
        context: Dict[str, Any] = {
            'metadata': None,
            'payload': None,
        }

        request['context']['content_type'] = task.response.content_type.name.lower()

        response: Optional[AsyncMessageResponse] = None
        exception: Optional[Exception] = None

        try:
            yield context

            response = async_message_request(self.zmq_client, request)
            context.update({
                'metadata': response.get('metadata', None),
                'payload': response.get('payload', None),
            })
        except Exception as e:
            exception = e
        finally:
            if response is not None:
                response_worker = response.get('worker', None)
                if self.worker_id is None:
                    self.worker_id = response_worker

                assert self.worker_id == response_worker
            else:
                response = {}

            if exception is not None:
                raise exception

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        self.say_hello(request)

        request_context = cast(AsyncMessageContext, dict(self.am_context))
        request_context['endpoint'] = request.endpoint

        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'context': request_context,
            'payload': request.source,
        }

        with self.request_context(request, am_request) as context:
            if request.method not in [RequestMethod.SEND, RequestMethod.RECEIVE]:
                raise NotImplementedError(f'{self.__class__.__name__}: no implementation for {request.method.name} requests')

        return (context['metadata'], context['payload'],)

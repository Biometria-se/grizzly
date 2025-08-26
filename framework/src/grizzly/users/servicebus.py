"""Send and receive messages on Azure Service Bus queues and topics.

!!! warning

    If `message.wait` is not set, `azure.servicebus` will wait until there is a message available, and hence block the scenario.

!!! danger

    Do not use `expression` to filter messages unless you do not care about the messages that does not match the expression.

    If you do care about them, you should setup a subscription to do the filtering in Azure.

User is based on `azure.servicebus` for communicating with Azure Service Bus. But creating a connection and session towards a queue or a topic
is a costly operation, and caching of the session was causing problems with `gevent` due to the sockets blocking and hence grizzly was
blocking when finished.

To get around this, the user implementation communicates with a stand-alone process via 0mq, which in turn communicates with Azure Service Bus.
`async-messaged` starts automagically when a scenario uses the `ServiceBusUser`.

## Request methods

Supports the following request methods:

* send
* receive

## Format

Format of `host` is the following, when using connection strings:

```plain
[Endpoint=]sb://<hostname>/;SharedAccessKeyName=<shared key name>;SharedAccessKey=<shared key>
```

When using credentials context variables `auth.tenant`, `auth.user.username` and `auth.user.password` has to be set, and the format of `host` should be:
```plain
sb://<qualified namespace>[.servicebus.windows.net]
```

`endpoint` in the request must have the prefix `queue:` or `topic:` followed by the name of the targeted
type. When receiving messages from a topic, the argument `subscription:` is mandatory. The format of endpoint is:

```plain
[queue|topic]:<endpoint name>[, subscription:<subscription name>][, expression:<expression>]
```

Where `<expression>` can be a XPath or jsonpath expression, depending on the specified content type. This argument is only allowed when
receiving messages. See example below.

The `endpoint` also has support for the following arguments:

| Name      | Type   | Description                                                                                                              | Default |
| --------- | ------ | ------------------------------------------------------------------------------------------------------------------------ | ------- |
| `verbose` | `bool` | all request related to this request is logged with `DEBUG` log level                                                     | `False` |
| `consume` | `bool` | if messages not matching `expression` should be consumed or abandoned, either way they are not not returned to test case | `False` |
| `forward` | `bool` | if subscriptions should be configured to forward to a queue, and messages is consumed from the queue                     | `False` |

These arguments are specified in `endpoint` as such:

```plain
<endpoint> | <argument>=<value>[, <argument>=<value>]
```

## Examples

Example of how to use it in a scenario:

```gherkin
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

```gherkin
Given a user of type "ServiceBus" load testing "sb://my-sbns"
And set context variable "message.wait" to "5"
And set context variable "auth.tenant" to "example.com"
And set context variable "auth.user.username" to "bob@example.com"
And set context variable "auth.user.password" to "secret"
Then receive request "queue-recv" from endpoint "queue:shared-queue, expression:$.document[?(@.name=='TPM report')].id"
And set response content type to "application/json"
Then receive request "topic-recv" from endpoint "topic:shared-topic, subscription:my-subscription, expression:/documents/document[@name='TPM Report']/id/text()"
And set response content type to "application/xml"
```
"""

from __future__ import annotations

import logging
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import parse_qs, urlparse

import zmq.green as zmq
from async_messaged.utils import async_message_request
from grizzly_common.arguments import get_unsupported_arguments, parse_arguments
from grizzly_common.text import bool_caster
from zmq import sugar as ztypes

from grizzly.tasks import RequestTask
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, RequestType, StrDict
from grizzly.types.locust import Environment, StopUser
from grizzly.utils import has_parameter, has_template
from grizzly.utils.protocols import zmq_disconnect

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from async_messaged import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse

    from grizzly.testdata.communication import GrizzlyDependencies

MAX_LENGTH = 65


@grizzlycontext(
    context={
        'message': {
            'wait': None,
        },
        'auth': {
            'tenant': None,
            'user': {
                'username': None,
                'password': None,
            },
        },
    },
)
class ServiceBusUser(GrizzlyUser):
    __dependencies__: ClassVar[GrizzlyDependencies] = {'async-messaged'}

    am_context: AsyncMessageContext
    worker_id: str | None
    zmq_context = zmq.Context()
    zmq_client: ztypes.Socket
    zmq_url = 'tcp://127.0.0.1:5554'
    hellos: set[str]

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        context_auth_user = self._context.get('auth', {}).get('user', {})
        username = context_auth_user.get('username', None)
        password = context_auth_user.get('password', None)
        tenant = self._context.get('auth', {}).get('tenant', None)

        if username is None and password is None:
            if not self.host.startswith('Endpoint='):
                user_host = self.host
                self.host = f'Endpoint={self.host}'
            else:
                user_host = self.host[9:]
        else:
            user_host = self.host

        # Replace semicolon separators between parameters to ? and & to make it "urlparse-compliant"
        # for validation
        user_host = user_host.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(user_host)

        if parsed.scheme != 'sb':
            message = f'{self.__class__.__name__}: "{parsed.scheme}" is not a supported scheme'
            raise ValueError(message)

        if username is None and password is None:
            if parsed.query == '':
                message = f'{self.__class__.__name__}: SharedAccessKeyName and SharedAccessKey must be in the query string'
                raise ValueError(message)

            params = parse_qs(parsed.query)

            if 'SharedAccessKeyName' not in params:
                message = f'{self.__class__.__name__}: SharedAccessKeyName must be in the query string'
                raise ValueError(message)

            if 'SharedAccessKey' not in params:
                message = f'{self.__class__.__name__}: SharedAccessKey must be in the query string'
                raise ValueError(message)

            context_url = self.host[9:]
        else:
            if tenant is None:
                message = f'{self.__class__.__name__}: does not have context variable auth.tenant set while auth.user is'
                raise ValueError(message)

            context_url = user_host

        self.am_context = {
            'url': context_url,
            'message_wait': self._context.get('message', {}).get('wait', None),
            'username': username,
            'password': password,
            'tenant': tenant,
        }

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

        self.hellos = set()
        self.worker_id = None

    def on_start(self) -> None:
        super().on_start()

        self.zmq_client = self.zmq_context.socket(zmq.REQ)
        self.zmq_client.setsockopt(zmq.LINGER, 0)
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

                self.disconnect(task)

        with suppress(Exception):
            zmq_disconnect(self.zmq_client, destroy_context=False)

        super().on_stop()

    def get_description(self, task: RequestTask) -> str:
        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'

        try:
            arguments = parse_arguments(task.endpoint, ':')
        except ValueError as e:
            raise RuntimeError(str(e)) from e
        endpoint_arguments = dict(arguments)

        with suppress(Exception):
            del endpoint_arguments['expression']

        cache_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

        if has_template(cache_endpoint) or has_parameter(cache_endpoint):
            self.environment.stats.log_error(RequestType.HELLO.name, task.name, 'cannot say helloa when endpoint is a templating string')
            self.logger.error('cannot say hello for %s when endpoint is a template', task.name)
            raise StopUser

        return f'{connection}={cache_endpoint}'

    def disconnect(self, task: RequestTask) -> None:
        description = self.get_description(task)

        if description not in self.hellos:
            return

        _, cache_endpoint = description.split('=', 1)

        context = cast('AsyncMessageContext', dict(self.am_context))
        context.update(
            {
                'endpoint': cache_endpoint,
            },
        )

        request: AsyncMessageRequest = {
            'action': RequestType.DISCONNECT.name,
            'context': context,
        }

        with suppress(Exception):  # noqa: SIM117
            with self.request_context(task, request):
                pass

        self.hellos.remove(description)

    def say_hello(self, task: RequestTask) -> None:
        description = self.get_description(task)

        if description in self.hellos:
            return

        _, cache_endpoint = description.split('=', 1)

        arguments = parse_arguments(task.endpoint, ':')

        request_context = cast('AsyncMessageContext', dict(self.am_context))
        request_context.update(
            {
                'endpoint': cache_endpoint,
            },
        )

        request: AsyncMessageRequest = {
            'action': RequestType.HELLO.name,
            'context': request_context,
        }

        with self.request_context(task, request):
            if 'queue' not in arguments and 'topic' not in arguments:
                message = 'endpoint needs to be prefixed with queue: or topic:'
                raise RuntimeError(message)

            if 'queue' in arguments and 'topic' in arguments:
                message = 'cannot specify both topic: and queue: in endpoint'
                raise RuntimeError(message)

            endpoint_type = 'topic' if 'topic' in arguments else 'queue'

            if len(arguments) > 1:
                if endpoint_type != 'topic' and 'subscription' in arguments:
                    message = 'argument subscription is only allowed if endpoint is a topic'
                    raise RuntimeError(message)

                unsupported_arguments = get_unsupported_arguments(['topic', 'queue', 'subscription', 'expression'], arguments)

                if len(unsupported_arguments) > 0:
                    message = f'arguments {", ".join(unsupported_arguments)} is not supported'
                    raise RuntimeError(message)

            if endpoint_type == 'topic' and arguments.get('subscription', None) is None and task.method.direction == RequestDirection.FROM:
                message = 'endpoint needs to include subscription when receiving messages from a topic'
                raise RuntimeError(message)

            if task.method.direction == RequestDirection.TO and arguments.get('expression', None) is not None:
                message = 'argument expression is only allowed when receiving messages'
                raise RuntimeError(message)

        self.hellos.add(description)

    @contextmanager
    def request_context(self, task: RequestTask, request: AsyncMessageRequest) -> Generator[StrDict, None, None]:
        name = task.name

        if len(name) > MAX_LENGTH:
            name = f'{name[:MAX_LENGTH]}...'

        request.update(
            {
                'worker': self.worker_id,
                'client': id(self),
            },
        )

        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'
        request['context'].update({'connection': connection})
        context: dict = {
            'metadata': None,
            'payload': None,
        }

        request['context']['content_type'] = task.response.content_type.name.lower()

        response: AsyncMessageResponse | None = None
        exception: Exception | None = None

        try:
            yield context

            response = async_message_request(self.zmq_client, request)
            context.update(
                {
                    'metadata': response.get('metadata', None),
                    'payload': response.get('payload', None),
                },
            )
        except Exception as e:
            exception = e
        finally:
            if response is not None:
                response_worker = response.get('worker', None)
                if self.worker_id is None:
                    self.worker_id = response_worker

                if self.worker_id != response_worker:
                    message = 'unexpected worker id in response'
                    raise AssertionError(message)
            else:
                response = {}

            if exception is not None:
                raise exception

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        self.say_hello(request)

        request_context = cast('AsyncMessageContext', dict(self.am_context))
        consume = bool_caster((request.arguments or {}).get('consume', 'False'))
        verbose = bool_caster((request.arguments or {}).get('verbose', 'False'))
        forward = bool_caster((request.arguments or {}).get('forward', 'False'))

        request_context.update({'endpoint': request.endpoint, 'consume': consume, 'verbose': verbose, 'forward': forward})

        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'context': request_context,
            'payload': request.source,
        }

        with self.request_context(request, am_request) as context:
            if request.method not in [RequestMethod.SEND, RequestMethod.RECEIVE]:
                message = f'{self.__class__.__name__}: no implementation for {request.method.name} requests'
                raise NotImplementedError(message)

        return (context['metadata'], context['payload'])

"""Task performs Azure SerciceBus operations to a specified endpoint.

## Step implementations

* [From endpoint payload][grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload]

* [From endpoint payload and metadata][grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload_and_metadata]

* [To endpoint file][grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_file]

## Arguments

| Name          | Type               | Description                                                                                                     | Default    |
| ------------- | ------------------ | --------------------------------------------------------------------------------------------------------------- | ---------- |
| `direction`   | `RequestDirection` | if the request is upstream or downstream                                                                        | _required_ |
| `endpoint`    | `str`              | specifies details to be able to perform the request, e.g. Service Bus resource, queue, topic, subscription etc. | _required_ |
| `name`        | `str`              | name used in `locust` statistics                                                                                | _required_ |
| `destination` | `str`              | *not used by this client*                                                                                       | `None`     |
| `source`      | `str`              | file path of local file that should be put on `endpoint`                                                        | `None`     |

## Format

### endpoint

```plain
sb://[<username>:<password>@]<sbns resource name>[.servicebus.windows.net]/[queue:<queue name>|topic:<topic name>[/subscription:<subscription name>]][/expression:<expression>][;SharedAccessKeyName=<policy name>;SharedAccessKey=<access key>][#[Consume=<consume>][&MessageWait=<wait>][&ContentType<content type>][&Tenant=<tenant>][&Empty=<empty>][&Unique=<unique>][&Verbose=<verbose>][&Forward=<forward>]]
```

All variables in the endpoint has support for [templating][framework.usage.variables.templating].

Network location:

| Name                   | Type  | Description                                                                     | Default    |
| ---------------------- | ----- | ------------------------------------------------------------------------------- | ---------- |
| `<username>`           | `str` | when using credentials, authenticate with this username                         | `None`     |
| `<password>`           | `str` | password for said user                                                          | `None`     |
| `<sbns resource name>` | `str` | must be specfied, Azure Service Bus Namespace name, with or without domain name | _required_ |

Path:

| Name                  | Type  | Description                                                                                                                                                                                                                         | Default        |
| --------------------- | ----- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------- |
| `<queue name>`        | _str_ | name of queue, prefixed with `queue:` of an existing queue                                                                                                                                                                          | _required_[^1] |
| `<topic name>`        | _str_ | name of topic, prefixed with `topic:` of an existing topic                                                                                                                                                                          | _required_[^1] |
| `<subscription name>` | _str_ | name of an subscription on `topic name`, either an existing, or one to be created (if step text containing SQL Filter rule is specified), the actual subscription name will be suffixed with unique id related to the user instance | `None`         |
| `<expression>`        | _str_ | JSON or XPath expression to filter out message on payload, only applicable when receiving messages                                                                                                                                  | `None`         |

[^1]: Mutally exclusive, either specify `queue:` / `<queue name>` or `topic:` / `<topic name>`, not both

Query:

| Name            | Type  | Description                                   | Default    |
| --------------- | ----- | --------------------------------------------- | ---------- |
| `<policy name>` | `str` | name of the Service Bus policy to be used     | _required_ |
| `<access key>`  | `str` | secret access key for specified `policy name` | _required_ |

Fragment:

| Name             | Type   | Description                                                                                                                                             | Default     |
| ---------------- | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `<consume>`      | `bool` | if messages should be consumed (removed from endpoint), or only peeked at (left on endpoint)                                                            | `True`      |
| `<wait>`         | `int`  | how many seconds to wait for a message to arrive on the endpoint                                                                                        | `None => ∞` |
| `<content type>` | `str`  | content type of response payload, should be used in combination with `<expression>`                                                                     | `None`      |
| `<tenant>`       | `str`  | when using credentials, tenant to authenticate with                                                                                                     | `None`      |
| `<empty>`        | `bool` | if endpoint should be emptied before each iteration                                                                                                     | `True`      |
| `<unique>`       | `bool` | if each instance should have their own endpoint, when set to `False` all instances will share                                                           | `True`      |
| `<verbose>`      | `bool` | verbose logging for only these requests                                                                                                                 | `False`     |
| `<forward>`      | `bool` | if a queue should be created and the subscription should forward to it, and consuming messages from the queue instead of directly from the subscription | `False`     |

If `<unique>` is `False`, it will not empty the endpoint between each iteration.

If connection strings is to be used for authentication, the following `endpoint` parts must be present:

* `<policy name>`

* `<access key>`

If credential is to be used for authentication, the following `endpoint` parts must be present:

* `<username>`

* `<password>`

* `<tenant>`

## Examples

```gherkin
Given value for variable "event" is "none"
Then get from "sb://$conf::sb.name$.servicebus.windows.net/topic:incoming-events/subscription:grizzly-/expression:'$.`this`[?active=true]';SharedAccessKeyName=$conf::sb.key.name$;SharedAccessKey=$conf::sb.key.secret$#Consume=True&MessageWait=$conf::sb.message.wait$&ContentType=json&Forward=True" with name "get-incoming-events" and save response payload in "event"
```

"""  # noqa: E501

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from json import dumps as jsondumps
from pathlib import Path
from platform import node as hostname
from textwrap import dedent
from typing import TYPE_CHECKING, ClassVar, cast
from urllib.parse import parse_qs, quote_plus, unquote_plus, urlparse

import zmq.green as zmq
from grizzly_common.arguments import parse_arguments
from grizzly_common.text import bool_caster
from grizzly_common.transformer import TransformerContentType
from zmq import sugar as ztypes

from grizzly.tasks import template
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, RequestType
from grizzly.utils.protocols import async_message_request_wrapper, zmq_disconnect

from . import ClientTask, client

if TYPE_CHECKING:  # pragma: no cover
    from async_messaged import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse

    from grizzly.scenarios import GrizzlyScenario
    from grizzly.testdata.communication import GrizzlyDependencies


@dataclass
class State:
    worker: str | None = field(init=False, default=None)
    parent: GrizzlyScenario
    _first_response: AsyncMessageResponse | None = field(init=False, default=None)
    client: ztypes.Socket
    context: AsyncMessageContext

    @property
    def parent_id(self) -> int:
        """Generate parent object instance unique ID."""
        return id(self.parent.user)

    @property
    def first_response(self) -> AsyncMessageResponse | None:
        """Return first response this task got."""
        return self._first_response

    @first_response.setter
    def first_response(self, value: AsyncMessageResponse | None) -> None:
        self._first_response = value
        if value is not None:
            self.worker = value.get('worker', None)


@template('context')
@client('sb')
class ServiceBusClientTask(ClientTask):
    __dependencies__: ClassVar[GrizzlyDependencies] = {'async-messaged'}

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: ztypes.Context

    _state: dict[GrizzlyScenario, State]
    context: AsyncMessageContext
    should_empty: bool

    def __init__(  # noqa: PLR0915
        self,
        direction: RequestDirection,
        endpoint: str,
        name: str | None = None,
        /,
        payload_variable: str | None = None,
        metadata_variable: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        text: str | None = None,
        method: RequestMethod | None = None,
    ) -> None:
        super().__init__(
            direction,
            endpoint,
            name,
            payload_variable=payload_variable,
            metadata_variable=metadata_variable,
            destination=destination,
            source=source,
            text=text,
            method=method,
        )

        # expression can contain characters which are not URL safe, e.g. ?
        # so we need to quote it first to make sure it does not mess up the URL parsing
        expression: str | None = None
        match = re.search(r'expression:(.*?)(\/|;|#)', self.endpoint)

        if match:
            expression = quote_plus(match.group(1))
            eoe = match.group(2)
            self.endpoint = re.sub(r'expression:(.*?)(\/|;|#)', f'expression:{expression}{eoe}', self.endpoint)

        url = self.endpoint.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(url)

        username = parsed.username
        password = parsed.password

        parameters = parse_qs(parsed.fragment)

        try:
            message_wait: int | None = int(parameters.get('MessageWait', ['0'])[0])
            if message_wait is not None and message_wait < 1:
                message_wait = None
        except ValueError as e:
            message = 'MessageWait parameter in endpoint fragment is not a valid integer'
            raise AssertionError(message) from e

        self.should_empty = bool_caster(parameters.get('Empty', ['True'])[0])
        consume = bool_caster(parameters.get('Consume', ['False'])[0])
        unique = bool_caster(parameters.get('Unique', ['True'])[0])
        verbose = bool_caster(parameters.get('Verbose', ['False'])[0])
        forward = bool_caster(parameters.get('Forward', ['False'])[0])

        if not unique:
            self.should_empty = False

        tenant = parameters.get('Tenant', [None])[0]

        content_type_fragment = parameters.get('ContentType', None)
        content_type = TransformerContentType.from_string(content_type_fragment[0]).name if content_type_fragment is not None else None

        context_endpoint = parsed.path[1:].replace('/', ', ')

        # unquote expression, if specified, now that we have constructed everything everything basaed on the URL
        try:
            endpoint_arguments = parse_arguments(context_endpoint, separator=':', unquote=False)
            subscription = endpoint_arguments.get('subscription', None)
            if subscription is not None:
                # If text is not None, it means we should create an unique subscription, that needs an unique suffix that consists of 8 characters
                max_length = 50 if self._text is None else 50 - 8
                assert len(subscription) <= max_length, f'subscription name is too long, max length is {max_length} characters'

            expression = endpoint_arguments.get('expression', None)
            if expression is not None:
                endpoint_arguments.update({'expression': unquote_plus(expression)})
                context_endpoint = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])
        except ValueError as e:
            if 'incorrect format in arguments: ""' not in str(e):
                raise AssertionError(e) from e

        connection = 'receiver' if direction == RequestDirection.FROM else 'sender'

        hostname = parsed.hostname

        assert hostname is not None, 'hostname was not found in endpoint'

        if not hostname.endswith('.servicebus.windows.net') and hostname.count('.') == 0:
            hostname = f'{hostname}.servicebus.windows.net'

        self.endpoint = f'{parsed.scheme}://{hostname}'

        parameters = parse_qs(parsed.query)

        if username is None and password is None:
            assert parsed.query != '', 'no query string found in endpoint'
            assert 'SharedAccessKeyName' in parameters, 'SharedAccessKeyName not found in query string of endpoint'
            assert 'SharedAccessKey' in parameters, 'SharedAccessKey not found in query string of endpoint'
            assert tenant is None, 'Tenant fragment in endpoint is not allowed when using connection string'
            self.endpoint = f'{self.endpoint}/;{parsed.query.replace("&", ";")}'
        else:
            assert tenant is not None, 'Tenant not found in fragment of endpoint'
            assert parsed.query == '', 'query string found in endpoint, which is not allowed when using credential authentication'

        self.context = {
            'url': self.endpoint,
            'connection': connection,
            'endpoint': context_endpoint,
            'message_wait': message_wait,
            'consume': consume,
            'unique': unique,
            'verbose': verbose,
            'username': username,
            'password': password,
            'tenant': tenant,
            'forward': forward,
        }

        if content_type is not None:
            self.context.update({'content_type': content_type})

        self._state = {}
        self._zmq_context = zmq.Context()

    def get_state(self, parent: GrizzlyScenario) -> State:
        state = self._state.get(parent, None)

        if state is None:
            context = self.context.copy()
            # add id of user as suffix to subscription name, to make it unique
            endpoint_arguments = parse_arguments(context['endpoint'], separator=':', unquote=False)

            # if text is not None, we should create an temporary unique subscription for this
            # specific task instance, this means we should change the subscription name
            if 'subscription' in endpoint_arguments and self._text is not None and self.context.get('unique', True):
                subscription = endpoint_arguments['subscription']
                quote = ''
                if subscription[0] in ['"', "'"] and subscription[-1] == subscription[0]:
                    quote = subscription[0]
                    subscription = subscription[1:-1]
                identifier_raw = f'{id(parent.user)}{hostname()}{datetime.now(tz=None).timestamp()}'
                identifier = sha256(identifier_raw.encode()).hexdigest()[:8]
                endpoint_arguments['subscription'] = f'{quote}{subscription}{identifier}{quote}'
                context['endpoint'] = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

            # context might have been destroyed as all existing sockets has been closed
            if self._zmq_context.closed:
                self._zmq_context = zmq.Context()

            state = State(
                parent=parent,
                client=cast('ztypes.Socket', self._zmq_context.socket(zmq.REQ)),
                context=context,
            )
            state.client.setsockopt(zmq.LINGER, 0)
            state.client.connect(self._zmq_url)
            self._state.update({parent: state})

        return state

    @ClientTask.text.setter  # type: ignore[has-type]
    def text(self, value: str) -> None:
        self._text = dedent(value).strip()

    def connect(self, parent: GrizzlyScenario) -> None:
        state = self.get_state(parent)

        parent.user.logger.debug('%d::sb connecting, state.worker=%r', state.parent_id, state.worker)

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.HELLO.name,
            'context': state.context,
        }

        response = async_message_request_wrapper(parent, state.client, request)

        if state.first_response is None:
            state.first_response = response

        parent.user.logger.debug('%d::sb connected to worker %r at %s', state.parent_id, state.worker, hostname())

    def disconnect(self, parent: GrizzlyScenario) -> None:
        state = self.get_state(parent)

        if state.worker is None:
            return

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.DISCONNECT.name,
            'context': state.context,
        }

        async_message_request_wrapper(parent, state.client, request)

        zmq_disconnect(state.client, destroy_context=False)

        del self._state[parent]

    def subscribe(self, parent: GrizzlyScenario) -> None:
        if self.text is None:
            return

        state = self.get_state(parent)

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.SUBSCRIBE.name,
            'context': state.context,
            'payload': self.text,
        }

        response = async_message_request_wrapper(parent, state.client, request)
        parent.user.logger.info(response['message'])

        state.first_response = response

    def unsubscribe(self, parent: GrizzlyScenario) -> None:
        if self.text is None:
            return

        state = self.get_state(parent)

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.UNSUBSCRIBE.name,
            'context': state.context,
        }

        response = async_message_request_wrapper(parent, state.client, request)
        parent.user.logger.info(response['message'])

    def empty(self, parent: GrizzlyScenario) -> None:
        state = self.get_state(parent)

        # only empty receiving instances, which has a subscription created
        if state.context.get('connection', 'sender') == 'sender' or self.text is None:
            return

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.EMPTY.name,
            'context': state.context,
        }

        response = async_message_request_wrapper(parent, state.client, request)
        message = response['message']

        if message is not None and len(message) > 0:
            parent.user.logger.info(response['message'])

    def on_start(self, parent: GrizzlyScenario) -> None:
        # create subscription before connecting to it
        if self.text is not None:
            self.subscribe(parent)
            if not self.context.get('unique', True):
                subscribers = parent.consumer.keystore_inc(self.context['endpoint'])
                parent.logger.debug('endpoint "%s" has %d subscribers', self.context['endpoint'], subscribers)

        self.connect(parent)

    def on_stop(self, parent: GrizzlyScenario) -> None:
        try:
            if self.text is not None:
                subscribers: int | None = None
                is_unique = self.context.get('unique', True)

                if not is_unique:
                    subscribers = parent.consumer.keystore_dec(self.context['endpoint'])
                    parent.logger.debug('endpoint "%s" has %d subscribers', self.context['endpoint'], subscribers)

                if subscribers is None or subscribers < 1:
                    self.unsubscribe(parent)
        except:
            parent.logger.exception('failed to unsubscribe')

        try:
            self.disconnect(parent)
        except:
            parent.logger.exception('failed to disconnect')

    def on_iteration(self, parent: GrizzlyScenario) -> None:
        try:
            if self.text is not None and self.should_empty:
                self.empty(parent)
        except:
            parent.logger.exception('failed to empty')

    def request(self, parent: GrizzlyScenario, request: AsyncMessageRequest) -> AsyncMessageResponse:
        response = None
        state = self.get_state(parent)

        with self.action(parent) as meta:
            if request['context'].get('url', None) is None:
                request['context'].update({'url': self.endpoint})

            response = async_message_request_wrapper(parent, state.client, request)

            response_length_source = ((response or {}).get('payload', None) or '').encode('utf-8')

            meta.update(
                {
                    'action': state.context['endpoint'],
                    'request': request.copy(),
                    'response_length': len(response_length_source),
                    'response': response,
                },
            )

        return response or {}

    def request_from(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        state = self.get_state(parent)

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'worker': state.worker,
            'context': state.context,
            'payload': None,
        }

        response = self.request(parent, request) or {}

        metadata = response.get('metadata', None)
        payload = response.get('payload', None)

        if payload is not None and self.payload_variable is not None:
            parent.user.set_variable(self.payload_variable, payload)

        if metadata is not None and self.metadata_variable is not None:
            parent.user.set_variable(self.metadata_variable, jsondumps(metadata))

        return metadata, payload

    def request_to(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        state = self.get_state(parent)

        source = parent.user.render(cast('str', self.source))
        source_file = Path(self._context_root) / 'requests' / source

        if source_file.exists():
            source = parent.user.render(source_file.read_text())

        request: AsyncMessageRequest = {
            'action': 'SEND',
            'worker': state.worker,
            'context': state.context,
            'payload': source,
        }

        response = self.request(parent, request)

        metadata = response.get('metadata', None)
        payload = response.get('payload', None)

        return metadata, payload

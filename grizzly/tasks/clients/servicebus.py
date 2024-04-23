"""@anchor pydoc:grizzly.tasks.clients.servicebus Service Bus
This task performs Azure SerciceBus operations to a specified endpoint.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_get_endpoint_payload}

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_get_endpoint_payload_metadata}

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_put_endpoint_file}

## Arguments

* `direction` _RequestDirection_ - if the request is upstream or downstream

* `endpoint` _str_ - specifies details to be able to perform the request, e.g. Service Bus resource, queue, topic, subscription etc.

* `name` _str_ - name used in `locust` statistics

* `destination` _str_ (optional) - **not used by this client**

* `source` _str_ (optional) - file path of local file that should be put on `endpoint`

## Format

### `endpoint`

```plain
sb://[<username>:<password>@]<sbns resource name>[.servicebus.windows.net]/[queue:<queue name>|topic:<topic name>[/subscription:<subscription name>]][/expression:<expression>][;SharedAccessKeyName=<policy name>;SharedAccessKey=<access key>][#[Consume=<consume>][&MessageWait=<wait>][&ContentType<content type>][&Tenant=<tenant>]]
```

All variables in the endpoint have support for {@link framework.usage.variables.templating}.

Network location:

* `<username>` _str_ - when using credentials, authenticate with this username

* `<password>` _str_  - password for said user

* `<sbns resource name>` _str_ - must be specfied, Azure Service Bus Namespace name, with or without domain name

Path:

* `<queue name>` _str_ - name of queue, prefixed with `queue:` of an existing queue (mutual exclusive<sup>1</sup>)

* `<topic name>` _str_ - name of topic, prefixed with `topic:` of an existing topic (mutual exclusive<sup>1</sup>)

* `<subscription name>` _str_ - name of an subscription on `topic name`, either an existing, or one to be created (if step text containing SQL Filter rule is specified), the actual subscription name will be suffixed with unique id related to the user instance

* `<expression>` _str_ - JSON or XPath expression to filter out message on payload, only applicable when receiving messages

<sup>1</sup> Either specify `queue:` or `topic`, not both

Query:

* `<policy name>` _str_ - name of the Service Bus policy to be used

* `<access key>` _str_ - secret access key for specified `policy name`

Fragment:

* `<consume>` _bool_ - if messages should be consumed (removed from endpoint), or only peeked at (left on endpoint) (default: `True`)

* `<wait>` _int_ - how many seconds to wait for a message to arrive on the endpoint (default: `∞`)

* `<content type>` _str_ - content type of response payload, should be used in combination with `<expression>`

* `<tenant>` _str_ - when using credentials, tenant to authenticate with

Parts listed below are mutally exclusive, e.g. either ones should be used, but no combinations between the two.

#### Connection strings

If connection strings is to be used for authenticating, the following `endpoint` parts must be present:

* `<policy name>`

* `<access key>`

#### Credential

If credential is to be used for authenticating, the following `endpoint` parts must be present:

* `<username>`

* `<password>`

* `<tenant>`

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
from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Set, cast
from urllib.parse import parse_qs, quote_plus, unquote_plus, urlparse

import zmq.green as zmq

from grizzly.tasks import template
from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.utils import async_message_request_wrapper
from grizzly_extras.arguments import parse_arguments
from grizzly_extras.transformer import TransformerContentType

from . import ClientTask, client, logger

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario
    from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse


@dataclass
class State:
    worker: Optional[str] = field(init=False, default=None)
    parent: GrizzlyScenario
    _first_response: Optional[AsyncMessageResponse] = field(init=False, default=None)
    client: zmq.Socket
    context: AsyncMessageContext

    @property
    def parent_id(self) -> int:
        """Generate parent object instance unique ID."""
        return id(self.parent.user)

    @property
    def first_response(self) -> Optional[AsyncMessageResponse]:
        """Return first response this task got."""
        return self._first_response

    @first_response.setter
    def first_response(self, value: Optional[AsyncMessageResponse]) -> None:
        self._first_response = value
        if value is not None:
            self.worker = value.get('worker', None)


@template('context')
@client('sb')
class ServiceBusClientTask(ClientTask):
    __dependencies__: ClassVar[Set[str]] = {'async-messaged'}

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    _state: Dict[GrizzlyScenario, State]
    context: AsyncMessageContext

    def __init__(  # noqa: PLR0915
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        payload_variable: Optional[str] = None,
        metadata_variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        text: Optional[str] = None,
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
        )

        # expression can contain characters which are not URL safe, e.g. ?
        # so we need to quote it first to make sure it does not mess up the URL parsing
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
            message_wait: Optional[int] = int(parameters.get('MessageWait', ['0'])[0])
            if message_wait is not None and message_wait < 1:
                message_wait = None
        except ValueError as e:
            message = 'MessageWait parameter in endpoint fragment is not a valid integer'
            raise AssertionError(message) from e

        consume_fragment = parameters.get('Consume', ['False'])[0]
        if consume_fragment not in ['True', 'False']:
            message = 'Consume parameter in endpoint fragment is not a valid boolean'
            raise AssertionError(message)

        consume = consume_fragment == 'True'

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
            'username': username,
            'password': password,
            'tenant': tenant,
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
            if 'subscription' in endpoint_arguments and self._text is not None:
                subscription = endpoint_arguments['subscription']
                quote = ''
                if subscription[0] in ['"', "'"] and subscription[-1] == subscription[0]:
                    quote = subscription[0]
                    subscription = subscription[1:-1]
                identifier_raw = f'{id(parent.user)}{hostname()}{datetime.now(tz=None).timestamp()}'
                identifier = sha256(identifier_raw.encode()).hexdigest()[:8]
                endpoint_arguments['subscription'] = f'{quote}{subscription}{identifier}{quote}'
                context['endpoint'] = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

            state = State(
                parent=parent,
                client=cast(zmq.Socket, self._zmq_context.socket(zmq.REQ)),
                context=context,
            )
            state.client.connect(self._zmq_url)
            self._state.update({parent: state})

        return state

    @ClientTask.text.setter  # type: ignore[has-type]
    def text(self, value: str) -> None:
        self._text = dedent(value).strip()

    def connect(self, parent: GrizzlyScenario) -> None:
        state = self.get_state(parent)

        logger.debug(f'{state.parent_id}::sb connecting, {state.worker=}')

        request: AsyncMessageRequest = {
            'worker': state.worker,
            'action': RequestType.HELLO.name,
            'context': state.context,
        }

        response = async_message_request_wrapper(parent, state.client, request)

        if state.first_response is None:
            state.first_response = response

        logger.debug(f'{state.parent_id}::sb connected to worker {state.worker} at {hostname()}')

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

        state.client.setsockopt(zmq.LINGER, 0)
        state.client.close()

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
        logger.info(response['message'])

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
        logger.info(response['message'])

    def on_start(self, parent: GrizzlyScenario) -> None:
        # create subscription before connecting to it
        if self.text is not None:
            self.subscribe(parent)

        self.connect(parent)

    def on_stop(self, parent: GrizzlyScenario) -> None:
        if self.text is not None:
            self.unsubscribe(parent)

        self.disconnect(parent)

    def request(self, parent: GrizzlyScenario, request: AsyncMessageRequest) -> AsyncMessageResponse:
        response = None
        state = self.get_state(parent)

        with self.action(parent) as meta:
            if request['context'].get('url', None) is None:
                request['context'].update({'url': self.endpoint})

            response = async_message_request_wrapper(parent, state.client, request)

            response_length_source = ((response or {}).get('payload', None) or '').encode('utf-8')

            meta.update({
                'action': state.context['endpoint'],
                'request': request.copy(),
                'response_length': len(response_length_source),
                'response': response,
            })

        return response or {}

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        state = self.get_state(parent)

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'worker': state.worker,
            'context': state.context,
            'payload': None,
        }

        response = self.request(parent, request)

        metadata = response.get('metadata', None)
        payload = response.get('payload', None)

        if payload is not None and self.payload_variable is not None:
            parent.user._context['variables'][self.payload_variable] = payload

        if metadata is not None and self.metadata_variable is not None:
            parent.user._context['variables'][self.metadata_variable] = jsondumps(metadata)

        return metadata, payload

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        state = self.get_state(parent)

        source = parent.render(cast(str, self.source))
        source_file = Path(self._context_root) / 'requests' / source

        if source_file.exists():
            source = parent.render(source_file.read_text())

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

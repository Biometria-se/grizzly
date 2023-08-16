# pylint: disable=line-too-long
"""This task performs Azure SerciceBus operations to a specified endpoint.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload}

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload_metadata}

* {@pylink grizzly.steps.scenario.tasks.step_task_client_put_endpoint_file}

## Arguments

* `direction` _RequestDirection_ - if the request is upstream or downstream

* `endpoint` _str_ - specifies details to be able to perform the request, e.g. Service Bus resource, queue, topic, subscription etc.

* `name` _str_ - name used in `locust` statistics

* `destination` _str_ (optional) - **not used by this client**

* `source` _str_ (optional) - file path of local file that should be put on `endpoint`

## Format

### `endpoint`

Value of this is basically the "Connection String" from Azure (without `Endpoint=` prefix), with some additional information.

``` plain
sb://<sbns resource name>.servicebus.windows.net/[queue:<queue name>|topic:<topic name>[/subscription:<subscription name>]][/expression:<expression>];SharedAccessKeyName=<policy name>;SharedAccessKey=<access key>[#[Consume=<consume>][&][MessageWait=<wait>][&][ContentType<content type>]]
```

All variables in the endpoint have support for {@link framework.usage.variables.templating}.

Network location:

* `<sbns resource name>` _str_ - must be specfied, Azure Service Bus Namespace name

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
"""  # noqa: E501
from typing import Optional, Dict, cast
from urllib.parse import urlparse, parse_qs
from platform import node as hostname
from pathlib import Path
from textwrap import dedent
from json import dumps as jsondumps
from dataclasses import dataclass, field

import zmq.green as zmq

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.transformer import TransformerContentType
from grizzly_extras.arguments import parse_arguments

from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.scenarios import GrizzlyScenario
from grizzly.tasks import template
from grizzly.utils import async_message_request_wrapper

from . import client, ClientTask, logger  # pylint: disable=unused-import


@dataclass
class State:
    worker: Optional[str] = field(init=False, default=None)
    parent: GrizzlyScenario
    _first_response: Optional[AsyncMessageResponse] = field(init=False, default=None)
    client: zmq.Socket
    context: AsyncMessageContext

    @property
    def parent_id(self) -> int:
        return id(self.parent.user)

    @property
    def first_response(self) -> Optional[AsyncMessageResponse]:
        return self._first_response

    @first_response.setter
    def first_response(self, value: Optional[AsyncMessageResponse]) -> None:
        self._first_response = value
        if value is not None:
            self.worker = value.get('worker', None)


@template('context')
@client('sb')
class ServiceBusClientTask(ClientTask):
    __dependencies__ = set(['async-messaged'])

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    _state: Dict[GrizzlyScenario, State]
    context: AsyncMessageContext

    def __init__(
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

        url = self.endpoint.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(url)

        self.endpoint = f'{parsed.scheme}://{parsed.netloc}/;{parsed.query.replace("&", ";")}'
        self._zmq_context = zmq.Context()

        parameters = parse_qs(parsed.fragment)

        try:
            message_wait: Optional[int] = int(parameters.get('MessageWait', ['0'])[0])
            if message_wait is not None and message_wait < 1:
                message_wait = None
        except ValueError:
            raise ValueError('MessageWait parameter in endpoint fragment is not a valid integer')

        consume_fragment = parameters.get('Consume', ['False'])[0]
        if consume_fragment not in ['True', 'False']:
            raise ValueError('Consume parameter in endpoint fragment is not a valid boolean')

        consume = consume_fragment == 'True'

        content_type_fragment = parameters.get('ContentType', None)
        if content_type_fragment is not None:
            content_type = TransformerContentType.from_string(content_type_fragment[0]).name
        else:
            content_type = None

        context_endpoint = parsed.path[1:].replace('/', ', ')

        connection = 'receiver' if direction == RequestDirection.FROM else 'sender'

        self.context = {
            'url': self.endpoint,
            'connection': connection,
            'endpoint': context_endpoint,
            'message_wait': message_wait,
            'consume': consume,
        }

        if content_type is not None:
            self.context.update({'content_type': content_type})

        self._state = {}

    def get_state(self, parent: 'GrizzlyScenario') -> State:
        state = self._state.get(parent, None)

        if state is None:
            context = self.context.copy()
            # add id of user as suffix to subscription name, to make it unique
            endpoint_arguments = parse_arguments(context['endpoint'], separator=':')

            if 'subscription' in endpoint_arguments:
                endpoint_arguments['subscription'] = f'{endpoint_arguments["subscription"]}_{id(parent.user)}'
                context['endpoint'] = ', '.join([f'{key}:{value}' for key, value in endpoint_arguments.items()])

            state = State(
                parent=parent,
                client=cast(zmq.Socket, self._zmq_context.socket(zmq.REQ)),
                context=context,
            )
            state.client.connect(self._zmq_url)
            self._state.update({parent: state})

        return state

    @ClientTask.text.setter  # type: ignore
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

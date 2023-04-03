# pylint: disable=line-too-long
"""This task performs Azure SerciceBus operations to a specified endpoint.


## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint}

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

* `<subscription name>` _str_ - name of an subscription on `topic name`, either an existing, or one to be created (if step text containing SQL Filter rule is specified)

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
from typing import Optional, cast
from urllib.parse import urlparse, parse_qs
from platform import node as hostname
from pathlib import Path
from textwrap import dedent

import zmq.green as zmq

from zmq.sugar.constants import REQ as ZMQ_REQ, LINGER as ZMQ_LINGER

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.transformer import TransformerContentType

from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.context import GrizzlyContextScenario
from grizzly.scenarios import GrizzlyScenario
from grizzly.tasks import template
from grizzly.utils import async_message_request_wrapper

from . import client, ClientTask, logger  # pylint: disable=unused-import


@template('context')
@client('sb')
class ServiceBusClientTask(ClientTask):
    __dependencies__ = set(['async-messaged'])

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    _client: Optional[zmq.Socket] = None
    worker_id: Optional[str]
    context: AsyncMessageContext
    _parent: Optional[GrizzlyScenario]

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        text: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        super().__init__(direction, endpoint, name, variable=variable, destination=destination, source=source, text=text, scenario=scenario)

        url = self.endpoint.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(url)

        self.endpoint = f'{parsed.scheme}://{parsed.netloc}/;{parsed.query.replace("&", ";")}'
        self._parent = None
        self.worker_id = None
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

        # zmq connection to async-messaged must be done when creating the instance
        self._client = cast(zmq.Socket, self._zmq_context.socket(ZMQ_REQ))
        self.client.connect(self._zmq_url)

    @property
    def parent(self) -> GrizzlyScenario:
        if self._parent is None:
            raise AttributeError('no parent set')

        return self._parent

    @parent.setter
    def parent(self, value: GrizzlyScenario) -> None:
        if self._parent is not None and value is not self._parent:
            raise AttributeError('parent already set, why are a different parent being set?')

        self._parent = value

    @property
    def client(self) -> zmq.Socket:
        if self._client is None:
            raise ConnectionError('not connected to async-messaged')

        return self._client

    @ClientTask.text.setter  # type: ignore
    def text(self, value: str) -> None:
        self._text = dedent(value).strip()

    def connect(self) -> None:
        if self.worker_id is not None:
            return

        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.HELLO.name,
            'context': self.context,
        }

        response = async_message_request_wrapper(self.parent, self.client, request)

        self.worker_id = response['worker']

        logger.debug(f'connected to worker {self.worker_id} at {hostname()}')

    def disconnect(self) -> None:
        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.DISCONNECT.name,
            'context': self.context,
        }

        async_message_request_wrapper(self.parent, self.client, request)

        self.worker_id = None
        self.client.setsockopt(ZMQ_LINGER, 0)
        self.client.close()
        self._client = None

    def subscribe(self) -> None:
        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.SUBSCRIBE.name,
            'context': self.context,
            'payload': self.text,
        }

        response = async_message_request_wrapper(self.parent, self.client, request)
        logger.info(response['message'])

    def unsubscribe(self) -> None:
        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.UNSUBSCRIBE.name,
            'context': self.context,
        }

        response = async_message_request_wrapper(self.parent, self.client, request)
        logger.info(response['message'])

    def on_start(self, parent: GrizzlyScenario) -> None:
        self.parent = parent

        # create subscription before connecting to it
        if self.text is not None:
            self.subscribe()

        self.connect()

    def on_stop(self, parent: GrizzlyScenario) -> None:
        self.parent = parent

        if self.text is not None:
            self.unsubscribe()

        self.disconnect()

    def request(self, parent: GrizzlyScenario, request: AsyncMessageRequest) -> AsyncMessageResponse:
        response = None

        with self.action(parent) as meta:
            if request['context'].get('url', None) is None:
                request['context'].update({'url': self.endpoint})

            response = async_message_request_wrapper(parent, self.client, request)

            response_length_source = ((response or {}).get('payload', None) or '').encode('utf-8')

            meta.update({
                'action': self.context['endpoint'],
                'request': request.copy(),
                'response_length': len(response_length_source),
                'response': response,
            })

        return response or {}

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'worker': self.worker_id,
            'context': self.context,
            'payload': None,
        }

        response = self.request(parent, request)

        metadata = response.get('metadata', None)
        payload = response.get('payload', None)

        if payload is not None and self.variable is not None:
            parent.user._context['variables'][self.variable] = payload

        return metadata, payload

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.render(cast(str, self.source))
        source_file = Path(self._context_root) / 'requests' / source

        if source_file.exists():
            source = parent.render(source_file.read_text())

        request: AsyncMessageRequest = {
            'action': 'SEND',
            'worker': self.worker_id,
            'context': self.context,
            'payload': source,
        }

        response = self.request(parent, request)

        metadata = response.get('metadata', None)
        payload = response.get('payload', None)

        return metadata, payload

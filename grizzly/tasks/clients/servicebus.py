"""This task performs Azure SerciceBus operations to a specified endpoint.


## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint}
"""
from typing import Optional, cast
from urllib.parse import urlparse, parse_qs
from platform import node as hostname

import zmq.green as zmq

from zmq.sugar.constants import REQ as ZMQ_REQ, LINGER as ZMQ_LINGER

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, async_message_request
from grizzly_extras.transformer import TransformerContentType

from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.context import GrizzlyContextScenario
from grizzly.scenarios import GrizzlyScenario

from . import client, ClientTask, logger  # pylint: disable=unused-import


@client('sb')
class ServiceBusClientTask(ClientTask):
    __dependencies__ = set(['async-messaged'])

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    _client: Optional[zmq.Socket] = None
    worker_id: Optional[str]
    context: AsyncMessageContext

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        url = endpoint.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(url)

        endpoint_url = f'{parsed.scheme}://{parsed.netloc}/;{parsed.query.replace("&", ";")}'

        super().__init__(direction, endpoint_url, name, variable=variable, destination=destination, source=source, scenario=scenario)

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
        self._zmq_context = zmq.Context()
        self.worker_id = None

        connection = 'receiver' if direction == RequestDirection.FROM else 'sender'

        self.context = {
            'url': endpoint_url,
            'connection': connection,
            'endpoint': context_endpoint,
            'message_wait': message_wait,
            'consume': consume,
        }

        if content_type is not None:
            self.context.update({'content_type': content_type})

    @property
    def client(self) -> zmq.Socket:
        if self._client is None:
            raise ConnectionError('not connected')

        return self._client

    @ClientTask.text.setter  # type: ignore
    def text(self, value: str) -> None:
        self._text = value

    def connect(self) -> None:
        if self._client is not None:
            return

        self._client = cast(zmq.Socket, self._zmq_context.socket(ZMQ_REQ))
        self.client.connect(self._zmq_url)

        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.HELLO.name,
            'context': self.context,
        }

        response = async_message_request(self.client, request)

        self.worker_id = response['worker']

        logger.debug(f'connected to worker {self.worker_id} at {hostname()}')

    def disconnect(self) -> None:
        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.DISCONNECT.name,
            'context': self.context,
        }

        async_message_request(self.client, request)

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

        response = async_message_request(self.client, request)
        logger.info(response['message'])

    def unsubscribe(self) -> None:
        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': RequestType.UNSUBSCRIBE.name,
            'context': self.context,
        }

        response = async_message_request(self.client, request)
        logger.info(response['message'])

    def on_start(self) -> None:
        self.connect()

        if self.text is not None:
            self.subscribe()

    def on_stop(self) -> None:
        if self.text is not None:
            self.unsubscribe()

        self.disconnect()

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        return None, None

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        return None, None

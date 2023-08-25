# pylint: disable=line-too-long
'''This task performs IBM MQM get and put opertions to a specified queue or topic.

This is useful if the scenario is another user type than `MessageQueueUser`, but the scenario still requires an action towards an MQ server.
Use {@pylink grizzly.tasks.transformer} task to extract specific parts of the message.

Grizzly *must* have been installed with the extra `mq` package and native IBM MQ libraries must be installed for being able to use this variable:

``` plain
pip3 install grizzly-loadtester[mq]
```

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload}

* {@pylink grizzly.steps.scenario.tasks.step_task_client_get_endpoint_payload_metadata}

* {@pylink grizzly.steps.scenario.tasks.step_task_client_put_endpoint_file}

## Arguments

* `direction` _RequestDirection_ - if the request is upstream or downstream

* `endpoint` _str_ - specifies details to be able to perform the request, e.g. account and container information

* `name` _str_ - name used in `locust` statistics

* `destination` _str_ (optional) - **not used by this client**

* `source` _str_ (optional) - file path of local file that should be put on `endpoint`

## Format

### `endpoint`

``` plain
mq[s]://<username>:<password>@]<hostname>[:<port>]/<endpoint>?QueueManager=<queue manager>&Channel=<channel>[&wait=<wait>][&heartbeat=<heartbeat>][&KeyFile=<key repo path>[&SslCipher=<ssl cipher>][&CertLabel=<certificate label>]][&HeaderType=<header type>][&MaxMessageSize=<number of bytes>]
```

All variables in the endpoint have support for {@link framework.usage.variables.templating}.

* `mq[s]` _str_ - must be specified, `mqs` implies connecting with TLS, if `KeyFile` is not set in querystring, it will look for a key repository in `./<username>`

* `username` _str_ (optional) - username to authenticate with, default `None`

* `password` _str_ (optional) - password to authenticate with, default `None`

* `hostname` _str_ - hostname of MQ server

* `port` _int_ (optional) - port on MQ server, default `1414`

* `endpoint` _str_ - prefixed with either `topic:` or `queue:` and then the name of the endpoint to perform operations on

* `wait` _int_ (optional) - number of seconds to wait for an message, default is to wait infinite (0 seconds)

* `heartbeat` _int_ (optional) - number of seconds between heartbeats, default is 300 seconds

* `QueueManager` _str_ - name of queue manager

* `Channel` _str_ - name of channel to connect to

* `KeyFile` _str_ (optional) - path to key repository for certificates needed to connect over TLS

* `SslCipher` _str_ (optional) - SSL cipher to use for connection, default `ECDHE_RSA_AES_256_GCM_SHA384`

* `CertLabel` _str_ (optional) - label of certificate in key repository, default `username`

* `HeaderType` _str_ (optional) - header type, can be `RFH2` for sending gzip compressed messages using RFH2 header, default `None`

* `MaxMessageSize` _int_ (optional) - maximum number of bytes a message can be for the client to accept it, default is `None` which implies that the client will throw `MQRC_TRUNCATED_MSG_FAILED`, adjust buffer and try again.
'''  # noqa: E501
from contextlib import contextmanager
from typing import Optional, Dict, Any, Generator, List, cast
from urllib.parse import urlparse, parse_qs, unquote
from pathlib import Path
from platform import node as hostname
from json import dumps as jsondumps

import zmq.green as zmq

from zmq.error import ZMQError

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageResponse, AsyncMessageRequest, async_message_request

from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.scenarios import GrizzlyScenario
from grizzly.testdata.utils import resolve_variable

from . import client, ClientTask, logger


try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


@client('mq', 'mqs')
class MessageQueueClientTask(ClientTask):
    __dependencies__ = set(['async-messaged'])

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context
    _worker: Dict[int, str]

    endpoint_path: str
    context: AsyncMessageContext
    max_message_size: Optional[int]

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
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            pymqi.raise_for_error(self.__class__)

        if destination is not None:
            raise ValueError(f'{self.__class__.__name__}: destination is not allowed')

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

        self.create_context()

        self._zmq_context = zmq.Context()
        self._worker = {}
        self.max_message_size = None

    def create_context(self) -> None:
        endpoint = cast(str, resolve_variable(self.grizzly, self.endpoint, guess_datatype=False))
        parsed = urlparse(endpoint)

        if (parsed.scheme or 'none') not in ['mq', 'mqs']:
            raise ValueError(f'{self.__class__.__name__}: "{parsed.scheme}" is not a supported scheme for endpoint')

        if len(parsed.hostname or '') < 1:
            raise ValueError(f'{self.__class__.__name__}: hostname not specified in "{self.endpoint}"')

        if len(parsed.path or '') < 2:
            raise ValueError(f'{self.__class__.__name__}: no valid path component found in "{self.endpoint}"')

        if len(parsed.query or '') < 1:
            raise ValueError(f'{self.__class__.__name__}: QueueManager and Channel must be specified in the query string of "{self.endpoint}"')

        username: Optional[str] = parsed.username
        password: Optional[str] = parsed.password
        port = parsed.port or 1414

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            raise ValueError(f'{self.__class__.__name__}: QueueManager must be specified in the query string')

        if 'Channel' not in params:
            raise ValueError(f'{self.__class__.__name__}: Channel must be specified in the query string')

        queue_manager = cast(str, resolve_variable(self.grizzly, unquote(params['QueueManager'][0])))
        channel = cast(str, resolve_variable(self.grizzly, unquote(params['Channel'][0])))

        self.endpoint_path = parsed.path[1:]

        key_file: Optional[str] = None
        cert_label: Optional[str] = None
        ssl_cipher: Optional[str] = None

        if 'KeyFile' in params:
            key_file = cast(str, resolve_variable(self.grizzly, unquote(params['KeyFile'][0])))
        elif parsed.scheme == 'mqs' and username is not None:
            key_file = username

        if key_file is not None:
            cert_label = params['CertLabel'][0] if 'CertLabel' in params else username
            ssl_cipher = params.get('SslCipher', ['ECDHE_RSA_AES_256_GCM_SHA384'])[0]

        message_wait = int(params['wait'][0]) if 'wait' in params else None
        heartbeat_interval = int(params['heartbeat'][0]) if 'heartbeat' in params else None
        header_type = params['HeaderType'][0].lower() if 'HeaderType' in params else None
        self.max_message_size = int(params['MaxMessageSize'][0]) if 'MaxMessageSize' in params else None

        endpoint_parts = [f'{parsed.scheme}://']
        endpoint_parts.append(parsed.netloc)
        endpoint_parts.append(f'/{self.endpoint_path}')
        endpoint_parts.append(f'?QueueManager={queue_manager}&Channel={channel}')
        if message_wait is not None:
            endpoint_parts.append(f'&wait={message_wait}')
        if heartbeat_interval is not None:
            endpoint_parts.append(f'&heartbeat={heartbeat_interval}')
        if key_file is not None and key_file != username:
            endpoint_parts.append(f'&KeyFile={key_file}')
        if cert_label is not None and cert_label != username:
            endpoint_parts.append(f'&CertLabel={cert_label}')
        if ssl_cipher is not None and ssl_cipher != 'ECDHE_RSA_AES_256_GCM_SHA384':
            endpoint_parts.append(f'&SslCipher={ssl_cipher}')
        if header_type is not None:
            endpoint_parts.append(f'&HeaderType={header_type}')

        self.endpoint = ''.join(endpoint_parts)

        self.context = cast(AsyncMessageContext, {
            'url': self.endpoint,
            'connection': f'{parsed.hostname}({port})',
            'queue_manager': queue_manager,
            'channel': channel,
            'username': username,
            'password': password,
            'key_file': key_file,
            'cert_label': cert_label,
            'ssl_cipher': ssl_cipher,
            'message_wait': message_wait,
            'heartbeat_interval': heartbeat_interval,
            'header_type': header_type,
        })

    @contextmanager
    def create_client(self) -> Generator[zmq.Socket, None, None]:
        client: Optional[zmq.Socket] = None

        try:
            client = cast(
                zmq.Socket,
                self._zmq_context.socket(zmq.REQ),
            )
            client.connect(self._zmq_url)

            yield client
        except ZMQError:
            logger.error('zmq error', exc_info=True)
            raise
        finally:
            if client is not None:
                client.setsockopt(zmq.LINGER, 0)
                client.close()

    def connect(self, client_id: int, client: zmq.Socket, meta: Dict[str, Any]) -> None:
        request: AsyncMessageRequest = {
            'action': RequestType.CONNECT(),
            'client': client_id,
            'context': self.context,
        }

        meta.update({'action': self.endpoint_path, 'direction': '<->'})
        response: Optional[AsyncMessageResponse] = None

        try:
            response = async_message_request(client, request)
        except:
            raise
        finally:
            meta.update({'response_length': len((response or {}).get('payload', None) or '')})

        self._worker.update({client_id: response['worker']})

    def request(self, parent: GrizzlyScenario, request: AsyncMessageRequest) -> AsyncMessageResponse:
        # always include full context in request
        endpoint = request['context']['endpoint']
        context = self.context.copy()
        context.update({'endpoint': endpoint})
        request.update({'context': context})

        with self.create_client() as client:
            client_id = id(parent.user)
            worker = self._worker.get(client_id, None)
            if worker is None:
                with self.action(parent, suppress=True) as meta:
                    self.connect(client_id, client, meta)
                    worker = self._worker.get(client_id, None)
                    parent.logger.debug(f'connected to worker {worker} at {hostname()}')

            if worker is None:
                raise RuntimeError(f'{parent.__class__.__name__}/{client_id} was unable to get an worker assigned')

            with self.action(parent) as meta:
                request.update({
                    'worker': worker,
                    'client': client_id,
                })
                response: Optional[AsyncMessageResponse] = None

                try:
                    response = async_message_request(client, request)
                    parent.logger.debug(f'got response from {worker} at {hostname()}')
                except:
                    raise
                finally:
                    response_length_source = ((response or {}).get('payload', None) or '').encode('utf-8')

                    meta.update({
                        'action': self.endpoint_path,
                        'request': request.copy(),
                        'response_length': len(response_length_source),
                        'response': response,
                    })

                payload = response.get('payload', None)
                if payload is None or len(payload.encode()) < 1:
                    raise RuntimeError('response did not contain any payload')

                return response

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        endpoint: List[str] = [self.endpoint_path]
        if self.max_message_size is not None:
            endpoint.append(f'max_message_size:{self.max_message_size}')

        request: AsyncMessageRequest = {
            'action': 'GET',
            'worker': None,
            'context': {
                'endpoint': ', '.join(endpoint),
            },
            'payload': None,
        }
        response = self.request(parent, request)

        if response is not None:
            if self.payload_variable is not None and response.get('payload', None) is not None:
                parent.user._context['variables'][self.payload_variable] = response['payload']

            if self.metadata_variable is not None and response.get('metadata', None) is not None:
                parent.user._context['variables'][self.metadata_variable] = jsondumps(response['metadata'])

        response = response or {}

        return response.get('metadata', None), response.get('payload', None)

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.render(cast(str, self.source))
        source_file = Path(self._context_root) / 'requests' / source

        if source_file.exists():
            source = parent.render(source_file.read_text())

        request: AsyncMessageRequest = {
            'action': 'PUT',
            'worker': None,
            'context': {
                'endpoint': self.endpoint_path,
            },
            'payload': source,
        }

        response = self.request(parent, request)

        return response.get('metadata', None), response.get('payload', None)

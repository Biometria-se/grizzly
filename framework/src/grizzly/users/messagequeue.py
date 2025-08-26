"""Get and put messages on with IBM MQ queues.

User is based on `pymqi` for communicating with IBM MQ. However `pymqi` uses native libraries which `gevent` (used by `locust`) cannot patch,
which causes any calls in `pymqi` to block the rest of `locust`. To get around this, the user implementation communicates with a stand-alone
process via 0mq, which in turn communicates with IBM MQ.

`async-messaged` starts automagically when a scenario uses `MessageQueueUser` and `pymqi` dependencies are installed.

## Request methods

Supports the following request methods:

* send
* put
* get
* receive

## Format

Format of `host` is the following:

```plain
mq://<hostname>:<port>/?QueueManager=<queue manager name>&Channel=<channel name>
```

`endpoint` in the request is the name of an MQ queue. This can also be combined with an expression, if
a specific message is to be retrieved from the queue. The format of endpoint is:

```plain
queue:<queue_name>[, expression:<expression>][, max_message_size:<max_message_size>]
```

`<expression>` can be a XPath or jsonpath expression, depending on the specified content type, see
[Get message with expression][grizzly.users.messagequeue--get-message-with-expression] example.

`<max_message_size>` is the maximum number of bytes a message can be for being able to accept it. If not set, the client will
reject the message with `MQRC_TRUNCATED_MSG_FAILED`, adjust the message buffer and try again. If set, and the message is bigger than
the specified size, the message will be rejected by the client and will eventually fail.

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
Then put request "test/queue-message.j2.json" with name "queue-message" to endpoint "queue:INCOMING.MESSAGES"
```

### Get message

Default behavior is to fail directly if there is no message on the queue. If the request should wait until a message is available,
set the time it should wait with `message.wait` (seconds) context variable.

To keep the connection alive during longer waiting periods, a heartbeat interval can be configured using the
`connection.heartbeat_interval` (seconds) context variable (default 300).

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.wait" to "5"
Then get request with name "get-queue-message" from endpoint "queue:INCOMING.MESSAGES"
```

In this example, the request will not fail if there is a message on queue within 5 seconds.

### Get message with expression

When specifying an expression, the messages on the queue are first browsed. If any message matches the expression, it is
later consumed from the queue. If no matching message was found during browsing, it is repeated again after a slight delay,
up until the specified `message.wait` seconds has elapsed. To use expressions, a content type must be specified for the get
request, e.g. `application/xml`:

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.wait" to "5"
Then get request with name "get-specific-queue-message" from endpoint "queue:INCOMING.MESSAGES, expression: //document[@id='abc123']"
And set response content type to "application/xml"
```

### Authentication

#### Username and password

```gherkin
Given a user of type "MessageQueue" load testing "mq://mqm:admin@mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "auth.username" to "<username>"
And set context variable "auth.password" to "<password>"
```

#### With TLS

A [key repository](https://www.ibm.com/docs/en/ibm-mq/7.5?topic=wstulws-setting-up-key-repository-unix-linux-windows-systems)
(`.kdb` and `.sth`) for the user is needed, and is specified with `auth.key_file` excluding the file extension.

```gherkin
Given a user of type "MessageQueue" load testing "mq://mqm:admin@mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "auth.username" to "<username>"
And set context variable "auth.password" to "<password>"
And set context variable "auth.key_file" to "<path to key file, excl. file extension>"
```

Default SSL cipher is `ECDHE_RSA_AES_256_GCM_SHA384`, change it by setting `auth.ssl_cipher` context variable.

Default certificate label is set to `auth.username`, change it by setting `auth.cert_label` context variable.

### Header type

Basic support exist for [RFH2](https://www.ibm.com/docs/en/ibm-mq/7.5?topic=2-overview), and communicating with MQ using gzip
compressed messages. When receiving messages, the RFH2 is automatically detected and (somewhat) supported. If RFH2 should be
added when sending messages, with gzip compression, the context variable `message.header_type` should be set to `RFH2`:

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.header_type" to "rfh2"
Then put request "test/queue-message.j2.json" with name "gzipped-message" to endpoint "queue:GZIPPED.MESSAGES"
```

Default header type is none, i.e. no header is added to the sent messages. To use no header, either set `message.header_type`
to `None` or omit setting the context variable at all.

To set a user value in the RFH2 header of the message, set `metadata` after the request, e.g.:

```gherkin
Then put request "test/queue-message.j2.json" with name "gzipped-message" to endpoint "queue:GZIPPED.MESSAGES"
And metadata "filename" is "my_filename"
```
"""

from __future__ import annotations

import logging
from contextlib import contextmanager, suppress
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import parse_qs, unquote, urlparse

import zmq.green as zmq
from async_messaged.utils import async_message_request
from grizzly_common.arguments import get_unsupported_arguments, parse_arguments
from zmq import sugar as ztypes

from grizzly.exceptions import StopScenario
from grizzly.types import GrizzlyResponse, RequestDirection, RequestType, StrDict
from grizzly.utils import merge_dicts
from grizzly.utils.protocols import zmq_disconnect

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Generator

    from async_messaged import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse

    from grizzly.tasks import RequestTask
    from grizzly.testdata.communication import GrizzlyDependencies
    from grizzly.types.locust import Environment

# no used here, but needed for sanity check
try:
    # do not fail grizzly if ibm mq dependencies are missing, some might
    # not be interested in MessageQueueUser.
    import pymqi
except:
    from grizzly_common import dummy_pymqi as pymqi


@grizzlycontext(
    context={
        'auth': {
            'username': None,
            'password': None,
            'key_file': None,
            'cert_label': None,
            'ssl_cipher': None,
        },
        'message': {
            'wait': None,
            'header_type': None,
        },
    },
)
class MessageQueueUser(GrizzlyUser):
    __dependencies__: ClassVar[GrizzlyDependencies] = {'async-messaged'}

    am_context: AsyncMessageContext
    worker_id: str | None
    zmq_context = zmq.Context()
    zmq_client: ztypes.Socket
    zmq_url = 'tcp://127.0.0.1:5554'

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        if pymqi.__name__ == 'grizzly_common.dummy_pymqi':
            pymqi.raise_for_error(self.__class__)

        super().__init__(environment, *args, **kwargs)

        # Get configuration values from host string
        parsed = urlparse(self.host or '')

        if parsed.scheme != 'mq':
            message = f'"{parsed.scheme}" is not a supported scheme for {self.__class__.__name__}'
            raise ValueError(message)

        if parsed.hostname is None or len(parsed.hostname) < 1:
            message = f'{self.__class__.__name__}: hostname is not specified in {self.host}'
            raise ValueError(message)

        if parsed.username is not None or parsed.password is not None:
            message = f'{self.__class__.__name__}: username and password should be set via context variables "auth.username" and "auth.password"'
            raise ValueError(message)

        if parsed.query == '':
            message = f'{self.__class__.__name__} needs QueueManager and Channel in the query string'
            raise ValueError(message)

        port = parsed.port or 1414

        self.am_context = {
            'url': self.host or '',
            'connection': f'{parsed.hostname}({port})',
        }

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            message = f'{self.__class__.__name__} needs QueueManager in the query string'
            raise ValueError(message)

        if 'Channel' not in params:
            message = f'{self.__class__.__name__} needs Channel in the query string'
            raise ValueError(message)

        self.am_context.update(
            {
                'queue_manager': unquote(params['QueueManager'][0]),
                'channel': unquote(params['Channel'][0]),
            },
        )

        self.logger.debug('auth context: %r', self._context.get('auth', {}))

        auth_context = self._context.get('auth', {})
        username = auth_context.get('username', None)
        message_context = self._context.get('message', {})
        header_type = message_context.get('header_type', None) or 'none'
        header_type = header_type.lower()
        if header_type not in ['rfh2', 'none']:
            message = f'{self.__class__.__name__} unsupported value for header_type: "{header_type}", supported ones are "None" and "RFH2"'
            raise ValueError(message)

        if header_type == 'none':
            header_type = None

        key_file = auth_context.get('key_file', None)

        if key_file is not None:
            key_file_path = self._context_root / f'{key_file}.kdb'

            if not key_file_path.exists():
                message = f'{self.__class__.__name__} key file {key_file} does not exist'
                raise ValueError(message)

            key_file = key_file_path.resolve().with_suffix('').as_posix()

        self.am_context.update(
            {
                'username': username,
                'password': auth_context.get('password', None),
                'key_file': key_file,
                'cert_label': auth_context.get('cert_label', None) or username,
                'ssl_cipher': auth_context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': message_context.get('wait', None),
                'heartbeat_interval': self._context.get('connection', {}).get('heartbeat_interval', None),
                'header_type': header_type,
            },
        )

        self.worker_id = None

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

    def on_start(self) -> None:
        self.logger.debug('on_start called')
        super().on_start()

        try:
            with self._request_context(
                {
                    'action': RequestType.CONNECT(),
                    'client': id(self),
                    'context': self.am_context,
                },
            ):
                self.zmq_client = self.zmq_context.socket(zmq.REQ)
                self.zmq_client.setsockopt(zmq.LINGER, 0)
                self.zmq_client.connect(self.zmq_url)
        except Exception as e:
            self.logger.exception('on_start failed')
            raise StopScenario from e

    def on_stop(self) -> None:
        self.logger.debug('on_stop called, worker_id=%s', self.worker_id)

        with suppress(Exception):
            if self.worker_id is not None:
                with self._request_context(
                    {
                        'action': RequestType.DISCONNECT(),
                        'worker': self.worker_id,
                        'client': id(self),
                        'context': self.am_context,
                    },
                ):
                    pass

        with suppress(Exception):
            zmq_disconnect(self.zmq_client, destroy_context=False)

        self.worker_id = None

        super().on_stop()

    @contextmanager
    def _request_context(self, am_request: AsyncMessageRequest) -> Generator[StrDict, None, None]:
        response: AsyncMessageResponse | None = None
        context: dict = {
            'metadata': None,
            'payload': None,
        }

        yield context
        response = async_message_request(self.zmq_client, am_request)

        payload = response.get('payload', None)
        metadata = response.get('metadata', None)

        worker_id = (response or {}).get('worker', None)

        if self.worker_id is None and worker_id is not None:
            self.worker_id = worker_id

        context.update(
            {
                'metadata': metadata,
                'payload': payload,
            },
        )

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        am_context = cast(
            'AsyncMessageContext',
            merge_dicts(
                cast('StrDict', self.am_context),
                {
                    'endpoint': request.endpoint,
                    'metadata': request.metadata,
                    'content_type': request.response.content_type.name.lower(),
                },
            ),
        )
        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'worker': self.worker_id,
            'client': id(self),
            'context': am_context,
            'payload': request.source,
        }

        with self._request_context(am_request) as response:
            # Parse the endpoint to validate queue name / expression parts
            arguments = parse_arguments(request.endpoint, ':')

            if 'queue' not in arguments:
                message = 'queue name must be prefixed with queue:'
                raise RuntimeError(message)

            unsupported_arguments = get_unsupported_arguments(['queue', 'expression', 'max_message_size'], arguments)
            if len(unsupported_arguments) > 0:
                message = f'arguments {", ".join(unsupported_arguments)} is not supported'
                raise RuntimeError(message)

            if 'expression' in arguments and request.method.direction != RequestDirection.FROM:
                message = 'argument "expression" is not allowed when sending to an endpoint'
                raise RuntimeError(message)

        return (response['metadata'], response['payload'])

'''Get and put messages on with IBM MQ queues.

User is based on `pymqi` for communicating with IBM MQ. However `pymqi` uses native libraries which `gevent` (used by `locust`) cannot patch,
which causes any calls in `pymqi` to block the rest of `locust`. To get around this, the user implementation communicates with a stand-alone
process via zmq, which in turn communicates with IBM MQ.

`async-messaged` starts automagically when a scenario uses `MessageQueueUser` and `pymqi` dependencies are installed.

## Request methods

Supports the following request methods:

* send
* put
* get
* receive

## Format

Format of `host` is the following:

``` plain
mq://<hostname>:<port>/?QueueManager=<queue manager name>&Channel=<channel name>
```

`endpoint` in the request is the name of an MQ queue. This can also be combined with an expression, if
a specific message is to be retrieved from the queue. The format of endpoint is:

``` plain
queue:<queue_name>[, expression:<expression>][, max_message_size:<max_message_size>]
```

Where `<expression>` can be a XPath or jsonpath expression, depending on the specified content type. See example below.
Where `<max_message_size>` is the maximum number of bytes a message can be for being able to accept it. If not set, the client will
reject the message with `MQRC_TRUNCATED_MSG_FAILED`, adjust the message buffer and try again. If set, and the message is bigger than
the specified size, the message will be rejected by the client and will eventually fail.

## Examples

Example of how to use it in a scenario:

``` gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
Then put request "test/queue-message.j2.json" with name "queue-message" to endpoint "queue:INCOMING.MESSAGES"
```
### Get message

Default behavior is to fail directly if there is no message on the queue. If the request should wait until a message is available,
set the time it should wait with `message.wait` (seconds) context variable.

To keep the connection alive during longer waiting periods, a heartbeat interval can be configured using the
`connection.heartbeat_interval` (seconds) context variable (default 300).

``` gherkin
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

``` gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.wait" to "5"
Then get request with name "get-specific-queue-message" from endpoint "queue:INCOMING.MESSAGES, expression: //document[@id='abc123']"
And set response content type to "application/xml"
```

### Authentication

#### Username and password

``` gherkin
Given a user of type "MessageQueue" load testing "mq://mqm:admin@mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "auth.username" to "<username>"
And set context variable "auth.password" to "<password>"
```

#### With TLS

A [key repository](https://www.ibm.com/docs/en/ibm-mq/7.5?topic=wstulws-setting-up-key-repository-unix-linux-windows-systems)
(3 files; `.kdb`, `.rdb` and `.sth`) for the user is needed, and is specified with `auth.key_file` excluding the file extension.

``` gherkin
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

``` gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.header_type" to "rfh2"
Then put request "test/queue-message.j2.json" with name "gzipped-message" to endpoint "queue:GZIPPED.MESSAGES"
```

Default header type is none, i.e. no header is added to the sent messages. To use no header, either set `message.header_type`
to `None` or omit setting the context variable at all.

To set a user value in the RFH2 header of the message, set `metadata` after the request, e.g.:

``` gherkin
Then put request "test/queue-message.j2.json" with name "gzipped-message" to endpoint "queue:GZIPPED.MESSAGES"
And metadata "filename" is "my_filename"
```
'''
import logging

from typing import Dict, Any, Generator, Tuple, Optional, cast
from urllib.parse import urlparse, parse_qs, unquote
from contextlib import contextmanager

import zmq.green as zmq

from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse, async_message_request
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments

from grizzly.types import GrizzlyResponse, RequestDirection, RequestType
from grizzly.types.locust import Environment
from grizzly.tasks import RequestTask
from grizzly.utils import merge_dicts
from grizzly.exceptions import StopScenario

from .base import GrizzlyUser, ResponseHandler


# no used here, but needed for sanity check
try:
    # do not fail grizzly if ibm mq dependencies are missing, some might
    # not be interested in MessageQueueUser.
    import pymqi  # pylint: disable=unused-import
except:
    from grizzly_extras import dummy_pymqi as pymqi


class MessageQueueUser(ResponseHandler, GrizzlyUser):
    _context: Dict[str, Any] = {
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
    }

    __dependencies__ = set(['async-messaged'])

    am_context: AsyncMessageContext
    worker_id: Optional[str]
    zmq_context = zmq.Context()
    zmq_client: zmq.Socket
    zmq_url = 'tcp://127.0.0.1:5554'

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            pymqi.raise_for_error(self.__class__)

        super().__init__(environment, *args, **kwargs)

        # Get configuration values from host string
        parsed = urlparse(self.host or '')

        if parsed.scheme != 'mq':
            raise ValueError(f'"{parsed.scheme}" is not a supported scheme for {self.__class__.__name__}')

        if parsed.hostname is None or len(parsed.hostname) < 1:
            raise ValueError(f'{self.__class__.__name__}: hostname is not specified in {self.host}')

        if parsed.username is not None or parsed.password is not None:
            raise ValueError(f'{self.__class__.__name__}: username and password should be set via context variables "auth.username" and "auth.password"')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__} needs QueueManager and Channel in the query string')

        port = parsed.port or 1414

        self.am_context = {
            'url': self.host or '',
            'connection': f'{parsed.hostname}({port})',
        }

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            raise ValueError(f'{self.__class__.__name__} needs QueueManager in the query string')

        if 'Channel' not in params:
            raise ValueError(f'{self.__class__.__name__} needs Channel in the query string')

        self.am_context.update({
            'queue_manager': unquote(params['QueueManager'][0]),
            'channel': unquote(params['Channel'][0]),
        })

        # Get configuration values from context
        self._context = merge_dicts(super().context(), self.__class__._context)

        auth_context = self._context.get('auth', {})
        username = auth_context.get('username', None)
        message_context = self._context.get('message', {})
        header_type = message_context.get('header_type', None) or 'none'
        header_type = header_type.lower()
        if header_type not in ['rfh2', 'none']:
            raise ValueError(f'{self.__class__.__name__} unsupported value for header_type: "{header_type}", supported ones are "None" and "RFH2"')
        elif header_type == 'none':
            header_type = None

        self.am_context.update({
            'username': username,
            'password': auth_context.get('password', None),
            'key_file': auth_context.get('key_file', None),
            'cert_label': auth_context.get('cert_label', None) or username,
            'ssl_cipher': auth_context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384',
            'message_wait': message_context.get('wait', None),
            'heartbeat_interval': self._context.get('connection', {}).get('heartbeat_interval', None),
            'header_type': header_type,
        })

        self.worker_id = None

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

    def on_start(self) -> None:
        self.logger.debug('on_start called')
        super().on_start()

        try:
            with self.request_context(None, {
                'action': RequestType.CONNECT(),
                'client': id(self),
                'context': self.am_context,
            }):
                self.zmq_client = self.zmq_context.socket(zmq.REQ)
                self.zmq_client.connect(self.zmq_url)
        except:
            self.logger.error('on_start failed', exc_info=True)
            raise StopScenario()

    def on_stop(self) -> None:
        self.logger.debug(f'on_stop called, {self.worker_id=}')
        if self.worker_id is None:
            return

        with self.request_context(None, {
            'action': RequestType.DISCONNECT(),
            'worker': self.worker_id,
            'client': id(self),
            'context': self.am_context,
        }):
            pass

        try:
            self.zmq_client.disconnect(self.zmq_url)
        except:
            pass

        self.worker_id = None

        super().on_stop()

    @contextmanager
    def request_context(self, request: Optional[RequestTask], am_request: AsyncMessageRequest) -> Generator[Dict[str, Any], None, None]:
        response: Optional[AsyncMessageResponse] = None
        context: Dict[str, Any] = {
            'metadata': None,
            'payload': None,
        }

        yield context
        response = async_message_request(self.zmq_client, am_request)

        context.update({
            'metadata': response.get('metadata', None),
            'payload': response.get('payload', None),
        })

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        am_context = cast(AsyncMessageContext, merge_dicts(
            cast(Dict[str, Any], self.am_context),
            {
                'endpoint': request.endpoint,
                'metadata': request.metadata,
                'content_type': request.response.content_type.name.lower(),
            },
        ))
        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'worker': self.worker_id,
            'client': id(self),
            'context': am_context,
            'payload': request.source,
        }

        with self.request_context(request, am_request) as response:
            # Parse the endpoint to validate queue name / expression parts
            arguments = parse_arguments(request.endpoint, ':')

            if 'queue' not in arguments:
                raise RuntimeError('queue name must be prefixed with queue:')

            unsupported_arguments = get_unsupported_arguments(['queue', 'expression', 'max_message_size'], arguments)
            if len(unsupported_arguments) > 0:
                raise RuntimeError(f'arguments {", ".join(unsupported_arguments)} is not supported')

            if 'expression' in arguments and request.method.direction != RequestDirection.FROM:
                raise RuntimeError('argument "expression" is not allowed when sending to an endpoint')

        return (response['metadata'], response['payload'],)

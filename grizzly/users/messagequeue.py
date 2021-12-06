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

```plain
mq://<hostname>:<port>/?QueueManager=<queue manager name>&Channel=<channel name>
```

`endpoint` in the request is the name of an MQ queue. This can also be combined with an expression, if
a specific message is to be retrieved from the queue. The format of endpoint is:

```plain
queue:<queue_name>[, expression:<expression>]
```

Where `<expression>` can be a XPath or jsonpath expression, depending on the specified content type. See example below.

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
Then put request "test/queue-message.j2.json" with name "queue-message" to endpoint "queue:INCOMING.MESSAGES"
```
### Get message

Default behavior is to fail directly if there is no message on the queue. If the request should wait until a message is available,
set the time it should wait with `message.wait` (seconds) context variable.

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
(3 files; `.kdb`, `.rdb` and `.sth`) for the user is needed, and is specified with `auth.key_file` excluding the file extension.

```gherkin
Given a user of type "MessageQueue" load testing "mq://mqm:admin@mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "auth.username" to "<username>"
And set context variable "auth.password" to "<password>"
And set context variable "auth.key_file" to "<path to key file, excl. file extension>"
```

Default SSL cipher is `ECDHE_RSA_AES_256_GCM_SHA384`, change it by setting `auth.ssl_cipher` context variable.

Default certificate label is set to `auth.username`, change it by setting `auth.cert_label` context variable.
'''
from typing import Dict, Any, Generator, Tuple, Optional, cast
from urllib.parse import urlparse, parse_qs, unquote
from contextlib import contextmanager
from time import monotonic as time


import zmq


from gevent import sleep as gsleep
from locust.exception import StopUser
from grizzly.types import RequestDirection
from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse, AsyncMessageError
from grizzly_extras.arguments import get_unsupported_arguments, parse_arguments
from grizzly_extras.transformer import TransformerContentType

from ..types import RequestDirection
from ..task import RequestTask
from ..utils import merge_dicts
from .meta import ContextVariables, ResponseHandler, RequestLogger
from . import logger


# no used here, but needed for sanity check
try:
    # do not fail grizzly if ibm mq dependencies are missing, some might
    # not be interested in MessageQueueUser.
    import pymqi  # pylint: disable=unused-import
except:
    from grizzly_extras import dummy_pymqi as pymqi

class MessageQueueUser(ResponseHandler, RequestLogger, ContextVariables):
    _context: Dict[str, Any] = {
        'auth': {
            'username': None,
            'password': None,
            'key_file': None,
            'cert_label': None,
            'ssl_cipher': None
        },
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

    def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            raise NotImplementedError('MessageQueueUser could not import pymqi, have you installed IBM MQ dependencies?')

        super().__init__(*args, **kwargs)

        # Get configuration values from host string
        parsed = urlparse(self.host)

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
            'url': self.host,
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
        self.am_context.update({
            'username': username,
            'password': auth_context.get('password', None),
            'key_file': auth_context.get('key_file', None),
            'cert_label': auth_context.get('cert_label', None) or username,
            'ssl_cipher': auth_context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384',
            'message_wait': self._context.get('message', {}).get('wait', None),
        })

        self.worker_id = None


    def request(self, request: RequestTask) -> None:
        request_name, endpoint, payload = self.render(request)

        @contextmanager
        def action(am_request: AsyncMessageRequest, name: str) -> Generator[Dict[str, Any], None, None]:
            exception: Optional[Exception] = None
            metadata: Dict[str, Any] = {
                'abort': False,
                'meta': False,
            }

            response: Optional[AsyncMessageResponse] = None

            try:
                start_time = time()

                yield metadata

                self.zmq_client.send_json(am_request)

                # do not block all other "threads", just it self
                while True:
                    try:
                        response = cast(AsyncMessageResponse, self.zmq_client.recv_json(flags=zmq.NOBLOCK))
                        break
                    except zmq.Again:
                        gsleep(0.1)

            except Exception as e:
                exception = e
            finally:
                total_time = int((time() - start_time) * 1000)  # do not include event handling in request time

                if response is not None:
                    if self.worker_id is None:
                        self.worker_id = response['worker']
                    else:
                        assert self.worker_id == response['worker'], f'worker changed from {self.worker_id} to {response["worker"]}'

                    mq_response_time = response.get('response_time', 0)

                    delta = total_time - mq_response_time
                    if delta > 100:  # @TODO: what is a suitable value?
                        logger.warning(f'{self.__class__.__name__}: communicating with async-messaged took {delta} ms')

                    if not response['success'] and exception is None:
                        exception = AsyncMessageError(response['message'])
                else:
                    response = {}

                try:
                    if not metadata.get('meta', False):
                        self.response_event.fire(
                            name=name,
                            request=request,
                            context=(
                                response.get('metadata', None),
                                response.get('payload', None),
                            ),
                            user=self,
                            exception=exception,
                        )
                except Exception as e:
                    if exception is None:
                        exception = e
                finally:
                    self.environment.events.request.fire(
                        request_type=f'mq:{am_request["action"][:4]}',
                        name=name,
                        response_time=total_time,
                        response_length=response.get('response_length', None) or 0,
                        context=self._context,
                        exception=exception,
                    )

                if exception is not None and metadata.get('abort', False):
                    try:
                        self.zmq_client.disconnect(self.zmq_url)
                    except:
                        pass

                    raise StopUser()

        name = f'{request.scenario.identifier} {request_name}'

        # connect to queue manager at first request
        if self.worker_id is None:
            with action({
                'action': 'CONN',
                'context': self.am_context
            }, self.am_context['connection']) as metadata:
                metadata.update({
                    'meta': True,
                    'abort': True,
                })
                self.zmq_client = self.zmq_context.socket(zmq.REQ)
                self.zmq_client.connect(self.zmq_url)

        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'worker': self.worker_id,
            'context': {
                'endpoint': endpoint,
            },
            'payload': payload,
        }

        if request.response.content_type != TransformerContentType.GUESS:
            am_request['context']['content_type'] = request.response.content_type.name.lower()

        with action(am_request, name) as metadata:
            metadata['abort'] = True
            # Parse the endpoint to validate queue name / expression parts
            try:
                arguments = parse_arguments(endpoint, ':')
            except ValueError as e:
                raise RuntimeError(str(e)) from e

            if 'queue' not in arguments:
                raise RuntimeError('queue name must be prefixed with queue:')

            unsupported_arguments = get_unsupported_arguments(['queue', 'expression'], arguments)
            if len(unsupported_arguments) > 0:
                raise RuntimeError(f'arguments {", ".join(unsupported_arguments)} is not supported')

            if 'expression' in arguments and request.method.direction != RequestDirection.FROM:
                raise RuntimeError('argument "expression" is not allowed when sending to an endpoint')

            metadata['abort'] = request.scenario.stop_on_failure

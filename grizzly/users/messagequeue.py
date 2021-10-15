'''Communicates with IBM MQ.

User is based on `pymqi` for communicating with IBM MQ. However `pymqi` uses native libraries which `gevent` (used by `locust`) cannot patch,
which causes any calls in `pymqi` to block the rest of `locust`. To get around this, the user implementation communicates with a stand-alone
process via zmq, which then in turn communicates with IBM MQ.

The message queue daemon process is started automagically when a scenario contains the `MessageQueueUser` and `pymqi` dependencies are installed.

Format of `host` is the following:

```plain
mq://<hostname>:<port>/?QueueManager=<queue manager name>&Channel=<channel name>
```

`endpoint` in the request is the name of an MQ queue.

Example of how to use it in a scenario:

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
Then put request "test/queue-message.j2.json" with name "queue-message" to endpoint "INCOMING.MESSAGES"
```

Supports the following request methods:

* send
* put
* get
* receive

## GET / RECEIVE

Default behavior is to fail directly if there is no message on the queue. If the request should wait until a message is available,
set the time it should wait with `message.wait` (seconds) context variable.

```gherkin
Given a user of type "MessageQueue" load testing "mq://mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "message.wait" to "5"
Then get request with name "get-queue-message" from endpoint "INCOMING.MESSAGES"
```

In this example, the request will not fail if there is a message on queue within 5 seconds.

## Authentication

### Username and password

```gherkin
Given a user of type "MessageQueue" load testing "mq://mqm:admin@mq.example.com/?QueueManager=QM01&Channel=SRVCONN01"
And set context variable "auth.username" to "<username>"
And set context variable "auth.password" to "<password>"
```

### With TLS

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
from typing import Dict, Any, Generator, Tuple, Optional
from urllib.parse import urlparse, parse_qs, unquote
from contextlib import contextmanager
from time import monotonic as time
from grizzly_extras.messagequeue import MessageQueueContext, MessageQueueRequest, MessageQueueResponse


import zmq


from gevent import sleep as gsleep
from locust.exception import StopUser, CatchResponseError

from .meta import ContextVariables, ResponseHandler, RequestLogger
from ..context import RequestContext
from ..testdata.utils import merge_dicts
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

    mq_context: MessageQueueContext
    worker_id: Optional[str]
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

        self.mq_context = {
            'connection': f'{parsed.hostname}({port})',
        }

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            raise ValueError(f'{self.__class__.__name__} needs QueueManager in the query string')

        if 'Channel' not in params:
            raise ValueError(f'{self.__class__.__name__} needs Channel in the query string')

        self.mq_context.update({
            'queue_manager': unquote(params['QueueManager'][0]),
            'channel': unquote(params['Channel'][0]),
        })

        # Get configuration values from context
        self._context = merge_dicts(super().context(), self.__class__._context)

        auth_context = self._context.get('auth', {})
        username = auth_context.get('username', None)
        self.mq_context.update({
            'username': username,
            'password': auth_context.get('password', None),
            'key_file': auth_context.get('key_file', None),
            'cert_label': auth_context.get('cert_label', None) or username,
            'ssl_cipher': auth_context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384',
            'message_wait': self._context.get('message', {}).get('wait', None),
        })

        self.worker_id = None


    def request(self, request: RequestContext) -> None:
        request_name, endpoint, payload = self.render(request)

        @contextmanager
        def action(mq_request: MessageQueueRequest, name: str, abort: bool, meta: bool = False) -> Generator[None, None, None]:
            exception: Optional[Exception] = None

            response: Optional[MessageQueueResponse] = None

            try:
                start_time = time()

                yield

                self.zmq_client.send_json(mq_request)

                # do not block all other "threads", just it self
                while True:
                    try:
                        response = self.zmq_client.recv_json(flags=zmq.NOBLOCK)
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
                        logger.warning(f'{self.__class__.__name__}: comunicating with messagequeue-daemon took {delta} ms')

                    if not response['success'] and exception is None:
                        exception = CatchResponseError(response['message'])
                else:
                    response = {}

                try:

                    if not meta:
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
                        request_type=f'mq:{mq_request["action"]}',
                        name=name,
                        response_time=total_time,
                        response_length=response.get('response_length', None) or 0,
                        context=self._context,
                        exception=exception,
                    )

                if exception is not None and abort:
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
                'context': self.mq_context
            }, self.mq_context['connection'], abort=True, meta=True):
                zmq_context = zmq.Context()
                self.zmq_client = zmq_context.socket(zmq.REQ)
                self.zmq_client.connect(self.zmq_url)

        message_queue_request: MessageQueueRequest = {
            'action': request.method.name,
            'worker': self.worker_id,
            'context': {
                'queue': endpoint,
            },
            'payload': payload,
        }

        with action(message_queue_request, name, abort=request.scenario.stop_on_failure):
            pass

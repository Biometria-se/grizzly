'''Communicates with IBM MQ.

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


from locust.exception import StopUser

from .meta import ContextVariables, ResponseHandler, RequestLogger
from ..types import RequestMethod
from ..context import RequestContext
from ..testdata.utils import merge_dicts


try:
    # do not fail grizzly if ibm mq dependencies are missing, some might
    # not be interested in MessageQueueUser.
    import pymqi
    has_dependency = True
except:
    has_dependency = False

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

    if not has_dependency:
        def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
            raise NotImplementedError('MessageQueueUser could not import pymqi, have you installed IBM MQ dependencies?')
    else:
        qmgr: Optional[pymqi.QueueManager]
        md: pymqi.MD
        gmo: Optional[pymqi.GMO]
        host: str
        port: int
        queue_manager: str
        channel: str
        username: str
        password: str
        key_file: Optional[str]
        cert_label: str

        _get_arguments: Tuple[Any, ...]

        def __init__(self, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
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

            self.hostname = parsed.hostname
            self.port = parsed.port or 1414

            params = parse_qs(parsed.query)

            if 'QueueManager' not in params:
                raise ValueError(f'{self.__class__.__name__} needs QueueManager in the query string')

            if 'Channel' not in params:
                raise ValueError(f'{self.__class__.__name__} needs Channel in the query string')

            self.queue_manager = unquote(params['QueueManager'][0])
            self.channel = unquote(params['Channel'][0])

            # Get configuration values from context
            self._context = merge_dicts(super().context(), self.__class__._context)

            auth_context = self._context.get('auth', {})
            self.username = auth_context.get('username', None)
            self.password = auth_context.get('password', None)

            self.key_file = auth_context.get('key_file', None)
            self.cert_label = auth_context.get('cert_label', None) or self.username
            self.ssl_cipher = auth_context.get('ssl_cipher', None) or 'ECDHE_RSA_AES_256_GCM_SHA384'

            message_wait = self._context.get('message', {}).get('wait', None)

            self.md = pymqi.MD()
            self._get_arguments = (None, self.md)

            if message_wait is not None and message_wait > 0:
                self.gmo = pymqi.GMO(
                    Options=pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING,
                    WaitInterval=message_wait*1000,
                )
                self._get_arguments += (self.gmo, )

            self.qmgr = None

        @contextmanager
        def _queue(self, endpoint: str) -> Generator[pymqi.Queue, None, None]:
            queue = pymqi.Queue(self.qmgr, endpoint)

            try:
                yield queue
            finally:
                queue.close()

        def request(self, request: RequestContext) -> None:
            response_length = 0
            request_name, endpoint, payload = self.render(request)

            @contextmanager
            def action(request_type: str, name: str, abort: bool, meta: bool = False) -> Generator[None, None, None]:
                exception: Optional[Exception] = None

                try:
                    start_time = time()
                    yield
                except Exception as e:
                    exception = e
                finally:
                    total_time = int((time() - start_time) * 1000)  # do not include event handling in request time

                    try:
                        if not meta:
                            self.response_event.fire(
                                name=name,
                                request=request,
                                context=(self.md.get(), payload),
                                user=self,
                                exception=exception,
                            )
                    except Exception as e:
                        if exception is None:
                            exception = e
                    finally:
                        self.environment.events.request.fire(
                            request_type=f'mq:{request_type}',
                            name=name,
                            response_time=total_time,
                            response_length=response_length,
                            context=self._context,
                            exception=exception,
                        )

                    if exception is not None and abort:
                        raise StopUser()

            name = f'{request.scenario.identifier} {request_name}'

            # connect to queue manager at first request
            if self.qmgr is None:
                conn_info = f'{self.hostname}({self.port})'
                with action('CONN', conn_info, abort=True, meta=True):
                    if self.key_file is not None:
                        cd = pymqi.CD(
                            ChannelName=self.channel.encode('utf-8'),
                            ConnectionName=conn_info.encode('utf-8'),
                            ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
                            TransportType=pymqi.CMQC.MQXPT_TCP,
                            SSLCipherSpec=self.ssl_cipher.encode('utf-8'),
                        )

                        sco = pymqi.SCO(
                            KeyRepository=self.key_file.encode('utf-8'),
                            CertificateLabel=self.cert_label.encode('utf-8'),
                        )

                        self.qmgr = pymqi.QueueManager(None)
                        self.qmgr.connect_with_options(
                            self.queue_manager,
                            user=self.username.encode('utf-8'),
                            password=self.password.encode('utf-8'),
                            cd=cd,
                            sco=sco,
                        )
                    else:
                        self.qmgr = pymqi.connect(
                            self.queue_manager,
                            self.channel,
                            conn_info,
                            self.username,
                            self.password,
                        )

            with action(request.method.name, name, abort=request.scenario.stop_on_failure):
                with self._queue(endpoint) as queue:
                    if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
                        response_length = len(payload) if payload is not None else 0
                        queue.put(payload, self.md)
                    elif request.method in [RequestMethod.RECEIVE, RequestMethod.GET]:
                        payload = queue.get(*self._get_arguments).decode('utf-8')
                        response_length = len(payload) if payload is not None else 0
                    else:
                        raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')

# pylint: disable=line-too-long
'''
@anchor pydoc:grizzly.testdata.variables.messagequeue Messagequeue
Listens for messages on IBM MQ.

Use {@pylink grizzly.tasks.transformer} task to extract specific parts of the message.

Grizzly *must* have been installed with the extra `mq` package and native IBM MQ libraries must be installed for being able to use this variable:

``` plain
pip3 install grizzly-loadtester[mq]
```

## Format

`queue:<queue_name>[, expression:<expression>] | url=<url>, wait=<wait>[, content_type=<content_type>][, repeat=<repeat>]`

Initial value is the name of the queue, prefixed with `queue:`, on the MQ server specified in argument `url`.
Expression is optional, and can be specified if a message matching specific criteria is to be fetched. The expression format depends on
content type, which needs to be specified as an argument (e.g. XPATH expressions are used with `application/xml` content type).

* `content_type` _str_ (optional) - specifies the content type of messages on the queue, needed if expressions are to be used when getting messages
* `repeat` _bool_ (optional) - if `True`, values read for the queue will be saved in a list and re-used if there are no new messages available
* `url` _str_ - see format of url below.
* `wait` _int_ - number of seconds to wait for a message on the queue
* `heartbeat_interval` _int_ - number of seconds to use for the heartbeat interval (default 300)
* `header_type` _str_ - header type, can be `RFH2` for sending gzip compressed messages using RFH2 header, default `None`

### URL format

``` plain
mq[s]://[<username>:<password>@]<hostname>[:<port>]/?QueueManager=<queue manager>&Channel=<channel>[&KeyFile=<key repository path>[&SslCipher=<ssl cipher>][&CertLabel=<certificate label>]]
```

All variables in the URL have support for {@link framework.usage.variables.templating}.

* `mq[s]` _str_ - must be specified, `mqs` implies connecting with TLS, if `KeyFile` is not set in querystring, it will look for a key repository in `./<username>`
* `username` _str_ (optional) - username to authenticate with, default `None`
* `password` _str_ (optional) - password to authenticate with, default `None`
* `hostname` _str_ - hostname of MQ server
* `port` _int_ (optional) - port on MQ server, default `1414`
* `QueueManager` _str_ - name of queue manager
* `Channel` _str_ - name of channel to connect to
* `KeyFile` _str_ (optional) - path to key repository for certificates needed to connect over TLS
* `SslCipher` _str_ (optional) - SSL cipher to use for connection, default `ECDHE_RSA_AES_256_GCM_SHA384`
* `CertLabel` _str_ (optional) - label of certificate in key repository, default `username`

## Example

``` gherkin
And value for variable "AtomicMessageQueue.document_id" is "queue:IN.DOCUMENTS | wait=120, url='mqs://mq_subscription:$conf::mq.password@mq.example.com/?QueueManager=QM1&Channel=SRV.CONN', repeat=True"
...
Given a user of type "RestApi" load testing "http://example.com"
...
Then get request "fetch-document" from "/api/v1/document/{{ AtomicMessageQueue.document_id }}"

### Using expression to get specific message

And value for variable "AtomicMessageQueue.document_id" is "queue:IN.DOCUMENTS, expression:'//DocumentReference[text()='123abc']' | wait=120, url='mqs://mq_subscription:$conf::mq.password@mq.example.com/?QueueManager=QM1&Channel=SRV.CONN', repeat=True"
And set response content type to "application/xml"
...
Given a user of type "RestApi" load testing "http://example.com"
...
Then get request "fetch-document" from "/api/v1/document/{{ AtomicMessageQueue.document_id }}"

```

When the scenario starts `grizzly` will wait up to 120 seconds until `AtomicMessageQueue.document_id` has been populated from a message on the queue `IN.DOCUMENTS`.

If there are no messages within 120 seconds, and it is the first iteration of the scenario, it will fail. If there has been at least one message on the queue since
the scenario started, it will use the oldest of those values, and then add it back in the end of the list again.
'''  # noqa: E501
import logging

from typing import Dict, Any, Type, Optional, List, cast
from urllib.parse import urlparse, parse_qs, unquote

from zmq.error import Again as ZMQAgain, ZMQError
from zmq.sugar.constants import NOBLOCK as ZMQ_NOBLOCK, REQ as ZMQ_REQ
import zmq.green as zmq

from gevent import sleep as gsleep
from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.arguments import split_value, parse_arguments
from grizzly_extras.transformer import TransformerContentType

from ...types import RequestType, bool_type, optional_str_lower_type
from ..utils import resolve_variable
from . import AtomicVariable

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


def atomicmessagequeue__base_type__(value: str) -> str:
    if '|' not in value:
        raise ValueError('AtomicMessageQueue: initial value must contain arguments')

    queue, queue_arguments = split_value(value)

    try:
        arguments = parse_arguments(queue_arguments)
    except ValueError as e:
        raise ValueError(f'AtomicMessageQueue: {str(e)}') from e

    try:
        endpoint = parse_arguments(queue, ':')
    except ValueError as e:
        raise ValueError(f'AtomicMessageQueue: {str(e)}') from e

    if 'queue' not in endpoint:
        raise ValueError('AtomicMessageQueue: queue name must be prefixed with queue:')

    for argument in ['url']:
        if argument not in arguments:
            raise ValueError(f'AtomicMessageQueue: {argument} parameter must be specified')

    for argument_name, argument_value in arguments.items():
        if argument_name not in AtomicMessageQueue.arguments:
            raise ValueError(f'AtomicMessageQueue: argument {argument_name} is not allowed')
        else:
            AtomicMessageQueue.arguments[argument_name](argument_value)
            if argument_name in ['wait', 'heartbeat_interval']:
                int(argument_value)

    # validate url
    url = arguments.get('url', None)
    parsed = urlparse(url)

    if parsed.scheme is None or parsed.scheme not in ['mq', 'mqs']:
        raise ValueError(f'AtomicMessageQueue: "{parsed.scheme}" is not a supported scheme for url')

    if parsed.hostname is None or len(parsed.hostname) < 1:
        raise ValueError(f'AtomicMessageQueue: hostname is not specified in "{url}"')

    if parsed.query == '':
        raise ValueError(f'AtomicMessageQueue: QueueManager and Channel must be specified in the query string of "{url}"')

    params = parse_qs(parsed.query)

    if 'QueueManager' not in params:
        raise ValueError('AtomicMessageQueue: QueueManager must be specified in the query string')

    if 'Channel' not in params:
        raise ValueError('AtomicMessageQueue: Channel must be specified in the query string')

    return f'{queue} | {queue_arguments}'


class AtomicMessageQueue(AtomicVariable[str]):
    __base_type__ = atomicmessagequeue__base_type__
    __dependencies__ = set(['async-messaged'])
    __on_consumer__ = True

    __initialized: bool = False

    _settings: Dict[str, Dict[str, Any]]
    _endpoint_clients: Dict[str, zmq.Socket]
    _endpoint_messages: Dict[str, List[str]]

    _zmq_url = 'tcp://127.0.0.1:5554'
    _zmq_context: zmq.Context

    arguments: Dict[str, Any] = {
        'content_type': TransformerContentType.from_string,
        'repeat': bool_type,
        'url': str,
        'wait': int,
        'heartbeat_interval': int,
        'header_type': optional_str_lower_type,
    }

    def __init__(self, variable: str, value: str):
        if pymqi.__name__ == 'grizzly_extras.dummy_pymqi':
            pymqi.raise_for_error(self.__class__)

        # silence uamqp loggers
        for uamqp_logger_name in ['uamqp', 'uamqp.c_uamqp']:
            logging.getLogger(uamqp_logger_name).setLevel(logging.ERROR)

        safe_value = self.__class__.__base_type__(value)

        settings = {'repeat': False, 'wait': None, 'heartbeat_interval': None, 'url': None, 'worker': None, 'context': None, 'header_type': None}

        queue_name, queue_arguments = split_value(safe_value)

        arguments = parse_arguments(queue_arguments)

        for argument, caster in self.__class__.arguments.items():
            if argument in arguments:
                settings[argument] = caster(arguments[argument])

        super().__init__(variable, queue_name)

        with self._semaphore:
            if self.__initialized:
                if variable not in self._endpoint_messages:
                    self._endpoint_messages[variable] = []

                if variable not in self._settings:
                    self._settings[variable] = settings

                if variable not in self._endpoint_clients:
                    self._endpoint_clients[variable] = self.create_client(variable, settings)

                return

            self._endpoint_messages = {variable: []}
            self._settings = {variable: settings}
            self._zmq_context = zmq.Context()
            self._endpoint_clients = {variable: self.create_client(variable, settings)}
            self.__initialized = True

    def create_context(self, settings: Dict[str, Any]) -> AsyncMessageContext:
        url = settings.get('url', None)
        parsed = urlparse(url)

        if parsed.scheme is None or parsed.scheme not in ['mq', 'mqs']:
            raise ValueError(f'{self.__class__.__name__}: "{parsed.scheme}" is not a supported scheme for url')

        if parsed.hostname is None or len(parsed.hostname) < 1:
            raise ValueError(f'{self.__class__.__name__}: hostname is not specified in "{url}"')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__}: QueueManager and Channel must be specified in the query string of "{url}"')

        paths: List[str] = []

        for path in parsed.path.split('/'):
            resolved = cast(str, resolve_variable(self.grizzly, path))
            paths.append(resolved)

        parsed = parsed._replace(path='/'.join(paths))

        querystrings = parse_qs(parsed.query)

        parameters: List[str] = []

        for querystring in querystrings:
            resolved = cast(str, resolve_variable(self.grizzly, querystrings[querystring][0]))
            parameters.append(f'{querystring}={resolved}')

        parsed = parsed._replace(query='&'.join(parameters))

        if '@' in parsed.netloc:
            credentials, host = parsed.netloc.split('@')
            host = cast(str, resolve_variable(self.grizzly, host))
            credentials = credentials.replace('::', '%%')
            username, password = credentials.split(':', 1)
            username = cast(str, resolve_variable(self.grizzly, username.replace('%%', '::')))
            password = cast(str, resolve_variable(self.grizzly, password.replace('%%', '::')))
            host = f'{username}:{password}@{host}'
        else:
            host = cast(str, resolve_variable(self.grizzly, parsed.netloc))

        parsed = parsed._replace(netloc=host)

        port = parsed.port or 1414

        params = parse_qs(parsed.query)

        if 'QueueManager' not in params:
            raise ValueError(f'{self.__class__.__name__}: QueueManager must be specified in the query string')

        if 'Channel' not in params:
            raise ValueError(f'{self.__class__.__name__}: Channel must be specified in the query string')

        key_file: Optional[str] = None
        cert_label: Optional[str] = None
        ssl_cipher: Optional[str] = None

        if 'KeyFile' in params:
            key_file = params['KeyFile'][0]
        elif parsed.scheme == 'mqs' and username is not None:
            key_file = username

        if key_file is not None:
            cert_label = params.get('CertLabel', [parsed.username])[0]
            ssl_cipher = params.get('SslCipher', ['ECDHE_RSA_AES_256_GCM_SHA384'])[0]

        return {
            'url': url,
            'connection': f'{parsed.hostname}({port})',
            'queue_manager': unquote(params['QueueManager'][0]),
            'channel': unquote(params['Channel'][0]),
            'username': parsed.username,
            'password': parsed.password,
            'key_file': key_file,
            'cert_label': cert_label,
            'ssl_cipher': ssl_cipher,
            'message_wait': settings.get('wait', None),
            'heartbeat_interval': settings.get('heartbeat_interval', None),
            'header_type': settings.get('header_type', None),
        }

    def create_client(self, variable: str, settings: Dict[str, Any]) -> zmq.Socket:
        self._settings[variable].update({'context': self.create_context(settings)})

        zmq_client = cast(
            zmq.Socket,
            self._zmq_context.socket(ZMQ_REQ),
        )
        zmq_client.connect(self._zmq_url)

        return zmq_client

    @classmethod
    def destroy(cls: Type['AtomicMessageQueue']) -> None:
        try:
            instance = cast(AtomicMessageQueue, cls.get())
            queue_clients = getattr(instance, '_endpoint_clients', None)

            if queue_clients is not None:
                variables = list(queue_clients.keys())[:]
                for variable in variables:
                    try:
                        instance.__delitem__(variable)
                    except:
                        pass

            instance._zmq_context.destroy()
            del instance._zmq_context
        except:
            pass

        super().destroy()

    @classmethod
    def clear(cls: Type['AtomicMessageQueue']) -> None:
        super().clear()

        instance = cast(AtomicMessageQueue, cls.get())
        variables = list(instance._settings.keys())

        for variable in variables:
            instance.__delitem__(variable)

    def __getitem__(self, variable: str) -> Optional[str]:
        with self._semaphore:
            queue_name = cast(str, self._get_value(variable))

            request: AsyncMessageRequest
            response: Optional[AsyncMessageResponse]

            # first request, connect to async-messaged
            if self._settings[variable].get('worker', None) is None:
                request = {
                    'action': RequestType.CONNECT(),
                    'context': self._settings[variable]['context'],
                }

                self._endpoint_clients[variable].send_json(request)

                response = None

                while True:
                    try:
                        response = self._endpoint_clients[variable].recv_json(flags=ZMQ_NOBLOCK)
                        break
                    except ZMQAgain:
                        gsleep(0.1)

                if response is None:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: no response when trying to connect')

                message = response.get('message', None)
                if not response['success']:
                    raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

                self._settings[variable]['worker'] = response['worker']

            request = {
                'action': 'GET',
                'worker': self._settings[variable]['worker'],
                'context': {
                    'endpoint': queue_name,
                },
                'payload': None
            }
            if 'content_type' in self._settings[variable]:
                request['context']['content_type'] = self._settings[variable]['content_type'].name.lower()

            self._endpoint_clients[variable].send_json(request)

            response = None

            while True:
                try:
                    response = cast(AsyncMessageResponse, self._endpoint_clients[variable].recv_json(flags=ZMQ_NOBLOCK))
                    break
                except ZMQAgain:
                    gsleep(0.1)

            if response is None:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: unknown error, no response')

            payload: Optional[str]
            message = response.get('message', None)
            if not response['success']:
                if message is not None and 'MQRC_NO_MSG_AVAILABLE' in message and self._settings[variable].get('repeat', False) and len(self._endpoint_messages[variable]) > 0:
                    payload = self._endpoint_messages[variable].pop(0)
                    self._endpoint_messages[variable].append(payload)

                    return payload

                raise RuntimeError(f'{self.__class__.__name__}.{variable}: {message}')

            payload = cast(Optional[str], response.get('payload', None))
            if payload is None or len(payload) < 1:
                raise RuntimeError(f'{self.__class__.__name__}.{variable}: payload in response was None')

            if self._settings[variable].get('repeat', False):
                self._endpoint_messages[variable].append(payload)

            return payload

    def __setitem__(self, variable: str, value: Optional[str]) -> None:
        pass

    def __delitem__(self, variable: str) -> None:
        with self._semaphore:
            try:
                del self._settings[variable]
                del self._endpoint_messages[variable]
                try:
                    self._endpoint_clients[variable].disconnect(self._zmq_url)
                except ZMQError:
                    pass
                del self._endpoint_clients[variable]
            except KeyError:
                pass

        super().__delitem__(variable)

'''Send and receive messages on Azure Service Bus queues and topics.

> **Note**: If `message.wait` is not set, `azure.servicebus` will wait until there is a message available, and hence block the scenario.

User is based on `azure.servicebus` for communicating with Azure Service Bus. But creating a connection and session towards a queue or a topic
is a costly operation, and caching of the session was causing problems with `gevent` due to the sockets blocking and hence locust/grizzly was
blocking when finished. To get around this, the user implementation communicates with a stand-alone process via zmq, which in turn communicates
with Azure Service Bus.

`async-messaged` starts automagically when a scenario uses the `ServiceBusUser`.

## Request methods

Supports the following request methods:

* send
* receive

## Format

Format of `host` is the following:

```plain
[Endpoint=]sb://<hostname>/;SharedAccessKeyName=<shared key name>;SharedAccessKey=<shared key>
```

`endpoint` in the request must have the prefix `queue:` or `topic:` followed by the name of the targeted
type. If you are going to receive messages from a topic, and additional `subscription:` som follow the specified `topic:`.

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKey=c2VjcmV0LXN0dWZm"
And set context variable "message.wait" to "5"
Then send request "queue-send" to endpoint "queue:shared-queue"
Then send request "topic-send" to endpoint "topic:shared-topic"
Then receive request "queue-recv" from endpoint "queue:shared-queue"
Then receive request "topic-recv" from endpoint "topic:shared-topic, subscription:my-subscription"
```
'''
from typing import Generator, Dict, Any, Tuple, Optional, Set, cast
from urllib.parse import urlparse, parse_qs
from time import monotonic as time
from contextlib import contextmanager

import zmq

from locust.exception import StopUser
from gevent import sleep as gsleep
from grizzly_extras.async_message import AsyncMessageContext, AsyncMessageResponse, AsyncMessageRequest, AsyncMessageError

from ..types import RequestMethod, RequestDirection
from ..task import RequestTask
from ..utils import merge_dicts
from .meta import ContextVariables, ResponseHandler, RequestLogger
from . import logger


class ServiceBusUser(ResponseHandler, RequestLogger, ContextVariables):
    _context: Dict[str, Any] = {
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
    hellos: Set[str]

    host: str

    request_name_map: Dict[str, str] = {
        'RECEIVE': 'RECV',
        'HELLO': 'HELO',
    }


    def __init__(self, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        if not self.host.startswith('Endpoint='):
            conn_str = self.host
            self.host = f'Endpoint={self.host}'
        else:
            conn_str = self.host[9:]

        # Replace semicolon separators between parameters to ? and & to make it "urlparse-compliant"
        # for validation
        conn_str = conn_str.replace(';', '?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.scheme != 'sb':
            raise ValueError(f'{self.__class__.__name__}: "{parsed.scheme}" is not a supported scheme')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKeyName and SharedAccessKey must be in the query string')

        params = parse_qs(parsed.query)

        if 'SharedAccessKeyName' not in params:
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKeyName must be in the query string')

        if 'SharedAccessKey' not in params:
            raise ValueError(f'{self.__class__.__name__}: SharedAccessKey must be in the query string')

        self._context = merge_dicts(super().context(), self.__class__._context)

        self.am_context = {
            'url': self.host[9:],
            'message_wait': self._context.get('message', {}).get('wait', None)
        }

        self.hellos = set()
        self.worker_id = None
        self.zmq_client = self.zmq_context.socket(zmq.REQ)
        self.zmq_client.connect(self.zmq_url)

        if self._scenario is not None:
            for task in self._scenario.tasks:
                if not isinstance(task, RequestTask):
                    continue

                endpoint = task.endpoint
                self.say_hello(task, endpoint)

    def say_hello(self, task: RequestTask, endpoint: str) -> None:
        if ('{{' in endpoint and '}}' in endpoint) or '$conf' in endpoint or '$env' in endpoint:
            logger.warning(f'{self.__class__.__name__}: cannot say hello for {task.name} when endpoint ({endpoint}) is a template')
            return

        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'
        if ',' in endpoint:
            name = endpoint.split(',', 1)[0]
        else:
            name = endpoint

        description = f'{connection}={endpoint}'
        name = f'{connection}={name.strip()}'

        if description in self.hellos:
            return

        context = cast(AsyncMessageContext, dict(self.am_context))
        context.update({
            'endpoint': endpoint,
        })

        request: AsyncMessageRequest = {
            'worker': self.worker_id,
            'action': 'HELLO',
            'context': context,
        }

        with self.async_action(task, request, name, meta=True):
            pass

        self.hellos.add(description)

    @contextmanager
    def async_action(self, task: RequestTask, request: AsyncMessageRequest, name: str, meta: bool = False) -> Generator[None, None, None]:
        request.update({'worker': self.worker_id})
        connection = 'sender' if task.method.direction == RequestDirection.TO else 'receiver'
        request['context'].update({'connection': connection})

        response: Optional[AsyncMessageResponse] = None
        exception: Optional[Exception] = None

        try:
            start_time = time()

            yield

            self.zmq_client.send_json(request)

            while True:
                try:
                    response = cast(AsyncMessageResponse, self.zmq_client.recv_json(flags=zmq.NOBLOCK))
                    break
                except zmq.Again:
                    gsleep(0.1)
        except Exception as e:
            exception = e
        finally:
            response_time = int((time() - start_time) * 1000)

            if response is not None:
                if self.worker_id is None:
                    self.worker_id = response.get('worker', None)

                assert self.worker_id == response.get('worker', '')

                if not response.get('success', False) and exception is None:
                    exception = AsyncMessageError(response['message'])
            else:
                response = {}

            try:
                if not meta:
                    self.response_event.fire(
                        name=f'{task.scenario.identifier} {task.name}',
                        request=task,
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
                action = self.request_name_map.get(request['action'], request['action'][:4])
                self.environment.events.request.fire(
                    request_type=f'sb:{action}',
                    name=name,
                    response_time=response_time,
                    response_length=(response or {}).get('response_length', None) or 0,
                    context=self._context,
                    exception=exception,
                )

        if exception is not None and not meta and task.scenario.stop_on_failure:
            try:
                self.zmq_client.disconnect(self.zmq_url)
            except:
                pass

            raise StopUser()

    def request(self, request: RequestTask) -> None:
        request_name, endpoint, payload = self.render(request)

        name = f'{request.scenario.identifier} {request_name}'

        self.say_hello(request, endpoint)

        context = cast(AsyncMessageContext, dict(self.am_context))
        context.update({
            'endpoint': endpoint,
        })

        am_request: AsyncMessageRequest = {
            'action': request.method.name,
            'context': context,
            'payload': payload,
        }

        with self.async_action(request, am_request, name):
            if request.method not in [RequestMethod.SEND, RequestMethod.RECEIVE]:
                raise NotImplementedError(f'{self.__class__.__name__}: no implementation for {request.method.name} requests')


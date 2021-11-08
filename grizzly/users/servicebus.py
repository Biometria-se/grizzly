'''Send and receive messages on Azure Service Bus queues and topics.

> **Warning**: An actual session towards the endpoint will not be created and connected until the request actually executes, the connection time is
> a big portion of the request time. Caching per endpoint causes problems with `gevent` and the open `amqp` sockets will block indefinitely.
<p></p>
> **Note**: If `message.wait` is not set, `azure.servicebus` will wait until there is a message available, and hence block the scenario.

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
Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKeyc2VjcmV0LXN0dWZm"
And set context variable "message.wait" to "5"
Then send request "queue-send" to endpoint "queue:shared-queue"
Then send request "topic-send" to endpoint "topic:shared-topic"
Then receive request "queue-recv" from endpoint "queue:shared-queue"
Then receive request "topic-recv" from endpoint "topic:shared-topic, subscription:my-subscription"
```
'''
import logging

from typing import Dict, Any, Iterable, Tuple, Callable, Optional, cast
from mypy_extensions import KwArg
from urllib.parse import urlparse, parse_qs
from time import monotonic as time

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType, ServiceBusSender, ServiceBusReceiver
from azure.servicebus.amqp import AmqpMessageBodyType
from azure.servicebus.amqp._amqp_message import DictMixin

from locust.exception import StopUser

from ..types import RequestMethod
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

    sb_client: ServiceBusClient
    host: str

    def __init__(self, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        conn_str = self.host
        if conn_str.startswith('Endpoint='):
            conn_str = conn_str[9:]

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

        self.sb_client = ServiceBusClient.from_connection_string(
            conn_str=self.host,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        self._context = merge_dicts(super().context(), self.__class__._context)

        # silence uamqp loggers
        logging.getLogger('uamqp').setLevel(logging.ERROR)

    @classmethod
    def from_message(cls, message: Optional[ServiceBusMessage]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        def to_dict(obj: Optional[DictMixin]) -> Dict[str, Any]:
            if obj is None:
                return {}

            result: Dict[str, Any] = {}

            for key, value in obj.items():
                result[key] = value

            return result

        if message is None:
            return None, None

        raw_amqp_message = message.raw_amqp_message
        metadata = to_dict(raw_amqp_message.properties)
        metadata.update(to_dict(raw_amqp_message.header))

        body = raw_amqp_message.body

        if raw_amqp_message.body_type == AmqpMessageBodyType.DATA:
            if isinstance(body, Iterable):
                payload = ''
                for buffer in body:
                    payload += buffer.decode('utf-8')
            else:
                payload = body.encode('utf-8')
        elif raw_amqp_message.body_type == AmqpMessageBodyType.SEQUENCE:
            payload = ''.join([v.encode('utf-8') for v in body])
        else:
            payload = str(body)

        return metadata, payload

    def get_sender_instance(self, request: RequestTask, endpoint: str) -> ServiceBusSender:
        arguments: Dict[str, Any] = {}
        endpoint_type, endpoint_name, _ = self.get_endpoint_details(request, endpoint)

        sender_type: Callable[[KwArg(Any)], ServiceBusSender]

        if endpoint_type == 'queue':
            arguments.update({'queue_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                self.sb_client.get_queue_sender,
            )
        else:
            arguments.update({'topic_name': endpoint_name})
            sender_type = cast(
                Callable[[KwArg(Any)], ServiceBusSender],
                self.sb_client.get_topic_sender,
            )

        return sender_type(**arguments)

    def get_receiver_instance(self, request: RequestTask, endpoint: str) -> ServiceBusReceiver:
        arguments: Dict[str, Any] = {}
        endpoint_type, endpoint_name, subscription_name = self.get_endpoint_details(request, endpoint)

        receiver_type: Callable[[KwArg(Any)], ServiceBusReceiver]

        max_wait_time = self._context.get('message', {}).get('wait', None)
        if max_wait_time is not None:
            arguments.update({'max_wait_time': max_wait_time})

        if endpoint_type == 'queue':
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], self.sb_client.get_queue_receiver)
            arguments.update({'queue_name': endpoint_name})
        else:
            receiver_type = cast(Callable[[KwArg(Any)], ServiceBusReceiver], self.sb_client.get_subscription_receiver)
            arguments.update({
                'topic_name': endpoint_name,
                'subscription_name': subscription_name,
            })

        return receiver_type(**arguments)

    @classmethod
    def get_endpoint_details(cls, request: RequestTask, endpoint: str) -> Tuple[str, str, Optional[str]]:
        if ':' not in endpoint:
            raise ValueError(f'{cls.__name__}: "{endpoint}" is not prefixed with queue: or topic:')

        endpoint_type: str
        endpoint_name: str
        subscription_name: Optional[str] = None

        endpoint_type, endpoint_details = [v.strip() for v in endpoint.split(':', 1)]

        if endpoint_type not in ['queue', 'topic']:
            raise ValueError(f'{cls.__name__}: only support endpoint types queue and topic, not {endpoint_type}')

        if ',' in endpoint_details:
            endpoint_name, endpoint_details = [v.strip() for v in endpoint_details.split(',', 1)]

            if request.method not in [RequestMethod.RECEIVE]:
                raise ValueError(f'{cls.__name__}: additional arguments in endpoint is not supported for {request.method.name}')

            detail_type, detail_value = [v.strip() for v in endpoint_details.split(':', 1)]

            if detail_type != 'subscription':
                raise ValueError(f'{cls.__name__}: argument {detail_type} is not supported')

            subscription_name = detail_value
        else:
            endpoint_name = endpoint_details

        if endpoint_type == 'topic' and subscription_name is None and request.method == RequestMethod.RECEIVE:
            raise ValueError(f'{cls.__name__}: endpoint needs to include subscription when receiving messages from a topic')

        return endpoint_type, endpoint_name, subscription_name

    def request(self, request: RequestTask) -> None:
        request_name, endpoint, payload = self.render(request)

        name = f'{request.scenario.identifier} {request_name}'

        try:
            start_time = time()
            response_length: int = 0
            exception: Optional[Exception] = None
            metadata: Optional[Dict[str, Any]] = None
            message: ServiceBusMessage

            if request.method in [RequestMethod.SEND]:
                message = ServiceBusMessage(payload)
                response_length = len(payload or '')
                with self.get_sender_instance(request, endpoint) as sender:
                    sender.send_messages(message)
                metadata, payload = self.from_message(message)
            elif request.method in [RequestMethod.RECEIVE]:
                with self.get_receiver_instance(request, endpoint) as receiver:
                    try:
                        message = cast(ServiceBusMessage, receiver.next())
                        receiver.complete_message(message)
                    except StopIteration:
                        raise RuntimeError(f'no message on {endpoint}')

                metadata, payload = self.from_message(message)
                response_length = len(payload or '')
            else:
                raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')
        except Exception as e:
            exception = e
        finally:
            total_time = int((time() - start_time) * 1000)
            logger.debug(f'{self.__class__.__name__}: {total_time=} ms')
            try:
                self.response_event.fire(
                    name=name,
                    request=request,
                    context=(
                        metadata,
                        payload,
                    ),
                    user=self,
                    exception=exception
                )
            except Exception as e:
                if exception is None:
                    exception = e
            finally:
                self.environment.events.request.fire(
                    request_type=f'sb:{request.method.name[:4]}',
                    name=name,
                    response_time=total_time,
                    response_length=response_length,
                    context=self._context,
                    exception=exception,
                )

            if exception is not None and (request.scenario.stop_on_failure or isinstance(exception, NotImplementedError)):
                raise StopUser()

'''Communicates with Azure Service Bus.

Format of `host` is the following:

```plain
[Endpoint=]sb://<hostname>/;SharedAccessKeyName=<shared key name>;SharedAccessKey=<shared key>
```

`endpoint` in the request must have the prefix `queue:` or `topic:` followed by the name of the targeted
type.

Example of how to use it in a scenario:

```gherkin
Given a user of type "ServiceBus" load testing "sb://sb.example.com/;SharedAccessKeyName=authorization-key;SharedAccessKeyc2VjcmV0LXN0dWZm"
Then send request "servicebus.msg" to endpoint "queue:shared-queue"
Then send request "servicebus.msg" to endpoint "topic:shared-topic"
```

Supports the following request methods:

* send
'''
from typing import Dict, Any, Tuple, Callable, Optional, cast
from mypy_extensions import KwArg
from urllib.parse import urlparse, parse_qs
from time import monotonic as time

from azure.servicebus import ServiceBusClient, ServiceBusMessage, TransportType, ServiceBusSender

from locust.exception import StopUser

from .meta import ContextVariables
from ..types import RequestMethod
from ..context import RequestContext
from ..testdata.utils import merge_dicts


class ServiceBusUser(ContextVariables):
    _context: Dict[str, Any] = {}
    client: ServiceBusClient
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
            raise ValueError(f'"{parsed.scheme}" is not a supported scheme for {self.__class__.__name__}')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__} needs SharedAccessKeyName and SharedAccessKey in the query string')

        params = parse_qs(parsed.query)

        if 'SharedAccessKeyName' not in params:
            raise ValueError(f'{self.__class__.__name__} needs SharedAccessKeyName in the query string')

        if 'SharedAccessKey' not in params:
            raise ValueError(f'{self.__class__.__name__} needs SharedAccessKey in the query string')

        self.client = ServiceBusClient.from_connection_string(
            conn_str=self.host,
            transport_type=TransportType.AmqpOverWebsocket,
        )

        self._context = merge_dicts(super().context(), self.__class__._context)

    def request(self, request: RequestContext) -> None:
        request_name, endpoint, payload = self.render(request)

        if ':' not in endpoint:
            raise ValueError(f'{endpoint} does not specify queue: or topic:')

        name = f'{request.scenario.identifier} {request_name}'

        endpoint_type, endpoint = endpoint.split(':', 1)
        if endpoint_type not in ['queue', 'topic']:
            raise ValueError(f'{self.__class__.__name__} supports endpoint types queue or topic only, and not {endpoint_type}')

        exception: Optional[Exception] = None
        start_time = time()

        try:
            single_message = ServiceBusMessage(payload)
            sender_type: Callable[[str, KwArg(Any)], ServiceBusSender]
            if endpoint_type == 'queue':
                sender_type = cast(
                    Callable[[str, KwArg(Any)], ServiceBusSender],
                    self.client.get_queue_sender,
                )
            else:
                sender_type = cast(
                    Callable[[str, KwArg(Any)], ServiceBusSender],
                    self.client.get_topic_sender,
                )

            with sender_type(endpoint) as sender:
                if request.method in [RequestMethod.SEND]:
                    sender.send_messages(single_message)
                else:
                    raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')
        except Exception as e:
            exception = e
        finally:
            total_time = int((time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type=f'sb:{request.method.name}',
                name=name,
                response_time=total_time,
                response_length=0,
                context=self._context,
                exception=exception,
            )

            if exception is not None and request.scenario.stop_on_failure:
                raise StopUser()

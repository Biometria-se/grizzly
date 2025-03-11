#"""@anchor pydoc:grizzly.users.messagequeue Message Queue
"""@anchor pydoc:grizzly.users.iothub Iot hub
Communicate with an Azure IoT hub device.

## Request methods

Supports the following request methods:

* send
* put
* get
* receive

## Metadata

The following properties is added to the metadata part of a message:

- `custom_properties` (dict)
- `message_id` (str)
- `expiry_time_utc` (str)
- `correlation_id` (str)
- `user_id` (str)
- `content_type` (str)
- `output_name` (str)
- `input_name` (str)
- `ack` (bool)
- `iothub_interface_id` (str)
- `size` (int)

## Sending

This user has support for sending IoT messages and uploading files. For the former the `endpoint` in the request must be `device-to-cloud`, otherwise
it will try to upload the request as a file. See [device-to-cloud communication guidance](https://learn.microsoft.com/en-us/azure/iot-hub/iot-hub-devguide-d2c-guidance)
for the difference.

## Receiving

Receiving cloud-to-device (C2D) messages is a little bit special, the `endpoint` in the in the request must be `cloud-to-device`. Also, all workers will communicate with master,
which also creates an device and register a message handler, which will receive **all** cloud-to-device messages and put them in an internal queue. The clients running on the
workers sends a message to master, and will receive a message from the internal queue (if there are any).

There are 3 other context variables that will control wether a message will be pushed to the internal queue or not:

- `expression.unique`, this is a JSON-path expression will extract a value from the message payload, push the whole message to the internal queue and save the value in a
  "volatile list", if a message with the same value is received within 5 seconds, it will not be handled. This is to avoid handling duplicates of messages, which can occur
  for different reasons. E.g. `$.body.event.id`.

- `expression.metadata` (bool), this a JSON-path expression that should validate to a boolean expression, messages for which this expression does not validate to `True` will be
  dropped and not pushed to the internal queue. E.g. `$.size>=1024`.

- `expression.payload` (bool), same as `expression.metadata` except it will validate against the actual message payload. E.g. `$.body.timestamp>='2024-09-23 20:39:00`, which will
  drop all messages having a timestamp before the specified date.

## Pipe arguments

See {@pylink grizzly.tasks.request} endpoint format for standard pipe arguments, in additional to those the following are also supported:

- `wait` (int), number of seconds to wait for a available message before failing request. If not specified it will wait indefinitely.

## Format

Format of `host` is the following:

```plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

The metadata values `content_type` and `content_encoding` can be set to gzip compress the payload before upload (see example below).

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

The same example with gzip compression enabled:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And metadata "content_encoding" is "gzip"
Then send request "test/blob.file" to endpoint "uploaded_blob_filename | content_type=octet_stream_utf8"
```

Example of how to receive unique messages from a thermometer of type `temperature` (considered that the application sets a custom property `message_type`)
```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And set context variable "expression.unique" to "$.device.name=='thermometer01'"
And set context variable "expression.metadata" to "$.custom_properties.message_type=='temperature'"
And value for variable "temperature" is "none"

Then receive request with name "iot-get-temperature" from endpoint "cloud-to-device | content_type=json, wait=180"
Then save response payload "$.device.temperature" in variable "temperature"
```

"""
from __future__ import annotations

import gzip
import json
import logging
from time import perf_counter
from typing import TYPE_CHECKING, Any, ClassVar, TypedDict, cast
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

from azure.iot.device import IoTHubDeviceClient
from azure.iot.device import Message as IotMessage
from azure.iot.device.exceptions import ClientError
from azure.storage.blob import BlobClient, ContentSettings
from gevent import sleep as gsleep
from gevent.queue import Empty, Queue

from grizzly.events import GrizzlyEventDecoder, event, events
from grizzly.exceptions import retry
from grizzly.testdata.communication import GrizzlyDependencies, GrizzlyMessageHandler, GrizzlyMessageMapping
from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.utils import has_template
from grizzly_extras.queue import VolatileDeque
from grizzly_extras.transformer import JsonTransformer, TransformerContentType

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


logger = logging.getLogger(__name__)


class MessageJsonSerializer(json.JSONEncoder):
    def default(self, value: Any) -> Any:
        serialized_value: Any
        if isinstance(value, UUID):
            serialized_value = str(value)
        elif isinstance(value, (bytes, bytearray)):
            serialized_value = value.decode()
        else:
            serialized_value = super().default(value)

        return serialized_value


class IotMessageDecoder(GrizzlyEventDecoder):
    def __call__(
        self,
        *args: Any,
        tags: dict[str, str | None] | None,
        return_value: Any,  # noqa: ARG002
        exception: Exception | None,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], dict[str, str | None]]:
        if tags is None:
            tags = {}

        instance = args[0]
        message = args[self.arg] if isinstance(self.arg, int) else kwargs.get(self.arg)

        metadata, _ = IotHubUser._unserialize_message(IotHubUser._serialize_message(message))

        tags = {
            'identifier': instance.device_id,
            **tags,
            **(metadata or {}).get('custom_properties', {}),
        }

        metrics: dict[str, Any] = {
            'size': (metadata or {}).get('size'),
            'message_id': (metadata or {}).get('message_id'),
            'error': None,
        }

        if exception is not None:
            metrics.update({'error': str(exception)})

        return metrics, tags


class IotMessageExpressions(TypedDict):
    unique: str | None
    metadata: str | None
    payload: str | None


class RegisterIotClient(TypedDict):
    id: str
    host: str
    expressions: IotMessageExpressions


class IotHubCloudToDeviceClient:
    logger: logging.Logger

    device_id: str
    host: str
    client: IoTHubDeviceClient
    expressions: IotMessageExpressions

    _unique: VolatileDeque[str]

    queue: Queue[Any]

    def __init__(self, data: RegisterIotClient) -> None:
        self.device_id = data['id']
        self.expressions = data['expressions']
        self.host = data['host']

        self.logger = logging.getLogger(f'{self.__class__.__name__}/{self.device_id}')

        self.client = IoTHubDeviceClient.create_from_connection_string(self.host, websockets=True)

        self._unique = VolatileDeque(timeout=20.0)
        self.queue = Queue()

        # register message handler
        self.client.on_message_received = self.message_handler
        self.logger.info('registered as C2D handler')

    @classmethod
    def _extract(cls, data: Any, expression: str) -> list[str]:
        # all IoT messages are in JSON format
        parser = JsonTransformer.parser(expression)

        if isinstance(data, str):
            data = JsonTransformer.transform(data)

        return parser(data)

    @event(events.user_event, tags={'type': 'iot::cloud-to-device'}, decoder=IotMessageDecoder(arg=1))  # 0 = self...
    def message_handler(self, message: IotMessage) -> None:
        serialized_message = IotHubUser._serialize_message(message)

        if any(expression is not None for expression in [self.expressions.values()]):
            metadata, payload = IotHubUser._unserialize_message(serialized_message)

            if self.expressions['metadata'] is not None:
                values = self._extract(metadata, self.expressions['metadata'])

                if len(values) < 1:
                    self.logger.debug('message id %s metadata did not match "%s"', message.message_id, self.expressions['metadata'])
                    return

            if self.expressions['payload'] is not None:
                values = self._extract(payload, self.expressions['payload'])

                if len(values) < 1:
                    self.logger.debug('message id %s payload did not match "%s"', message.message_id, self.expressions['payload'])
                    return

            if self.expressions['unique'] is not None:
                values = self._extract(payload, self.expressions['unique'])

                if len(values) < 1:
                    self.logger.debug('message id %s was an unknown message, no matches for "%s"', message.message_id, self.expressions['unique'])
                    return

                value = next(iter(values))

                if value in self._unique:
                    self.logger.warning('message id %s contained "%s", which has already been received within %d seconds', message.message_id, value, self._unique.timeout)
                    return

                self._unique.append(value)

        self.logger.info('C2D message received: %s', serialized_message)
        self.queue.put_nowait(serialized_message)


class IotHubRegisterCloudToDeviceHandler(GrizzlyMessageHandler):
    __message_types__: ClassVar[GrizzlyMessageMapping] = {'response': 'consume_register_iot_message', 'request': 'produce_register_iot_message'}

    @classmethod
    def create_response(cls, key: int, request: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG003
        return {
            'created': IotHubCloudToDeviceHandler.register_client(cast(RegisterIotClient, request)),
        }

    @classmethod
    def send_register(cls, client: IotHubUser) -> bool:
        request = {
            'id': client.device_id,
            'host': client.host,
            'expressions': {
                'metadata': client._expression_metadata,
                'payload': client._expression_payload,
                'unique': client._expression_unique,
            },
        }

        response = cls.send_request(client, request)

        return cast(bool, response['created'])


class IotHubCloudToDeviceHandler(GrizzlyMessageHandler):
    __message_types__: ClassVar[GrizzlyMessageMapping] = {'response': 'consume_iot_message', 'request': 'produce_iot_message'}

    _clients: ClassVar[dict[str, IotHubCloudToDeviceClient]] = {}

    @classmethod
    def create_response(cls, key: int, request: dict[str, Any]) -> dict[str, Any]:  # noqa: ARG003
        logger.error('! creating response for request=%r', request)
        device_id = request['id']
        timeout = float(request['timeout']) * 0.999
        client = cls._clients[device_id]

        # get from queue, non blocking
        start = perf_counter()
        count = 0
        while True:
            count += 1
            try:
                serialized_message = client.queue.get_nowait()
            except Empty:
                delta = perf_counter() - start

                if delta >= timeout:
                    message = f'no message within {timeout} seconds'
                    raise RuntimeError(message) from None

                if count % 25 == 0:
                    count = 0
                    client.logger.error('! still no C2D message received within %f seconds', delta)  # noqa: TRY400

                gsleep(0.1)
            else:
                return {
                    'serialized_message': serialized_message,
                }

    @classmethod
    def register_client(cls, data: RegisterIotClient) -> bool:
        device_id = data['id']
        created = False

        with cls.semaphore:
            if device_id not in cls._clients:
                cls._clients.update({device_id: IotHubCloudToDeviceClient(data)})
                logger.error('! created client for %s', device_id)
                created = True

        return created

    @classmethod
    def get_message(cls, client: IotHubUser, *, timeout: float) -> str:
        request: dict[str, Any] = {
            'id': client.device_id,
            'host': client.host,
            'timeout': timeout,
        }

        logger.error('! get_message for %s, request=%r', client.__class__.__name__, request)

        response = cls.send_request(client, request, timeout=timeout)

        return cast(str, response['serialized_message'])


@grizzlycontext(context={
    'expression': {
        'unique': None,
        'metadata': None,
        'payload': None,
    },
})
class IotHubUser(GrizzlyUser):
    __dependencies__: ClassVar[GrizzlyDependencies] = {IotHubCloudToDeviceHandler, IotHubRegisterCloudToDeviceHandler}

    iot_client: IoTHubDeviceClient
    device_id: str

    _unique: VolatileDeque[str]
    _expression_unique: str | None

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        conn_str = self.host

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        if not conn_str.startswith('HostName='):
            message = f'{self.__class__.__name__} host needs to start with "HostName=": {self.host}'
            raise ValueError(message)

        conn_str = conn_str.replace('HostName=', 'iothub://', 1).replace(';', '/?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.query == '':
            message = f'{self.__class__.__name__} needs DeviceId and SharedAccessKey in the query string'
            raise ValueError(message)

        params = parse_qs(parsed.query)
        if 'DeviceId' not in params:
            message = f'{self.__class__.__name__} needs DeviceId in the query string'
            raise ValueError(message)

        self.device_id = params['DeviceId'][0]

        if 'SharedAccessKey' not in params:
            message = f'{self.__class__.__name__} needs SharedAccessKey in the query string'
            raise ValueError(message)

        self._expression_unique = self._context.get('expression', {}).get('unique', None)
        self._expression_payload = self._context.get('expression', {}).get('payload', None)
        self._expression_metadata = self._context.get('expression', {}).get('metadata', None)

    @classmethod
    def _serialize_message(cls, message: IotMessage) -> str:
        metadata = {
            'custom_properties': message.custom_properties,
            'message_id': message.message_id,
            'expiry_time_utc': message.expiry_time_utc,
            'correlation_id': message.correlation_id,
            'user_id': message.user_id,
            'content_type': message.content_encoding,
            'output_name': message.output_name,
            'input_name': message.input_name,
            'ack': message.ack,
            'iothub_interface_id': message.iothub_interface_id,
            'size': message.get_size(),
        }

        return json.dumps({'metadata': metadata, 'payload': message.data}, cls=MessageJsonSerializer)

    @classmethod
    def _unserialize_message(cls, serialized_message: str) -> GrizzlyResponse:
        message = json.loads(serialized_message)
        return message.get('metadata'), message.get('payload')

    def on_start(self) -> None:
        super().on_start()

        if IotHubRegisterCloudToDeviceHandler.send_register(self):
            self.logger.error('! created C2D client when registered')
        else:
            self.logger.error('! C2D client exists')

        self.iot_client = IoTHubDeviceClient.create_from_connection_string(self.host, websockets=True)

    def on_state(self, *, state: ScenarioState) -> None:
        super().on_state(state=state)

    def on_stop(self) -> None:
        self.iot_client.disconnect()
        super().on_stop()

    def _request_receive(self, request: RequestTask) -> GrizzlyResponse:
        message_wait = int((request.arguments or {}).get('wait', '-1'))

        try:
            serialized_message = IotHubCloudToDeviceHandler.get_message(self, timeout=message_wait)
            return self._unserialize_message(serialized_message)
        except RuntimeError:
            message = f'no message received within {message_wait} seconds'
            raise RuntimeError(message) from None

    def _request_send(self, request: RequestTask) -> GrizzlyResponse:
        source = cast(str, request.source)  # it hasn't come here if it was None
        message = IotMessage(data=None, message_id=uuid4())

        if has_template(source):
            source = self.render(source, variables={'__message__': message})

        message.data = source

        if request.response.content_type != TransformerContentType.UNDEFINED:
            message.content_type = request.response.content_type.value

        message.content_encoding = request.metadata.get('content_encoding', None) or 'utf-8'

        self.iot_client.send_message(message)

        self.logger.debug('sent D2C message %s', str(message.message_id))

        return self._unserialize_message(self._serialize_message(message))

    def _request_file_upload(self, request: RequestTask) -> GrizzlyResponse:
        filename = request.endpoint
        storage_info: dict[str, Any] | None = None

        try:
            with retry(retries=3, exceptions=(ClientError,), backoff=1.0) as context:
                storage_info = cast(dict[str, Any], context.execute(self.iot_client.get_storage_info_for_blob, filename))

                sas_url = 'https://{}/{}/{}{}'.format(
                    storage_info['hostName'],
                    storage_info['containerName'],
                    storage_info['blobName'],
                    storage_info['sasToken'],
                )

            if storage_info is None:
                raise RuntimeError

            with BlobClient.from_blob_url(sas_url) as blob_client:
                content_type: str | None = None
                content_encoding: str | None = None

                if request.response.content_type != TransformerContentType.UNDEFINED:
                    content_type = request.response.content_type.value

                if request.metadata:
                    content_encoding = request.metadata.get('content_encoding', None)

                if content_encoding == 'gzip':
                    compressed_payload: bytes = gzip.compress(cast(str, request.source).encode())
                    content_settings = ContentSettings(content_type=content_type, content_encoding=content_encoding)
                    metadata = blob_client.upload_blob(compressed_payload, content_settings=content_settings)
                elif content_encoding:
                    error_message = f'Unhandled request content_encoding in IotHubUser: {content_encoding}'
                    raise RuntimeError(error_message)
                else:
                    metadata = blob_client.upload_blob(request.source)

                self.logger.debug('uploaded blob to IoT hub, filename: %s, correlationId: %s', filename, storage_info['correlationId'])

            self.iot_client.notify_blob_upload_status(
                correlation_id=storage_info['correlationId'],
                is_success=True,
                status_code=200,
                status_description=f'OK: {filename}',
            )
        except:
            if storage_info is not None:
                self.iot_client.notify_blob_upload_status(
                    correlation_id=storage_info['correlationId'],
                    is_success=False,
                    status_code=500,
                    status_description=f'Failed: {filename}',
                )
                self.logger.exception('failed to upload file "%s" to IoT hub', filename)
            else:
                self.logger.exception('failed to get storage info for blob "%s"', filename)

            raise
        else:
            if metadata is not None:
                metadata.update({
                    'sasUrl': sas_url,
                })

            return metadata, request.source

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        if request.method not in [RequestMethod.SEND, RequestMethod.PUT, RequestMethod.GET, RequestMethod.RECEIVE]:
            error_message = f'{self.__class__.__name__} has not implemented {request.method.name}'
            raise NotImplementedError(error_message)

        metadata: dict[str, Any] | None = None
        payload: str | None = None

        if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
            if not request.source:
                error_message = f'Cannot upload empty payload to endpoint {request.endpoint} in IotHubUser'
                raise RuntimeError(error_message)

            if request.endpoint == 'device-to-cloud':
                metadata, payload = self._request_send(request)
            else:
                metadata, payload = self._request_file_upload(request)
        else:  # RECEIVE, GET
            metadata, payload = self._request_receive(request)

        return metadata, payload

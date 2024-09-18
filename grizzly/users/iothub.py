"""Put files to Azure IoT hub.

## Request methods

Supports the following request methods:

* send
* put
* get
* receive

## Format

Format of `host` is the following:

```plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

For `SEND` and `PUT` requests "endpoint" must be `device-to-cloud` to tell the user to send a message to the IoT hub. If "endpoint"
is not set to this, it will be interprented as the desired filename of an uploaded file.

For `GET` and `RECEIVE` requests "endpoint" must be `cloud-to-device` to tell the user to receive a message from the IoT hub.

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

"""
from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING, Any, ClassVar, cast
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob import BlobClient, ContentSettings
from gevent import sleep as gsleep

from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.utils import has_template
from grizzly_extras.queue import VolatileDeque
from grizzly_extras.transformer import JsonTransformer, TransformerContentType

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


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


@grizzlycontext(context={
    'expression': {
        'unique': None,
        'metadata': None,
        'payload': None,
    },
    'ignore': {
        'time': None,
    },
})
class IotHubUser(GrizzlyUser):
    iot_client: IoTHubDeviceClient
    device_id: str

    _startup_ignore: bool
    _startup_ignore_count: int

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

        self._startup_ignore = True
        self._startup_ignore_count = 0
        self._unique = VolatileDeque(timeout=5.0)
        self._expression_unique = self._context.get('expression', {}).get('unique', None)
        self._expression_payload = self._context.get('expression', {}).get('payload', None)
        self._expression_metadata = self._context.get('expression', {}).get('metadata', None)

    def _serialize_message(self, message: Message) -> str:
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

    def _unserialize_message(self, serialized_message: str) -> GrizzlyResponse:
        message = json.loads(serialized_message)
        return message.get('metadata'), message.get('payload')

    def _extract(self, data: Any, expression: str) -> list[str]:
        # all IoT messages are in JSON format
        parser = JsonTransformer.parser(expression)

        if isinstance(data, str):
            data = JsonTransformer.transform(data)

        return parser(data)

    def message_handler(self, message: Message) -> None:
        serialized_message = self._serialize_message(message)

        if self._startup_ignore:
            self._startup_ignore_count += 1
            self.logger.info('ignoring message: %s', serialized_message)  # @TODO: debug
            return

        if any(expression is not None for expression in [self._expression_unique, self._expression_metadata, self._expression_payload]):
            metadata, payload = self._unserialize_message(serialized_message)

            if self._expression_metadata is not None:
                values = self._extract(metadata, self._expression_metadata)

                if len(values) < 1:
                    self.logger.info('message id %s metadata did not match "%s"', message.message_id, self._expression_metadata)  # @TODO: debug
                    return

            if self._expression_payload is not None:
                values = self._extract(payload, self._expression_payload)

                if len(values) < 1:
                    self.logger.info('message id %s payload did not match "%s"', message.message_id, self._expression_payload)  # @TODO: debug
                    return

            if self._expression_unique is not None:
                values = self._extract(payload, self._expression_unique)

                if len(values) < 1:
                    self.logger.info('message id %s was an unknown message, no matches for "%s"', message.message_id, self._expression_unique)  # @TODO: debug
                    return

                value = next(iter(values))

                if value in self._unique:
                    self.logger.info('message id %s contained "%s", which has already been received within %d seconds', message.message_id, value, self._unique.timeout)
                    return

                self._unique.append(value)

        self.logger.info('C2D message received: %s', serialized_message)  # @TODO: debug
        self.consumer.keystore_push(f'cloud-to-device::{self.device_id}', serialized_message)

    def on_start(self) -> None:
        super().on_start()
        self.iot_client = IoTHubDeviceClient.create_from_connection_string(self.host, websockets=True)

    def on_state(self, *, state: ScenarioState) -> None:
        super().on_state(state=state)

        if state == ScenarioState.RUNNING:
            device_clients = self.consumer.keystore_inc(f'clients::{self.device_id}')

            # only have one C2D handler per device, independent on how many users of this type is spawned
            if device_clients == 1:
                ignore_time = float(self._context.get('ignore', {}).get('time', '1.5'))

                self.iot_client.on_message_received = self.message_handler
                self.logger.info('registered device %s as C2D handler', self.device_id)

                gsleep(ignore_time)

                self._startup_ignore = False

                if self._startup_ignore_count > 0:
                    self.logger.info('consumed %d message that was left on the C2D queue for device %s', self._startup_ignore_count, self.device_id)

    def on_stop(self) -> None:
        self.iot_client.disconnect()
        super().on_stop()

    def _request_receive(self, request: RequestTask) -> GrizzlyResponse:
        message_wait = int((request.arguments or {}).get('wait', '-1'))

        return self._unserialize_message(self.consumer.keystore_pop(f'{request.endpoint}::{self.device_id}', wait=message_wait))

    def _request_send(self, request: RequestTask) -> GrizzlyResponse:
        source = cast(str, request.source)  # it hasn't come here if it was None
        message = Message(data=None, message_id=uuid4())

        if has_template(source):
            source = self.render(source, variables={'__message__': message})

        message.data = source

        if request.response.content_type != TransformerContentType.UNDEFINED:
            message.content_type = request.response.content_type.value

        message.content_encoding = request.metadata.get('content_encoding', None) or 'utf-8'

        self.iot_client.send_message(message)

        self.logger.info('sent D2C message %s', str(message.message_id))  # @TODO: debug

        return self._unserialize_message(self._serialize_message(message))

    def _request_file_upload(self, request: RequestTask) -> GrizzlyResponse:
        filename = request.endpoint
        storage_info: dict[str, Any] | None = None

        try:
            storage_info = cast(dict[str, Any], self.iot_client.get_storage_info_for_blob(filename))

            sas_url = 'https://{}/{}/{}{}'.format(
                storage_info['hostName'],
                storage_info['containerName'],
                storage_info['blobName'],
                storage_info['sasToken'],
            )

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

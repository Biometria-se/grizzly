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
from time import perf_counter
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob import BlobClient, ContentSettings
from gevent import sleep as gsleep

from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState
from grizzly.utils import has_template
from grizzly_extras.transformer import TransformerContentType, transformer

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


@grizzlycontext(context={})
class IotHubUser(GrizzlyUser):
    iot_client: IoTHubDeviceClient
    device_id: str

    _startup_ignore: bool
    _startup_ignore_count: int

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

    def serialize_message(self, message: Message) -> str:
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

    def unserialize_message(self, serialized_message: str) -> GrizzlyResponse:
        message = json.loads(serialized_message)
        return message.get('metadata'), message.get('payload')

    def message_handler(self, message: Message) -> None:
        if self._startup_ignore:
            self._startup_ignore_count += 1
            return

        serialized_message = self.serialize_message(message)
        self.logger.info('C2D message received: %s', serialized_message)  # @TODO: debug
        self.consumer.keystore_push(f'cloud-to-device::{self.device_id}', serialized_message)

    def on_start(self) -> None:
        super().on_start()
        self.iot_client = IoTHubDeviceClient.create_from_connection_string(self.host, websockets=True)

    def on_state(self, *, state: ScenarioState) -> None:
        super().on_state(state=state)

        if state == ScenarioState.RUNNING:
            device_clients = self.consumer.keystore_inc(f'clients::{self.device_id}')

            self.iot_client.on_message_received = self.message_handler
            self.logger.info('registered device %s as C2D handler #%d', self.device_id, device_clients)

            gsleep(1.5)

            self._startup_ignore = False

            if self._startup_ignore_count > 0:
                self.logger.info('consumed %d message that was left on the C2D queue for device %s', self._startup_ignore_count, self.device_id)

    def on_stop(self) -> None:
        self.iot_client.disconnect()
        super().on_stop()

    def _request_receive(self, request: RequestTask) -> GrizzlyResponse:
        payload: str | None = None

        started = int(perf_counter())
        message_wait = int((request.arguments or {}).get('wait', '-1'))

        while payload is None:
            metadata, payload = self.unserialize_message(self.consumer.keystore_pop(f'{request.endpoint}::{self.device_id}'))
            log_message = f'{metadata=}, {payload=}'

            payload_expression = (request.arguments or {}).get('payload_expression', None)
            metadata_expression = (request.arguments or {}).get('metadata_expression', None)

            # <!-- filter cloud-to-device messages
            if payload_expression is not None and payload is not None:
                log_message = f'{log_message}, {payload_expression=}'
                transform = transformer.available.get(request.response.content_type, None)

                if transform is None:
                    error_message = f'could not find a transformer for {request.response.content_type.name}'
                    raise TypeError(error_message)

                get_values = transform.parser(payload_expression)
                values = get_values(transform.transform(payload))

                log_message = f'{log_message}, {values=}'

                # expression had no matches in payload
                if len(values) < 1:
                    payload = None
                    gsleep(0.1)

            if metadata_expression is not None and metadata:
                log_message = f'{log_message}, {metadata_expression=}'
                transform = transformer.available.get(TransformerContentType.JSON, None)

                if transform is None:
                    error_message = 'could not find a transformer for JSON'
                    raise TypeError(error_message)

                get_values = transform.parser(metadata_expression)
                values = get_values(metadata)

                log_message = f'{log_message}, {values=}'

                # expression had no matches in metadata, hence we're not interested in payload
                if len(values) < 1:
                    payload = None
                    gsleep(0.1)
            # // -->

            self.logger.info(log_message)  # @TODO: debug

            # do not wait forever, if `wait` has been specified in the request arguments
            if message_wait > -1:
                delta = int(perf_counter()) - started

                if delta >= message_wait:
                    error_message = f'no C2D message received for {self.device_id} in {message_wait} seconds'
                    raise RuntimeError(error_message)

        return metadata, payload

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

        return self.unserialize_message(self.serialize_message(message))

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

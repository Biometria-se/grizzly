"""Put files to Azure IoT hub.

## Request methods

Supports the following request methods:

* send
* put

## Format

Format of `host` is the following:

```plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

`endpoint` in the request is the desired filename for the uploaded file.

The metadata values `content_type` and `content_encoding` can be set to
gzip compress the payload before upload (see example below).

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

The same example with gzip compression enabled:

```gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
And metadata "content_type" is "application/octet-stream; charset=utf-8"
And metadata "content_encoding" is "gzip"
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```

"""
from __future__ import annotations

import gzip
import json
from typing import TYPE_CHECKING, Any, Optional, cast
from urllib.parse import parse_qs, urlparse

from azure.iot.device import IoTHubDeviceClient, Message
from azure.storage.blob import BlobClient, ContentSettings

from grizzly.types import GrizzlyResponse, RequestMethod, ScenarioState

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


def serialize_message(message: Message) -> str:
    payload = str(message)
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

    return json.dumps({'metadata': metadata, 'payload': payload})


def unserialize_message(blob: str) -> GrizzlyResponse:
    message = json.loads(blob)
    return message.get('metadata'), message.get('payload')


@grizzlycontext(context={})
class IotHubUser(GrizzlyUser):
    iot_client: IoTHubDeviceClient
    device_id: str

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

    def message_handler(self, message: Message) -> None:
        self.logger.info('received: %r', message)
        self.consumer.keystore_push(f'queue::{self.device_id}', serialize_message(message))

    def on_start(self) -> None:
        super().on_start()
        self.iot_client = IoTHubDeviceClient.create_from_connection_string(self.host, websockets=True)

    def on_state(self, *, state: ScenarioState) -> None:
        super().on_state(state=state)

        if state == ScenarioState.RUNNING:
            device_clients = self.consumer.keystore_inc(f'clients::{self.device_id}')

            self.iot_client.on_message_received = self.message_handler
            self.logger.info('%s client %d, registered C2D handler', self.device_id, device_clients)

    def on_stop(self) -> None:
        self.iot_client.disconnect()
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        if request.method not in [RequestMethod.SEND, RequestMethod.PUT, RequestMethod.GET, RequestMethod.RECEIVE]:
            message = f'{self.__class__.__name__} has not implemented {request.method.name}'
            raise NotImplementedError(message)

        storage_info: Optional[dict[str, Any]] = None
        metadata: dict[str, Any] | None = None
        payload: str | None = None

        try:
            if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
                if not request.source:
                    message = f'Cannot upload empty payload to endpoint {request.endpoint} in IotHubUser'
                    raise RuntimeError(message)

                filename = request.endpoint

                storage_info = cast(dict[str, Any], self.iot_client.get_storage_info_for_blob(filename))

                sas_url = 'https://{}/{}/{}{}'.format(
                    storage_info['hostName'],
                    storage_info['containerName'],
                    storage_info['blobName'],
                    storage_info['sasToken'],
                )

                with BlobClient.from_blob_url(sas_url) as blob_client:
                    content_type: Optional[str] = None
                    content_encoding: Optional[str] = None
                    if request.metadata:
                        content_type = request.metadata.get('content_type', None)
                        content_encoding = request.metadata.get('content_encoding', None)

                    if content_encoding == 'gzip':
                        compressed_payload: bytes = gzip.compress(request.source.encode())
                        content_settings = ContentSettings(content_type=content_type, content_encoding=content_encoding)
                        blob_client.upload_blob(compressed_payload, content_settings=content_settings)
                    elif content_encoding:
                        message = f'Unhandled request content_encoding in IotHubUser: {content_encoding}'
                        raise RuntimeError(message)
                    else:
                        blob_client.upload_blob(request.source)

                    self.logger.debug('uploaded blob to IoT hub, filename: %s, correlationId: %s', filename, storage_info['correlationId'])

                self.iot_client.notify_blob_upload_status(
                    correlation_id=storage_info['correlationId'],
                    is_success=True,
                    status_code=200,
                    status_description=f'OK: {filename}',
                )
                metadata = {}
                payload = request.source
            else:  # RECEIVE, GET
                metadata, payload = unserialize_message(self.consumer.keystore_pop(f'queue::{self.device_id}'))
                self.logger.info('metadata=%r, payload=%r', metadata, payload)
        except Exception:
            if storage_info:
                self.iot_client.notify_blob_upload_status(
                    correlation_id=storage_info['correlationId'],
                    is_success=False,
                    status_code=500,
                    status_description=f'Failed: {filename}',
                )
                self.logger.exception('failed to upload file "%s" to IoT hub', filename)

            raise
        else:
            return metadata, payload

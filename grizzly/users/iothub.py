'''Put files to Azure IoT hub.

## Request methods

Supports the following request methods:

* send
* put

## Format

Format of `host` is the following:

``` plain
HostName=<hostname>;DeviceId=<device key>;SharedAccessKey=<access key>
```

`endpoint` in the request is the desired filename for the uploaded file.

## Examples

Example of how to use it in a scenario:

``` gherkin
Given a user of type "IotHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```
'''

from typing import Dict, Any, Tuple
from urllib.parse import urlparse, parse_qs

from azure.iot.device import IoTHubDeviceClient
from azure.storage.blob import BlobClient

from grizzly.types import RequestMethod, GrizzlyResponse
from grizzly.types.locust import Environment
from grizzly.tasks import RequestTask
from grizzly.utils import merge_dicts

from .base import GrizzlyUser


class IotHubUser(GrizzlyUser):
    iot_client: IoTHubDeviceClient
    _context: Dict[str, Any] = {}

    def __init__(self, environment: Environment, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        conn_str = self.host

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        if not conn_str.startswith('HostName='):
            raise ValueError(f'{self.__class__.__name__} host needs to start with "HostName=": {self.host}')
        conn_str = conn_str.replace('HostName=', 'iothub://', 1).replace(';', '/?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__} needs DeviceId and SharedAccessKey in the query string')

        params = parse_qs(parsed.query)
        if 'DeviceId' not in params:
            raise ValueError(f'{self.__class__.__name__} needs DeviceId in the query string')

        if 'SharedAccessKey' not in params:
            raise ValueError(f'{self.__class__.__name__} needs SharedAccessKey in the query string')

        self._context = merge_dicts(super().context(), self.__class__._context)

    def on_start(self) -> None:
        super().on_start()
        self.iot_client = IoTHubDeviceClient.create_from_connection_string(self.host)

    def on_stop(self) -> None:
        self.iot_client.disconnect()
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        try:
            filename = request.endpoint

            storage_info = self.iot_client.get_storage_info_for_blob(filename)

            if request.method not in [RequestMethod.SEND, RequestMethod.PUT]:
                raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')

            sas_url = 'https://{}/{}/{}{}'.format(
                storage_info['hostName'],
                storage_info['containerName'],
                storage_info['blobName'],
                storage_info['sasToken']
            )

            with BlobClient.from_blob_url(sas_url) as blob_client:
                blob_client.upload_blob(request.source)
                self.logger.debug(f'Uploaded blob to IoT hub, filename: {filename}, correlationId: {storage_info["correlationId"]}')

            self.iot_client.notify_blob_upload_status(
                storage_info['correlationId'], True, 200, f'OK: {filename}'
            )
            return {}, request.source
        except Exception as e:
            if not isinstance(e, NotImplementedError):
                self.iot_client.notify_blob_upload_status(
                    storage_info['correlationId'], False, 500, f'Failed: {filename}'
                )
            self.logger.error(f'Failed to upload file "{filename}" to IoT hub', exc_info=e)

            raise e

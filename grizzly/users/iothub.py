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
Given a user of type "IoTHub" load testing "HostName=my_iot_host_name;DeviceId=my_device;SharedAccessKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "uploaded_blob_filename"
```
'''

from typing import Dict, Any, Tuple, Optional
from urllib.parse import urlparse, parse_qs
from time import perf_counter as time

from azure.iot.device import IoTHubDeviceClient
from azure.storage.blob import BlobClient
from locust.exception import StopUser
from locust.env import Environment

from . import logger
from .base import GrizzlyUser
from ..types import RequestMethod, GrizzlyResponse, RequestType
from ..tasks import RequestTask
from ..utils import merge_dicts


class IotHubUser(GrizzlyUser):
    client: IoTHubDeviceClient
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

        self.client = IoTHubDeviceClient.create_from_connection_string(self.host)

        self._context = merge_dicts(super().context(), self.__class__._context)

    def request(self, request: RequestTask) -> GrizzlyResponse:
        request_name, endpoint, payload, _, _ = self.render(request)

        name = f'{request.scenario.identifier} {request_name}'

        exception: Optional[Exception] = None
        start_time = time()
        response_length = 0

        try:
            filename = endpoint

            storage_info = self.client.get_storage_info_for_blob(filename)

            sas_url = 'https://{}/{}/{}{}'.format(
                storage_info['hostName'],
                storage_info['containerName'],
                storage_info['blobName'],
                storage_info['sasToken']
            )

            with BlobClient.from_blob_url(sas_url) as blob_client:
                if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
                    blob_client.upload_blob(payload)
                    logger.debug(f'Uploaded blob to IoT hub, filename: {filename}, correlationId: {storage_info["correlationId"]}')
                    response_length = len(payload or '')

                    self.client.notify_blob_upload_status(
                        storage_info['correlationId'], True, 200, f'OK: {filename}'
                    )
                else:
                    raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')
        except Exception as e:
            exception = e
        finally:
            total_time = int((time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type=RequestType.from_method(request.method),
                name=name,
                response_time=total_time,
                response_length=response_length,
                context=self._context,
                exception=exception
            )

            if exception is not None:
                logger.error(f'Failed to upload file "{filename}" to IoT hub', exc_info=exception)
                if isinstance(exception, NotImplementedError):
                    raise StopUser()
                elif request.scenario.failure_exception is not None:
                    raise request.scenario.failure_exception()

            return {}, payload

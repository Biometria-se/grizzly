'''Put files to Azure Blob Storage.

## Request methods

Supports the following request methods:

* send
* put

## Format

Format of `host` is the following:

```plain
[DefaultEndpointsProtocol=]https;EndpointSuffix=<hostname>;AccountName=<account name>;AccountKey=<account key>
```

`endpoint` in the request is the name of the blob storage container. Name of the targeted file in the container
is either `name` or based on the file name of `source`.

## Examples

Example of how to use it in a scenario:

```gherkin
Given a user of type "BlobStorage" load testing "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "azure-blobstorage-container-name"
```
'''
import os

from typing import Dict, Any, Tuple, Optional
from urllib.parse import urlparse, parse_qs
from time import perf_counter as time

from azure.storage.blob import BlobServiceClient
from locust.exception import StopUser

from .meta import ContextVariables
from ..types import RequestMethod, GrizzlyResponse
from ..task import RequestTask
from ..utils import merge_dicts

class BlobStorageUser(ContextVariables):
    client: BlobServiceClient
    host: str
    _context: Dict[str, Any] = {}

    def __init__(self, *args: Tuple[Any], **kwargs: Dict[str, Any]) -> None:
        super().__init__(*args, **kwargs)

        conn_str = self.host
        if conn_str.startswith('DefaultEndpointsProtocol='):
            conn_str = conn_str[25:]

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        conn_str = conn_str.replace(';EndpointSuffix=', '://', 1).replace(';', '/?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.scheme != 'https':
            raise ValueError(f'"{parsed.scheme}" is not supported for {self.__class__.__name__}')

        if parsed.query == '':
            raise ValueError(f'{self.__class__.__name__} needs AccountName and AccountKey in the query string')

        params = parse_qs(parsed.query)
        if 'AccountName' not in params:
            raise ValueError(f'{self.__class__.__name__} needs AccountName in the query string')

        if 'AccountKey' not in params:
            raise ValueError(f'{self.__class__.__name__} needs AccountKey in the query string')

        self.client = BlobServiceClient.from_connection_string(conn_str=self.host)
        self._context = merge_dicts(super().context(), self.__class__._context)

    def request(self, request: RequestTask) -> GrizzlyResponse:
        request_name, endpoint, payload = self.render(request)

        name = f'{request.scenario.identifier} {request_name}'

        exception: Optional[Exception] = None
        start_time = time()

        try:
            with self.client.get_blob_client(container=endpoint, blob=os.path.basename(request_name)) as blob_client:
                if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
                    blob_client.upload_blob(payload)
                else:
                    raise NotImplementedError(f'{self.__class__.__name__} has not implemented {request.method.name}')
        except Exception as e:
            exception = e
        finally:
            total_time = int((time() - start_time) * 1000)
            self.environment.events.request.fire(
                request_type=f'bs:{request.method.name[:4]}',
                name=name,
                response_time=total_time,
                response_length=0,
                context=self._context,
                exception=exception
            )

            if exception is not None:
                if isinstance(exception, NotImplementedError):
                    raise StopUser()
                elif request.scenario.failure_exception is not None:
                    raise request.scenario.failure_exception()

            return {}, payload

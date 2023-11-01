"""Put files to Azure Blob Storage.

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
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from azure.storage.blob import BlobServiceClient

from grizzly.types import GrizzlyResponse, RequestMethod
from grizzly.utils import merge_dicts

from .base import GrizzlyUser

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


class BlobStorageUser(GrizzlyUser):
    blob_client: BlobServiceClient

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        conn_str: str = self.host
        if conn_str.startswith('DefaultEndpointsProtocol='):
            conn_str = conn_str[25:]

        # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
        # for validation
        conn_str = conn_str.replace(';EndpointSuffix=', '://', 1).replace(';', '/?', 1).replace(';', '&')

        parsed = urlparse(conn_str)

        if parsed.scheme != 'https':
            message = f'"{parsed.scheme}" is not supported for {self.__class__.__name__}'
            raise ValueError(message)

        if parsed.query == '':
            message = f'{self.__class__.__name__} needs AccountName and AccountKey in the query string'
            raise ValueError(message)

        params = parse_qs(parsed.query)
        if 'AccountName' not in params:
            message = f'{self.__class__.__name__} needs AccountName in the query string'
            raise ValueError(message)

        if 'AccountKey' not in params:
            message = f'{self.__class__.__name__} needs AccountKey in the query string'
            raise ValueError(message)

        self._context = merge_dicts(super().context(), self.__class__._context)

    def on_start(self) -> None:
        """Create blob storage client when user starts."""
        super().on_start()
        self.blob_client = BlobServiceClient.from_connection_string(conn_str=self.host)

    def on_stop(self) -> None:
        """Disconnect blob storage client when user stops."""
        self.blob_client.close()
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        """Perform a blob storage request based on request task."""
        blob = Path(request.endpoint).name
        container = request.endpoint
        print(f'{request.endpoint=}, {container=}, {blob=}')

        if container.endswith(blob):
            container = str(Path(container).parent)
        else:
            blob = self._normalize(request.name)

        with self.blob_client.get_blob_client(container=container, blob=blob) as blob_client:
            if request.method in [RequestMethod.SEND, RequestMethod.PUT]:
                blob_client.upload_blob(request.source)
            else:  # pragma: no cover
                message = f'{self.__class__.__name__} has not implemented {request.method.name}'
                raise NotImplementedError(message)

        return {}, request.source

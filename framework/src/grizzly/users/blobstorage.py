"""Perform operations towards Azure Blob (container) Storage.

## Request methods

Supports the following request methods:

* send
* put
* receive
* get

## Format

Format of `host` is the following, when using connection strings:

```plain
[DefaultEndpointsProtocol=]https;EndpointSuffix=<hostname>;AccountName=<account name>;AccountKey=<account key>
```

When using credentials context variables `auth.tenant`, `auth.user.username` and `auth.user.passwords` has to be set, and the format `host` should be:
```plain
bs://<storage account name>[.blob.core.windows.net]
```

`endpoint` in the request is the name of the blob storage container. Name of the targeted file in the container
is either `name` or based on the file name of `source`.

## Examples

With connection string:

```gherkin
Given a user of type "BlobStorage" load testing "DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=examplestorage;AccountKey=xxxyyyyzzz=="
Then send request "test/blob.file" to endpoint "azure-blobstorage-container-name"
```

With credentials:

```gherkin
Given a user of type "BlobStorage" load testing "bs://examplestorage"
And set context variable "auth.tenant" to "example.com"
And set context variable "auth.user.username" to "bob@example.com"
And set context variable "auth.user.password" to "secret"
Then send request "test/blob.file" to endpoint "azure-blobstorage-container-name"
```

"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

from azure.storage.blob import BlobServiceClient
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential

from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.utils import normalize

from . import GrizzlyUser, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


@grizzlycontext(
    context={
        'auth': {
            'tenant': None,
            'user': {
                'username': None,
                'password': None,
            },
        },
    },
)
class BlobStorageUser(GrizzlyUser):
    blob_client: BlobServiceClient

    credential: AzureAadCredential | None = None
    url: str

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.url = self.host

        context_auth_user = self._context.get('auth', {}).get('user', {})
        username = context_auth_user.get('username', None)
        password = context_auth_user.get('password', None)

        if username is None and password is None:
            self.url = self.url.removeprefix('DefaultEndpointsProtocol=')

            # Replace semicolon separators between parameters to ? and & and massage it to make it "urlparse-compliant"
            # for validation
            self.url = self.url.replace(';', '://?', 1).replace(';', '&')

            parsed = urlparse(self.url)

            if parsed.scheme != 'https':
                message = f'"{parsed.scheme}" is not supported for {self.__class__.__name__}'
                raise ValueError(message)

            params = parse_qs(parsed.query)
            if 'AccountName' not in params:
                message = f'{self.__class__.__name__} needs AccountName in the query string'
                raise ValueError(message)

            if 'AccountKey' not in params:
                message = f'{self.__class__.__name__} needs AccountKey in the query string'
                raise ValueError(message)
        else:
            host_parsed = urlparse(self.host)
            tenant = self._context.get('auth', {}).get('tenant', None)

            if tenant is None:
                message = f'{self.__class__.__name__} does not have context variable auth.tenant set while auth.user is'
                raise ValueError(message)

            self.url = f'https://{host_parsed.hostname}'

            if not self.url.endswith('.blob.core.windows.net'):
                self.url = f'{self.url}.blob.core.windows.net'

            self.credential = AzureAadCredential(username, password, tenant, AuthMethod.USER, host=self.url)

    def on_start(self) -> None:
        super().on_start()

        if self.credential is None:
            self.blob_client = BlobServiceClient.from_connection_string(conn_str=self.host)
        else:
            self.blob_client = BlobServiceClient(account_url=self.url, credential=self.credential)

    def on_stop(self) -> None:
        self.blob_client.close()
        super().on_stop()

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        blob = Path(request.endpoint).name
        container = request.endpoint

        if container.endswith(blob):
            container = str(Path(container).parent)
        else:
            blob = normalize(request.name)

        if request.method not in [RequestMethod.SEND, RequestMethod.PUT, RequestMethod.RECEIVE, RequestMethod.GET]:
            message = f'{self.__class__.__name__} has not implemented {request.method.name}'
            raise NotImplementedError(message)

        with self.blob_client.get_blob_client(container=container, blob=blob) as blob_client:
            if request.method.direction == RequestDirection.TO:
                blob_client.upload_blob(request.source, overwrite=True)
            else:
                downloader = blob_client.download_blob()
                request.source = downloader.readall().decode('utf-8')

        properties = blob_client.get_blob_properties()
        headers = dict(properties.items())

        return headers, request.source

'''This task performs Azure Blob Storage get or put operations to a specified endpoint.

This is useful if the scenario is another user type than `BlobStorageUser`, but the scenario still requires an action towards a blob container.

Endpoint is specified in the format:

```plain
sb://<AccountName>?AccountKey=<AccountKey>&Container=<Container>
```

All variables in the endpoint supports templating, but not the whole string.

Example:

```plain
sb://my-storage?AccountKey=aaaabbbb==&Container=my-container
```

This will be resolved to `DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbbb==;EndpointSuffix=core.windows.net`, and operations
will be performed in container `my-container`.

Instances of this task is created with the step expression, if endpoint is defined with scheme `bs`:
* ~~[`step_task_client_get_endpoint`](/grizzly/usage/steps/scenario/tasks/#step_task_client_get_endpoint)~~
* [`step_task_client_put_endpoint_file`](/grizzly/usage/steps/scenario/tasks/#step_task_client_put_endpoint_file)
* [`step_task_client_put_endpoint_text`](/grizzly/usage/steps/scenario/tasks/#step_task_client_put_endpoint_text)
'''
from typing import Any, Optional, cast
from urllib.parse import urlparse, parse_qs
from pathlib import Path

from jinja2 import Template
from azure.storage.blob import BlobServiceClient

from . import client, ClientTask
from ...scenarios import GrizzlyScenario
from ...types import RequestDirection
from ...testdata.utils import resolve_variable


@client('sb')
class BlobStorageClientTask(ClientTask):
    account_name: str
    account_key: str
    container: str
    service_client: BlobServiceClient

    def __init__(
        self, direction: RequestDirection, endpoint: str, /, source: str, destination: Optional[str] = None,
    ) -> None:
        super().__init__(direction, endpoint, destination=destination, source=source)

        parsed = urlparse(self.endpoint)

        if parsed.hostname is None:
            raise ValueError(f'{self.__class__.__name__}: could not find accout name in {self.endpoint}')

        parameters = parse_qs(parsed.params)

        if 'AccountKey' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find AccountKey in {self.endpoint}')

        if 'Container' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find Container in {self.endpoint}')

        self.account_name = cast(str, resolve_variable(self.grizzly, parsed.hostname, guess_datatype=False))
        self.account_key = cast(str, resolve_variable(self.grizzly, parameters['AccountKey'][0], guess_datatype=False))
        self.container = cast(str, resolve_variable(self.grizzly, parameters['Container'][0], guess_datatype=False))

        self.service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)

    @property
    def connection_string(self) -> str:
        return f'DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net'

    def get(self, parent: GrizzlyScenario) -> Any:
        return super().get(parent)

    def put(self, parent: GrizzlyScenario) -> Any:
        source = cast(str, resolve_variable(self.grizzly, cast(str, self.source), guess_datatype=False))
        if self.destination is not None:
            destination = cast(str, resolve_variable(self.grizzly, self.destination, guess_datatype=False))
        else:
            destination = Path(source).name

        source_file = Path(parent.user._context_root) / 'requests' / source

        if source_file.exists():
            source = source_file.read_text()

        source = Template(source).render(**parent.user._context['variables'])
        destination = Template(destination).render(**parent.user._context['variables'])

        with self.action(parent) as meta:
            with self.service_client.get_blob_client(container=self.container, blob=destination) as blob_client:
                blob_client.upload_blob(source)
                meta['response_length'] = len(source)

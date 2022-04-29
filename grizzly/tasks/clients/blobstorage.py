'''This task performs Azure Blob Storage get or put operations to a specified endpoint.

This is useful if the scenario is another user type than `BlobStorageUser`, but the scenario still requires an action towards a blob container.

Endpoint is specified in the format:

```plain
bs[s]://<AccountName>?AccountKey=<AccountKey>&Container=<Container>
```

All variables in the endpoint supports templating, but not the whole string.

Content-Type of an uploaded file will automagically be guessed based on the [rendered] destination file extension.

Example:

```plain
bss://my-storage?AccountKey=aaaabbbb==&Container=my-container
```

This will be resolved to `DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbbb==;EndpointSuffix=core.windows.net`, and operations
will be performed in container `my-container`.

Instances of this task is created with the step expression, if endpoint is defined with scheme `bs`:

* [`step_task_client_put_endpoint_file_destination`](/grizzly/framework/usage/steps/scenario/tasks/#step_task_client_put_endpoint_file_destination)
'''
import logging

from typing import Any, Optional, cast
from urllib.parse import urlparse, parse_qs
from pathlib import Path
from mimetypes import guess_type as mimetype_guess

from azure.storage.blob import BlobServiceClient, ContentSettings


from . import client, ClientTask
from ...scenarios import GrizzlyScenario
from ...context import GrizzlyContextScenario
from ...types import RequestDirection
from ...testdata.utils import resolve_variable

# disable verbose INFO logging
azure_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azure_logger.setLevel(logging.ERROR)


@client('bs', 'bss')
class BlobStorageClientTask(ClientTask):
    account_name: str
    account_key: str
    container: str
    service_client: BlobServiceClient

    _endpoints_protocol: str

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str, /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        super().__init__(direction, endpoint, variable=variable, destination=destination, source=source, scenario=scenario)

        parsed = urlparse(self.endpoint)

        self._endpoints_protocol = 'http' if parsed.scheme == 'bs' else 'https'

        if self.source is None and self.direction == RequestDirection.TO:
            raise ValueError(f'{self.__class__.__name__}: source must be set for direction {self.direction.name}')

        if parsed.netloc is None or len(parsed.netloc) < 1:
            raise ValueError(f'{self.__class__.__name__}: could not find account name in {self.endpoint}')

        parameters = parse_qs(parsed.query)

        if 'AccountKey' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find AccountKey in {self.endpoint}')

        if 'Container' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find Container in {self.endpoint}')

        self.account_name = cast(str, resolve_variable(self.grizzly, parsed.netloc, guess_datatype=False))
        self.account_key = cast(str, resolve_variable(self.grizzly, parameters['AccountKey'][0], guess_datatype=False))
        self.container = cast(str, resolve_variable(self.grizzly, parameters['Container'][0], guess_datatype=False))

        self.service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)

    @property
    def connection_string(self) -> str:
        return f'DefaultEndpointsProtocol={self._endpoints_protocol};AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net'

    def get(self, parent: GrizzlyScenario) -> Any:
        return super().get(parent)

    def put(self, parent: GrizzlyScenario) -> Any:
        source = parent.render(cast(str, self.source))

        if self.destination is not None:
            destination = parent.render(self.destination)
        else:
            destination = Path(source).name

        source_file = Path(self._context_root) / 'requests' / source

        if source_file.exists():
            source = source_file.read_text()
            source = parent.render(source)

        content_type, _ = mimetype_guess(destination)

        if content_type is None:
            content_type = 'application/octet-stream'

        content_settings = ContentSettings(content_type=content_type)

        with self.action(parent) as meta:
            with self.service_client.get_blob_client(container=self.container, blob=destination) as blob_client:
                blob_client.upload_blob(source, content_settings=content_settings)
                meta['response_length'] = len(source)
                meta['action'] = f'{self.container}'

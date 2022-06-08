'''This task performs Azure Blob Storage put operations to a specified endpoint.

This is useful if the scenario is another user type than `BlobStorageUser`, but the scenario still requires an action towards a blob container.

Only supports `RequestDirection.TO`.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.step_task_client_put_endpoint_file_destination}

## Arguments

* `direction` _RequestDirection_ - if the request is upstream or downstream

* `endpoint` _str_ - specifies details to be able to perform the request, e.g. account and container information

* `name` _str_ - name used in `locust` statistics

* `destination` _str_ (optional) - name of the file when uploaded, if not specified the basename of `source` will be used

* `source` _str_ (optional) - file path of local file that should be saved in `Container`

## Format

### `endpoint`

``` plain
bs[s]://<AccountName>?AccountKey=<AccountKey>&Container=<Container>
```

* `AccountName` _str_ - name of storage account

* `AccountKey` _str_ - secret key to be able to "connect" to the storage account

* `Container` _str_ - name of the container to perform the request on

All variables in the endpoint supports {@link framework.usage.variables.templating}, but not the whole string.

### `destination`

The MIME type of an uploaded file will automagically be guessed based on the [rendered] destination file extension.
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
        endpoint: str,
        name: Optional[str] = None,
        /,
        variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        scenario: Optional[GrizzlyContextScenario] = None,
    ) -> None:
        super().__init__(direction, endpoint, name, variable=variable, destination=destination, source=source, scenario=scenario)

        parsed = urlparse(self.endpoint)

        self._endpoints_protocol = 'http' if parsed.scheme == 'bs' else 'https'

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

        content_type, _ = mimetype_guess(destination)

        if content_type is None:
            content_type = 'application/octet-stream'

        content_settings = ContentSettings(content_type=content_type)

        with self.action(parent, action=self.container) as meta:
            source_file = Path(self._context_root) / 'requests' / source

            if not source_file.exists():
                raise FileNotFoundError(source)

            source = parent.render(source_file.read_text())

            with self.service_client.get_blob_client(container=self.container, blob=destination) as blob_client:
                blob_client.upload_blob(source, content_settings=content_settings)
                meta['response_length'] = len(source)

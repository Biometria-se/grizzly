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
bs[s]://<AccountName>?AccountKey=<AccountKey>&Container=<Container>[&Overwrite=<bool>]
```

* `AccountName` _str_ - name of storage account

* `AccountKey` _str_ - secret key to be able to "connect" to the storage account

* `Container` _str_ - name of the container to perform the request on

* `Overwrite` _bool_ - if files should be overwritten if they already exists in `Container` (default: `False`)

### `destination`

The MIME type of an uploaded file will automagically be guessed based on the [rendered] destination file extension.
'''
import logging

from typing import Optional, List, cast
from urllib.parse import urlparse, parse_qs, quote
from pathlib import Path
from mimetypes import guess_type as mimetype_guess

from azure.storage.blob import BlobServiceClient, ContentSettings

from grizzly.types import RequestDirection, GrizzlyResponse, bool_type
from grizzly.scenarios import GrizzlyScenario

from . import client, ClientTask

# disable verbose INFO logging
azure_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azure_logger.setLevel(logging.ERROR)


@client('bs', 'bss')
class BlobStorageClientTask(ClientTask):
    account_name: str
    account_key: str
    container: str
    service_client: BlobServiceClient
    overwrite: bool

    _endpoints_protocol: str

    def __init__(
        self,
        direction: RequestDirection,
        endpoint: str,
        name: Optional[str] = None,
        /,
        payload_variable: Optional[str] = None,
        metadata_variable: Optional[str] = None,
        source: Optional[str] = None,
        destination: Optional[str] = None,
        text: Optional[str] = None,
    ) -> None:
        super().__init__(
            direction,
            endpoint,
            name,
            payload_variable=payload_variable,
            metadata_variable=metadata_variable,
            destination=destination,
            source=source,
            text=text,
        )

        parsed = urlparse(self.endpoint)

        self._endpoints_protocol = 'http' if self._scheme == 'bs' else 'https'

        if parsed.netloc is None or len(parsed.netloc) < 1:
            raise ValueError(f'{self.__class__.__name__}: could not find account name in {self.endpoint}')

        # See urllib/parse.py:771-774, explicit + characters are replaced with white space,
        # AccountKey could contain explicit + characters, so we must quote them first.
        parsed_query: List[str] = []
        if len(parsed.query) > 0:
            for parameter in parsed.query.split('&'):
                key, value = parameter.split('=', 1)
                parsed_query.append(f'{quote(key)}={quote(value)}')

        parameters = parse_qs('&'.join(parsed_query))

        if 'AccountKey' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find AccountKey in {self.endpoint}')

        if 'Container' not in parameters:
            raise ValueError(f'{self.__class__.__name__}: could not find Container in {self.endpoint}')

        self.account_name = parsed.netloc
        self.account_key = parameters['AccountKey'][0]
        self.container = parameters['Container'][0]
        if 'Overwrite' in parameters:
            self.overwrite = bool_type(parameters['Overwrite'][0])
        else:
            self.overwrite = False

        self.service_client = BlobServiceClient.from_connection_string(conn_str=self.connection_string)

    @property
    def connection_string(self) -> str:
        return f'DefaultEndpointsProtocol={self._endpoints_protocol};AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net'

    def get(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        raise NotImplementedError(f'{self.__class__.__name__} has not implemented GET')  # pragma: no cover

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
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

            meta.update({'request': {
                'url': f'{destination}@{self.container}',
                'payload': source,
                'metadata': None,
            }})

            with self.service_client.get_blob_client(container=self.container, blob=destination) as blob_client:
                blob_client.upload_blob(source, content_settings=content_settings, overwrite=self.overwrite)
                meta['response_length'] = len(source.encode('utf-8'))

            meta.update({'response': {}})

        return None, None

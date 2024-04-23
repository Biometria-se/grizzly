"""@anchor pydoc:grizzly.tasks.clients.blobstorage Blob Storage
This task performs Azure Blob Storage put operations to a specified endpoint.

This is useful if the scenario is another user type than `BlobStorageUser`, but the scenario still requires an action towards a blob container.

Only supports `RequestDirection.TO`.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_put_endpoint_file_destination}

## Arguments

* `direction` _RequestDirection_ - if the request is upstream or downstream

* `endpoint` _str_ - specifies details to be able to perform the request, e.g. account and container information

* `name` _str_ - name used in `locust` statistics

* `destination` _str_ (optional) - name of the file when uploaded, if not specified the basename of `source` will be used

* `source` _str_ (optional) - file path of local file that should be saved in `Container`

## Format

### `endpoint`

Using connection strings:
```plain
bs[s]://<AccountName>/<Container>?AccountKey=<AccountKey>[# Overwrite=<bool>]
```
* `AccountName` _str_ - name of storage account

* `AccountKey` _str_ - secret key to be able to "connect" to the storage account

* `Container` _str_ - name of the container to perform the request on

* `Overwrite` _bool_ - if files should be overwritten if they already exists in `Container` (default: `False`)

Using credentials:
```plain
bs[s]://<username>:<password>@<AccountName>/<Container># Tenant=<tenant>[&Overwrite=<bool>]
```

* `username` _str_ - username to connect as

* `password` _str_  - password for said user

* `AccountName` _str_ - name of storage account

* `Tenant` _str_  - name of tenant to authenticate with

* `Container` _str_ - name of the container to perform the request on

* `Overwrite` _bool_ - if files should be overwritten if they already exists in `Container` (default: `False`)

### `destination`

The MIME type of an uploaded file will automagically be guessed based on the [rendered] destination file extension.
"""
from __future__ import annotations

import logging
from mimetypes import guess_type as mimetype_guess
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, cast
from urllib.parse import parse_qs, quote, urlparse

from azure.storage.blob import BlobServiceClient, ContentSettings

from grizzly.types import GrizzlyResponse, RequestDirection, bool_type
from grizzly_extras.azure.aad import AuthMethod, AzureAadCredential

from . import ClientTask, client

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario

# disable verbose INFO logging
azure_logger = logging.getLogger('azure.core.pipeline.policies.http_logging_policy')
azure_logger.setLevel(logging.ERROR)


@client('bs', 'bss')
class BlobStorageClientTask(ClientTask):
    service_client: BlobServiceClient
    overwrite: bool
    container: str

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

        username = parsed.username
        password = parsed.password
        use_credential = username is not None and password is not None

        if parsed.hostname is None or len(parsed.hostname) < 1:
            message = f'{self.__class__.__name__}: could not find storage account name in {self.endpoint}'
            raise AssertionError(message)

        # See urllib/parse.py:771-774, explicit + characters are replaced with white space,
        # AccountKey could contain explicit + characters, so we must quote them first.
        parsed_query: List[str] = []
        if len(parsed.query) > 0:
            for parameter in parsed.query.split('&'):
                key, value = parameter.split('=', 1)
                parsed_query.append(f'{quote(key)}={quote(value)}')

        parameters = parse_qs('&'.join(parsed_query))
        fragments = parse_qs(parsed.fragment)

        assert 'Container' not in parameters, f'{self.__class__.__name__}: container should be the path in the URL, not in the querystring'

        self.container = parsed.path.lstrip('/')

        assert len(self.container) > 0, f'{self.__class__.__name__}: no container name found in URL {self.endpoint}'
        assert self.container.count('/') == 0, f'{self.__class__.__name__}: "{self.container}" is not a valid container name, should be one branch'

        if not use_credential:
            assert 'AccountKey' in parameters, f'{self.__class__.__name__}: could not find AccountKey in {self.endpoint}'
            account_name = parsed.hostname
            account_key = parameters['AccountKey'][0]

            conn_str = f'DefaultEndpointsProtocol={self._endpoints_protocol};AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net'
            self.service_client = BlobServiceClient.from_connection_string(conn_str=conn_str)
        else:
            assert 'Tenant' in fragments, f'{self.__class__.__name__}: could not find Tenant in fragments of {self.endpoint}'

            tenant = fragments.get('Tenant', [''])[0]
            account_url = f'{self._endpoints_protocol}://{parsed.hostname}'

            if not account_url.endswith('.blob.core.windows.net'):
                account_url = f'{account_url}.blob.core.windows.net'

            credential = AzureAadCredential(username, cast(str, password), tenant, AuthMethod.USER, host=account_url)
            self.service_client = BlobServiceClient(account_url=account_url, credential=credential)

        self.overwrite = bool_type(fragments.get('Overwrite', ['False'])[0])

    def get(self, _: GrizzlyScenario) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented GET'
        raise NotImplementedError(message)

    def put(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.render(cast(str, self.source))

        destination = parent.render(self.destination) if self.destination is not None else Path(source).name

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
                meta['response_length'] = len(source.encode())

            meta.update({'response': {}})

        return None, None

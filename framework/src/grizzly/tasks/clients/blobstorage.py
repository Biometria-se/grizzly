"""Task performs Azure Blob Storage put operations to a specified endpoint.

This is useful if the scenario is another user type than `BlobStorageUser`, but the scenario still requires an action towards a blob container.

Only supports `RequestDirection.TO`.

## Step implementations

* [To endpoint file destination][grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_file_destination]

## Arguments

| Name          | Type               | Description                                                                                 | Default    |
| ------------- | ------------------ | ------------------------------------------------------------------------------------------- | ---------- |
| `direction`   | `RequestDirection` | if the request is upstream or downstream                                                    | _required_ |
| `endpoint`    | `str`              | specifies details to be able to perform the request, e.g. account and container information | _required_ |
| `name`        | `str`              | name used in `locust` statistics                                                            | _required_ |
| `destination` | `str`              | name of the file when uploaded, if not specified the basename of `source` will be used      | `None`     |
| `source`      | `str`              | file path of local file that should be saved in `Container`                                 | `None`     |

## Format

### endpoint

Using connection strings:

```plain
bs[s]://<AccountName>/<Container>?AccountKey=<AccountKey>[# Overwrite=<bool>]
```

| Name          | Type   | Description                                                          | Default    |
| ------------- | ------ | -------------------------------------------------------------------- | ---------- |
| `AccountName` | `str`  | name of storage account                                              | _required_ |
| `AccountKey`  | `str`  | secret key to be able to "connect" to the storage account            | _required_ |
| `Container`   | `str`  | name of the container to perform the request on                      | _required_ |
| `Overwrite`   | `bool` | if files should be overwritten if they already exists in `Container` | `False`    |

Using credentials:
```plain
bs[s]://<username>:<password>@<AccountName>/<Container># Tenant=<tenant>[&Overwrite=<bool>]
```

| Name          | Type   | Description                                                          | Default    |
| ------------- | ------ | -------------------------------------------------------------------- | ---------- |
| `username`    | `str`  | username to authenticate with                                        | _required_ |
| `password`    | `str`  | password to authenticate with                                        | _required_ |
| `AccountName` | `str`  | name of storage account                                              | _required_ |
| `Tenant`      | `str`  | name of tenant to authenticate with                                  | _required_ |
| `Container`   | `str`  | name of the container to perform the request on                      | _required_ |
| `Overwrite`   | `bool` | if files should be overwritten if they already exists in `Container` | `False`    |

### destination

The MIME type of an uploaded file will automagically be guessed based on the (rendered) destination file extension.

## Examples

```gherkin
Then put to "upload/incoming.j2.txt" to "bss://$conf::storage.name$/$conf::storage.container$?AccountKey=$conf::storage.key$#Overwrite=True" with name "upload" as "2025/08/13/incoming.txt"
```
"""  # noqa: E501

from __future__ import annotations

import logging
from mimetypes import guess_type as mimetype_guess
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qs, quote, urlparse

from azure.storage.blob import BlobServiceClient, ContentSettings
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential

from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, bool_type

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
        name: str | None = None,
        /,
        payload_variable: str | None = None,
        metadata_variable: str | None = None,
        source: str | None = None,
        destination: str | None = None,
        text: str | None = None,
        method: RequestMethod | None = None,
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
            method=method,
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
        parsed_query: list[str] = []
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

            credential = AzureAadCredential(username, cast('str', password), tenant, AuthMethod.USER, host=account_url)
            self.service_client = BlobServiceClient(account_url=account_url, credential=credential)

        self.overwrite = bool_type(fragments.get('Overwrite', ['False'])[0])

    def request_from(self, _: GrizzlyScenario) -> GrizzlyResponse:  # pragma: no cover
        message = f'{self.__class__.__name__} has not implemented GET'
        raise NotImplementedError(message)

    def request_to(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.user.render(cast('str', self.source))

        destination = parent.user.render(self.destination) if self.destination is not None else Path(source).name

        content_type, _ = mimetype_guess(destination)

        if content_type is None:
            content_type = 'application/octet-stream'

        content_settings = ContentSettings(content_type=content_type)

        with self.action(parent, action=self.container) as meta:
            source_file = Path(self._context_root) / 'requests' / source

            if not source_file.exists():
                raise FileNotFoundError(source)

            source = parent.user.render(source_file.read_text())

            meta.update(
                {
                    'request': {
                        'url': f'{destination}@{self.container}',
                        'payload': source,
                        'metadata': None,
                    },
                },
            )

            with self.service_client.get_blob_client(container=self.container, blob=destination) as blob_client:
                blob_client.upload_blob(source, content_settings=content_settings, overwrite=self.overwrite)
                meta['response_length'] = len(source.encode())

            meta.update({'response': {}})

        return None, None

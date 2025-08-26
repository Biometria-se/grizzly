"""Task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

## Step implementations

* [From endpoint payload][grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload]

* [From endpoint payload and metadata][grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload_and_metadata]

* [To endpoint text][grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_text]

* [To endpoint file][grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_file] (`source` will become contents of the specified file)

## Arguments

| Name        | Type               | Description                                            | Default    |
| ----------- | ------------------ | ------------------------------------------------------ | ---------- |
| `direction` | `RequestDirection` | impicit specified by which step implementation is used | _required_ |
| `endpoint`  | `str`              | URL to perform GET request from                        | _required_ |
| `name`      | `str`              | name used in `locust` statistics                       | _required_ |

## Authentication

To enable authentication for `HttpClientTask` the `auth` context tree has to be correctly set. This is done by using
[Set context variable][grizzly.steps.setup.step_setup_set_context_variable] step, where the branches are prefixed with `<host>/`, e.g.:

```gherkin
And value for variable "foobar" is "none"
And value for variable "url" is "https://www.example.com/api/test"
And set context variable "www.example.com/auth.user.username" to "bob"
And set context variable "www.example.com/auth.user.password" to "password"
And set context variable "www.example.com/auth.user.redirect_uri" to "/authenticated"
And set context variable "www.example.com/auth.provider" to "https://login.example.com/oauth2"
And set context variable "www.example.com/auth.client.id" to "aaaa-bbbb-cccc-dddd"

Then get from "https://{{ url }}" with name "authenticated-get" and save response payload in "foobar"
```

This will make any requests towards `www.example.com` to get a token from `http://login.example.com/oauth2` and use it in any
requests towards `www.example.com`.

For more details, see [AAD][grizzly.auth.aad] documentation.

"""

from __future__ import annotations

import logging
from json import dumps as jsondumps
from pathlib import Path
from time import time
from typing import TYPE_CHECKING, ClassVar, cast

from geventhttpclient import Session
from grizzly_common.transformer import TransformerContentType

from grizzly.auth import AAD, GrizzlyHttpAuthClient, RefreshTokenDistributor, refresh_token
from grizzly.tasks import RequestTaskResponse
from grizzly.testdata.utils import read_file
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, StrDict, bool_type
from grizzly.types.locust import ResponseError
from grizzly.utils import has_template, is_file, merge_dicts
from grizzly.utils.protocols import http_populate_cookiejar, ssl_context_factory

from . import ClientTask, client

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from geventhttpclient.useragent import CompatResponse

    from grizzly.scenarios import GrizzlyScenario
    from grizzly.testdata.communication import GrizzlyDependencies


@client('http', 'https')
class HttpClientTask(ClientTask, GrizzlyHttpAuthClient):
    __dependencies__: ClassVar[GrizzlyDependencies] = {RefreshTokenDistributor}

    arguments: StrDict
    metadata: StrDict
    session_started: float | None
    host: str
    verify: bool
    response: RequestTaskResponse
    ssl_context_factory: Callable | None

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
        self.verify = True

        if source is not None and is_file(source):
            source = read_file(source)

        super().__init__(
            direction,
            endpoint,
            name,
            payload_variable=payload_variable,
            metadata_variable=metadata_variable,
            source=source,
            destination=destination,
            text=text,
            method=method,
        )

        if 'timeout' in self.arguments:
            self.timeout = float(self.arguments['timeout'])
            del self.arguments['timeout']
        else:
            self.timeout = 10.0

        if 'verify' in self.arguments:
            self.verify = bool_type(self.arguments['verify'])
            del self.arguments['verify']

        if 'client_cert' in self.arguments:
            client_cert = self.arguments['client_cert']
            del self.arguments['client_cert']
        else:
            client_cert = None

        if 'client_key' in self.arguments:
            client_key = self.arguments['client_key']
            del self.arguments['client_key']
        else:
            client_key = None

        if client_cert is not None and client_key is not None:
            client_cert = (Path(self._context_root) / client_cert).resolve()
            client_key = (Path(self._context_root) / client_key).resolve()

            if not Path(client_cert).exists() or not Path(client_key).exists():
                message = f'either {client_cert} or {client_key} does not exist'
                raise ValueError(message)

            self.ssl_context_factory = ssl_context_factory(cert=(client_cert, client_key))
        else:
            self.ssl_context_factory = None

        self.cookies = {}
        self.metadata = {
            'x-grizzly-user': f'{self.__class__.__name__}::{id(self)}',
        }

        if self.content_type != TransformerContentType.UNDEFINED:
            self.metadata.update(
                {
                    'Content-Type': self.content_type.value,
                },
            )

        self.session_started = None
        self.__class__._context = {
            'verify_certificates': self.verify,
            'metadata': None,
            'auth': None,
        }

        self.__class__._context.update({'host': self.host})
        self.__class__._context = merge_dicts(self.__class__._context, self._scenario.context)

        if not hasattr(self, 'logger'):
            self.logger = logging.getLogger(f'{self.__class__.__name__}/{id(self)}')

        self.response = RequestTaskResponse()

    def on_start(self, parent: GrizzlyScenario) -> None:
        super().on_start(parent)

        self.environment = self.grizzly.state.locust.environment

        self.session_started = time()
        metadata = self._context.get('metadata', None) or {}
        self.metadata.update(metadata)

    def _handle_response(self, parent: GrizzlyScenario, meta: StrDict, url: str, response: CompatResponse) -> GrizzlyResponse:
        text = response.text
        payload = text.decode() if isinstance(text, bytearray | bytes) else text

        metadata = {key: value for key, value in response.headers.items()}  # noqa: C416

        exception: Exception | None = None

        if response.status_code not in self.response.status_codes or response.url != url:
            parent.logger.error('%s returned %d', response.url, response.status_code)
            message = f'{response.status_code} not in {self.response.status_codes}: {response.url} returned "{payload}"'
            exception = ResponseError(message)
        else:
            if self.payload_variable is not None:
                parent.user.set_variable(self.payload_variable, payload)

            if self.metadata_variable is not None:
                parent.user.set_variable(self.metadata_variable, jsondumps(metadata))

        meta.update(
            {
                'response_length': len(payload.encode()),
                'response': {
                    'url': response.url,
                    'metadata': metadata,
                    'payload': payload,
                    'status': response.status_code,
                },
                'exception': exception,
            },
        )

        return metadata, payload

    @refresh_token(AAD)
    def request_from(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        with self.action(parent) as meta:
            url = parent.user.render(self.endpoint)

            meta.update(
                {
                    'request': {
                        'url': url,
                        'metadata': self.metadata,
                        'payload': None,
                    },
                },
            )

            with Session(
                insecure=not self.verify,
                network_timeout=self.timeout,
                ssl_context_factory=self.ssl_context_factory,
                max_retries=0,
            ) as client:
                http_populate_cookiejar(client, self.cookies, url=url)
                response = client.get(url, headers=self.metadata, **self.arguments)

            return self._handle_response(parent, meta, url, response)

    @refresh_token(AAD)
    def request_to(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.user.render(cast('str', self.source))

        if has_template(source):
            source = parent.user.render(source)

        with self.action(parent) as meta:
            url = parent.user.render(self.endpoint)

            meta.update(
                {
                    'request': {
                        'url': url,
                        'metadata': self.metadata,
                        'payload': source,
                    },
                },
            )

            with Session(insecure=not self.verify, network_timeout=self.timeout, ssl_context_factory=self.ssl_context_factory) as client:
                http_populate_cookiejar(client, self.cookies, url=url)
                response = client.request(self.method.name, url, data=source, headers=self.metadata, **self.arguments)

            return self._handle_response(parent, meta, url, response)

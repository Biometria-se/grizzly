"""@anchor pydoc:grizzly.tasks.clients.http HTTP
This task performs a HTTP request to a specified endpoint.

This is useful if the scenario is using a non-HTTP user or a request to a URL other than the one under testing is needed, e.g. for testdata.

## Step implementations

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload}

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_from_endpoint_payload_metadata}

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_text}

* {@pylink grizzly.steps.scenario.tasks.clients.step_task_client_to_endpoint_file} (`source` will become contents of the specified file)

## Arguments

* `direction` _RequestDirection_ - only `RequestDirection.FROM` is implemented

* `endpoint` _str_ - URL to perform GET request from

* `name` _str_ - name used in `locust` statistics

## Authentication

To enable authentication for `HttpClientTask` the `auth` context tree has to be correctly set. This is done by using
{@pylink grizzly.steps.setup.step_setup_set_context_variable} where the branches are prefixed with `<host>/`, e.g.:

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

For more details, see {@pylink grizzly.auth.aad}.
"""
from __future__ import annotations

import logging
from json import dumps as jsondumps
from time import time
from typing import TYPE_CHECKING, Any, Optional, cast

from geventhttpclient import Session
from locust.exception import CatchResponseError

from grizzly.auth import AAD, GrizzlyHttpAuthClient, refresh_token
from grizzly.tasks import RequestTaskResponse
from grizzly.testdata.utils import read_file
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod, bool_type
from grizzly.utils import has_template, is_file, merge_dicts
from grizzly.utils.protocols import http_populate_cookiejar
from grizzly_extras.arguments import parse_arguments, split_value
from grizzly_extras.text import has_separator
from grizzly_extras.transformer import TransformerContentType

from . import ClientTask, client

if TYPE_CHECKING:  # pragma: no cover
    from geventhttpclient.useragent import CompatResponse

    from grizzly.scenarios import GrizzlyScenario


@client('http', 'https')
class HttpClientTask(ClientTask, GrizzlyHttpAuthClient):
    arguments: dict[str, Any]
    metadata: dict[str, Any]
    session_started: Optional[float]
    host: str
    verify: bool
    response: RequestTaskResponse

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
        method: Optional[RequestMethod] = None,
    ) -> None:
        self.verify = True

        if has_separator('|', endpoint):
            endpoint, endpoint_arguments = split_value(endpoint)
            arguments = parse_arguments(endpoint_arguments, unquote=False)

            if 'verify' in arguments:
                self.verify = bool_type(arguments['verify'])
                del arguments['verify']

            if len(arguments) > 0:
                endpoint = f'{endpoint} | {", ".join([f"{key}={value}" for key, value in arguments.items()])}'

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

        self.arguments = {}
        self.cookies = {}
        self.metadata = {
            'x-grizzly-user': f'{self.__class__.__name__}::{id(self)}',
        }

        if self.content_type != TransformerContentType.UNDEFINED:
            self.metadata.update({
                'Content-Type': self.content_type.value,
            })

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

    def _handle_response(self, parent: GrizzlyScenario, meta: dict[str, Any], url: str, response: CompatResponse) -> GrizzlyResponse:
        text = response.text
        payload = text.decode() if isinstance(text, (bytearray, bytes)) else text

        metadata = dict(response.headers)

        exception: Optional[Exception] = None

        if response.status_code not in self.response.status_codes or response.url != url:
            parent.logger.error('%s returned %d', response.url, response.status_code)
            message = f'{response.status_code} not in {self.response.status_codes}: {payload}'
            exception = CatchResponseError(message)
        else:
            if self.payload_variable is not None:
                parent.user.set_variable(self.payload_variable, payload)

            if self.metadata_variable is not None:
                parent.user.set_variable(self.metadata_variable, jsondumps(metadata))

        meta.update({
            'response_length': len(payload.encode()),
            'response': {
                'url': response.url,
                'metadata': metadata,
                'payload': payload,
                'status': response.status_code,
            },
            'exception': exception,
        })

        return metadata, payload

    @refresh_token(AAD)
    def request_from(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        with self.action(parent) as meta:
            url = parent.user.render(self.endpoint)

            meta.update({'request': {
                'url': url,
                'metadata': self.metadata,
                'payload': None,
            }})


            with Session(insecure=not self.verify) as client:
                http_populate_cookiejar(client, self.cookies, url=url)
                response = client.get(url, headers=self.metadata, **self.arguments)

            return self._handle_response(parent, meta, url, response)

    @refresh_token(AAD)
    def request_to(self, parent: GrizzlyScenario) -> GrizzlyResponse:
        source = parent.user.render(cast(str, self.source))

        if has_template(source):
            source = parent.user.render(source)

        with self.action(parent) as meta:
            url = parent.user.render(self.endpoint)

            meta.update({'request': {
                'url': url,
                'metadata': self.metadata,
                'payload': source,
            }})

            with Session(insecure=not self.verify) as client:
                http_populate_cookiejar(client, self.cookies, url=url)
                response = client.request(self.method.name, url, data=source, headers=self.metadata, **self.arguments)

            return self._handle_response(parent, meta, url, response)

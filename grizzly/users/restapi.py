"""@anchor pydoc:grizzly.users.restapi RestAPI
Communicates with HTTP and HTTPS, with built-in support for Azure authenticated endpoints.

## Request methods

Supports the following request methods:

* get
* put
* post

## Format

Format of `host` is the following:

```plain
http[s]://<hostname>
```

## Examples

Example on how to use it in a scenario:

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
Then post request "test/request.j2.json" to endpoint "/api/test"
Then get request from endpoint "/api/test"
```

To change how often the token should be refreshed, default is 3000 seconds:
```gherkin
And set context variable "auth.refresh_time" to "3500"
```

### Authentication

See {@pylink grizzly.auth.aad}.

### Multipart/form-data

RestApi supports posting of multipart/form-data content-type, and in that case additional arguments needs to be passed with the request:

* `multipart_form_data_name` _str_ - the name of the input form

* `multipart_form_data_filename` _str_ - the filename

Example:
```gherkin
Then post request "path/my_template.j2.xml" with name "FormPost" to endpoint "example.url.com | content_type=multipart/form-data, multipart_form_data_filename=my_filename, multipart_form_data_name=form_name"
```

"""  # noqa: E501
from __future__ import annotations

import json
from abc import ABCMeta
from time import time
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, cast

import requests
from locust.contrib.fasthttp import FastHttpSession
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from locust.exception import ResponseError

from grizzly.auth import AAD, GrizzlyHttpAuthClient, refresh_token
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.utils import safe_del
from grizzly_extras.transformer import TransformerContentType

from . import AsyncRequests, GrizzlyUser, GrizzlyUserMeta, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


class RestApiUserMeta(GrizzlyUserMeta, ABCMeta):
    pass


@grizzlycontext(context={
    'verify_certificates': True,
    'auth': {
        'refresh_time': 3000,
        'provider': None,
        'client': {
            'id': None,
            'secret': None,
            'resource': None,
        },
        'user': {
            'username': None,
            'password': None,
            'otp_secret': None,
            'redirect_uri': None,
            'initialize_uri': None,
        },
    },
    'metadata': None,
})
class RestApiUser(GrizzlyUser, AsyncRequests, GrizzlyHttpAuthClient, metaclass=RestApiUserMeta):  # type: ignore[misc]
    session_started: Optional[float]
    headers: Dict[str, str]
    environment: Environment

    timeout: ClassVar[float] = 60.0

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.headers: Dict[str, str] = {
            'Content-Type': 'application/json',
            'x-grizzly-user': self.__class__.__name__,
        }

        self.session_started = None

        metadata = self._context.get('metadata', None)
        if metadata is not None:
            metadata = cast(Dict[str, str], metadata)
            self.headers.update(metadata)

        self.client = FastHttpSession(
            environment=self.environment,
            base_url=self.host,
            user=self,
            insecure=not self._context.get('verify_certificates', True),
            max_retries=1,
            connection_timeout=self.timeout,
            network_timeout=self.timeout,
        )

        self.parent = None
        self.cookies = {}

    def on_start(self) -> None:
        super().on_start()

        self.session_started = time()

    def _get_error_message(self, response: FastResponseContextManager) -> str:
        if response.text is None:
            return f'unknown response {type(response)}'

        if len(response.text) < 1:
            text = requests.status_codes._codes.get(response.status_code, ('unknown', None))  # type: ignore[attr-defined]
            message = str(text[0]).replace('_', ' ')
        else:
            try:
                payload = json.loads(response.text)

                # special handling for dynamics error messages
                if 'Message' in payload:
                    message = payload['Message'].split('\\n', 1)[0].replace('\n', ' ')
                elif 'error_description' in payload:
                    message = payload['error_description'].split('\r\n')[0]
                else:
                    message = response.text
            except json.decoder.JSONDecodeError:
                message = response.text

        return message

    def async_request_impl(self, request: RequestTask) -> GrizzlyResponse:
        """Use FastHttpSession instance for each asynchronous requests."""
        client = FastHttpSession(
            environment=self.environment,
            base_url=self.host,
            user=self,
            insecure=not self._context.get('verify_certificates', True),
            max_retries=1,
            connection_timeout=self.timeout,
            network_timeout=self.timeout,
        )

        return cast(GrizzlyResponse, self._request(request, client))

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        """Use HttpSession for synchronous requests."""
        return cast(GrizzlyResponse, self._request(request, self.client))

    @refresh_token(AAD)
    def _request(self, request: RequestTask, client: FastHttpSession) -> GrizzlyResponse:
        """Perform a HTTP request using the provided client. Requests are authenticated if needed."""
        if request.method not in [RequestMethod.GET, RequestMethod.PUT, RequestMethod.POST]:
            message = f'{request.method.name} is not implemented for {self.__class__.__name__}'
            raise NotImplementedError(message)

        if request.response.content_type == TransformerContentType.UNDEFINED:
            request.response.content_type = TransformerContentType.JSON
        elif request.response.content_type == TransformerContentType.XML:
            self.headers.update({'Content-Type': 'application/xml'})
        elif request.response.content_type == TransformerContentType.MULTIPART_FORM_DATA:
            safe_del(self.headers, 'Content-Type')

        if request.metadata is not None:
            self.headers.update(request.metadata)

        parameters: Dict[str, Any] = {'headers': self.headers}

        url = f'{self.host}{request.endpoint}'

        if request.method.direction == RequestDirection.TO and request.source is not None:
            if request.response.content_type == TransformerContentType.JSON:
                try:
                    parameters['json'] = json.loads(request.source)
                except json.decoder.JSONDecodeError as e:
                    message = f'{url}: failed to decode'
                    self.logger.exception('%s: %s', url, request.source)

                    # this is a fundemental error, so we'll always stop the user
                    raise SyntaxError(message) from e
            elif request.response.content_type == TransformerContentType.MULTIPART_FORM_DATA and request.arguments:
                parameters['files'] = {request.arguments['multipart_form_data_name']: (request.arguments['multipart_form_data_filename'], request.source)}
            else:
                parameters['data'] = request.source.encode('utf-8')

        headers: Optional[Dict[str, str]] = None
        payload: Optional[str] = None

        with client.request(
            method=request.method.name,
            name=request.name,
            url=url,
            catch_response=True,
            cookies=self.cookies,
            **parameters,
        ) as response:
            # monkey patch, so we don't get two request events
            response._report_request = lambda *_: None

            if response._manual_result is None:
                if response.status_code in request.response.status_codes:
                    response.success()
                else:
                    message = self._get_error_message(response)
                    message = f'{response.status_code} not in {request.response.status_codes}: {message}'
                    self.logger.error('%% %r', response._manual_result)
                    response.failure(ResponseError(message))

            headers = dict(response.headers.items()) if response.headers not in [None, {}] else None
            payload = response.text

        exception = response.request_meta.get('exception', None)

        if exception is not None:
            raise exception

        return (headers, payload)

    def add_context(self, context: Dict[str, Any]) -> None:
        """If context change contains a username we should re-authenticate. This is forced by removing the Authorization header."""
        # something change in auth context, we need to re-authenticate
        if context.get('auth', {}).get('user', {}).get('username', None) is not None:
            safe_del(self.headers, 'Authorization')

        super().add_context(context)

# pylint: disable=line-too-long
"""
@anchor pydoc:grizzly.users.restapi RestAPI
Communicates with HTTP and HTTPS, with built-in support for Azure authenticated endpoints.

## Request methods

Supports the following request methods:

* get
* put
* post

## Format

Format of `host` is the following:

``` plain
http[s]://<hostname>
```

## Examples

Example on how to use it in a scenario:

``` gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
Then post request "test/request.j2.json" to endpoint "/api/test"
Then get request from endpoint "/api/test"
```

To change how often the token should be refreshed, default is 3000 seconds:
``` gherkin
And set context variable "auth.refresh_time" to "3500"
```

### Authentication

See {@pylink grizzly.auth.aad}.

### Multipart/form-data

RestApi supports posting of multipart/form-data content-type, and in that case additional arguments needs to be passed with the request:

* `multipart_form_data_name` _str_ - the name of the input form

* `multipart_form_data_filename` _str_ - the filename

Example:

``` gherkin
Then post request "path/my_template.j2.xml" with name "FormPost" to endpoint "example.url.com | content_type=multipart/form-data, multipart_form_data_filename=my_filename, multipart_form_data_name=form_name"
```
"""  # noqa: E501
import json

from typing import Dict, Optional, Any, Tuple, Union, cast
from time import time
from abc import ABCMeta

from locust.contrib.fasthttp import FastHttpSession

from grizzly_extras.transformer import TransformerContentType

from grizzly.types import GrizzlyResponse, RequestMethod, RequestDirection, GrizzlyResponseContextManager
from grizzly.types.locust import Environment, StopUser
from grizzly.utils import merge_dicts, safe_del
from grizzly.tasks import RequestTask
from grizzly.clients import ResponseEventSession
from grizzly.auth import GrizzlyHttpAuthClient, AAD, refresh_token
from locust.user.users import UserMeta

from .base import ResponseHandler, GrizzlyUser, HttpRequests, AsyncRequests

from urllib3 import disable_warnings as urllib3_disable_warnings
urllib3_disable_warnings()


class RestApiUserMeta(UserMeta, ABCMeta):
    pass


class RestApiUser(ResponseHandler, GrizzlyUser, HttpRequests, AsyncRequests, GrizzlyHttpAuthClient, metaclass=RestApiUserMeta):  # type: ignore[misc]
    session_started: Optional[float]
    headers: Dict[str, str]
    environment: Environment

    _context = {
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
    }

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self.headers: Dict[str, str] = {
            'Content-Type': 'application/json',
            'x-grizzly-user': self.__class__.__name__,
        }

        self.session_started = None
        self._context = merge_dicts(
            super().context(),
            # this is needed since we create a new class with this class as sub class, context will be messed up otherwise
            # in other words, don't use RestApiUser._context. This should only be used in classes which are direct created
            # in grizzly
            self.__class__._context,
        )

        metadata = self._context.get('metadata', None)
        if metadata is not None:
            metadata = cast(Dict[str, str], metadata)
            self.headers.update(metadata)

        self.parent = None
        self.cookies = {}

    def on_start(self) -> None:
        super().on_start()

        self.session_started = time()

    def get_error_message(self, response: GrizzlyResponseContextManager) -> str:
        if response.text is None:
            return f'unknown response {type(response)}'

        if len(response.text) < 1:
            if response.status_code == 400:
                message = 'bad request'
            elif response.status_code == 401:
                message = 'unauthorized'
            elif response.status_code == 403:
                message = 'forbidden'
            elif response.status_code == 404:
                message = 'not found'
            else:
                message = 'unknown'
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
        client = FastHttpSession(
            environment=self.environment,
            base_url=self.host,
            user=self,
            insecure=not self._context.get('verify_certificates', True),
            max_retries=1,
            connection_timeout=60.0,
            network_timeout=60.0,
        )

        return cast(GrizzlyResponse, self._request(request, client))

    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        return cast(GrizzlyResponse, self._request(request, self.client))

    @refresh_token(AAD)
    def _request(self, request: RequestTask, client: Union[FastHttpSession, ResponseEventSession]) -> GrizzlyResponse:
        if request.method not in [RequestMethod.GET, RequestMethod.PUT, RequestMethod.POST]:
            raise NotImplementedError(f'{request.method.name} is not implemented for {self.__class__.__name__}')

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

        if isinstance(client, ResponseEventSession):
            parameters.update({
                'request': request,
                'verify': self._context.get('verify_certificates', True),
            })

        if request.method.direction == RequestDirection.TO and request.source is not None:
            if request.response.content_type == TransformerContentType.JSON:
                try:
                    parameters['json'] = json.loads(request.source)
                except json.decoder.JSONDecodeError:
                    self.logger.error(f'{url}: failed to decode: {request.source=}')

                    # this is a fundemental error, so we'll always stop the user
                    raise StopUser()
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
            response._report_request = lambda *args: None

            if not isinstance(client, ResponseEventSession):
                # monkey patch in request body... not available otherwise
                setattr(response, 'request_body', request.source)

            if response._manual_result is None:
                if response.status_code in request.response.status_codes:
                    response.success()
                else:
                    message = self.get_error_message(response)
                    response.failure(f'{response.status_code} not in {request.response.status_codes}: {message}')

            if response._manual_result is not True and self._scenario.failure_exception is not None:
                raise self._scenario.failure_exception()

            headers = dict(response.headers.items()) if response.headers not in [None, {}] else None
            payload = response.text

        exception = response.request_meta.get('exception', None)

        if exception is not None:
            raise exception

        return (headers, payload,)

    def add_context(self, context: Dict[str, Any]) -> None:
        # something change in auth context, we need to re-authenticate
        if context.get('auth', {}).get('user', {}).get('username', None) is not None:
            safe_del(self.headers, 'Authorization')

        super().add_context(context)

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

It is possible to change authenticated user during runtime by using {@pylink grizzly.steps.scenario.setup.step_setup_set_context_variable} step expressions inbetween other tasks.
To change user both `auth.user.username` and `auth.user.password` has to be changed (even though maybe only one of them changes value).

This will then cache the `Authorization` token for the current user, and if changed back to that user there is no need to re-authenticate again, unless `refresh_time` for the first login
expires.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And repeat for "2" iterations
And set context variable "user.auth.username" to "bob"
And set context variable "user.auth.password" to "foobar"

Then get request from endpoint "/api/test"

Given set context variable "user.auth.username" to "alice"
And set context variable "user.auth.password" to "hello world"

Then get request from endpoint "/api/test"

Given set context variable "user.auth.username" to "bob"
And set context variable "user.auth.password" to "foobar"
```

In the above, hypotetical scenario, there will 2 "AAD OAuth2 user token" requests, once for user "bob" and one for user "alice", both done in the first iteration. The second iteration
the cached authentication tokens will be re-used.

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
from copy import copy
from datetime import datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
from http.cookiejar import Cookie
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple, cast
from urllib.parse import urlparse

import requests
from locust.contrib.fasthttp import FastHttpSession
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager
from locust.exception import ResponseError

from grizzly.auth import AAD, GrizzlyHttpAuthClient, refresh_token
from grizzly.types import GrizzlyResponse, RequestDirection, RequestMethod
from grizzly.utils import merge_dicts, safe_del
from grizzly_extras.transformer import TransformerContentType

from . import AsyncRequests, GrizzlyUser, GrizzlyUserMeta, grizzlycontext

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.tasks import RequestTask
    from grizzly.types.locust import Environment


class RestApiUserMeta(GrizzlyUserMeta, ABCMeta):
    pass

class HtmlTitleParser(HTMLParser):
    title: Optional[str]

    _look: bool

    def __init__(self) -> None:
        super().__init__()

        self.title = None
        self._look = False

    @property
    def should_look(self) -> bool:
        return self._look and self.title is None

    def handle_starttag(self, tag: str, _attrs: List[Tuple[str, Optional[str]]]) -> None:
        self._look = tag == 'title' and self.title is None

    def handle_data(self, data: str) -> None:
        if self.should_look:
            self.title = data

    def handle_endtag(self, tag: str) -> None:
        if tag == 'title' and self.should_look:
            self._look = False


@grizzlycontext(context={
    'verify_certificates': True,
    'auth': {
        'refresh_time': 3000,
        'provider': None,
        'tenant': None,
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
    '__cached_auth__': {},
    '__context_change_history__': set(),
})
class RestApiUser(GrizzlyUser, AsyncRequests, GrizzlyHttpAuthClient, metaclass=RestApiUserMeta):  # type: ignore[misc]
    environment: Environment
    domain: str

    timeout: ClassVar[float] = 60.0

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        self.metadata = merge_dicts({
            'Content-Type': 'application/json',
            'x-grizzly-user': self.__class__.__name__,
        }, self.metadata)

        self.client = FastHttpSession(
            environment=self.environment,
            base_url=self.host,
            user=self,
            insecure=not self._context.get('verify_certificates', True),
            max_retries=1,
            connection_timeout=self.timeout,
            network_timeout=self.timeout,
        )

        host_parsed = urlparse(self.host)
        self.domain = host_parsed.netloc

        self.parent = None
        self.cookies = {}

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

                parser = HtmlTitleParser()
                parser.feed(message)

                if parser.title is not None:
                    message = parser.title.strip()

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
        request_headers = copy(request.metadata or {})

        if request.method not in [RequestMethod.GET, RequestMethod.PUT, RequestMethod.POST]:
            message = f'{request.method.name} is not implemented for {self.__class__.__name__}'
            raise NotImplementedError(message)

        if request.response.content_type == TransformerContentType.UNDEFINED:
            request.response.content_type = TransformerContentType.JSON

        request_headers.update({'Content-Type': request.response.content_type.value})

        parameters: Dict[str, Any] = {}

        if len(request_headers) > 0:
            parameters.update({'headers': request_headers})

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

        # from response...
        headers: Optional[Dict[str, str]] = None
        payload: Optional[str] = None

        client.cookiejar.clear()

        for name, value in self.cookies.items():
            client.cookiejar.set_cookie(Cookie(
                version=0,
                name=name,
                value=value,
                port=None,
                port_specified=False,
                domain=self.domain,
                domain_specified=True,
                domain_initial_dot=False,
                path='/',
                path_specified=True,
                secure=self.host.startswith('https'),
                expires=None,
                discard=False,
                comment=None,
                comment_url=None,
                rest={},
            ))

        with client.request(
            method=request.method.name,
            name=request.name,
            url=url,
            catch_response=True,
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
                    if response.status_code == 401 and self.credential is not None and self.credential._access_token is not None:
                        token_expires = datetime.fromtimestamp(self.credential._access_token.expires_on, tz=timezone.utc).astimezone(tz=None)
                        message = f'{message} (token expires {token_expires})'

                    response.failure(ResponseError(message))

            headers = dict(response.headers.items()) if response.headers not in [None, {}] else None
            payload = response.text

        exception = response.request_meta.get('exception', None)

        if exception is not None:
            raise exception

        return (headers, payload)

    def add_context(self, context: Dict[str, Any]) -> None:
        """If added context contains changes in `auth`, we should cache current `Authorization` token and force re-auth for a new, if the auth
        doesn't exist in the cache.

        To force a re-authentication, both auth.user.username and auth.user.password needs be set, even though the actual value is only changed
        for one of them.
        """
        current_username = self._context.get('auth', {}).get('user', {}).get('username', None)
        current_password = self._context.get('auth', {}).get('user', {}).get('password', None)
        current_credential = self.credential

        changed_username = context.get('auth', {}).get('user', {}).get('username', None)
        changed_password = context.get('auth', {}).get('user', {}).get('password', None)

        # check if we're currently have Authorization header connected to a username and password,
        # and if we're changing either username or password.
        # if so, we need to cache current username+password token
        if (
            current_username is not None
            and current_password is not None
            and self.__context_change_history__ == set()
            and current_credential is not None
            and (changed_username is not None or changed_password is not None)
        ):
            cache_key_plain = f'{current_username}:{current_password}'
            cache_key = sha256(cache_key_plain.encode()).hexdigest()

            if cache_key not in self.__cached_auth__:
                self.__cached_auth__.update({cache_key: current_credential})

        super().add_context(context)

        changed_username_path = 'auth.user.username'
        changed_password_path = 'auth.user.password'  # noqa: S105

        # update change history if needed
        context_change_history = self.__context_change_history__
        if context_change_history != {changed_username_path, changed_password_path}:
            if changed_username is not None and changed_username_path not in context_change_history:
                self.__context_change_history__.add(changed_username_path)

            if changed_password is not None and changed_password_path not in context_change_history:
                self.__context_change_history__.add(changed_password_path)

        # everything needed to force re-auth is not in place
        if self.__context_change_history__ != {changed_username_path, changed_password_path}:
            return

        # every context change needed to force re-auth is in place, clear change history
        self.__context_change_history__.clear()

        username = self._context.get('auth', {}).get('user', {}).get('username', None)
        password = self._context.get('auth', {}).get('user', {}).get('password', None)

        if username is None or password is None:
            return

        # check if current username+password has a cached Authorization token
        cache_key_plain = f'{username}:{password}'
        cache_key = sha256(cache_key_plain.encode()).hexdigest()

        cached_credential = self.__cached_auth__.get(cache_key, None)

        self.credential = cached_credential

        # force re-auth
        if cached_credential is None:
            safe_del(self.metadata, 'Authorization')
            safe_del(self.cookies, '.AspNetCore.Cookies')

'''Communicates with HTTP and HTTPS, with built-in support for Azure authenticated endpoints.

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

#### Client secret

``` gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.client.tenant" "<tenant name/guid>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.client.secret" to "<client secret>"
And set context variable "auth.client.resource" to "<resource url/guid>"
```

#### Username and password

`auth.user.redirect_uri` needs to correspond to the endpoint that the client secret is registrered for.

``` gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
```
'''
import json
import re

from typing import Dict, Optional, Any, Tuple, List, Union, cast
from time import time, perf_counter as time_perf_counter
from functools import wraps
from enum import Enum
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from locust.contrib.fasthttp import FastHttpSession
from locust.exception import StopUser
from locust.env import Environment

import requests

from ..types import GrizzlyResponse, RequestType, WrappedFunc, GrizzlyResponseContextManager
from ..utils import merge_dicts
from ..types import RequestMethod
from ..tasks import RequestTask
from ..clients import ResponseEventSession
from .base import RequestLogger, ResponseHandler, GrizzlyUser, HttpRequests, AsyncRequests
from . import logger

from urllib3 import disable_warnings as urllib3_disable_warnings
urllib3_disable_warnings()


class AuthMethod(Enum):
    NONE = 1
    CLIENT = 2
    USER = 3


class refresh_token:
    def __call__(self, func: WrappedFunc) -> WrappedFunc:
        @wraps(func)
        def refresh_token(cls: 'RestApiUser', *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            auth_context = cls._context['auth']

            use_auth_client = (
                auth_context.get('client', {}).get('id', None) is not None
                and auth_context.get('client', {}).get('secret', None) is not None
            )
            use_auth_user = (
                auth_context.get('client', {}).get('id', None) is not None
                and auth_context.get('user', {}).get('username', None) is not None
                and auth_context.get('user', {}).get('password', None) is not None
                and auth_context.get('user', {}).get('redirect_uri', None) is not None
            )

            if use_auth_client:
                auth_method = AuthMethod.CLIENT
            elif use_auth_user:
                auth_method = AuthMethod.USER
            else:
                auth_method = AuthMethod.NONE

            if auth_method is not AuthMethod.NONE and cls.session_started is not None:
                session_now = time()
                session_duration = session_now - cls.session_started

                # refresh token if session has been alive for at least refresh_time
                if session_duration >= auth_context.get('refresh_time', 3000) or cls.headers.get('Authorization', None) is None:
                    cls.get_token(auth_method)
            else:
                try:
                    del cls.headers['Authorization']
                except KeyError:
                    pass

            return func(cls, *args, **kwargs)

        return cast(WrappedFunc, refresh_token)


class RestApiUser(ResponseHandler, RequestLogger, GrizzlyUser, HttpRequests, AsyncRequests):
    session_started: Optional[float]
    headers: Dict[str, Optional[str]]
    host: str
    _context: Dict[str, Any] = {
        'verify_certificates': True,
        'auth': {
            'refresh_time': 3000,
            'url': None,
            'client': {
                'id': None,
                'secret': None,
                'resource': None,
                'tenant': None,
            },
            'user': {
                'username': None,
                'password': None,
                'redirect_uri': None,
            },
        },
        'metadata': None,
    }

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        self.headers = {
            'Authorization': None,
            'Content-Type': 'application/json',
            'x-grizzly-user': f'{self.__class__.__name__}',
        }

        self.session_started = None
        self._context = merge_dicts(
            super().context(),
            # this is needed since we create a new class with this class as sub class, context will be messed up otherwise
            # in other words, don't use RestApiUser._context. This should only be used in classes which are direct created
            # in grizzly
            self.__class__._context,
        )

        headers = self._context.get('metadata', None)
        if headers is not None:
            self.headers.update(headers)

    def on_start(self) -> None:
        self.session_started = time()

    def get_token(self, auth_method: AuthMethod) -> None:
        if auth_method == AuthMethod.CLIENT:
            self.get_client_token()
        elif auth_method == AuthMethod.USER:
            self.get_user_token()
        else:
            pass

    def get_user_token(self) -> None:
        def _parse_response_config(response: requests.Response) -> Dict[str, Any]:
            match = re.search(r'Config={(.*?)};', response.text, re.MULTILINE)

            if not match:
                raise ValueError(f'no config found in response from {response.url}')

            return cast(Dict[str, Any], json.loads(f'{{{match.group(1)}}}'))

        def update_state(state: Dict[str, str], response: requests.Response) -> Dict[str, Any]:
            config = _parse_response_config(response)

            for key in state.keys():
                if key in config:
                    state[key] = str(config[key])
                elif key in response.headers:
                    state[key] = str(response.headers[key])
                else:
                    raise ValueError(f'unexpected response body from {response.url}: missing "{key}" in config')

            return config

        def generate_uuid() -> str:
            uuid = uuid4().hex

            return '{}-{}-{}-{}-{}'.format(
                uuid[0:8],
                uuid[8:12],
                uuid[12:16],
                uuid[16:20],
                uuid[20:]
            )

        headers_ua: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0'
        }

        auth_user_context = self._context['auth']['user']
        start_time = time_perf_counter()
        total_response_length = 0
        exception: Optional[Exception] = None

        try:
            if self._context['auth']['url'] is None:
                try:
                    [_, tenant] = auth_user_context['username'].rsplit('@', 1)
                    if tenant is None or len(tenant) < 1:
                        raise RuntimeError()
                except Exception:
                    raise ValueError(f'auth.url was not set and could not find tenant part in {auth_user_context["username"]}')
                self._context['auth']['url'] = f'https://login.microsoftonline.com/{tenant}/oauth2/authorize'

            auth_url_parsed = urlparse(self._context['auth']['url'])

            total_response_length = 0

            with requests.Session() as client:
                headers: Dict[str, str]
                payload: Dict[str, Any]
                data: Dict[str, Any]
                state: Dict[str, str] = {
                    'hpgact': '',
                    'hpgid': '',
                    'sFT': '',
                    'sCtx': '',
                    'apiCanary': '',
                    'canary': '',
                    'correlationId': '',
                    'sessionId': '',
                    'x-ms-request-id': '',
                    'country': '',
                }

                # <!-- request 1
                client_id = self._context['auth']['client']['id']
                client_request_id = generate_uuid()
                username_lowercase = auth_user_context['username'].lower()

                redirect_uri_parsed = urlparse(auth_user_context['redirect_uri'])

                if len(redirect_uri_parsed.netloc) == 0:
                    redirect_uri = f"{self._context['host']}{auth_user_context['redirect_uri']}"
                else:
                    redirect_uri = auth_user_context['redirect_uri']

                params: Dict[str, List[str]] = {
                    'response_type': ['id_token'],
                    'client_id': [client_id],
                    'redirect_uri': [redirect_uri],
                    'state': [generate_uuid()],
                    'client-request-id': [client_request_id],
                    'x-client-SKU': ['Js'],
                    'x-client-Ver': ['1.0.18'],
                    'nonce': [generate_uuid()],
                }

                headers = {
                    'Host': str(auth_url_parsed.netloc),
                    **headers_ua,
                }

                response = client.get(cast(str, self._context['auth']['url']), headers=headers, params=params)
                logger.debug(f'user auth request 1: {response.url} ({response.status_code})')
                total_response_length += int(response.headers.get('content-length', '0'))

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 1: {response.url} had unexpected status code {response.status_code}')

                referer = response.url

                config = update_state(state, response)
                # // request 1 -->

                # <!-- request 2
                url_parsed = urlparse(config['urlGetCredentialType'])
                params = parse_qs(url_parsed.query)

                url = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}'
                params['mkt'] = ['sv-SE']

                headers = {
                    'Accept': 'application/json',
                    'Host': str(auth_url_parsed.netloc),
                    'ContentType': 'application/json; charset=UTF-8',
                    'canary': state['apiCanary'],
                    'client-request-id': client_request_id,
                    'hpgact': state['hpgact'],
                    'hpgid': state['hpgid'],
                    'hpgrequestid': state['sessionId'],
                    **headers_ua,
                }

                payload = {
                    'username': username_lowercase,
                    'isOtherIdpSupported': True,
                    'checkPhones': False,
                    'isRemoteNGCSupported': True,
                    'isCookieBannerShown': False,
                    'isFidoSupported': True,
                    'originalRequest': state['sCtx'],
                    'country': state['country'],
                    'forceotclogin': False,
                    'isExternalFederationDisallowed': False,
                    'isRemoteConnectSupported': False,
                    'federationFlags': 0,
                    'isSignup': False,
                    'flowToken': state['sFT'],
                    'isAccessPassSupported': True,
                }

                response = client.post(url, headers=headers, params=params, json=payload)
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 2: {response.url} ({response.status_code})')

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 2: {response.url} had unexpected status code {response.status_code}')

                data = cast(Dict[str, Any], json.loads(response.text))
                if 'error' in data:
                    error = data['error']
                    raise RuntimeError(f'error response from {url}: code={error["code"]}, message={error["message"]}')

                state['apiCanary'] = data['apiCanary']
                assert state['sFT'] == data['FlowToken'], 'flow token between user auth request 1 and 2 differed'
                # // request 2 -->

                # <!-- request 3
                assert config['urlPost'].startswith('https://'), f"response from {response.url} contained unexpected value '{config['urlPost']}'"
                url = config['urlPost']

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Host': str(auth_url_parsed.netloc),
                    'Referer': referer,
                    **headers_ua,
                }

                payload = {
                    'i13': '0',
                    'login': username_lowercase,
                    'loginfmt': username_lowercase,
                    'type': '11',
                    'LoginOptions': '3',
                    'lrt': '',
                    'lrtPartition': '',
                    'hisRegion': '',
                    'hisScaleUnit': '',
                    'passwd': auth_user_context['password'],
                    'ps': '2',  # postedLoginStateViewId
                    'psRNGCDefaultType': '',
                    'psRNGCEntropy': '',
                    'psRNGCSLK': '',
                    'canary': state['canary'],
                    'ctx': state['sCtx'],
                    'hpgrequestid': state['sessionId'],
                    'flowToken': state['sFT'],
                    'PPSX': '',
                    'NewUser': '1',
                    'FoundMSAs': '',
                    'fspost': '0',
                    'i21': '0',  # wasLearnMoreShown
                    'CookieDisclosure': '0',
                    'IsFidoSupported': '1',
                    'isSignupPost': '0',
                    'i19': '16369',  # time on page
                }

                response = client.post(url, headers=headers, data=payload)
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 3: {response.url} ({response.status_code})')

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 3: {response.url} had unexpected status code {response.status_code}')

                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    logger.error(exception_message)
                    raise RuntimeError(exception_message)

                user_proofs = config.get('arrUserProofs', [])

                if len(user_proofs) > 0:
                    user_proof = user_proofs[0]
                    error_message = f'{username_lowercase} requires MFA for login: {user_proof["authMethodId"]} = {user_proof["display"]}'
                    logger.error(error_message)
                    raise RuntimeError(error_message)

                # update state
                state['sessionId'] = config['sessionId']
                state['sFT'] = config['sFT']
                # // request 3 -->

                #  <!-- request 4
                assert not config['urlPost'].startswith('https://'), f"unexpected response from {response.url}, incorrect username and/or password?"
                url = f'{str(auth_url_parsed.scheme)}://{str(auth_url_parsed.netloc)}{config["urlPost"]}'

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Host': str(auth_url_parsed.netloc),
                    'Referer': referer,
                    **headers_ua,
                }

                payload = {
                    'LoginOptions': '3',
                    'type': '28',
                    'ctx': state['sCtx'],
                    'hprequestid': state['sessionId'],
                    'flowToken': state['sFT'],
                    'canary': state['canary'],
                    'i2': '',
                    'i17': '',
                    'i18': '',
                    'i19': '1337',
                }

                response = client.post(url, headers=headers, data=payload, allow_redirects=False)
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 4: {response.url} ({response.status_code})')

                if response.status_code != 302:
                    raise RuntimeError(f'user auth request 4: {response.url} had unexpected status code {response.status_code}')

                assert 'Location' in response.headers, f'Location header was not found in response from {response.url}'

                token_url = response.headers['Location']
                assert token_url.startswith(f'{redirect_uri}'), f'unexpected redirect URI, got {token_url} but expected {redirect_uri}'
                # // request 4 -->

                token_url_parsed = urlparse(token_url)
                fragments = parse_qs(token_url_parsed.fragment)
                assert 'id_token' in fragments, f'could not find id_token in {token_url}'
                id_token = fragments['id_token'][0]

                self.headers['Authorization'] = f'Bearer {id_token}'
                self.session_started = time()
        except Exception as e:
            exception = e
        finally:
            name = self.__class__.__name__.rsplit('_', 1)[-1]

            request_meta = {
                'request_type': 'GET',
                'response_time': int((time_perf_counter() - start_time) * 1000),
                'name': f'{name} OAuth2 user token',
                'context': self._context,
                'response': None,
                'exception': exception,
                'response_length': total_response_length,
            }

            self.environment.events.request.fire(**request_meta)

            if exception is not None:
                raise StopUser()

    def get_client_token(self) -> None:
        name = self.__class__.__name__.rsplit('_', 1)[-1]

        auth_client_context = self._context['auth']['client']
        resource = auth_client_context['resource'] if 'resource' in auth_client_context and auth_client_context['resource'] is not None else self.host

        if 'url' not in self._context['auth'] or self._context['auth']['url'] is None:
            if 'tenant' not in auth_client_context or auth_client_context['tenant'] is None:
                raise ValueError('auth.client.tenant and auth.url is not set, one of them is needed!')
            tenant = auth_client_context['tenant']
            self._context['auth']['url'] = f'https://login.microsoftonline.com/{tenant}/oauth2/token'

        parameters: Dict[str, Any] = {
            'data': {
                'grant_type': 'client_credentials',
                'client_id': auth_client_context['id'],
                'client_secret': auth_client_context['secret'],
                'resource': resource,
            },
            'verify': self._context.get('verify_certificates', True),
        }

        with self.client.request(
            'POST',
            self._context['auth']['url'],
            name=f'{name} OAuth2 client token',
            request=None,
            catch_response=True,
            **parameters,
        ) as response:
            if response.status_code == 200:
                payload = json.loads(response.text)
                access_token = str(payload['access_token'])

                self.headers['Authorization'] = f'Bearer {access_token}'
                self.session_started = time()

                response.success()
            else:
                message = self.get_error_message(response)

                response.failure(f'{response.status_code}: {message}')

                raise StopUser()

    def get_error_message(self, response: GrizzlyResponseContextManager) -> str:
        if response.text is None:
            return f'unknown response {type(response)}'

        if len(response.text) < 1:
            if response.status_code == 401:
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

    def async_request(self, request: RequestTask) -> GrizzlyResponse:
        client = FastHttpSession(
            environment=self.environment,
            base_url=self.host,
            user=self,
            insecure=not self._context.get('verify_certificates', True),
            max_retries=1,
            connection_timeout=60.0,
            network_timeout=60.0,
        )

        return self._request(client, request)

    def request(self, request: RequestTask) -> GrizzlyResponse:
        return self._request(self.client, request)

    @refresh_token()
    def _request(self, client: Union[FastHttpSession, ResponseEventSession], request: RequestTask) -> GrizzlyResponse:
        if request.method not in [RequestMethod.GET, RequestMethod.PUT, RequestMethod.POST]:
            raise NotImplementedError(f'{request.method.name} is not implemented for {self.__class__.__name__}')

        request_name, endpoint, payload = self.render(request)

        parameters: Dict[str, Any] = {'headers': self.headers}

        url = f'{self.host}{endpoint}'

        # only render endpoint once, so needs to be done here
        if isinstance(client, ResponseEventSession):
            parameters.update({
                'request': request,
                'verify': self._context.get('verify_certificates', True),
            })

        name = f'{request.scenario.identifier} {request_name}'

        if payload is not None:
            try:
                parameters['json'] = json.loads(payload)
            except json.decoder.JSONDecodeError as exception:
                # so that locust treats it as a failure
                self.environment.events.request.fire(
                    request_type=RequestType.from_method(request.method),
                    name=name,
                    response_time=0,
                    response_length=0,
                    context=self._context,
                    exception=exception,
                )
                logger.error(f'{url}: failed to decode: {payload=}')

                # this is a fundemental error, so we'll always stop the user
                raise StopUser()

        with client.request(
            method=request.method.name,
            name=name,
            url=url,
            catch_response=True,
            **parameters,
        ) as response:
            if not isinstance(client, ResponseEventSession):
                # monkey patch in request body... not available otherwise
                setattr(response, 'request_body', payload)

                self.response_event.fire(
                    name=name,
                    request=request,
                    context=response,
                    user=self,
                )

            if response._manual_result is None:
                if response.status_code in request.response.status_codes:
                    response.success()
                else:
                    message = self.get_error_message(response)
                    response.failure(f'{response.status_code} not in {request.response.status_codes}: {message}')

            if response._manual_result is not True and request.scenario.failure_exception is not None:
                raise request.scenario.failure_exception()

            headers = dict(response.headers.items()) if response.headers not in [None, {}] else None

            return headers, response.text

    def add_context(self, context: Dict[str, Any]) -> None:
        if context.get('auth', {}).get('user', {}).get('username', None) is not None:
            self.headers['Authorization'] = None

        super().add_context(context)

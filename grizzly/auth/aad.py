"""@anchor pydoc:grizzly.auth.aad Azure Active Directory
Grizzly provides a way to get tokens via Azure Active Directory (AAD), in the framework this is implemented by {@pylink grizzly.users.restapi}
load user and {@pylink grizzly.tasks.clients.http} client task, via the `@refresh_token` decorator.

It is possible to use it in custom code as well, by implementing a custom class that inherits `grizzly.auth.GrizzlyHttpAuthClient`.

For information about how to set context variables:

* {@pylink grizzly.steps.background.setup.step_setup_set_global_context_variable}

* {@pylink grizzly.steps.scenario.setup.step_setup_set_context_variable}

Context variable values supports {@link framework.usage.variables.templating}.

There are two ways to get an token, see below.

## Client secret

Using client secret for an app registration.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.tenant" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.client.secret" to "<client secret>"
And set context variable "auth.client.resource" to "<resource url/guid>"
```

## Username and password

Using a username and password, with optional MFA authentication.

`auth.user.redirect_uri` needs to correspond to the endpoint that the client secret is registrered for.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.provider" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
```

### MFA / TOTP

If the user is required to have a MFA method, support for software based TOTP tokens are supported. The user **must** first have this method configured.

#### Configure TOTP

1. Login to the accounts [My signins](https://mysignins.microsoft.com/security-info)

2. Click on `Security info`

3. Click on `Add sign-in method`

4. Choose `Authenticator app`

5. Click on `I want to use another authenticator app`

6. Click on `Next`

7. Click on `Can't scan image?`

8. Copy `Secret key` and save it some where safe

9. Click on `Next`

10. Open a terminal and run the following command:

    === "Bash"

        ```bash
        OTP_SECRET="<secret key from step 8>" grizzly-cli auth
        ```
    === "PowerShell"

        ```powershell
        $Env:OTP_SECRET = "<secret key from step 8>"
        grizzly-cli auth
        ```

11. Copy the code generate from above command, go back to the browser and paste it into the text field and click `Next`

12. Finish the wizard

The user now have software based TOTP tokens as MFA method, where `grizzly` will act as the authenticator app.

#### Example

In addition to the "Username and password" example, the context variable `auth.user.otp_secret` must also be set.

```gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.tenant" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
And set context variable "auth.user.otp_secret" to "asdfasdf"  # <-- `Secret key` from Step 8 in "Configure TOTP"
```
"""
from __future__ import annotations

import json
import logging
import re
from base64 import urlsafe_b64encode
from datetime import datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
from http.server import HTTPServer, SimpleHTTPRequestHandler
from secrets import token_urlsafe
from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Tuple, Type, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

import requests
from azure.core.credentials import AccessToken
from gevent import Greenlet
from pyotp import TOTP
from requests.adapters import HTTPAdapter, Retry
from typing_extensions import Self

from grizzly.utils import safe_del

from . import AuthMethod, AuthType, GrizzlyTokenCredential, RefreshToken

if TYPE_CHECKING:
    from types import TracebackType

logger = logging.getLogger(__name__)


class AzureAadError(Exception):
    pass


class AzureAadFlowError(AzureAadError):
    pass


class FormPostParser(HTMLParser):
    action: Optional[str]
    id_token: Optional[str]
    client_info: Optional[str]
    state: Optional[str]
    session_state: Optional[str]

    def __init__(self) -> None:
        super().__init__()

        self.action = None
        self.id_token = None
        self.client_info = None
        self.state = None
        self.session_state = None

    @property
    def payload(self) -> Dict[str, str]:
        assert self.id_token is not None, 'could not find id_token in response'
        assert self.client_info is not None, 'could not find client_info in response'
        assert self.state is not None, 'could not find state in response'
        assert self.session_state is not None, 'could not find session_state in response'

        return {
            'id_token': self.id_token,
            'client_info': self.client_info,
            'state': self.state,
            'session_state': self.session_state,
        }

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag == 'form':
            for attr, value in attrs:
                if attr == 'action':
                    self.action = value
        elif tag == 'input':
            prop_name: Optional[str] = None
            prop_value: Optional[str] = None

            for attr, value in attrs:
                if attr == 'name':
                    prop_name = value
                elif attr == 'value':
                    prop_value = value

            if prop_name is not None and prop_value is not None:
                setattr(self, prop_name, prop_value)

class AzureAadWebserver:
    enable: bool
    credential: AzureAadCredential

    _http_server: HTTPServer
    _greenlet: Greenlet
    _redirect: Optional[str]

    def __init__(self, credential: AzureAadCredential) -> None:
        self.credential = credential
        self.enable = (self.credential.redirect is None and self.credential.initialize is None)

    def _start(self) -> None:
        if not self.enable:
            return

        # start http server and do stuff here
        self._http_server = HTTPServer(
            ('127.0.0.1', 0), SimpleHTTPRequestHandler, bind_and_activate=False,
        )
        self._http_server.timeout = 0.5
        self._http_server.allow_reuse_address = True
        self._http_server.server_bind()
        self._http_server.server_activate()

        def serve_forever(httpd: HTTPServer) -> None:
            with httpd:
                try:
                    httpd.serve_forever()
                except OSError as e:
                    # will be thrown when closing the socket in disconnect, on windows only.
                    if 'WinError 10038' not in str(e):
                        raise

        self._greenlet = Greenlet.spawn(serve_forever, self._http_server)

    def _stop(self) -> None:
        if not self.enable:
            return

        self._http_server.server_close()
        self._greenlet.kill(block=False)

    def __enter__(self) -> Self:
        self._redirect = self.credential.redirect

        self._start()

        if self.enable:
            self.credential.redirect = f'http://localhost:{self._http_server.server_port}'

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> bool:
        self._stop()

        self.credential.redirect = self._redirect

        return exc is None


class AzureAadCredential(GrizzlyTokenCredential):
    provider_url_template: ClassVar[str] = 'https://login.microsoftonline.com/{tenant}/oauth2/v2.0'

    username: Optional[str]
    password: str
    scope: str | None
    client_id: str
    tenant: str
    otp_secret: str | None
    refresh_time: int = 3000

    redirect: str | None
    initialize: str | None

    auth_type: AuthType

    _access_token: AccessToken | None
    _webserver: AzureAadWebserver

    def __init__(  # noqa: PLR0913
        self,
        username: Optional[str],
        password: str,
        tenant: str,
        auth_method: AuthMethod,
        /,
        host: str,
        redirect: str | None = None,
        initialize: str | None = None,
        otp_secret: str | None = None,
        scope: str | None = None,
        client_id: str = '04b07795-8ddb-461a-bbee-02f9e1bf7b46',
    ) -> None:
        self.username = username
        self.password = password
        self.tenant = tenant
        self.auth_method = auth_method

        self.host = host
        self.client_id = client_id
        """
        If `client_id` is not specified, the client id for `Azure Command Line Tool` will be used.
        """

        self.scope = scope
        self.redirect = redirect
        self.initialize = initialize
        self.otp_secret = otp_secret

        self._access_token = None

        self.auth_type = AuthType.HEADER if self.initialize is None else AuthType.COOKIE
        self._webserver = AzureAadWebserver(self)

    @property
    def access_token(self) -> AccessToken:
        scopes: tuple[str, ...] = ()
        if self.scope is not None:
            scopes += (self.scope,)

        return self.get_token(*scopes)

    @property
    def webserver(self) -> AzureAadWebserver:
        return self._webserver

    def get_tenant(self, tenant_id: Optional[str]) -> str:
        tenant = tenant_id if tenant_id is not None else self.tenant

        parsed_tenant = urlparse(tenant)
        if len(parsed_tenant.netloc) > 0:
            path = parsed_tenant.path.lstrip('/')
            tenant, _ = path.split('/', 1)

        return tenant

    def get_token(
        self,
        *scopes: str,
        claims: str | None = None,
        tenant_id: str | None = None,
        **_kwargs: Any,
    ) -> AccessToken:
        now = datetime.now(tz=timezone.utc).timestamp()

        if self._access_token is None or self._access_token.expires_on <= now:
            self._refreshed = self._access_token is not None and self._access_token.expires_on <= now

            if self.auth_method == AuthMethod.USER:
                with self.webserver:
                    self._access_token = self.get_oauth_authorization(
                        *scopes, claims=claims, tenant_id=tenant_id,
                    )
            else:
                self._access_token = self.get_oauth_token(tenant_id=tenant_id)

        return cast(AccessToken, self._access_token)

    def get_oauth_authorization(  # noqa: C901, PLR0915
        self, *scopes: str, claims: str | None = None, tenant_id: str | None = None,  # noqa: ARG002
    ) -> AccessToken:
        tenant = self.get_tenant(tenant_id)

        def _parse_response_config(response: requests.Response) -> dict[str, Any]:
            match = re.search(r'Config={(.*?)};', response.text, re.MULTILINE)

            if not match:
                message = f'no config found in response from {response.url}'
                raise ValueError(message)

            return cast(Dict[str, Any], json.loads(f'{{{match.group(1)}}}'))

        def update_state(
            state: dict[str, str], response: requests.Response,
        ) -> dict[str, Any]:
            config = _parse_response_config(response)

            for key in state:
                if key in config:
                    state[key] = str(config[key])
                elif key in response.headers:
                    state[key] = str(response.headers[key])
                else:
                    message = f'unexpected response body from {response.url}: missing "{key}" in config'
                    raise ValueError(message)

            return config

        def generate_uuid() -> str:
            uuid = uuid4().hex

            return f'{uuid[0:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:]}'

        def generate_pkcs() -> tuple[str, str]:
            code_verifier: bytes = urlsafe_b64encode(
                token_urlsafe(96)[:128].encode('ascii'),
            )

            code_challenge = urlsafe_b64encode(sha256(code_verifier).digest()).decode(
                'ascii',
            )[:-1]

            return code_verifier.decode('ascii'), code_challenge

        if self.initialize is None and self.redirect is None:
            message = 'neither initialize or redirect URIs has been set'
            raise AzureAadError(message)

        headers_ua: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0',
        }

        initialize_uri = self.initialize
        redirect_uri = self.redirect
        provider_url = self.provider_url_template.format(tenant=tenant)
        provider_parsed = urlparse(provider_url)

        is_token_v2_0: bool = True
        if initialize_uri is None:
            redirect_uri = cast(str, self.redirect)

        verify = True
        username_lowercase = cast(str, self.username).lower()

        with requests.Session() as session:
            retries = Retry(total=3, connect=3, read=3, status=0, backoff_factor=0.1)
            session.mount('https://', HTTPAdapter(max_retries=retries))

            headers: Dict[str, str]
            payload: Dict[str, Any]
            code_verifier: Optional[str] = None
            code_challenge: Optional[str] = None
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
            if initialize_uri is None and redirect_uri is not None:
                # and redirect_uri is not None:
                client_id = self.client_id
                client_request_id = generate_uuid()

                redirect_uri_parsed = urlparse(redirect_uri)

                if len(redirect_uri_parsed.netloc) == 0:
                    redirect_uri = f"{self.host}{redirect_uri}"

                url = f'{provider_url}/authorize'

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

                if is_token_v2_0:
                    default_scopes = ['openid', 'profile', 'offline_access']
                    scope = ' '.join(scopes) if len(scopes) > 0 else ' '.join(default_scopes)

                    code_verifier, code_challenge = generate_pkcs()
                    params.update({
                        'response_mode': ['fragment'],
                        'response_type': ['code'],
                        'code_challenge_method': ['S256'],
                        'code_challenge': [code_challenge],
                        'scope': [scope],
                    })

                headers = {
                    'Host': provider_parsed.netloc,
                    **headers_ua,
                }

                response = session.get(url, headers=headers, params=params, allow_redirects=False)
            elif initialize_uri is not None and redirect_uri is None:
                initialize_uri_parsed = urlparse(initialize_uri)
                if len(initialize_uri_parsed.netloc) < 1:
                    initialize_uri = f'{self.host}{initialize_uri}'

                initialize_uri_parsed = urlparse(initialize_uri)

                response = session.get(initialize_uri, verify=verify)

                is_token_v2_0 = 'v2.0' in response.url
            else:
                message = 'both initialize and redirect URIs cannot be set'
                raise AzureAadError(message)

            logger.debug(
                'user auth request 1: %s (%d), is_token_v2_0=%r, provider_url=%s, initialize_uri=%s, redirect_uri=%s',
                response.url,
                response.status_code,
                is_token_v2_0,
                provider_url,
                initialize_uri,
                redirect_uri,
            )

            if response.status_code != 200:
                message = f'user auth request 1: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            referer = response.url

            try:
                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    raise AzureAadFlowError(exception_message)

                config = update_state(state, response)
            except ValueError as e:
                raise AzureAadFlowError(str(e)) from None
            # // request 1 -->

            # <!-- request 2
            url_parsed = urlparse(config['urlGetCredentialType'])
            params = parse_qs(url_parsed.query)

            url = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}'
            host = url_parsed.netloc
            params['mkt'] = ['sv-SE']

            headers = {
                'Accept': 'application/json',
                'Host': host,
                'ContentType': 'application/json; charset=UTF-8',
                'canary': state['apiCanary'],
                'client-request-id': state['correlationId'],
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

            response = session.post(url, headers=headers, params=params, json=payload, allow_redirects=False)

            logger.debug('user auth request 2: %s (%d)', response.url, response.status_code)

            if response.status_code != 200:
                message = f'user auth request 2: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            data = cast(Dict[str, Any], json.loads(response.text))
            if 'error' in data:
                error = data['error']
                message = f'error response from {url}: code={error["code"]}, message={error["message"]}'
                raise AzureAadFlowError(message)

            # update state with changed values
            state['apiCanary'] = data['apiCanary']
            state['sFT'] = data['FlowToken']
            # // request 2 -->

            # <!-- request 3
            if initialize_uri is None:
                if not config['urlPost'].startswith('https://'):
                    message = f"response from {response.url} contained unexpected value '{config['urlPost']}'"
                    raise AzureAadFlowError(message)

                url = config['urlPost']
            else:
                if config['urlPost'].startswith('https://'):
                    message = f"response from {response.url} contained unexpected value '{config['urlPost']}'"
                    raise AzureAadFlowError(message)

                url = f'{url_parsed.scheme}://{host}{config["urlPost"]}'

            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': host,
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
                'passwd': self.password,
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

            response = session.post(url, headers=headers, data=payload)

            logger.debug('user auth request 3: %s (%d)', response.url, response.status_code)

            if response.status_code != 200:
                message = f'user auth request 3: {response.url} had unexpected status code {response.status_code}'
                raise AzureAadFlowError(message)

            if self.redirect is None or (self.redirect is not None and self.redirect not in response.url):
                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    logger.error(exception_message)
                    raise AzureAadFlowError(exception_message)

                config = update_state(state, response)

                user_proofs = config.get('arrUserProofs', [])

                if len(user_proofs) > 0:
                    otp_secret = self.otp_secret
                    otp_user_proofs = [
                        user_proof
                        for user_proof in user_proofs
                        if user_proof.get('authMethodId', None) == 'PhoneAppOTP' and 'SoftwareTokenBasedTOTP' in user_proof.get('phoneAppOtpTypes', [])
                    ]

                    if len(otp_user_proofs) != 1:
                        user_proof = user_proofs[0]

                        if otp_secret is None:
                            error_message = f'{username_lowercase} requires MFA for login: {user_proof["authMethodId"]} = {user_proof["display"]}'
                        else:
                            error_message = f'{username_lowercase} is assumed to use TOTP for MFA, but does not have that authentication method configured'

                        logger.error(error_message)

                        raise AzureAadFlowError(error_message)

                    if otp_secret is None:
                        message = f'{username_lowercase} requires TOTP for MFA, but auth.user.otp_secret is not set'
                        raise AzureAadFlowError(message)

                    # <!-- begin auth
                    poll_start = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    url = config['urlBeginAuth']

                    headers = {
                        'Canary': state['apiCanary'],
                        'Client-Request-Id': state['correlationId'],
                        'Hpgrequestid': state['x-ms-request-id'],
                        'Hpgact': state['hpgact'],
                        'Hpgid': state['hpgid'],
                        'Origin': host,
                        'Referer': referer,
                    }

                    payload = {
                        'AuthMethodId': 'PhoneAppOTP',
                        'Method': 'BeginAuth',
                        'ctx': state['sCtx'],
                        'flowToken': state['sFT'],
                    }

                    response = session.post(url, headers=headers, json=payload)

                    logger.debug('user auth request BeginAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request BeginAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    payload = response.json()

                    if not payload['Success']:
                        error_message = f'user auth request BeginAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                        logger.error(error_message)
                        raise AzureAadFlowError(error_message)

                    state.update({
                        'sCtx': payload['Ctx'],
                        'sFT': payload['FlowToken'],
                        'correlationId': payload['CorrelationId'],
                        'sessionId': payload['SessionId'],
                        'x-ms-request-id': response.headers.get('X-Ms-Request-Id', state['x-ms-request-id']),
                    })
                    poll_end = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
                    # // begin auth -->

                    # <!-- end auth
                    totp = TOTP(otp_secret)
                    totp_code = totp.now()
                    url = config['urlEndAuth']
                    payload = {
                        'AdditionalAuthData': totp_code,
                        'AuthMethodId': 'PhoneAppOTP',
                        'Ctx': state['sCtx'],
                        'FlowToken': state['sFT'],
                        'Method': 'EndAuth',
                        'PollCount': 1,
                        'SessionId': state['sessionId'],
                    }

                    response = session.post(url, headers=headers, json=payload)
                    logger.debug('user auth request EndAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request EndAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    payload = response.json()

                    if not payload['Success']:
                        error_message = f'user auth request EndAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                        logger.error(error_message)
                        raise AzureAadFlowError(error_message)

                    state.update({
                        'sCtx': payload['Ctx'],
                        'sFT': payload['FlowToken'],
                        'correlationId': payload['CorrelationId'],
                        'sessionId': payload['SessionId'],
                        'x-ms-request-id': response.headers.get('X-Ms-Request-Id', state['x-ms-request-id']),
                    })
                    # // end auth -->

                    # <!-- process auth
                    url = config['urlPost']

                    headers = {
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Host': host,
                        'Referer': referer,
                        **headers_ua,
                    }

                    payload = {
                        'type': 19,
                        'GeneralVerify': False,
                        'request': state['sCtx'],
                        'mfaLastPollStart': poll_start,
                        'mfaLastPollEnd': poll_end,
                        'mfaAuthMethod': 'PhoneAppOTP',
                        'otc': int(totp_code),
                        'login': username_lowercase,
                        'flowToken': state['sFT'],
                        'hpgrequestid': state['x-ms-request-id'],
                        'sacxt': '',
                        'hideSmsInMfaProofs': False,
                        'canary': state['canary'],
                        'i19': 14798,
                    }

                    response = session.post(url, headers=headers, data=payload)
                    logger.debug('user auth request EndAuth: %s (%d)', response.url, response.status_code)

                    if response.status_code != 200:
                        message = f'user auth request ProcessAuth: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    try:
                        config = _parse_response_config(response)
                        exception_message = config.get('strServiceExceptionMessage', None)

                        if exception_message is not None and len(exception_message.strip()) > 0:
                            logger.error(exception_message)
                            raise AzureAadFlowError(exception_message)
                    except ValueError:  # pragma: no cover
                        pass

                    config = update_state(state, response)
                    # // process auth -->
                # // request 3 -->

                #  <!-- request 4
                if config['urlPost'].startswith('https://'):
                    message = f"unexpected response from {response.url}, incorrect username and/or password?"
                    raise AzureAadFlowError(message)

                url = f'https://{host}{config["urlPost"]}'

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Host': host,
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
                    'i19': '1337',
                }

                # does not seem to be needed for token v2.0, so only add them for v1.0
                if not is_token_v2_0:
                    payload.update({
                        'i2': '',
                        'i17': '',
                        'i18': '',
                    })

                response = session.post(url, headers=headers, data=payload, allow_redirects=False)
                logger.debug('user auth request 4: %s (%d)', response.url, response.status_code)

                if initialize_uri is None:
                    if response.status_code != 302:
                        try:
                            config = _parse_response_config(response)
                            exception_message = config.get('strServiceExceptionMessage', None)

                            if exception_message is not None and len(exception_message.strip()) > 0:
                                raise AzureAadFlowError(exception_message)
                        except ValueError:
                            pass

                        message = f'user auth request 4: {response.url} had unexpected status code {response.status_code}'
                        raise AzureAadFlowError(message)

                    if 'Location' not in response.headers:
                        message = f'Location header was not found in response from {response.url}'
                        raise AzureAadFlowError(message)

                    token_url = response.headers['Location']
                    if not token_url.startswith(f'{redirect_uri}'):
                        message = f'unexpected redirect URI, got {token_url} but expected {redirect_uri}'
                        raise AzureAadFlowError(message)
                    # // request 4 -->

                    token_url_parsed = urlparse(token_url)
                    fragments = parse_qs(token_url_parsed.fragment)

                    # exchange received with with a token
                    if is_token_v2_0:
                        assert code_verifier is not None, 'no code verifier has been generated!'
                        assert 'code' in fragments, f'could not find code in {token_url}'
                        code = fragments['code'][0]
                        return self.get_oauth_token(code=code, verifier=code_verifier, tenant_id=tenant)

                    if 'id_token' not in fragments:
                        message = f'could not find id_token in {token_url}'
                        raise AzureAadFlowError(message)

                    token = fragments['id_token'][0]
                    expires_on = int(datetime.now(tz=timezone.utc).timestamp()) + int(fragments.get('expires_in', ['3500'])[0])

                    return AccessToken(token, expires_on)

                # token comes in the form of a cookie
                parser = FormPostParser()
                parser.feed(response.text)

                if response.status_code != 200 or parser.action is None:
                    try:
                        config = _parse_response_config(response)
                        exception_message = config.get('strServiceExceptionMessage', None)

                        if exception_message is not None and len(exception_message.strip()) > 0:
                            raise AzureAadFlowError(exception_message)
                    except ValueError:
                        pass

                    message = f'user auth request 4: {response.url} had unexpected status code {response.status_code}'
                    raise AzureAadFlowError(message)

                origin = f'https://{host}'

                headers.update({
                    'Origin': origin,
                    'Referer': origin,
                })

                safe_del(headers, 'Host')

                response = session.post(parser.action, headers=headers, data=parser.payload, allow_redirects=True, verify=verify)

                if response.status_code != 200:
                    message = f'user auth request 5: {response.url} had unexpected status code {response.status_code}'
                    raise AzureAadFlowError(message)

                for cookie in session.cookies:
                    domain = cookie.domain[1:] if cookie.domain_initial_dot else cookie.domain

                    if domain in initialize_uri:
                        expires_on = cookie.expires or int(datetime.now(tz=timezone.utc).timestamp() + 3500)
                        if cookie.value is None:
                            message = 'token cookie did not contain a value'
                            raise AzureAadFlowError(message)

                        return AccessToken(cookie.value, expires_on)

                message = 'did not find AAD cookie in authorization flow response session'
                raise AzureAadFlowError(message)

            # authenticated against a service principal in azure
            code_url_parsed = urlparse(response.url)
            fragments = parse_qs(code_url_parsed.fragment)

            if code_verifier is None:
                message = 'no code verifier has been generated!'
                raise AzureAadError(message)

            if 'code' not in fragments:
                message = f'could not find `code` in {response.url}'
                raise AzureAadFlowError(message)

            code = fragments['code'][0]

            return self.get_oauth_token(code=code, verifier=code_verifier)

    def get_oauth_token(
        self, *, code: Optional[str] = None, verifier: Optional[str] = None, resource: Optional[str] = None, tenant_id: Optional[str] = None,
    ) -> AccessToken:
        tenant = self.get_tenant(tenant_id)

        provider_url = self.provider_url_template.format(tenant=tenant)

        url = f'{provider_url}/token'

        # parameters valid for both versions
        parameters: dict[str, Any] = {
            'data': {'grant_type': None, 'client_id': self.client_id},
            'verify': True,
        }

        # build generic header values, but remove stuff that shouldn't be part
        # of authentication flow
        headers = {}

        if self.auth_type == AuthType.HEADER:
            redirect_uri = cast(str, self.redirect)
            redirect_uri_parsed = urlparse(redirect_uri)
        else:
            redirect_uri_parsed = urlparse(self.host)
            if len(redirect_uri_parsed.scheme) < 1:
                redirect_uri_parsed = redirect_uri_parsed._replace(scheme='https')

            if len(redirect_uri_parsed.netloc) < 1:
                redirect_uri_parsed = redirect_uri_parsed._replace(netloc=redirect_uri_parsed.path, path='')

        origin = f'{redirect_uri_parsed.scheme}://{redirect_uri_parsed.netloc}'

        headers.update({'Origin': origin, 'Referer': origin})

        if verifier is not None:
            parameters['data'].update(
                {
                    'grant_type': 'authorization_code',
                    'redirect_uri': redirect_uri,
                    'code': code,
                    'code_verifier': verifier,
                },
            )
        else:
            parameters['data'].update(
                {
                    'grant_type': 'client_credentials',
                    'client_secret': self.password,
                    'scope': resource,
                    'tenant': tenant,
                },
            )

        parameters.update({'headers': headers, 'allow_redirects': (code is None and verifier is None)})

        with requests.Session() as session:
            retries = Retry(total=3, connect=3, read=3, status=0, backoff_factor=0.1)
            session.mount('https://', HTTPAdapter(max_retries=retries))

            response = session.post(url, **parameters)
            payload = json.loads(response.text)

            if response.status_code != 200:
                raise AzureAadFlowError(payload['error_description'])

            token = payload.get('id_token', payload.get('access_token', None))
            if token is None:
                message = 'neither `id_token` or `access_token` was found in payload'
                raise AzureAadFlowError(message)
            expires_on = int(
                datetime.now(tz=timezone.utc).timestamp()
                + payload.get('expires_in', self.refresh_time),
            )

            return AccessToken(token, expires_on)


class AAD(RefreshToken):
    __TOKEN_CREDENTIAL_TYPE__ = AzureAadCredential

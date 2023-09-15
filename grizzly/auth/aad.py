"""
@anchor pydoc:grizzly.auth.aad Azure Active Directory
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

``` gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.provider" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.client.secret" to "<client secret>"
And set context variable "auth.client.resource" to "<resource url/guid>"
```

## Username and password

Using a username and password, with optional MFA authentication.

`auth.user.redirect_uri` needs to correspond to the endpoint that the client secret is registrered for.

``` gherkin
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

11. Copy the code generate from above command and click `Next`

12. Finish the wizard

The user now have software based TOTP tokens as MFA method.



#### Example

In addition to the "Username and password" example, the context variable `auth.user.otp_secret` must also be set.

``` gherkin
Given a user of type "RestApi" load testing "https://api.example.com"
And set context variable "auth.provider" to "<provider>"
And set context variable "auth.client.id" to "<client id>"
And set context variable "auth.user.username" to "alice@example.onmicrosoft.com"
And set context variable "auth.user.password" to "HemL1gaArn3!"
And set context variable "auth.user.redirect_uri" to "/app-registrered-redirect-uri"
And set context variable "auth.user.otp_secret" to "asdfasdf"  # <-- !!
```
"""
import re
import json
import logging

from typing import Dict, Any, Tuple, Optional, List, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4
from secrets import token_urlsafe
from hashlib import sha256
from base64 import urlsafe_b64encode
from time import perf_counter as time_perf_counter
from datetime import datetime
from html.parser import HTMLParser

import requests

from pyotp import TOTP

from grizzly.utils import safe_del
from grizzly.types.locust import StopUser
from . import RefreshToken, GrizzlyHttpAuthClient, AuthType


logger = logging.getLogger(__name__)


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


class AAD(RefreshToken):
    @classmethod
    def get_oauth_authorization(cls, client: GrizzlyHttpAuthClient) -> Tuple[AuthType, str]:
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

        def generate_pkcs() -> Tuple[str, str]:
            code_verifier: bytes = urlsafe_b64encode(token_urlsafe(96)[:128].encode('ascii'))

            code_challenge = urlsafe_b64encode(
                sha256(code_verifier).digest()
            ).decode('ascii')[:-1]

            return code_verifier.decode('ascii'), code_challenge

        headers_ua: Dict[str, str] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:88.0) Gecko/20100101 Firefox/88.0'
        }

        auth_context = client._context.get('auth', None)
        assert auth_context is not None, 'context variable auth is not set'
        auth_user_context = auth_context.get('user', None)
        assert auth_user_context is not None, 'context variable auth.user is not set'

        initialize_uri = auth_user_context.get('initialize_uri', None)
        redirect_uri = auth_user_context.get('redirect_uri', None)

        assert initialize_uri is None or redirect_uri is None, 'both auth.user.initialize_uri and auth.user.redirect_uri is set'

        is_token_v2_0: Optional[bool] = None
        if initialize_uri is None:
            redirect_uri = cast(str, redirect_uri)
            auth_client_context = auth_context.get('client', None)
            assert auth_client_context is not None, 'context variable auth.client is not set'
            provider_url = auth_context.get('provider', None)
            assert provider_url is not None, 'context variable auth.provider is not set'
            auth_provider_parsed = urlparse(provider_url)
            is_token_v2_0 = 'v2.0' in provider_url

        start_time = time_perf_counter()
        total_response_length = 0
        exception: Optional[Exception] = None
        verify = client._context.get('verify_certificates', True)
        username_lowercase = cast(str, auth_user_context['username']).lower()

        try:
            total_response_length = 0

            with requests.Session() as session:
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
                if initialize_uri is None:
                    # <!-- dummy stuff, done earlier
                    assert auth_client_context is not None
                    assert redirect_uri is not None
                    # // -->
                    client_id = cast(str, auth_client_context['id'])
                    client_request_id = generate_uuid()

                    redirect_uri_parsed = urlparse(redirect_uri)

                    if len(redirect_uri_parsed.netloc) == 0:
                        redirect_uri = f"{client.host}{redirect_uri}"

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

                    code_verifier: Optional[str] = None
                    code_challenge: Optional[str] = None

                    if is_token_v2_0:
                        params.update({
                            'response_mode': ['fragment'],
                        })

                        code_verifier, code_challenge = generate_pkcs()
                        params.update({
                            'response_type': ['code'],
                            'code_challenge_method': ['S256'],
                            'code_challenge': [code_challenge],
                            'scope': ['openid profile offline_access'],
                        })

                    headers = {
                        'Host': auth_provider_parsed.netloc,
                        **headers_ua,
                    }

                    response = session.get(url, headers=headers, params=params, allow_redirects=False)
                else:
                    initialize_uri_parsed = urlparse(initialize_uri)
                    if len(initialize_uri_parsed.netloc) < 1:
                        initialize_uri = f'{client.host}{initialize_uri}'

                    initialize_uri_parsed = urlparse(initialize_uri)

                    response = session.get(initialize_uri, verify=verify)

                    is_token_v2_0 = 'v2.0' in response.url

                logger.debug(f'user auth request 1: {response.url} ({response.status_code})')
                total_response_length += int(response.headers.get('content-length', '0'))

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 1: {response.url} had unexpected status code {response.status_code}')

                referer = response.url

                config = update_state(state, response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    raise RuntimeError(exception_message)
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
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 2: {response.url} ({response.status_code})')

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 2: {response.url} had unexpected status code {response.status_code}')

                data = cast(Dict[str, Any], json.loads(response.text))
                if 'error' in data:
                    error = data['error']
                    raise RuntimeError(f'error response from {url}: code={error["code"]}, message={error["message"]}')

                # update state with changed values
                state['apiCanary'] = data['apiCanary']
                state['sFT'] = data['FlowToken']
                # // request 2 -->

                # <!-- request 3
                if initialize_uri is None:
                    assert config['urlPost'].startswith('https://'), f"response from {response.url} contained unexpected value '{config['urlPost']}'"
                    url = config['urlPost']
                else:
                    assert not config['urlPost'].startswith('https://'), f"response from {response.url} contained unexpected value '{config['urlPost']}'"
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

                response = session.post(url, headers=headers, data=payload)
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 3: {response.url} ({response.status_code})')

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 3: {response.url} had unexpected status code {response.status_code}')

                config = update_state(state, response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    raise RuntimeError(exception_message)

                user_proofs = config.get('arrUserProofs', [])

                if len(user_proofs) > 0:
                    otp_secret = auth_user_context.get('otp_secret', None)
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

                        raise RuntimeError(error_message)
                    else:
                        assert otp_secret is not None, f'{username_lowercase} requires TOTP for MFA, but auth.user.otp_secret is not set'

                        # <!-- begin auth
                        poll_start = int(datetime.utcnow().timestamp() * 1000)
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
                        total_response_length += int(response.headers.get('content-length', '0'))
                        logger.debug(f'user auth request BeginAuth: {response.url} ({response.status_code})')

                        if response.status_code != 200:
                            raise RuntimeError(f'user auth request BeginAuth: {response.url} had unexpected status code {response.status_code}')

                        payload = response.json()

                        if not payload['Success']:
                            error_message = f'user auth request BeginAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                            logger.error(error_message)
                            raise RuntimeError(error_message)

                        state.update({
                            'sCtx': payload['Ctx'],
                            'sFT': payload['FlowToken'],
                            'correlationId': payload['CorrelationId'],
                            'sessionId': payload['SessionId'],
                            'x-ms-request-id': response.headers.get('X-Ms-Request-Id', state['x-ms-request-id']),
                        })
                        poll_end = int(datetime.utcnow().timestamp() * 1000)
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
                        total_response_length += int(response.headers.get('content-length', '0'))
                        logger.debug(f'user auth request EndAuth: {response.url} ({response.status_code})')

                        if response.status_code != 200:
                            raise RuntimeError(f'user auth request EndAuth: {response.url} had unexpected status code {response.status_code}')

                        payload = response.json()

                        if not payload['Success']:
                            error_message = f'user auth request EndAuth: {payload.get("ErrCode", -1)} - {payload.get("Message", "unknown")}'
                            logger.error(error_message)
                            raise RuntimeError(error_message)

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
                        total_response_length += int(response.headers.get('content-length', '0'))
                        logger.debug(f'user auth request EndAuth: {response.url} ({response.status_code})')

                        if response.status_code != 200:
                            raise RuntimeError(f'user auth request ProcessAuth: {response.url} had unexpected status code {response.status_code}')

                        try:
                            config = _parse_response_config(response)
                            exception_message = config.get('strServiceExceptionMessage', None)

                            if exception_message is not None and len(exception_message.strip()) > 0:
                                raise RuntimeError(exception_message)
                        except ValueError:  # pragma: no cover
                            pass

                        config = update_state(state, response)
                        # // process auth -->
                # // request 3 -->

                #  <!-- request 4
                assert not config['urlPost'].startswith('https://'), f"unexpected response from {response.url}, incorrect username and/or password?"
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
                total_response_length += int(response.headers.get('content-length', '0'))

                logger.debug(f'user auth request 4: {response.url} ({response.status_code})')

                if initialize_uri is None:
                    if response.status_code != 302:
                        try:
                            config = _parse_response_config(response)
                            exception_message = config.get('strServiceExceptionMessage', None)

                            if exception_message is not None and len(exception_message.strip()) > 0:
                                raise RuntimeError(exception_message)
                        except ValueError:
                            pass

                        raise RuntimeError(f'user auth request 4: {response.url} had unexpected status code {response.status_code}')

                    assert 'Location' in response.headers, f'Location header was not found in response from {response.url}'

                    token_url = response.headers['Location']
                    assert token_url.startswith(f'{redirect_uri}'), f'unexpected redirect URI, got {token_url} but expected {redirect_uri}'
                    # // request 4 -->

                    token_url_parsed = urlparse(token_url)
                    fragments = parse_qs(token_url_parsed.fragment)

                    # exchange received with with a token
                    if is_token_v2_0:
                        assert code_verifier is not None, 'no code verifier has been generated!'
                        assert 'code' in fragments, f'could not find code in {token_url}'
                        code = fragments['code'][0]
                        return cls.get_oauth_token(client, (code, code_verifier,))
                    else:
                        assert 'id_token' in fragments, f'could not find id_token in {token_url}'
                        token = fragments['id_token'][0]
                        return (AuthType.HEADER, token,)
                else:
                    parser = FormPostParser()
                    parser.feed(response.text)

                    if response.status_code != 200 or parser.action is None:
                        try:
                            config = _parse_response_config(response)
                            exception_message = config.get('strServiceExceptionMessage', None)

                            if exception_message is not None and len(exception_message.strip()) > 0:
                                raise RuntimeError(exception_message)
                        except ValueError:
                            pass

                        raise RuntimeError(f'user auth request 4: {response.url} had unexpected status code {response.status_code}')

                    origin = f'https://{host}'

                    headers.update({
                        'Origin': origin,
                        'Referer': origin,
                    })

                    safe_del(headers, 'Host')

                    response = session.post(parser.action, headers=headers, data=parser.payload, allow_redirects=True, verify=verify)

                    if response.status_code != 200:
                        raise RuntimeError(f'user auth request 5: {response.url} had unexpected status code {response.status_code}')

                    for cookie in session.cookies:
                        domain = cookie.domain[1:] if cookie.domain_initial_dot else cookie.domain

                        if domain in initialize_uri:
                            return AuthType.COOKIE, f'{cookie.name}={cookie.value}'

                    raise RuntimeError('did not find AAD cookie in authorization flow response session')
        except Exception as e:
            exception = e
            logger.error(str(e), exc_info=True)
        finally:
            scenario_index = client._scenario.identifier

            if is_token_v2_0 is None:
                version = ''
            else:
                version = 'v1.0' if not is_token_v2_0 else 'v2.0'

            request_meta = {
                'request_type': 'AUTH',
                'response_time': int((time_perf_counter() - start_time) * 1000),
                'name': f'{scenario_index} {cls.__name__} OAuth2 user token {version}',
                'context': client._context,
                'response': None,
                'exception': exception,
                'response_length': total_response_length,
            }

            client.environment.events.request.fire(**request_meta)

            if exception is not None:
                raise StopUser()

    @classmethod
    def get_oauth_token(cls, client: GrizzlyHttpAuthClient, pkcs: Optional[Tuple[str, str]] = None) -> Tuple[AuthType, str]:
        auth_context = client._context.get('auth', None)
        assert auth_context is not None, 'context variable auth is not set'
        provider_url = auth_context.get('provider', None)
        assert provider_url is not None, 'context variable auth.provider is not set'

        auth_client_context = auth_context.get('client', None)
        assert auth_client_context is not None, 'context variable auth.client is not set'
        resource = auth_client_context.get('resource', client.host)

        auth_user_context = auth_context.get('user', None)

        is_token_v2_0 = 'v2.0' in provider_url

        url = f'{provider_url}/token'

        if is_token_v2_0:
            version = 'v2.0'
        else:
            version = 'v1.0'

        # parameters valid for both versions
        parameters: Dict[str, Any] = {
            'data': {
                'grant_type': None,
                'client_id': auth_client_context['id'],
            },
            'verify': True,
        }

        # build generic header values, but remove stuff that shouldn't be part
        # of authentication flow
        headers = {**client.headers}
        safe_del(headers, 'Authorization')
        safe_del(headers, 'Content-Type')
        safe_del(headers, 'Ocp-Apim-Subscription-Key')

        start_time = time_perf_counter()

        if pkcs is not None:  # token v2.0, authorization_code
            assert auth_user_context is not None, 'context variable auth.user is not set'
            code, code_verifier = pkcs

            redirect_uri = auth_user_context['redirect_uri']
            assert redirect_uri is not None, 'context variable auth.user.redirect_uri is not set'
            redirect_uri_parsed = urlparse(redirect_uri)

            if len(redirect_uri_parsed.netloc) == 0:
                redirect_uri = f"{client.host}{redirect_uri}"
                redirect_uri_parsed = urlparse(redirect_uri)

            origin = f'{redirect_uri_parsed.scheme}://{redirect_uri_parsed.netloc}'

            headers.update({
                'Origin': origin,
                'Referer': origin,
            })

            parameters['data'].update({
                'grant_type': 'authorization_code',
                'redirect_uri': redirect_uri,
                'code': code,
                'code_verifier': code_verifier,
            })
        elif not is_token_v2_0:  # token v1.0
            parameters['data'].update({
                'grant_type': 'client_credentials',
                'client_secret': auth_client_context['secret'],
                'resource': resource,
            })
        elif is_token_v2_0:  # token v2.0
            tenant = auth_context.get('tenant', None)
            if tenant is None:
                provider_url_parsed = urlparse(provider_url)
                tenant, _ = provider_url_parsed.path[1:].split('/', 1)

            parameters['data'].update({
                'grant_type': 'client_credentials',
                'client_secret': auth_client_context['secret'],
                'scope': resource,
                'tenant': tenant,
            })

        parameters.update({'headers': headers, 'allow_redirects': (pkcs is None)})

        exception: Optional[Exception] = None

        response_length: int = 0

        try:
            with requests.Session() as session:
                response = session.post(url, **parameters)
                response_length = len(response.text.encode())
                payload = json.loads(response.text)

                if response.status_code != 200:
                    raise RuntimeError(payload['error_description'])

                if pkcs is None:
                    token = str(payload['access_token'])
                else:
                    token = str(payload['id_token'])

                return (AuthType.HEADER, token,)
        except Exception as e:
            exception = e
            logger.error(str(e), exc_info=True)
        finally:
            scenario_index = client._scenario.identifier

            request_meta = {
                'request_type': 'AUTH',
                'response_time': int((time_perf_counter() - start_time) * 1000),
                'name': f'{scenario_index} {cls.__name__} OAuth2 user token {version}',
                'context': client._context,
                'response': None,
                'exception': exception,
                'response_length': response_length,
            }

            if pkcs is None:
                client.environment.events.request.fire(**request_meta)

            if exception is not None:
                raise StopUser()

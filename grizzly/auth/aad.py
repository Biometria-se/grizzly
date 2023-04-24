import re
import json
import logging

from typing import Literal, Dict, Any, Tuple, Optional, List, cast
from urllib.parse import parse_qs, urlparse
from uuid import uuid4
from secrets import token_urlsafe
from hashlib import sha256
from base64 import urlsafe_b64encode
from time import perf_counter as time_perf_counter

import requests

from grizzly.utils import safe_del
from grizzly.types.locust import StopUser
from . import RefreshToken, AuthMethod, GrizzlyHttpAuthClient


logger = logging.getLogger(__name__)


class AAD(RefreshToken):
    @classmethod
    def get_token(cls, client: GrizzlyHttpAuthClient, auth_method: Literal[AuthMethod.CLIENT, AuthMethod.USER]) -> str:
        if auth_method == AuthMethod.CLIENT:
            return cls.get_aad_oauth_token(client)
        else:
            return cls.get_aad_oauth_authorization(client)

    @classmethod
    def get_aad_oauth_authorization(cls, client: GrizzlyHttpAuthClient) -> str:
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
        auth_client_context = auth_context.get('client', None)
        assert auth_client_context is not None, 'context variable auth.client is not set'
        provider_url = auth_context.get('provider', None)
        assert provider_url is not None, 'context variable auth.provider is not set'

        start_time = time_perf_counter()
        total_response_length = 0
        exception: Optional[Exception] = None
        auth_provider_parsed = urlparse(provider_url)
        is_token_v2_0 = 'v2.0' in provider_url

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
                client_id = cast(str, auth_client_context['id'])
                client_request_id = generate_uuid()
                username_lowercase = cast(str, auth_user_context['username']).lower()

                redirect_uri = cast(str, auth_user_context['redirect_uri'])

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
                    code_verifier, code_challenge = generate_pkcs()
                    params.update({
                        'response_type': ['code'],
                        'response_mode': ['fragment'],
                        'scope': ['openid profile offline_access'],
                        'code_challenge_method': ['S256'],
                        'code_challenge': [code_challenge],
                    })

                headers = {
                    'Host': str(auth_provider_parsed.netloc),
                    **headers_ua,
                }

                response = session.get(url, headers=headers, params=params)
                logger.debug(f'user auth request 1: {response.url} ({response.status_code})')
                total_response_length += int(response.headers.get('content-length', '0'))

                if response.status_code != 200:
                    raise RuntimeError(f'user auth request 1: {response.url} had unexpected status code {response.status_code}')

                referer = response.url

                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
                    raise RuntimeError(exception_message)

                config = update_state(state, response)
                # // request 1 -->

                # <!-- request 2
                url_parsed = urlparse(config['urlGetCredentialType'])
                params = parse_qs(url_parsed.query)

                url = f'{url_parsed.scheme}://{url_parsed.netloc}{url_parsed.path}'
                params['mkt'] = ['sv-SE']

                headers = {
                    'Accept': 'application/json',
                    'Host': str(auth_provider_parsed.netloc),
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

                response = session.post(url, headers=headers, params=params, json=payload)
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
                    'Host': str(auth_provider_parsed.netloc),
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

                config = _parse_response_config(response)

                exception_message = config.get('strServiceExceptionMessage', None)

                if exception_message is not None and len(exception_message.strip()) > 0:
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
                url = f'{str(auth_provider_parsed.scheme)}://{str(auth_provider_parsed.netloc)}{config["urlPost"]}'

                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Host': str(auth_provider_parsed.netloc),
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
                    return cls.get_aad_oauth_token(client, (code, code_verifier,))
                else:
                    assert 'id_token' in fragments, f'could not find id_token in {token_url}'
                    token = fragments['id_token'][0]
                    return token
        except Exception as e:
            exception = e
            logger.error(str(e), exc_info=True)
        finally:
            name = client.__class__.__name__.rsplit('_', 1)[-1]

            version = 'v1.0' if not is_token_v2_0 else 'v2.0'

            request_meta = {
                'request_type': 'GET',
                'response_time': int((time_perf_counter() - start_time) * 1000),
                'name': f'{name} {cls.__name__} OAuth2 user token {version}',
                'context': client._context,
                'response': None,
                'exception': exception,
                'response_length': total_response_length,
            }

            client.environment.events.request.fire(**request_meta)

            if exception is not None:
                raise StopUser()

    @classmethod
    def get_aad_oauth_token(cls, client: GrizzlyHttpAuthClient, pkcs: Optional[Tuple[str, str]] = None) -> str:
        name = client.__class__.__name__.rsplit('_', 1)[-1]

        auth_context = client._context.get('auth', None)
        assert auth_context is not None, 'context variable auth is not set'
        provider_url = auth_context.get('provider', None)
        assert provider_url is not None, 'context variable auth.provider is not set'
        is_token_v2_0 = 'v2.0' in provider_url

        auth_client_context = auth_context.get('client', None)
        assert auth_client_context is not None, 'context variable auth.client is not set'
        auth_user_context = auth_context.get('user', None)
        assert auth_user_context is not None, 'context variable auth.user is not set'
        resource = auth_client_context.get('resource', client.host)

        url = f'{provider_url}/token'

        if pkcs is not None:
            version = 'v2.0'
        else:
            version = 'v1.0'

        # parameters valid for both versions
        parameters: Dict[str, Any] = {
            'data': {
                'grant_type': None,
                'client_id': auth_client_context['id'],
            },
            'verify': client._context.get('verify_certificates', True),
        }

        # build generic header values, but remove stuff that shouldn't be part
        # of authentication flow
        headers = {**client.headers}
        safe_del(headers, 'Authorization')
        safe_del(headers, 'Content-Type')
        safe_del(headers, 'Ocp-Apim-Subscription-Key')

        start_time = time_perf_counter()

        if pkcs is None:  # token v1.0
            parameters['data'].update({
                'grant_type': 'client_credentials',
                'client_secret': auth_client_context['secret'],
                'resource': resource,
            })
        else:  # token v2.0
            code, code_verifier = pkcs

            redirect_uri = auth_user_context['redirect_uri']
            assert redirect_uri is not None, 'context variable auth.user.redirect_uri is not set'
            redirect_uri_parsed = urlparse(redirect_uri)

            if len(redirect_uri_parsed.netloc) == 0:
                redirect_uri = f"{client.host}{redirect_uri}"

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
            parameters.update({'allow_redirects': False})

        parameters.update({'headers': headers})

        exception: Optional[Exception] = None

        response_length: int = 0

        try:
            with requests.Session() as session:
                response = session.post(url, **parameters)

                response_length = len(response.text.encode())

                payload = json.loads(response.text)

                if pkcs is None:
                    token = str(payload['access_token'])
                else:
                    token = str(payload['id_token'])

                return token
        except Exception as e:
            exception = e
            logger.error(str(e), exc_info=True)
        finally:
            name = client.__class__.__name__.rsplit('_', 1)[-1]

            version = 'v1.0' if not is_token_v2_0 else 'v2.0'

            request_meta = {
                'request_type': 'GET',
                'response_time': int((time_perf_counter() - start_time) * 1000),
                'name': f'{name} {cls.__name__} OAuth2 user token {version}',
                'context': client._context,
                'response': None,
                'exception': exception,
                'response_length': response_length,
            }

            if pkcs is None:
                client.environment.events.request.fire(**request_meta)

            if exception is not None:
                raise StopUser()

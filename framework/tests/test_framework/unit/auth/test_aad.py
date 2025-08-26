"""Unit tests of grizzly.auth.aad."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import product
from json import dumps as jsondumps
from typing import TYPE_CHECKING, cast
from urllib.parse import urlparse

import pytest
from grizzly.auth import AAD, AccessToken, RefreshToken
from grizzly.types import StrDict, ZoneInfo
from grizzly.users import RestApiUser
from grizzly_common.azure.aad import AuthMethod, AzureAadCredential, AzureAadError, AzureAadFlowError
from requests.cookies import create_cookie
from requests.models import Response

from test_framework.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    import requests
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


def test_aad() -> None:
    assert issubclass(AAD, RefreshToken)
    assert AAD.__TOKEN_CREDENTIAL_TYPE__ is AzureAadCredential


class TestAzureAadCredential:
    def test_get_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        get_oauth_token_mock = mocker.patch(
            'grizzly.auth.aad.AzureAadCredential.get_oauth_token',
            return_value=AccessToken('asdf', expires_on=int(datetime.now(tz=timezone.utc).timestamp())),
        )

        get_oauth_authorization_mock = mocker.patch(
            'grizzly.auth.aad.AzureAadCredential.get_oauth_authorization',
            side_effect=[
                AccessToken('asdf', expires_on=int(datetime.now(tz=timezone.utc).timestamp()) + 3600),
                AccessToken('foobar', expires_on=int(datetime.now(tz=timezone.utc).timestamp()) + 4000),
            ],
        )

        credential = AzureAadCredential(
            'bob',
            'secret',
            'example.com',
            AuthMethod.CLIENT,
            host='https://example.com',
            redirect='https://example.com/login-callback',
            initialize=None,
        )

        credential.get_token('profile', 'offline', claims='profile', tenant_id='foobar')
        get_oauth_authorization_mock.assert_not_called()
        get_oauth_token_mock.assert_called_once_with(tenant_id='foobar')
        get_oauth_token_mock.reset_mock()

        credential = AzureAadCredential(
            'bob',
            'secret',
            'example.com',
            AuthMethod.USER,
            host='https://example.com',
            redirect='https://example.com/login-callback',
            initialize=None,
        )

        access_token = credential.get_token('profile', 'offline', claims='profile', tenant_id='foobar')
        get_oauth_token_mock.assert_not_called()
        get_oauth_authorization_mock.assert_called_once_with('profile', 'offline', claims='profile', tenant_id='foobar')
        get_oauth_authorization_mock.reset_mock()

        assert access_token is credential.get_token()

        # token is refreshed
        datetime_mock = mocker.patch('grizzly_common.azure.aad.datetime')
        datetime_mock.now.return_value = datetime.now(tz=timezone.utc) + timedelta(seconds=5000)

        assert access_token is not credential.get_token()

    @pytest.mark.skip(reason='needs real secrets')
    def test_get_oauth_authorization_real_initialize_uri(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        from grizzly.tasks.clients import HttpClientTask
        from grizzly.types import RequestDirection

        parent = grizzly_fixture(user_type=RestApiUser)
        grizzly = grizzly_fixture.grizzly
        grizzly.scenario.variables['test_payload'] = 'none'

        http_client_task = type('TestHttpClientTask', (HttpClientTask,), {'__scenario__': grizzly.scenario})

        task_factory = http_client_task(
            RequestDirection.FROM,
            '<url to request>',
            payload_variable='test_payload',
        )

        parent.user._context.update(
            {
                '<host of url to request>': {
                    'auth': {
                        'user': {
                            'username': '<username>',
                            'password': '<password>',
                            'initialize_uri': '<url that initializes the login by redirecting to microsoftonline.com>',
                        },
                    },
                    'verify_certificates': False,
                },
            },
        )

        task = task_factory()

        task.on_start(parent)

        with caplog.at_level(logging.DEBUG):
            task(parent)

        payload = parent.user.variables.get('test_payload', None)
        assert payload is not None

    @pytest.mark.skip(reason='needs real secrets')
    def test_get_oauth_authorization_provider(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        from grizzly.tasks.clients import HttpClientTask
        from grizzly.types import RequestDirection

        parent = grizzly_fixture(user_type=RestApiUser)
        grizzly = grizzly_fixture.grizzly
        parent.user.set_variable('test_payload', 'none')

        http_client_task = type('TestHttpClientTask', (HttpClientTask,), {'__scenario__': grizzly.scenario})

        task_factory = http_client_task(
            RequestDirection.FROM,
            '<request url>',
            payload_variable='test_payload',
        )

        parent.user._context.update(
            {
                '<host of request url>': {
                    'auth': {
                        'tenant': '<tenant to authenticate with>',
                        'client': {
                            'id': '<client id of application @ tenant>',
                        },
                        'user': {
                            'username': '<username>',
                            'password': '<password>',
                            'redirect_uri': '<url that tenant will redirect to with token in url fragment',
                        },
                    },
                },
            },
        )
        task_factory.metadata.update(
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
            },
        )

        task = task_factory()

        task.on_start(parent)

        with caplog.at_level(logging.DEBUG):
            task(parent)

        payload = parent.user.variables.get('test_payload', None)
        assert payload is not None

    @pytest.mark.parametrize(('version', 'login_start'), product(['v2.0'], ['initialize_uri', 'redirect_uri']))
    def test_get_oauth_authorization(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture, version: str, login_start: str) -> None:  # noqa: PLR0915, C901
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)
        original_class_name = parent.user.__class__.__name__
        try:
            access_token = AccessToken('asdf', expires_on=1)

            is_token_v2_0 = version == 'v2.0'

            credential = AzureAadCredential(
                'test-user@example.com',
                'secret',
                'example.com',
                AuthMethod.USER,
                host='https://example.com',
                redirect='https://www.example.com/login-callback',
                initialize=None,
            )
            get_oauth_token_mock = mocker.patch.object(credential, 'get_oauth_token', return_value=None)

            if is_token_v2_0:
                get_oauth_token_mock.return_value = access_token

            class Error(Enum):
                REQUEST_1_NO_DOLLAR_CONFIG = 100
                REQUEST_1_HTTP_STATUS = 101
                REQUEST_1_MISSING_STATE = 102
                REQUEST_1_DOLLAR_CONFIG_ERROR = 103
                REQUEST_2_HTTP_STATUS = 200
                REQUEST_2_ERROR_MESSAGE = 201
                REQUEST_3_HTTP_STATUS = 300
                REQUEST_3_ERROR_MESSAGE = 301
                REQUEST_3_MFA_REQUIRED = 302
                REQUEST_3_MFA_TOPT = 330
                REQUEST_3_MFA_BEGIN_AUTH_STATUS = 331
                REQUEST_3_MFA_BEGIN_AUTH_FAILURE = 332
                REQUEST_3_MFA_END_AUTH_STATUS = 341
                REQUEST_3_MFA_END_AUTH_FAILURE = 342
                REQUEST_3_MFA_PROCESS_AUTH_STATUS = 351
                REQUEST_3_MFA_PROCESS_AUTH_FAILURE = 352
                REQUEST_4_HTTP_STATUS = 400
                REQUEST_4_HTTP_STATUS_CONFIG = 401
                REQUEST_5_HTTP_STATUS = 500
                REQUEST_5_NO_COOKIE = 501

            def mock_request_session(inject_error: Error | None = None) -> None:  # noqa: PLR0915, C901
                def request(self: requests.Session, method: str, url: str, name: str | None = None, **kwargs: StrDict) -> requests.Response:  # noqa: ARG001, PLR0915, C901, PLR0912
                    response = Response()
                    response.status_code = 200
                    response.url = url
                    self.cookies.clear()

                    if method == 'GET' and (url.endswith(('/authorize', '/app/login'))):
                        if url.endswith('/app/login'):
                            response.url = 'https://login.example.com/oauth2/v2.0/authorize'

                        if inject_error == Error.REQUEST_1_NO_DOLLAR_CONFIG:
                            response._content = b''
                        else:
                            dollar_config_raw = {
                                'hpgact': 1800,
                                'hpgid': 11,
                                'sFT': 'xxxxxxxxxxxxxxxxxxx',
                                'sCtx': 'yyyyyyyyyyyyyyyyyyy',
                                'apiCanary': 'zzzzzzzzzzzzzzzzzz',
                                'canary': 'canary=1:1',
                                'correlationId': 'aa-bb-cc',
                                'sessionId': 'session-a-b-c',
                                'country': 'SE',
                                'urlGetCredentialType': 'https://login.example.com/GetCredentialType?mkt=en-US',
                                'urlPost': 'https://login.example.com/login',
                            }
                            if inject_error == Error.REQUEST_1_MISSING_STATE:
                                del dollar_config_raw['hpgact']
                            elif inject_error == Error.REQUEST_1_DOLLAR_CONFIG_ERROR:
                                dollar_config_raw['strServiceExceptionMessage'] = 'oh no!'

                            if login_start == 'initialize_uri':
                                dollar_config_raw.update({'urlPost': '/login'})

                            dollar_config = jsondumps(dollar_config_raw)
                            response._content = f'$Config={dollar_config};'.encode()

                        if inject_error == Error.REQUEST_1_HTTP_STATUS:
                            response.status_code = 400

                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'
                    elif method == 'POST' and url.endswith('/GetCredentialType'):
                        data: StrDict = {
                            'FlowToken': 'xxxxxxxxxxxxxxxxxxx',
                            'apiCanary': 'zzzzzzzzzzzz',
                        }

                        if inject_error == Error.REQUEST_2_ERROR_MESSAGE:
                            data = {
                                'error': {
                                    'code': 12345678,
                                    'message': 'error! error!',
                                },
                            }

                        payload = jsondumps(data)
                        response._content = payload.encode()
                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                        if inject_error == Error.REQUEST_2_HTTP_STATUS:
                            response.status_code = 400
                    elif method == 'POST' and url.endswith('/login'):
                        dollar_dict = {
                            'hpgact': 1800,
                            'hpgid': 11,
                            'sFT': 'xxxxxxxxxxxxxxxxxxx',
                            'sCtx': 'yyyyyyyyyyyyyyyyyyy',
                            'apiCanary': 'zzzzzzzzzzzzzzzzzz',
                            'canary': 'canary=1:1',
                            'correlationId': 'aa-bb-cc',
                            'sessionId': 'session-a-b-c',
                            'country': 'SE',
                            'urlGetCredentialType': 'https://test.nu/GetCredentialType?mkt=en-US',
                            'urlPost': '/kmsi',
                        }

                        if inject_error == Error.REQUEST_3_ERROR_MESSAGE:
                            dollar_dict.update(
                                {
                                    'strServiceExceptionMessage': 'failed big time',
                                },
                            )
                        elif inject_error == Error.REQUEST_3_MFA_REQUIRED:
                            dollar_dict.update(
                                {
                                    'arrUserProofs': [
                                        {
                                            'authMethodId': 'fax',
                                            'display': '+46 1234',
                                        },
                                    ],
                                },
                            )
                        elif inject_error is not None and inject_error.value >= 330 and inject_error.value < 400:
                            dollar_dict.update(
                                {
                                    'arrUserProofs': [
                                        {
                                            'authMethodId': 'PhoneAppNotification',
                                            'data': 'PhoneAppNotification',
                                            'display': '+XX XXXXXXXXX',
                                            'isDefault': True,
                                            'isLocationAware': False,
                                        },
                                        {
                                            'authMethodId': 'PhoneAppOTP',
                                            'data': 'PhoneAppOTP',
                                            'display': '+XX XXXXXXXXX',
                                            'isDefault': False,
                                            'isLocationAware': False,
                                            'phoneAppOtpTypes': ['MicrosoftAuthenticatorBasedTOTP', 'SoftwareTokenBasedTOTP'],
                                        },
                                    ],
                                    'urlBeginAuth': 'https://test.nu/common/SAS/BeginAuth',
                                    'urlEndAuth': 'https://test.nu/common/SAS/EndAuth',
                                    'urlPost': 'https://test.nu/common/SAS/ProcessAuth',
                                },
                            )

                        dollar_config = jsondumps(dollar_dict)
                        response._content = f'$Config={dollar_config};'.encode()
                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'
                        if inject_error == Error.REQUEST_3_HTTP_STATUS:
                            response.status_code = 400
                    elif method == 'POST' and url.endswith('/common/SAS/BeginAuth'):
                        headers = kwargs.get('headers', {})
                        assert headers == {
                            'Canary': ANY(str),
                            'Client-Request-Id': ANY(str),
                            'Hpgrequestid': ANY(str),
                            'Hpgact': ANY(str),
                            'Hpgid': ANY(str),
                            'Origin': ANY(str),
                            'Referer': ANY(str),
                        }

                        request_json = kwargs.get('json', {})
                        assert request_json.get('AuthMethodId', None) == 'PhoneAppOTP'
                        assert request_json.get('Method', None) == 'BeginAuth'
                        assert request_json.get('ctx', None) is not None
                        assert request_json.get('flowToken', None) is not None

                        response_json = {
                            'Success': True,
                            'ResultValue': 'Success',
                            'Message': None,
                            'AuthMethod': 'PhoneAppOTP',
                            'ErrCode': 0,
                            'Retry': False,
                            'FlowToken': request_json.get('flowToken', None),
                            'Ctx': request_json.get('ctx', None),
                            'SessionId': headers.get('Hpgrequestid', None),
                            'CorrelationId': headers.get('Client-Request-Id', None),
                            'Timestamp': datetime.now(tz=ZoneInfo('UTC')).isoformat(),
                            'Entropy': 0,
                            'ReselectUIOption': 0,
                        }

                        if inject_error == Error.REQUEST_3_MFA_BEGIN_AUTH_FAILURE:
                            response_json.update(
                                {
                                    'Success': False,
                                    'ResultValue': 'Failure',
                                    'Message': 'some error, probably',
                                    'ErrCode': 1337,
                                },
                            )

                        response._content = jsondumps(response_json).encode('utf-8')
                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                        if inject_error == Error.REQUEST_3_MFA_BEGIN_AUTH_STATUS:
                            response.status_code = 400
                    elif method == 'POST' and url.endswith('/common/SAS/EndAuth'):
                        headers = kwargs.get('headers', {})
                        assert headers.get('Canary', None) is not None
                        assert headers.get('Client-Request-Id', None) is not None
                        assert headers.get('Hpgrequestid', None) is not None
                        assert headers.get('Hpgact', None) is not None
                        assert headers.get('Hpgid', None) is not None

                        request_json = kwargs.get('json', {})
                        assert request_json.get('AdditionalAuthData', None) is not None
                        assert request_json.get('AuthMethodId', None) == 'PhoneAppOTP'
                        assert request_json.get('Ctx', None) is not None
                        assert request_json.get('FlowToken', None) is not None
                        assert request_json.get('Method', None) == 'EndAuth'
                        assert request_json.get('PollCount', None) == 1
                        assert request_json.get('SessionId', None) is not None

                        response_json = {
                            'Success': True,
                            'ResultValue': 'Success',
                            'Message': None,
                            'AuthMethodId': 'PhoneAppOTP',
                            'ErrCode': 0,
                            'Retry': False,
                            'FlowToken': request_json.get('FlowToken', None),
                            'Ctx': request_json.get('Ctx', None),
                            'SessionId': headers.get('Hpgrequestid', None),
                            'CorrelationId': headers.get('Client-Request-Id', None),
                            'Timestamp': datetime.now(tz=ZoneInfo('UTC')).isoformat(),
                            'Entropy': 0,
                            'ReselectUIOption': 0,
                        }

                        if inject_error == Error.REQUEST_3_MFA_END_AUTH_FAILURE:
                            response_json.update(
                                {
                                    'Success': False,
                                    'ResultValue': 'Failure',
                                    'Message': 'some error, for sure',
                                    'ErrCode': 7331,
                                },
                            )

                        response._content = jsondumps(response_json).encode('utf-8')
                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                        if inject_error == Error.REQUEST_3_MFA_END_AUTH_STATUS:
                            response.status_code = 400
                    elif method == 'POST' and url.endswith('/common/SAS/ProcessAuth'):
                        dollar_dict = {
                            'hpgact': 1800,
                            'hpgid': 11,
                            'sFT': 'xxxxxxxxxxxxxxxxxxx',
                            'sCtx': 'yyyyyyyyyyyyyyyyyyy',
                            'apiCanary': 'zzzzzzzzzzzzzzzzzz',
                            'canary': 'canary=1:1',
                            'correlationId': 'aa-bb-cc',
                            'sessionId': 'session-a-b-c',
                            'country': 'SE',
                            'urlGetCredentialType': 'https://test.nu/GetCredentialType?mkt=en-US',
                            'urlPost': '/kmsi',
                        }

                        if inject_error == Error.REQUEST_3_MFA_PROCESS_AUTH_FAILURE:
                            dollar_dict.update({'strServiceExceptionMessage': 'service failure'})

                        dollar_config = jsondumps(dollar_dict)
                        response._content = f'$Config={dollar_config};'.encode()
                        response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                        if inject_error == Error.REQUEST_3_MFA_PROCESS_AUTH_STATUS:
                            response.status_code = 500
                    elif method == 'POST' and url.endswith('/kmsi'):
                        if inject_error == Error.REQUEST_4_HTTP_STATUS:
                            if login_start == 'redirect_uri':
                                response.status_code = 200
                            else:
                                response.status_code = 302
                        elif inject_error == Error.REQUEST_4_HTTP_STATUS_CONFIG:
                            response.status_code = 400
                            dollar_config = jsondumps(
                                {
                                    'strServiceExceptionMessage': 'error! error! error!',
                                },
                            )
                            response._content = f'$Config={dollar_config};'.encode()
                        elif login_start == 'redirect_uri':
                            response.status_code = 302
                        else:
                            response.status_code = 200

                        if login_start == 'redirect_uri':
                            redirect_uri_parsed = urlparse(credential.redirect)
                            redirect_uri = f'{credential.host}{credential.redirect}' if len(redirect_uri_parsed.netloc) == 0 else credential.redirect

                            token_name = 'code' if is_token_v2_0 else 'id_token'

                            response.headers['Location'] = f'{redirect_uri}#{token_name}={access_token.token}'
                        elif inject_error != Error.REQUEST_4_HTTP_STATUS_CONFIG:
                            response._content = f"""<form action="https://www.example.com/app/login/signin-oidc" method="post">
                                <input type="hidden" name="id_token" value="{access_token.token}"/>
                                <input type="hidden" name="client_info" value="0000aaaa1111bbbb"/>
                                <input type="hidden" name="state" value="1111bbbb2222cccc"/>
                                <input type="hidden" name="session_state" value="2222cccc3333dddd"/>
                            </form>
                            """.encode()
                    elif method == 'POST' and url.endswith('/signin-oidc'):
                        if inject_error == Error.REQUEST_5_HTTP_STATUS:
                            response.status_code = 500
                        elif inject_error != Error.REQUEST_5_NO_COOKIE:
                            self.cookies.set_cookie(create_cookie('auth', access_token.token, domain='example.com'))
                    else:
                        response._content = jsondumps({'error_description': 'error'}).encode('utf-8')

                    return response

                mocker.patch(
                    'requests.Session.request',
                    request,
                )

            auth_user_uri = 'http://www.example.com/app/authenticated' if login_start == 'redirect_uri' else 'http://www.example.com/app/login'

            parent.user._context = {
                'host': 'https://www.example.com',
                'auth': {
                    'client': {
                        'id': 'aaaa',
                    },
                    'user': {
                        'username': 'test-user@example.com',
                        'password': 'H3ml1g4Arn3',
                        'redirect_uri': None,
                        'initialize_uri': None,
                    },
                    'provider': None,
                },
            }
            parent.user.host = cast('str', parent.user._context['host'])

            mock_request_session()

            # both initialize and provider uri set
            credential.redirect = credential.initialize = auth_user_uri

            with pytest.raises(AzureAadError, match='both initialize and redirect URIs cannot be set'):
                credential.get_oauth_authorization()

            credential.redirect = credential.initialize = None

            attr_name, _ = login_start.split('_', 1)
            setattr(credential, attr_name, auth_user_uri)

            # test when login sequence returns bad request
            mock_request_session(Error.REQUEST_1_HTTP_STATUS)

            authorize_url = 'https://login.microsoftonline.com/example.com' if login_start == 'redirect_uri' else 'https://login.example.com'

            with pytest.raises(AzureAadFlowError, match=f'user auth request 1: {authorize_url}/oauth2/v2.0/authorize had unexpected status code 400'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_2_HTTP_STATUS)

            with pytest.raises(AzureAadFlowError, match='user auth request 2: https://login.example.com/GetCredentialType had unexpected status code 400'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_3_HTTP_STATUS)

            with pytest.raises(AzureAadFlowError, match='user auth request 3: https://login.example.com/login had unexpected status code 400'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_3_ERROR_MESSAGE)

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match='failed big time'):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == 'failed big time'
            caplog.clear()

            mock_request_session(Error.REQUEST_3_MFA_REQUIRED)

            expected_error_message = 'test-user@example.com requires MFA for login: fax = +46 1234'

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match=re.escape(expected_error_message)):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            credential.otp_secret = 'abcdefghij'  # noqa: S105

            expected_error_message = 'test-user@example.com is assumed to use TOTP for MFA, but does not have that authentication method configured'

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match=re.escape(expected_error_message)):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            credential.otp_secret = None
            expected_status_code = 200 if login_start == 'redirect_uri' else 302

            mock_request_session(Error.REQUEST_4_HTTP_STATUS)
            with pytest.raises(AzureAadFlowError, match=f'user auth request 4: https://login.example.com/kmsi had unexpected status code {expected_status_code}'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_4_HTTP_STATUS_CONFIG)
            with pytest.raises(AzureAadFlowError, match='error! error! error!'):
                credential.get_oauth_authorization()

            # test error handling when login sequence response doesn't contain expected payload
            mock_request_session(Error.REQUEST_1_NO_DOLLAR_CONFIG)
            with pytest.raises(AzureAadFlowError, match=f'no config found in response from {authorize_url}/oauth2/v2.0/authorize'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_1_MISSING_STATE)
            with pytest.raises(AzureAadFlowError, match=f'unexpected response body from {authorize_url}/oauth2/v2.0/authorize: missing "hpgact" in config'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_1_DOLLAR_CONFIG_ERROR)
            with pytest.raises(AzureAadFlowError, match='oh no!'):
                credential.get_oauth_authorization()

            mock_request_session(Error.REQUEST_2_ERROR_MESSAGE)
            with pytest.raises(AzureAadFlowError, match='error response from https://login.example.com/GetCredentialType: code=12345678, message=error! error!'):
                credential.get_oauth_authorization()

            # successful login sequence
            mock_request_session()

            assert credential.get_oauth_authorization() == SOME(AccessToken, token=access_token.token)

            if not is_token_v2_0 or login_start == 'initialize_uri':
                get_oauth_token_mock.assert_not_called()
            else:
                get_oauth_token_mock.assert_called_once_with(code='asdf', verifier=ANY(str), tenant_id='example.com')
                get_oauth_token_mock.reset_mock()

            # test no host in redirect/initialize uri
            auth_user_uri = '/app/authenticated' if login_start == 'redirect_uri' else '/app/login'

            attr_name, _ = login_start.split('_', 1)
            setattr(credential, attr_name, auth_user_uri)

            assert credential.get_oauth_authorization() == SOME(AccessToken, token=access_token.token)

            if login_start == 'initialize_uri':
                mock_request_session(Error.REQUEST_5_HTTP_STATUS)
                with pytest.raises(AzureAadFlowError, match='user auth request 5: https://www.example.com/app/login/signin-oidc had unexpected status code 500'):
                    credential.get_oauth_authorization()

                mock_request_session(Error.REQUEST_5_NO_COOKIE)
                with pytest.raises(AzureAadFlowError, match='did not find AAD cookie in authorization flow response session'):
                    credential.get_oauth_authorization()

            get_oauth_token_mock.reset_mock()

            # <!-- OTP
            # auth.user.otp_secret not set
            mock_request_session(Error.REQUEST_3_MFA_TOPT)

            credential.otp_secret = None

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match='test-user@example.com requires TOTP for MFA, but auth.user.otp_secret is not set'):
                credential.get_oauth_authorization()

            # LC_ALL=C tr -dc 'A-Z2-7' </dev/urandom | head -c 16; echo
            credential.otp_secret = '466FCZN2PQZTGOEJ'  # noqa: S105

            # BeginAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_BEGIN_AUTH_STATUS)
            with (
                caplog.at_level(logging.ERROR),
                pytest.raises(
                    AzureAadFlowError,
                    match='user auth request BeginAuth: https://test.nu/common/SAS/BeginAuth had unexpected status code 400',
                ),
            ):
                credential.get_oauth_authorization()

            # BeginAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_BEGIN_AUTH_FAILURE)

            expected_error_message = 'user auth request BeginAuth: 1337 - some error, probably'

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match=expected_error_message):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # EndAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_END_AUTH_STATUS)

            with (
                caplog.at_level(logging.ERROR),
                pytest.raises(
                    AzureAadFlowError,
                    match='user auth request EndAuth: https://test.nu/common/SAS/EndAuth had unexpected status code 400',
                ),
            ):
                credential.get_oauth_authorization()

            # EndAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_END_AUTH_FAILURE)

            expected_error_message = 'user auth request EndAuth: 7331 - some error, for sure'

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match=expected_error_message):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # ProcessAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_PROCESS_AUTH_STATUS)

            with (
                caplog.at_level(logging.ERROR),
                pytest.raises(
                    AzureAadFlowError,
                    match='user auth request ProcessAuth: https://test.nu/common/SAS/ProcessAuth had unexpected status code 500',
                ),
            ):
                credential.get_oauth_authorization()

            # ProcessAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_PROCESS_AUTH_FAILURE)

            expected_error_message = 'service failure'

            with caplog.at_level(logging.ERROR), pytest.raises(AzureAadFlowError, match=expected_error_message):
                credential.get_oauth_authorization()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # Successful MFA flow
            mock_request_session(Error.REQUEST_3_MFA_TOPT)

            assert credential.get_oauth_authorization() == SOME(AccessToken, token=access_token.token)

            if not is_token_v2_0 or login_start == 'initialize_uri':
                get_oauth_token_mock.assert_not_called()
            else:
                get_oauth_token_mock.assert_called_once_with(code='asdf', verifier=ANY(str), tenant_id='example.com')
                get_oauth_token_mock.reset_mock()
            # // OTP -->
        finally:
            parent.user.__class__.__name__ = original_class_name

    @pytest.mark.parametrize(
        'grant_type',
        [
            'client_credentials::v2',
            'authorization_code::v2',
        ],
    )
    def test_get_oauth_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, grant_type: str) -> None:  # noqa: PLR0915
        parent = grizzly_fixture(user_type=RestApiUser)

        grant_type, _ = grant_type.split('::', 1)  # always v2.0 now

        original_class_name = parent.user.__class__.__name__
        try:
            parent.user.__class__.__name__ = f'{parent.user.__class__.__name__}_001'

            def mock_requests_post(payload: str, status_code: int) -> MagicMock:
                response = Response()
                response.status_code = status_code
                response._content = payload.encode()

                return mocker.patch('grizzly_common.azure.aad.requests.Session.post', return_value=response)

            assert isinstance(parent.user, RestApiUser)

            access_token = AccessToken('asdf', expires_on=1)
            credential = AzureAadCredential(
                'test-user@example.com',
                'secret',
                'example.com',
                AuthMethod.CLIENT,
                host='https://example.com',
                client_id='asdf',
                redirect='https://example.com/auth',
            )

            if grant_type == 'authorization_code':
                pkcs = {
                    'code': 'code',
                    'verifier': 'code_verifier',
                }
                token_name = 'id_token'  # noqa: S105
            else:
                pkcs = {}
                token_name = 'access_token'  # noqa: S105

            parent.user._context['host'] = parent.user.host = 'https://example.com'
            credential.host = 'https://example.com'

            payload = jsondumps({'error_description': 'fake error message'})
            requests_mock = mock_requests_post(payload, 400)

            with pytest.raises(AzureAadFlowError, match='fake error message'):
                credential.get_oauth_token(resource='asdf', **pkcs)

            data: dict[str, str] = {}
            if pkcs == {}:
                data.update(
                    {
                        'scope': 'asdf',
                        'tenant': 'example.com',
                    },
                )
            else:
                data.update(
                    {
                        'redirect_uri': 'https://example.com/auth',
                        'code': pkcs['code'],
                        'code_verifier': pkcs['verifier'],
                    },
                )

            requests_mock.assert_called_once_with(
                'https://login.microsoftonline.com/example.com/oauth2/v2.0/token',
                verify=True,
                data=SOME(dict, grant_type=grant_type, client_id='asdf', **data),
                headers=ANY(dict),
                allow_redirects=(pkcs == {}),
            )
            requests_mock.reset_mock()

            parent.user.metadata.update(
                {
                    'Authorization': 'foobar',
                    'Content-Type': 'plain/text',
                    'Ocp-Apim-Subscription-Key': 'secret',
                },
            )
            parent.user._context['verify_certificates'] = False

            requests_mock = mock_requests_post(jsondumps({token_name: 'asdf'}), 200)

            assert credential.get_oauth_token(resource='asdf', **pkcs) == SOME(AccessToken, token=access_token.token)

            data = {}
            headers: dict[str, str] = {}
            if pkcs == {}:
                data.update({'scope': 'asdf', 'tenant': 'example.com'})
            else:
                data.update(
                    {
                        'redirect_uri': f'{parent.user.host}/auth',
                        'code': pkcs['code'],
                        'code_verifier': pkcs['verifier'],
                    },
                )
                headers.update({'Origin': 'https://example.com', 'Referer': 'https://example.com'})

            requests_mock.assert_called_once_with(
                'https://login.microsoftonline.com/example.com/oauth2/v2.0/token',
                verify=True,
                data=SOME(dict, grant_type=grant_type, client_id='asdf', **data),
                headers=ANY(dict),
                allow_redirects=(pkcs == {}),
            )
            _, kwargs = requests_mock.call_args_list[-1]
            headers = kwargs.get('headers', None)

            assert 'Authorization' not in headers
            assert 'Content-Type' not in headers
            assert 'Ocp-Apim-Subscription-Key' not in headers
        finally:
            parent.user.__class__.__name__ = original_class_name

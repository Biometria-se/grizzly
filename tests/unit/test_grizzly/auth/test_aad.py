"""Unit tests of grizzly.auth.aad."""
from __future__ import annotations

import logging
from datetime import datetime
from enum import Enum
from itertools import product
from json import dumps as jsondumps
from time import time
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, cast
from urllib.parse import urlparse

import pytest
from requests.cookies import create_cookie
from requests.models import Response

from grizzly.auth import AAD, AuthMethod, AuthType
from grizzly.types import ZoneInfo
from grizzly.types.locust import StopUser
from grizzly.users import RestApiUser
from grizzly.utils import safe_del
from tests.helpers import ANY, SOME

if TYPE_CHECKING:  # pragma: no cover
    from unittest.mock import MagicMock

    import requests
    from _pytest.logging import LogCaptureFixture

    from tests.fixtures import GrizzlyFixture, MockerFixture


class TestAAD:
    def test_get_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)

        get_oauth_token_mock = mocker.patch(
            'grizzly.auth.aad.AAD.get_oauth_token',
            return_value=None,
        )

        get_oauth_authorization_mock = mocker.patch(
            'grizzly.auth.aad.AAD.get_oauth_authorization',
            return_value=None,
        )

        AAD.get_token(parent.user, AuthMethod.CLIENT)
        get_oauth_authorization_mock.assert_not_called()
        get_oauth_token_mock.assert_called_once_with(parent.user)
        get_oauth_token_mock.reset_mock()

        AAD.get_token(parent.user, AuthMethod.USER)
        get_oauth_token_mock.assert_not_called()
        get_oauth_authorization_mock.assert_called_once_with(parent.user)
        get_oauth_authorization_mock.reset_mock()

    @pytest.mark.skip(reason='needs real secrets')
    def test_get_oauth_authorization_real_initialize_uri(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        from grizzly.tasks.clients import HttpClientTask
        from grizzly.types import RequestDirection

        parent = grizzly_fixture(user_type=RestApiUser)
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables['test_payload'] = 'none'

        task_factory = HttpClientTask(
            RequestDirection.FROM,
            '',
            payload_variable='test_payload',
        )

        parent.user._context.update({
            'host': {
                'auth': {
                    'user': {
                        'username': '',
                        'password': '',
                        'initialize_uri': '',
                    },
                },
                'verify_certificates': False,
            },
        })

        task = task_factory()

        task.on_start(parent)

        with caplog.at_level(logging.DEBUG):
            task(parent)

        payload = parent.user._context['variables'].get('test_payload', None)
        assert payload is not None

    @pytest.mark.skip(reason='needs real secrets')
    def test_get_oauth_authorization_provider(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        from grizzly.tasks.clients import HttpClientTask
        from grizzly.types import RequestDirection

        parent = grizzly_fixture(user_type=RestApiUser)
        grizzly = grizzly_fixture.grizzly
        grizzly.state.variables['test_payload'] = 'none'

        task_factory = HttpClientTask(
            RequestDirection.FROM,
            '',
            payload_variable='test_payload',
        )

        parent.user._context.update({
            'host': {
                'auth': {
                    'provider': '',
                    'client': {
                        'id': '',
                    },
                    'user': {
                        'username': '',
                        'password': '',
                        'redirect_uri': '',
                    },
                },
            },
        })
        task_factory.metadata.update({'Ocp-Apim-Subscription-Key': ''})

        task = task_factory()

        task.on_start(parent)

        with caplog.at_level(logging.DEBUG):
            task(parent)

    @pytest.mark.parametrize(('version', 'login_start'), product(['v1.0', 'v2.0'], ['initialize_uri', 'redirect_uri']))
    def test_get_oauth_authorization(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture, version: str, login_start: str) -> None:  # noqa: PLR0915, C901
        parent = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(parent.user, RestApiUser)
        original_class_name = parent.user.__class__.__name__
        try:
            fake_token = 'asdf'  # noqa: S105

            is_token_v2_0 = version == 'v2.0'

            fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')
            mocker.patch('grizzly.auth.aad.time_perf_counter', return_value=0.0)
            get_oauth_token_mock = mocker.patch.object(AAD, 'get_oauth_token', return_value=None)

            if is_token_v2_0:
                get_oauth_token_mock.return_value = (AuthType.HEADER, fake_token)

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

            def mock_request_session(inject_error: Optional[Error] = None) -> None:  # noqa: PLR0915, C901
                def request(self: requests.Session, method: str, url: str, name: Optional[str] = None, **kwargs: Dict[str, Any]) -> requests.Response:  # noqa: ARG001, PLR0915, C901, PLR0912
                    response = Response()
                    response.status_code = 200
                    response.url = url
                    self.cookies.clear()

                    if method == 'GET' and (url.endswith(('/authorize', '/app/login'))):
                        if url.endswith('/app/login'):
                            affix = '/v2.0' if version == 'v2.0' else ''
                            response.url = f'https://login.example.com/oauth2{affix}/authorize'

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
                        data: Dict[str, Any] = {
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
                        response._content = payload.encode('utf-8')
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
                            dollar_dict.update({
                                'strServiceExceptionMessage': 'failed big time',
                            })
                        elif inject_error == Error.REQUEST_3_MFA_REQUIRED:
                            dollar_dict.update({
                                'arrUserProofs': [{
                                    'authMethodId': 'fax',
                                    'display': '+46 1234',
                                }],
                            })
                        elif inject_error is not None and inject_error.value >= 330 and inject_error.value < 400:
                            dollar_dict.update({
                                'arrUserProofs': [{
                                    'authMethodId': 'PhoneAppNotification',
                                    'data': 'PhoneAppNotification',
                                    'display': '+XX XXXXXXXXX',
                                    'isDefault': True,
                                    'isLocationAware': False,
                                }, {
                                    'authMethodId': 'PhoneAppOTP',
                                    'data': 'PhoneAppOTP',
                                    'display': '+XX XXXXXXXXX',
                                    'isDefault': False,
                                    'isLocationAware': False,
                                    'phoneAppOtpTypes': ['MicrosoftAuthenticatorBasedTOTP', 'SoftwareTokenBasedTOTP'],
                                }],
                                'urlBeginAuth': 'https://test.nu/common/SAS/BeginAuth',
                                'urlEndAuth': 'https://test.nu/common/SAS/EndAuth',
                                'urlPost': 'https://test.nu/common/SAS/ProcessAuth',
                            })

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
                            response_json.update({
                                'Success': False,
                                'ResultValue': 'Failure',
                                'Message': 'some error, probably',
                                'ErrCode': 1337,
                            })

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
                            response_json.update({
                                'Success': False,
                                'ResultValue': 'Failure',
                                'Message': 'some error, for sure',
                                'ErrCode': 7331,
                            })

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
                            dollar_config = jsondumps({
                                'strServiceExceptionMessage': 'error! error! error!',
                            })
                            response._content = f'$Config={dollar_config};'.encode()
                        elif login_start == 'redirect_uri':
                            response.status_code = 302
                        else:
                            response.status_code = 200

                        auth_user_context = parent.user._context['auth']['user']

                        if login_start == 'redirect_uri':
                            redirect_uri_parsed = urlparse(auth_user_context['redirect_uri'])
                            if len(redirect_uri_parsed.netloc) == 0:
                                redirect_uri = f"{parent.user._context['host']}{auth_user_context['redirect_uri']}"
                            else:
                                redirect_uri = auth_user_context['redirect_uri']

                            token_name = 'code' if is_token_v2_0 else 'id_token'

                            response.headers['Location'] = f'{redirect_uri}#{token_name}={fake_token}'
                        elif inject_error != Error.REQUEST_4_HTTP_STATUS_CONFIG:
                            response._content = f"""<form action="https://www.example.com/app/login/signin-oidc" method="post">
                                <input type="hidden" name="id_token" value="{fake_token}"/>
                                <input type="hidden" name="client_info" value="0000aaaa1111bbbb"/>
                                <input type="hidden" name="state" value="1111bbbb2222cccc"/>
                                <input type="hidden" name="session_state" value="2222cccc3333dddd"/>
                            </form>
                            """.encode()
                    elif method == 'POST' and url.endswith('/signin-oidc'):
                        if inject_error == Error.REQUEST_5_HTTP_STATUS:
                            response.status_code = 500
                        elif inject_error != Error.REQUEST_5_NO_COOKIE:
                            self.cookies.set_cookie(create_cookie('auth', fake_token, domain='www.example.com'))
                    else:
                        response._content = jsondumps({'error_description': 'error'}).encode('utf-8')

                    return response

                mocker.patch(
                    'requests.Session.request',
                    request,
                )

            session_started = time()

            auth_user_uri = 'http://www.example.com/app/authenticated' if login_start == 'redirect_uri' else 'http://www.example.com/app/login'

            provider_url = 'https://login.example.com/oauth2'
            if version != 'v1.0':
                provider_url = f'{provider_url}/{version}'

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
            parent.user.host = cast(str, parent.user._context['host'])

            mock_request_session()

            # both initialize and provider uri set
            cast(dict, parent.user._context['auth'])['user'].update({'initialize_uri': auth_user_uri, 'redirect_uri': auth_user_uri})

            with pytest.raises(AssertionError, match='both auth.user.initialize_uri and auth.user.redirect_uri is set'):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_not_called()

            cast(dict, parent.user._context['auth'])['user'].update({'initialize_uri': None, 'redirect_uri': None})

            cast(dict, parent.user._context['auth'])['user'].update({login_start: auth_user_uri})

            if login_start == 'redirect_uri':
                # test when auth.provider is not set
                with pytest.raises(AssertionError, match='context variable auth.provider is not set'):
                    AAD.get_oauth_authorization(parent.user)

                fire_spy.assert_not_called()

                parent.user._context['auth']['provider'] = provider_url

            # test when login sequence returns bad request
            mock_request_session(Error.REQUEST_1_HTTP_STATUS)

            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=f'user auth request 1: {provider_url}/authorize had unexpected status code 400'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_2_HTTP_STATUS)

            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='user auth request 2: https://login.example.com/GetCredentialType had unexpected status code 400'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_3_HTTP_STATUS)

            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='user auth request 3: https://login.example.com/login had unexpected status code 400'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_3_ERROR_MESSAGE)

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='failed big time'),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == 'failed big time'
            caplog.clear()

            mock_request_session(Error.REQUEST_3_MFA_REQUIRED)

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            expected_error_message = 'test-user@example.com requires MFA for login: fax = +46 1234'

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=expected_error_message),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            parent.user._context['auth']['user']['otp_secret'] = 'abcdefghij'  # noqa: S105

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            expected_error_message = 'test-user@example.com is assumed to use TOTP for MFA, but does not have that authentication method configured'

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=expected_error_message),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            parent.user._context['auth']['user']['otp_secret'] = None

            mock_request_session(Error.REQUEST_4_HTTP_STATUS)

            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            assert parent.user.session_started is None
            expected_status_code = 200 if login_start == 'redirect_uri' else 302

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=f'user auth request 4: https://login.example.com/kmsi had unexpected status code {expected_status_code}'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_4_HTTP_STATUS_CONFIG)

            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            assert parent.user.session_started is None

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='error! error! error!'),
            )
            fire_spy.reset_mock()

            # test error handling when login sequence response doesn't contain expected payload
            mock_request_session(Error.REQUEST_1_NO_DOLLAR_CONFIG)
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(ValueError, message=f'no config found in response from {provider_url}/authorize'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_1_MISSING_STATE)
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(ValueError, message=f'unexpected response body from {provider_url}/authorize: missing "hpgact" in config'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_1_DOLLAR_CONFIG_ERROR)
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='oh no!'),
            )
            fire_spy.reset_mock()

            mock_request_session(Error.REQUEST_2_ERROR_MESSAGE)
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='error response from https://login.example.com/GetCredentialType: code=12345678, message=error! error!'),
            )
            fire_spy.reset_mock()

            # successful login sequence
            mock_request_session()

            parent.user.session_started = session_started

            expected_auth: Tuple[AuthType, str]
            expected_auth = (AuthType.HEADER, fake_token) if login_start == 'redirect_uri' else (AuthType.COOKIE, f'auth={fake_token}')

            assert AAD.get_oauth_authorization(parent.user) == expected_auth

            if not is_token_v2_0 or login_start == 'initialize_uri':
                get_oauth_token_mock.assert_not_called()
            else:
                get_oauth_token_mock.assert_called_once_with(parent.user, ('asdf', ANY(str)))
                get_oauth_token_mock.reset_mock()

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=None,
            )
            fire_spy.reset_mock()

            # test no host in redirect/initialize uri
            auth_user_uri = '/app/authenticated' if login_start == 'redirect_uri' else '/app/login'

            parent.user._context['auth']['user'][login_start] = auth_user_uri

            assert AAD.get_oauth_authorization(parent.user) == expected_auth

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=None,
            )
            fire_spy.reset_mock()

            if login_start == 'initialize_uri':
                mock_request_session(Error.REQUEST_5_HTTP_STATUS)
                with pytest.raises(StopUser):
                    AAD.get_oauth_authorization(parent.user)

                fire_spy.assert_called_once_with(
                    request_type='AUTH',
                    response_time=0,
                    name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                    context=parent.user._context,
                    response_length=0,
                    response=None,
                    exception=ANY(RuntimeError, message='user auth request 5: https://www.example.com/app/login/signin-oidc had unexpected status code 500'),
                )
                fire_spy.reset_mock()

                mock_request_session(Error.REQUEST_5_NO_COOKIE)
                with pytest.raises(StopUser):
                    AAD.get_oauth_authorization(parent.user)

                fire_spy.assert_called_once_with(
                    request_type='AUTH',
                    response_time=0,
                    name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                    context=parent.user._context,
                    response_length=0,
                    response=None,
                    exception=ANY(RuntimeError, message='did not find AAD cookie in authorization flow response session'),
                )
                fire_spy.reset_mock()

            get_oauth_token_mock.reset_mock()

            # <!-- OTP
            # auth.user.otp_secret not set
            mock_request_session(Error.REQUEST_3_MFA_TOPT)

            parent.user._context['auth']['user']['otp_secret'] = None

            parent.user.session_started = session_started

            expected_auth = (AuthType.HEADER, fake_token) if login_start == 'redirect_uri' else (AuthType.COOKIE, f'auth={fake_token}')

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(AssertionError, message='test-user@example.com requires TOTP for MFA, but auth.user.otp_secret is not set'),
            )
            fire_spy.reset_mock()

            # LC_ALL=C tr -dc 'A-Z2-7' </dev/urandom | head -c 16; echo
            parent.user._context['auth']['user']['otp_secret'] = '466FCZN2PQZTGOEJ'  # noqa: S105

            # BeginAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_BEGIN_AUTH_STATUS)

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='user auth request BeginAuth: https://test.nu/common/SAS/BeginAuth had unexpected status code 400'),
            )
            fire_spy.reset_mock()

            # BeginAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_BEGIN_AUTH_FAILURE)

            expected_error_message = 'user auth request BeginAuth: 1337 - some error, probably'

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=expected_error_message),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # EndAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_END_AUTH_STATUS)

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='user auth request EndAuth: https://test.nu/common/SAS/EndAuth had unexpected status code 400'),
            )
            fire_spy.reset_mock()

            # EndAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_END_AUTH_FAILURE)

            expected_error_message = 'user auth request EndAuth: 7331 - some error, for sure'

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=expected_error_message),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # ProcessAuth, response status
            mock_request_session(Error.REQUEST_3_MFA_PROCESS_AUTH_STATUS)

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message='user auth request ProcessAuth: https://test.nu/common/SAS/ProcessAuth had unexpected status code 500'),
            )
            fire_spy.reset_mock()

            # ProcessAuth, payload failure
            mock_request_session(Error.REQUEST_3_MFA_PROCESS_AUTH_FAILURE)

            expected_error_message = 'service failure'

            with caplog.at_level(logging.ERROR), pytest.raises(StopUser):
                AAD.get_oauth_authorization(parent.user)

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=ANY(RuntimeError, message=expected_error_message),
            )
            fire_spy.reset_mock()

            assert caplog.messages[-1] == expected_error_message
            caplog.clear()

            # Successful MFA flow
            mock_request_session(Error.REQUEST_3_MFA_TOPT)

            assert AAD.get_oauth_authorization(parent.user) == expected_auth

            if not is_token_v2_0 or login_start == 'initialize_uri':
                get_oauth_token_mock.assert_not_called()
            else:
                get_oauth_token_mock.assert_called_once_with(parent.user, ('asdf', ANY(str)))
                get_oauth_token_mock.reset_mock()

            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=0,
                name=f'001 AAD OAuth2 user token {version}: test-user@example.com',
                context=parent.user._context,
                response_length=0,
                response=None,
                exception=None,
            )
            fire_spy.reset_mock()
            # // OTP -->
        finally:
            parent.user.__class__.__name__ = original_class_name

    @pytest.mark.parametrize('grant_type', [
        'client_credentials::v1',
        'client_credentials::v2',
        'authorization_code::v2',
    ])
    def test_get_oauth_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, grant_type: str) -> None:  # noqa: PLR0912, PLR0915
        parent = grizzly_fixture(user_type=RestApiUser)

        grant_type, version = grant_type.split('::', 1)

        original_class_name = parent.user.__class__.__name__
        try:
            parent.user.__class__.__name__ = f'{parent.user.__class__.__name__}_001'

            def mock_requests_post(payload: str, status_code: int) -> MagicMock:
                response = Response()
                response.status_code = status_code
                response._content = payload.encode()

                return mocker.patch('grizzly.auth.aad.requests.Session.post', return_value=response)

            assert isinstance(parent.user, RestApiUser)

            provider_url = 'https://login.example.com/foobarinc/oauth2'

            if grant_type == 'authorization_code':
                pkcs = ('code', 'code_verifier')
                token_name = 'id_token'  # noqa: S105
            else:
                pkcs = None
                token_name = 'access_token'  # noqa: S105
                if version == 'v2':
                    provider_url = f'{provider_url}/v2.0'

            fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

            safe_del(parent.user._context, 'auth')

            with pytest.raises(AssertionError, match='context variable auth is not set'):
                AAD.get_oauth_token(parent.user, pkcs)

            parent.user._context['auth'] = {}

            with pytest.raises(AssertionError, match='context variable auth.provider is not set'):
                AAD.get_oauth_token(parent.user, pkcs)

            parent.user._context['auth'].update({'provider': provider_url})

            with pytest.raises(AssertionError, match='context variable auth.client is not set'):
                AAD.get_oauth_token(parent.user, pkcs)

            parent.user._context['auth'].update({'client': {'id': None, 'secret': None, 'resource': None}})

            if grant_type == 'authorization_code':
                with pytest.raises(AssertionError, match='context variable auth.user is not set'):
                    AAD.get_oauth_token(parent.user, pkcs)

                parent.user._context['auth'].update({'user': {'username': None, 'password': None, 'redirect_uri': '/auth', 'initialize_uri': None}})

            parent.user._context['host'] = parent.user.host = 'https://example.com'
            parent.user._context['auth'].update({
                'provider': provider_url,
                'client': {
                    'id': 'asdf',
                    'secret': 'asdf',
                    'resource': 'asdf',
                },
            })

            payload = jsondumps({'error_description': 'fake error message'})
            requests_mock = mock_requests_post(payload, 400)

            session_started = time()

            assert parent.user.session_started is None

            with pytest.raises(StopUser):
                AAD.get_oauth_token(parent.user, pkcs)

            data: Dict[str, str] = {}
            if pkcs is None:
                if version == 'v1':
                    data.update({'resource': 'asdf'})
                else:
                    data.update({
                        'scope': 'asdf',
                        'tenant': 'foobarinc',
                    })
            else:
                data.update({
                    'redirect_uri': f'{parent.user.host}/auth',
                    'code': pkcs[0],
                    'code_verifier': pkcs[1],
                })

            requests_mock.assert_called_once_with(
                f'{provider_url}/token',
                verify=True,
                data=SOME(dict, grant_type=grant_type, client_id='asdf', **data),
                headers=ANY(dict),
                allow_redirects=(pkcs is None),
            )
            requests_mock.reset_mock()

            assert parent.user.session_started is None

            if pkcs is not None:
                fire_spy.assert_not_called()
            else:
                fire_spy.assert_called_once_with(
                    request_type='AUTH',
                    response_time=ANY(int),
                    name=f'001 AAD OAuth2 client token {version}.0: asdf',
                    context=parent.user._context,
                    response=None,
                    exception=ANY(RuntimeError, message='fake error message'),
                    response_length=len(payload.encode()),
                )
                fire_spy.reset_mock()

            parent.user.session_started = session_started
            parent.user.metadata.update({
                'Authorization': 'foobar',
                'Content-Type': 'plain/text',
                'Ocp-Apim-Subscription-Key': 'secret',
            })
            parent.user._context['verify_certificates'] = False

            requests_mock = mock_requests_post(jsondumps({token_name: 'asdf'}), 200)

            assert AAD.get_oauth_token(parent.user, pkcs) == (AuthType.HEADER, 'asdf')

            assert parent.user.session_started >= session_started
            data = {}
            headers: Dict[str, str] = {}
            if pkcs is None:
                if version == 'v1':
                    data.update({'resource': 'asdf'})
                else:
                    data.update({'scope': 'asdf', 'tenant': 'foobarinc'})
            else:
                data.update({
                    'redirect_uri': f'{parent.user.host}/auth',
                    'code': pkcs[0],
                    'code_verifier': pkcs[1],
                })
                headers.update({'Origin': 'https://example.com', 'Referer': 'https://example.com'})

            requests_mock.assert_called_once_with(
                f'{provider_url}/token',
                verify=True,
                data=SOME(dict, grant_type=grant_type, client_id='asdf', **data),
                headers=ANY(dict),
                allow_redirects=(pkcs is None),
            )
            _, kwargs = requests_mock.call_args_list[-1]
            headers = kwargs.get('headers', None)

            assert 'Authorization' not in headers
            assert 'Content-Type' not in headers
            assert 'Ocp-Apim-Subscription-Key' not in headers

            parent.user._context['auth'].update({'provider': None})

            with pytest.raises(AssertionError, match='context variable auth.provider is not set'):
                AAD.get_oauth_token(parent.user, pkcs)
        finally:
            parent.user.__class__.__name__ = original_class_name

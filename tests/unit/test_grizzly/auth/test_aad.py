import logging

from typing import Any, Dict, Optional, cast
from time import time
from json import dumps as jsondumps
from enum import Enum
from unittest.mock import ANY, MagicMock
from urllib.parse import urlparse

import pytest
import requests

from requests.models import Response
from _pytest.logging import LogCaptureFixture

from grizzly.auth import AuthMethod, AAD, GrizzlyAuthHttpContext, GrizzlyAuthHttpContextUser
from grizzly.users import RestApiUser
from grizzly.types.locust import StopUser
from grizzly.utils import safe_del

from tests.fixtures import MockerFixture, GrizzlyFixture


class TestAAD:
    def test_get_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, user, _ = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(user, RestApiUser)

        get_oauth_token_mock = mocker.patch(
            'grizzly.auth.aad.AAD.get_oauth_token',
            return_value=None,
        )

        get_oauth_authorization_mock = mocker.patch(
            'grizzly.auth.aad.AAD.get_oauth_authorization',
            return_value=None,
        )

        AAD.get_token(user, AuthMethod.CLIENT)
        get_oauth_authorization_mock.assert_not_called()
        get_oauth_token_mock.assert_called_once_with(user)
        get_oauth_token_mock.reset_mock()

        AAD.get_token(user, AuthMethod.USER)
        get_oauth_token_mock.assert_not_called()
        get_oauth_authorization_mock.assert_called_once_with(user)
        get_oauth_authorization_mock.reset_mock()

    @pytest.mark.parametrize('version', ['v1.0', 'v2.0'])
    def test_get_oauth_authorization(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, caplog: LogCaptureFixture, version: str) -> None:
        _, user, _ = grizzly_fixture(user_type=RestApiUser)

        assert isinstance(user, RestApiUser)
        user.__class__.__name__ = f'{user.__class__.__name__}_001'

        fake_token = 'asdf'

        is_token_v2_0 = version == 'v2.0'

        fire_spy = mocker.spy(user.environment.events.request, 'fire')
        mocker.patch('grizzly.auth.aad.time_perf_counter', return_value=0.0)
        get_oauth_token_mock = mocker.patch.object(AAD, 'get_oauth_token', return_value=None)

        if is_token_v2_0:
            get_oauth_token_mock.return_value = fake_token

        class Error(Enum):
            REQUEST_1_NO_DOLLAR_CONFIG = 0
            REQUEST_1_DOLLAR_CONFIG_ERROR = 10
            REQUEST_1_HTTP_STATUS = 1
            REQUEST_2_HTTP_STATUS = 2
            REQUEST_3_HTTP_STATUS = 3
            REQUEST_4_HTTP_STATUS = 4
            REQUEST_4_HTTP_STATUS_CONFIG = 9
            REQUEST_2_ERROR_MESSAGE = 5
            REQUEST_1_MISSING_STATE = 6
            REQUEST_3_ERROR_MESSAGE = 7
            REQUEST_3_MFA_REQUIRED = 8

        def mock_request_session(inject_error: Optional[Error] = None) -> None:
            def request(self: 'requests.Session', method: str, url: str, name: Optional[str] = None, **kwargs: Dict[str, Any]) -> requests.Response:
                response = Response()
                response.status_code = 200
                response.url = url

                if method == 'GET' and url.endswith('/authorize'):
                    if inject_error == Error.REQUEST_1_NO_DOLLAR_CONFIG:
                        response._content = ''.encode('utf-8')
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
                            'urlGetCredentialType': 'https://test.nu/GetCredentialType?mkt=en-US',
                            'urlPost': 'https://test.nu/login',
                        }
                        if inject_error == Error.REQUEST_1_MISSING_STATE:
                            del dollar_config_raw['hpgact']
                        elif inject_error == Error.REQUEST_1_DOLLAR_CONFIG_ERROR:
                            dollar_config_raw['strServiceExceptionMessage'] = 'oh no!'

                        dollar_config = jsondumps(dollar_config_raw)
                        response._content = f'$Config={dollar_config};'.encode('utf-8')

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
                                'message': 'error! error!'
                            }
                        }

                    payload = jsondumps(data)
                    response._content = payload.encode('utf-8')
                    response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'

                    if inject_error == Error.REQUEST_2_HTTP_STATUS:
                        response.status_code = 400
                elif method == 'POST' and url.endswith('/login'):
                    if inject_error == Error.REQUEST_3_ERROR_MESSAGE:
                        dollar_config = jsondumps({
                            'strServiceExceptionMessage': 'failed big time',
                        })
                    elif inject_error == Error.REQUEST_3_MFA_REQUIRED:
                        dollar_config = jsondumps({
                            'arrUserProofs': [{
                                'authMethodId': 'fax',
                                'display': '+46 1234',
                            }]
                        })
                    else:
                        dollar_config = jsondumps({
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
                        })
                    response._content = f'$Config={dollar_config};'.encode('utf-8')
                    response.headers['x-ms-request-id'] = 'aaaa-bbbb-cccc-dddd'
                    if inject_error == Error.REQUEST_3_HTTP_STATUS:
                        response.status_code = 400
                elif method == 'POST' and url.endswith('/kmsi'):
                    if inject_error == Error.REQUEST_4_HTTP_STATUS:
                        response.status_code = 200
                    elif inject_error == Error.REQUEST_4_HTTP_STATUS_CONFIG:
                        response.status_code = 400
                        dollar_config = jsondumps({
                            'strServiceExceptionMessage': 'error! error! error!'
                        })
                        response._content = f'$Config={dollar_config};'.encode('utf-8')
                    else:
                        response.status_code = 302
                    auth_user_context = user._context['auth']['user']
                    redirect_uri_parsed = urlparse(auth_user_context['redirect_uri'])
                    if len(redirect_uri_parsed.netloc) == 0:
                        redirect_uri = f"{user._context['host']}{auth_user_context['redirect_uri']}"
                    else:
                        redirect_uri = auth_user_context['redirect_uri']

                    if is_token_v2_0:
                        token_name = 'code'
                    else:
                        token_name = 'id_token'

                    response.headers['Location'] = f'{redirect_uri}#{token_name}={fake_token}'
                else:
                    response._content = jsondumps({'error_description': 'error'}).encode('utf-8')

                return response

            mocker.patch(
                'requests.Session.request',
                request,
            )

        session_started = time()

        user._context = {
            'host': 'https://backend.example.com',
            'auth': {
                'client': {
                    'id': 'aaaa',
                },
                'user': {
                    'username': 'test-user@example.com',
                    'password': 'H3ml1g4Arn3',
                    'redirect_uri': 'http://www.example.com/authenticated',
                },
                'provider': None,
            }
        }
        user.host = cast(str, user._context['host'])

        mock_request_session()

        # test when auth.provider is not set
        with pytest.raises(AssertionError) as ae:
            AAD.get_oauth_authorization(user)
        assert str(ae.value) == 'context variable auth.provider is not set'

        fire_spy.assert_not_called()

        provider_url = 'https://login.example.com/oauth2'

        if version != 'v1.0':
            provider_url = f'{provider_url}{version}'

        cast(GrizzlyAuthHttpContext, user._context['auth'])['provider'] = provider_url

        # test when login sequence returns bad request
        mock_request_session(Error.REQUEST_1_HTTP_STATUS)

        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == f'user auth request 1: {provider_url}/authorize had unexpected status code 400'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_2_HTTP_STATUS)

        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'user auth request 2: https://test.nu/GetCredentialType had unexpected status code 400'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_3_HTTP_STATUS)

        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'user auth request 3: https://test.nu/login had unexpected status code 400'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_3_ERROR_MESSAGE)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'failed big time'
        fire_spy.reset_mock()

        assert caplog.messages[-1] == 'failed big time'
        caplog.clear()

        mock_request_session(Error.REQUEST_3_MFA_REQUIRED)

        with caplog.at_level(logging.ERROR):
            with pytest.raises(StopUser):
                AAD.get_oauth_authorization(user)

        expected_error_message = 'test-user@example.com requires MFA for login: fax = +46 1234'

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == expected_error_message
        fire_spy.reset_mock()

        assert caplog.messages[-1] == expected_error_message
        caplog.clear()

        mock_request_session(Error.REQUEST_4_HTTP_STATUS)

        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        assert user.session_started is None

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'user auth request 4: https://login.example.com/kmsi had unexpected status code 200'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_4_HTTP_STATUS_CONFIG)

        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        assert user.session_started is None

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'error! error! error!'
        fire_spy.reset_mock()

        # test error handling when login sequence response doesn't contain expected payload
        mock_request_session(Error.REQUEST_1_NO_DOLLAR_CONFIG)
        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == f'no config found in response from {provider_url}/authorize'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_1_MISSING_STATE)
        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == f'unexpected response body from {provider_url}/authorize: missing "hpgact" in config'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_1_DOLLAR_CONFIG_ERROR)
        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'oh no!'
        fire_spy.reset_mock()

        mock_request_session(Error.REQUEST_2_ERROR_MESSAGE)
        with pytest.raises(StopUser):
            AAD.get_oauth_authorization(user)

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=ANY,
        )
        _, kwargs = fire_spy.call_args_list[-1]
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'error response from https://test.nu/GetCredentialType: code=12345678, message=error! error!'
        fire_spy.reset_mock()

        # successful login sequence
        mock_request_session()

        user.session_started = session_started

        assert AAD.get_oauth_authorization(user) == 'asdf'

        if not is_token_v2_0:
            get_oauth_token_mock.assert_not_called()
        else:
            get_oauth_token_mock.assert_called_once_with(user, (ANY, ANY,))
            get_oauth_token_mock.reset_mock()

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=None,
        )
        fire_spy.reset_mock()

        # test no host in redirect uri
        cast(GrizzlyAuthHttpContextUser, cast(GrizzlyAuthHttpContext, user._context['auth'])['user'])['redirect_uri'] = '/authenticated'

        assert AAD.get_oauth_authorization(user) == 'asdf'

        fire_spy.assert_called_once_with(
            request_type='AUTH',
            response_time=0,
            name=f'001 AAD OAuth2 user token {version}',
            context=user._context,
            response_length=0,
            response=None,
            exception=None,
        )
        fire_spy.reset_mock()

    @pytest.mark.parametrize('grant_type', [
        'client_credentials::v1',
        'client_credentials::v2',
        'authorization_code::v2',
    ])
    def test_get_oauth_token(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, grant_type: str) -> None:
        _, user, _ = grizzly_fixture(user_type=RestApiUser)

        grant_type, version = grant_type.split('::', 1)

        user.__class__.__name__ = f'{user.__class__.__name__}_001'

        def mock_requests_post(payload: str, status_code: int) -> MagicMock:
            response = Response()
            response.status_code = status_code
            response._content = payload.encode()

            return mocker.patch('grizzly.auth.aad.requests.Session.post', return_value=response)

        assert isinstance(user, RestApiUser)

        provider_url = 'https://login.example.com/foobarinc/oauth2'

        if grant_type == 'authorization_code':
            pkcs = ('code', 'code_verifier',)
            token_name = 'id_token'
        else:
            pkcs = None
            token_name = 'access_token'
            if version == 'v2':
                provider_url = f'{provider_url}/v2.0'

        fire_spy = mocker.spy(user.environment.events.request, 'fire')

        safe_del(user._context, 'auth')

        with pytest.raises(AssertionError) as ae:
            AAD.get_oauth_token(user, pkcs)
        assert str(ae.value) == 'context variable auth is not set'

        user._context['auth'] = {}

        with pytest.raises(AssertionError) as ae:
            AAD.get_oauth_token(user, pkcs)
        assert str(ae.value) == 'context variable auth.provider is not set'

        cast(GrizzlyAuthHttpContext, user._context['auth']).update({'provider': provider_url})

        with pytest.raises(AssertionError) as ae:
            AAD.get_oauth_token(user, pkcs)
        assert str(ae.value) == 'context variable auth.client is not set'

        cast(GrizzlyAuthHttpContext, user._context['auth']).update({'client': {'id': None, 'secret': None, 'resource': None}})

        if grant_type == 'authorization_code':
            with pytest.raises(AssertionError) as ae:
                AAD.get_oauth_token(user, pkcs)
            assert str(ae.value) == 'context variable auth.user is not set'

            cast(GrizzlyAuthHttpContext, user._context['auth']).update({'user': {'username': None, 'password': None, 'redirect_uri': '/auth'}})

        user._context['host'] = user.host = 'https://example.com'
        cast(GrizzlyAuthHttpContext, user._context['auth']).update({
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

        assert user.session_started is None

        with pytest.raises(StopUser):
            AAD.get_oauth_token(user, pkcs)

        requests_mock.assert_called_once_with(f'{provider_url}/token', verify=True, data=ANY, headers=ANY, allow_redirects=(pkcs is None))
        _, kwargs = requests_mock.call_args_list[-1]
        data = kwargs.get('data', None)
        assert data['grant_type'] == grant_type
        assert data['client_id'] == 'asdf'
        if pkcs is None:
            if version == 'v1':
                assert data['resource'] == 'asdf'
                assert len(data) == 4
            else:
                print(f'{data=}')
                assert data['scope'] == 'asdf'
                assert data['tenant'] == 'foobarinc'
                assert len(data) == 5
        else:
            assert data['redirect_uri'] == f'{user.host}/auth'
            assert data['code'] == pkcs[0]
            assert data['code_verifier'] == pkcs[1]
            assert len(data) == 5
        assert user.session_started is None

        if pkcs is not None:
            fire_spy.assert_not_called()
        else:
            fire_spy.assert_called_once_with(
                request_type='AUTH',
                response_time=ANY,
                name=f'001 AAD OAuth2 user token {version}.0',
                context=user._context,
                response=None,
                exception=ANY,
                response_length=len(payload.encode()),
            )

            _, kwargs = fire_spy.call_args_list[-1]
            exception = kwargs.get('exception', None)
            assert isinstance(exception, RuntimeError)
            assert str(exception) == 'fake error message'

        requests_mock.reset_mock()

        user.session_started = session_started
        user.headers.update({
            'Authorization': 'foobar',
            'Content-Type': 'plain/text',
            'Ocp-Apim-Subscription-Key': 'secret',
        })
        user._context['verify_certificates'] = False

        requests_mock = mock_requests_post(jsondumps({token_name: 'asdf'}), 200)

        assert AAD.get_oauth_token(user, pkcs) == 'asdf'

        assert user.session_started >= session_started
        requests_mock.assert_called_once_with(f'{provider_url}/token', verify=False, data=ANY, headers=ANY, allow_redirects=(pkcs is None))
        _, kwargs = requests_mock.call_args_list[-1]
        data = kwargs.get('data', None)
        headers = kwargs.get('headers', None)

        assert data['grant_type'] == grant_type
        assert data['client_id'] == 'asdf'
        assert 'Authorization' not in headers
        assert 'Content-Type' not in headers
        assert 'Ocp-Apim-Subscription-Key' not in headers

        if pkcs is None:
            if version == 'v1':
                assert data['resource'] == 'asdf'
                assert len(data) == 4
            else:
                assert data['scope'] == 'asdf'
                assert data['tenant'] == 'foobarinc'
                assert len(data) == 5
        else:
            assert data['redirect_uri'] == f'{user.host}/auth'
            assert data['code'] == pkcs[0]
            assert data['code_verifier'] == pkcs[1]
            assert len(data) == 5
            assert headers.get('Origin', None) == 'https://example.com'
            assert headers.get('Referer', None) == 'https://example.com'

        cast(GrizzlyAuthHttpContext, user._context['auth']).update({'provider': None})

        with pytest.raises(AssertionError) as ae:
            AAD.get_oauth_token(user, pkcs)
        assert str(ae.value) == 'context variable auth.provider is not set'

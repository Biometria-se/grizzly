"""Unit tests for grizzly.users.sftp."""
from __future__ import annotations

import shutil
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Optional

import pytest
from paramiko.sftp_client import SFTPClient
from paramiko.transport import Transport

from grizzly.exceptions import RestartScenario
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.users.sftp import SftpClientSession, SftpUser
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture

    from tests.fixtures import BehaveFixture, GrizzlyFixture, ParamikoFixture


class TestSftpClientSession:
    def test(self, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        # ruff: noqa: B009
        paramiko_fixture()

        context = SftpClientSession('example.org', 1337)

        assert context.host == 'example.org'
        assert context.port == 1337
        assert getattr(context, 'username') is None
        assert getattr(context, 'key') is None
        assert getattr(context, 'key_file') is None
        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None

        with pytest.raises(NotImplementedError, match='private key authentication is not supported'), context.session('username', 'password', '~/.ssh/id_rsa'):
            pass

        assert context.host == 'example.org'
        assert context.port == 1337
        assert getattr(context, 'username') is None
        assert getattr(context, 'key') is None
        assert getattr(context, 'key_file') is None
        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None

        username_transport: Optional[Transport] = None
        username_client: Optional[SFTPClient] = None

        # start session for user username
        with context.session('username', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert getattr(context, 'username') is None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            username_transport = context._transport
            username_client = context._client
        assert context.username == 'username'

        # change username, and hence start a new client
        with context.session('test-user', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert getattr(context, 'username') is None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            assert username_client is not context._client
            assert username_transport is not context._transport

        context.close()

        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None
        assert getattr(context, 'username') is None

        def _from_transport(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None) -> Optional[SFTPClient]:  # noqa: ARG001
            return None

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.from_transport',
            _from_transport,
        )

        with pytest.raises(RuntimeError, match='there is no client'), context.session('test-user', 'password') as session:
            pass


class TestSftpUser:
    def test_create(self, behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = str(test_context.parent)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type('SftpTestUser', (SftpUser, ), {'__scenario__': behave_fixture.grizzly.scenario})

        assert issubclass(test_cls, SftpUser)

        try:
            test_cls.host = 'http://example.com'

            with pytest.raises(ValueError, match='"http" is not supported'):
                test_cls(behave_fixture.locust.environment)

            test_cls.host = 'sftp://username:password@example.com'

            with pytest.raises(ValueError, match='username and password should be set via context variables "auth.username" and "auth.password"'):
                test_cls(behave_fixture.locust.environment)

            test_cls.host = 'sftp://example.com/pub/test'

            with pytest.raises(ValueError, match='only hostname and port should be included as host'):
                test_cls(behave_fixture.locust.environment)

            test_cls.host = 'sftp://example.com'

            with pytest.raises(ValueError, match='"auth.username" context variable is not set'):
                test_cls(behave_fixture.locust.environment)

            test_cls.__context__['auth']['username'] = 'syrsa'

            with pytest.raises(ValueError, match='"auth.password" or "auth.key" context variable must be set'):
                test_cls(behave_fixture.locust.environment)

            test_cls.__context__['auth']['password'] = 'hemligaarne'  # noqa: S105

            user = test_cls(behave_fixture.locust.environment)

            assert user.port == 22
            assert user.host == 'example.com'

            test_cls.host = 'sftp://example.com:1337'
            user = test_cls(behave_fixture.locust.environment)

            assert user.port == 1337
            assert user.host == 'example.com'

            test_cls.__context__['auth']['key_file'] = '~/.ssh/id_rsa'

            with pytest.raises(NotImplementedError, match='key authentication is not implemented'):
                test_cls(behave_fixture.locust.environment)
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    def test_on_start(self, behave_fixture: BehaveFixture, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        paramiko_fixture()
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        test_cls = type(
            'SftpTestUser',
            (SftpUser, ),
            {
                '__scenario__': behave_fixture.grizzly.scenario,
                'host': 'sftp://example.org',
                '__context__': {
                    'auth': {
                        'username': 'test',
                        'password': 'hemligaarne',
                        'key_file': None,
                    },
                },
            },
        )

        assert issubclass(test_cls, SftpUser)

        user = test_cls(behave_fixture.locust.environment)

        sftp_client_mock = mocker.spy(SftpClientSession, '__init__')
        session_spy = mocker.patch.object(SftpClientSession, 'session', return_value=mocker.MagicMock())

        assert not hasattr(user, 'session')

        user.on_start()

        assert hasattr(user, 'session')
        sftp_client_mock.assert_called_once_with(user.sftp_client, user.host, user.port)
        session_spy.assert_called_once_with(**user._context['auth'])
        session_spy.return_value.__enter__.assert_called_once()

    def test_on_stop(self, behave_fixture: BehaveFixture, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        paramiko_fixture()
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        session_spy = mocker.MagicMock()
        test_cls = type(
            'SftpTestUser',
            (SftpUser, ),
            {
                '__scenario__': behave_fixture.grizzly.scenario,
                'host': 'sftp://example.org',
                '__context__': {
                    'auth': {
                        'username': 'test',
                        'password': 'hemligaarne',
                        'key_file': None,
                    },
                },
                'session': session_spy,
            },
        )

        assert issubclass(test_cls, SftpUser)

        user = test_cls(behave_fixture.locust.environment)

        user.on_stop()

        session_spy.assert_not_called()
        session_spy.__exit__.assert_called_once_with(None, None, None)

    def test_request(self, grizzly_fixture: GrizzlyFixture, paramiko_fixture: ParamikoFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture) -> None:  # noqa: PLR0915
        paramiko_fixture()

        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = str(test_context.parent)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        test_cls = type(
            'SftpTestUser',
            (SftpUser, ),
            {
                '__scenario__': grizzly.scenario,
                'host': 'sftp://example.org',
                '__context__': {
                    'auth': {
                        'username': 'test',
                        'password': 'hemligaarne',
                        'key_file': None,
                    },
                },
            },
        )

        assert issubclass(test_cls, SftpUser)

        try:
            user = test_cls(grizzly_fixture.behave.locust.environment)
            assert isinstance(user, SftpUser)
            parent._user = user

            user.on_start()

            mocker.patch('grizzly.users.base.grizzly_user.perf_counter', return_value=0.0)

            request = RequestTask(RequestMethod.SEND, name='test', endpoint='/tmp')  # noqa: S108
            request.source = 'test/file.txt'

            fire_spy = mocker.spy(user.environment.events.request, 'fire')
            response_event_spy = mocker.spy(user.event_hook, 'fire')

            parent.user._scenario.failure_exception = None
            with pytest.raises(StopUser):
                parent.user.request(request)

            fire_spy.assert_called_once_with(
                request_type='SEND',
                name=f'{user._scenario.identifier} test',
                response_time=0,
                response_length=0,
                context=user._context,
                exception=ANY(NotImplementedError, message='SftpTestUser has not implemented SEND'),
            )
            fire_spy.reset_mock()

            parent.user._scenario.failure_exception = StopUser
            with pytest.raises(StopUser):
                parent.user.request(request)

            fire_spy.assert_called_once_with(
                request_type='SEND',
                name=f'{user._scenario.identifier} test',
                response_time=0,
                response_length=0,
                context=user._context,
                exception=ANY(NotImplementedError, message='SftpTestUser has not implemented SEND'),
            )
            fire_spy.reset_mock()

            parent.user._scenario.failure_exception = RestartScenario
            with pytest.raises(StopUser):
                parent.user.request(request)

            fire_spy.assert_called_once_with(
                request_type='SEND',
                name=f'{user._scenario.identifier} test',
                response_time=0,
                response_length=0,
                context=user._context,
                exception=ANY(NotImplementedError, message='SftpTestUser has not implemented SEND'),
            )
            fire_spy.reset_mock()
            response_event_spy.reset_mock()

            request.method = RequestMethod.GET

            parent.user.request(request)

            response_event_spy.assert_called_once_with(
                name=f'{user._scenario.identifier} test',
                request=ANY(RequestTask),
                context=(
                    {'host': user.host, 'method': 'get', 'path': '/tmp'},  # noqa: S108
                    str(user._download_root / Path(request.endpoint).name),
                ),
                user=user,
                exception=None,
            )
            response_event_spy.reset_mock()

            request.method = RequestMethod.PUT
            request.source = None

            with pytest.raises(RestartScenario):
                parent.user.request(request)

            response_event_spy.assert_called_once_with(
                name=f'{user._scenario.identifier} test',
                request=ANY(RequestTask),
                context=(None, None),
                user=user,
                exception=ANY(ValueError, message='SftpTestUser: request "001 test" does not have a payload, incorrect method specified'),
            )
            response_event_spy.reset_mock()

            request.source = 'foo.bar'

            parent.user.request(request)

            response_event_spy.assert_called_once_with(
                name=f'{user._scenario.identifier} test',
                request=ANY(RequestTask),
                context=(
                    {'host': user.host, 'method': 'put', 'path': '/tmp'},  # noqa: S108
                    str(Path(test_context_root) / 'requests' / 'foo.bar'),
                ),
                user=user,
                exception=None,
            )
            response_event_spy.reset_mock()
            fire_spy.reset_mock()

            mocker.patch.object(user.event_hook, 'fire', side_effect=[RuntimeError('error error')])

            parent.user._scenario.failure_exception = StopUser

            with pytest.raises(StopUser):
                parent.user.request(request)

            fire_spy.assert_called_once_with(
                request_type='PUT',
                name=f'{user._scenario.identifier} test',
                response_time=0,
                response_length=ANY(int),
                context=user._context,
                exception=ANY(RuntimeError, message='error error'),
            )
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.skip(reason='needs preconditions outside of pytest, has to be executed explicitly manually')
    def test_real(self, grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory) -> None:
        """Execute real test, needs a running sftp server.

        ```bash
        mkdir /tmp/sftp-upload; \
        docker run --rm -it -p 2222:22 -v /tmp/sftp-upload:/home/foo/upload atmoz/sftp:alpine foo:pass:1000:::upload; \
        rm -rf /tmp/sftp-upload
        ```
        """
        parent = grizzly_fixture()

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context_root = str(test_context.parent)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            (test_context / 'test.txt').write_text('this is a file that is going to be put on the actual sftp server\n')

            test_cls = type(
                'SftpTestUser',
                (SftpUser, ),
                {
                    '__scenario__': grizzly_fixture.grizzly.scenario,
                    'host': 'sftp://host.docker.internal:2222',
                    '__context__': {
                        'auth': {
                            'username': 'foo',
                            'password': 'pass',
                            'key_file': None,
                        },
                    },
                },
            )

            assert issubclass(test_cls, SftpUser)

            _user = test_cls(grizzly_fixture.behave.locust.environment)

            assert isinstance(_user, SftpUser)
            parent._user = _user

            request = RequestTask(RequestMethod.PUT, name='test', endpoint='/upload')
            request.source = 'test.txt'

            parent.user.request(request)

            request = RequestTask(RequestMethod.GET, name='test', endpoint='/upload/test.txt')

            parent.user.request(request)

            localpath = test_context / 'download' / 'test.txt'

            assert localpath.exists()
            assert localpath.read_text().strip() == 'this is a file that is going to be put on the actual sftp server'
        finally:
            del environ['GRIZZLY_CONTEXT_ROOT']

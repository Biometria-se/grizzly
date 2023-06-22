import shutil

from os import path, environ

import pytest

from _pytest.tmpdir import TempPathFactory
from pytest_mock import MockerFixture

from grizzly.users.sftp import SftpUser
from grizzly.clients import SftpClientSession
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from grizzly.tasks import RequestTask
from grizzly.exceptions import RestartScenario

from tests.fixtures import ParamikoFixture, BehaveFixture, GrizzlyFixture


class TestSftpUser:
    def test_create(self, behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))
        SftpUser.__scenario__ = behave_fixture.grizzly.scenario

        try:
            SftpUser.host = 'http://example.com'

            with pytest.raises(ValueError):
                SftpUser(behave_fixture.locust.environment)

            SftpUser.host = 'sftp://username:password@example.com'

            with pytest.raises(ValueError):
                SftpUser(behave_fixture.locust.environment)

            SftpUser.host = 'sftp://example.com/pub/test'

            with pytest.raises(ValueError):
                SftpUser(behave_fixture.locust.environment)

            SftpUser.host = 'sftp://example.com'

            with pytest.raises(ValueError):
                SftpUser(behave_fixture.locust.environment)

            SftpUser._context['auth']['username'] = 'syrsa'

            with pytest.raises(ValueError):
                SftpUser(behave_fixture.locust.environment)

            SftpUser._context['auth']['password'] = 'hemligaarne'

            user = SftpUser(behave_fixture.locust.environment)

            assert user.port == 22
            assert user.host == 'example.com'

            SftpUser.host = 'sftp://example.com:1337'
            user = SftpUser(behave_fixture.locust.environment)

            assert user.port == 1337
            assert user.host == 'example.com'

            SftpUser._context['auth']['key_file'] = '~/.ssh/id_rsa'

            with pytest.raises(NotImplementedError):
                SftpUser(behave_fixture.locust.environment)
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    def test_on_start(self, behave_fixture: BehaveFixture, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        paramiko_fixture()
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        SftpUser.__scenario__ = behave_fixture.grizzly.scenario
        SftpUser.host = 'sftp://example.org'
        SftpUser._context['auth'] = {
            'username': 'test',
            'password': 'hemligaarne',
            'key_file': None,
        }
        user = SftpUser(behave_fixture.locust.environment)

        sftp_client_mock = mocker.spy(SftpClientSession, '__init__')
        session_spy = mocker.patch.object(SftpClientSession, 'session', return_value=mocker.MagicMock())

        assert not hasattr(user, 'session')

        user.on_start()

        assert hasattr(user, 'session')
        sftp_client_mock.assert_called_once_with(user.sftp_client, user.host, user.port)
        session_spy.assert_called_once_with(**user._context['auth'])
        assert session_spy.return_value.__enter__.call_count == 1

    def test_on_stop(self, behave_fixture: BehaveFixture, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        paramiko_fixture()
        behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

        SftpUser.__scenario__ = behave_fixture.grizzly.scenario
        SftpUser.host = 'sftp://example.org'
        SftpUser._context['auth'] = {
            'username': 'test',
            'password': 'hemligaarne',
            'key_file': None,
        }
        user = SftpUser(behave_fixture.locust.environment)
        setattr(user, 'session', None)
        session_spy = mocker.patch.object(user, 'session', return_value=mocker.MagicMock())

        user.on_stop()

        assert session_spy.call_count == 0

        assert session_spy.__exit__.call_count == 1
        args, kwargs = session_spy.__exit__.call_args_list[-1]
        assert kwargs == {}
        assert len(args) == 3
        assert args == (None, None, None,)

    def test_request(self, grizzly_fixture: GrizzlyFixture, paramiko_fixture: ParamikoFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture) -> None:
        paramiko_fixture()

        grizzly = grizzly_fixture.grizzly
        parent = grizzly_fixture()

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            SftpUser.__scenario__ = grizzly.scenario
            SftpUser.host = 'sftp://example.org'
            SftpUser._context['auth'] = {
                'username': 'test',
                'password': 'hemligaarne',
                'key_file': None,
            }
            user = SftpUser(grizzly_fixture.behave.locust.environment)
            parent._user = user

            user.on_start()

            mocker.patch('grizzly.users.base.grizzly_user.perf_counter', return_value=0.0)

            request = RequestTask(RequestMethod.SEND, name='test', endpoint='/tmp')
            request.source = 'test/file.txt'

            fire_spy = mocker.spy(user.environment.events.request, 'fire')
            response_event_spy = mocker.spy(user.response_event, 'fire')

            parent.user._scenario.failure_exception = None
            with pytest.raises(StopUser):
                parent.user.request(request)

            assert fire_spy.call_count == 1
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            parent.user._scenario.failure_exception = StopUser
            with pytest.raises(StopUser):
                parent.user.request(request)

            assert fire_spy.call_count == 2
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            parent.user._scenario.failure_exception = RestartScenario
            with pytest.raises(StopUser):
                parent.user.request(request)

            assert fire_spy.call_count == 3
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            parent.user.logger.error('failing test')

            request.method = RequestMethod.GET

            parent.user.request(request)

            assert response_event_spy.call_count == 4
            _, kwargs = response_event_spy.call_args_list[-1]
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert isinstance(kwargs.get('request', None), RequestTask)
            assert kwargs.get('context', None) == ({
                'host': user.host,
                'method': 'get',
                'path': '/tmp',
            }, path.join(user._download_root, path.basename(request.endpoint)))
            assert kwargs.get('user', None) is user
            assert kwargs.get('exception', '') is None

            request.method = RequestMethod.PUT
            request.source = None

            with pytest.raises(RestartScenario):
                parent.user.request(request)

            assert response_event_spy.call_count == 5
            _, kwargs = response_event_spy.call_args_list[-1]

            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert isinstance(kwargs.get('request', None), RequestTask)
            assert kwargs.get('context', None) == (None, None,)
            assert kwargs.get('user', None) is user
            exception = kwargs.get('exception', None)

            assert exception is not None
            assert isinstance(exception, ValueError)
            assert 'SftpUser: request "001 test" does not have a payload, incorrect method specified' in str(exception)

            request.source = 'foo.bar'

            parent.user.request(request)

            assert response_event_spy.call_count == 6
            _, kwargs = response_event_spy.call_args_list[-1]
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert isinstance(kwargs.get('request', None), RequestTask)
            assert kwargs.get('context', None) == ({
                'host': user.host,
                'method': 'put',
                'path': '/tmp',
            }, path.join(test_context_root, 'requests', 'foo.bar'))
            assert kwargs.get('user', None) is user
            assert kwargs.get('exception', '') is None

            mocker.patch.object(user.response_event, 'fire', side_effect=[RuntimeError('error error')])

            parent.user._scenario.failure_exception = StopUser

            with pytest.raises(StopUser):
                parent.user.request(request)

            assert fire_spy.call_count == 7
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'PUT'
            assert kwargs.get('name', None) == f'{user._scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) > 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', '')
            assert exception is not None
            assert isinstance(exception, RuntimeError)
            assert str(exception) == 'error error'
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.skip(reason='needs preconditions outside of pytest, has to be executed explicitly manually')
    def test_real(self, grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory) -> None:
        # first start sftp server:
        #  mkdir /tmp/sftp-upload; \
        #  docker run --rm -it -p 2222:22 -v /tmp/sftp-upload:/home/foo/upload atmoz/sftp:alpine foo:pass:1000:::upload; \
        #  rm -rf /tmp/sftp-upload

        parent = grizzly_fixture()

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            with open(path.join(test_context, 'test.txt'), 'w') as fd:
                fd.write('this is a file that is going to be put on the actual sftp server\n')
                fd.flush()

            SftpUser.__scenario__ = grizzly_fixture.grizzly.scenario
            SftpUser.host = 'sftp://host.docker.internal:2222'
            SftpUser._context['auth'] = {
                'username': 'foo',
                'password': 'pass',
                'key_file': None,
            }
            _user = SftpUser(grizzly_fixture.behave.locust.environment)
            parent._user = _user

            request = RequestTask(RequestMethod.PUT, name='test', endpoint='/upload')
            request.source = 'test.txt'

            parent.user.request(request)

            request = RequestTask(RequestMethod.GET, name='test', endpoint='/upload/test.txt')

            parent.user.request(request)

            localpath = path.join(test_context_root, 'requests', 'download', 'test.txt')

            assert path.exists(localpath)

            with open(localpath, 'r') as fd:
                assert fd.read().strip() == 'this is a file that is going to be put on the actual sftp server'
        finally:
            del environ['GRIZZLY_CONTEXT_ROOT']

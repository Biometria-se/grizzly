import shutil

from os import path, environ

import pytest

from _pytest.tmpdir import TempPathFactory
from pytest_mock import MockerFixture
from locust.exception import StopUser

from grizzly.users.sftp import SftpUser
from grizzly.clients import SftpClientSession
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.exceptions import RestartScenario

from ...fixtures import LocustFixture, ParamikoFixture


class TestSftpUser:
    def test_create(self, locust_fixture: LocustFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            SftpUser.host = 'http://test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_fixture.env)

            SftpUser.host = 'sftp://username:password@test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_fixture.env)

            SftpUser.host = 'sftp://test.nu/pub/test'

            with pytest.raises(ValueError):
                SftpUser(locust_fixture.env)

            SftpUser.host = 'sftp://test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_fixture.env)

            SftpUser._context['auth']['username'] = 'syrsa'

            with pytest.raises(ValueError):
                SftpUser(locust_fixture.env)

            SftpUser._context['auth']['password'] = 'hemligaarne'

            user = SftpUser(locust_fixture.env)

            assert isinstance(user.sftp_client, SftpClientSession)
            assert user.sftp_client.port == 22
            assert user.sftp_client.host == 'test.nu'

            SftpUser.host = 'sftp://test.nu:1337'
            user = SftpUser(locust_fixture.env)

            assert isinstance(user.sftp_client, SftpClientSession)
            assert user.sftp_client.port == 1337
            assert user.sftp_client.host == 'test.nu'

            SftpUser._context['auth']['key_file'] = '~/.ssh/id_rsa'

            with pytest.raises(NotImplementedError):
                SftpUser(locust_fixture.env)
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    def test_request(self, locust_fixture: LocustFixture, paramiko_fixture: ParamikoFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture) -> None:
        paramiko_fixture()

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            SftpUser.host = 'sftp://example.org'
            SftpUser._context['auth'] = {
                'username': 'test',
                'password': 'hemligaarne',
                'key_file': None,
            }
            user = SftpUser(locust_fixture.env)

            mocker.patch('grizzly.users.sftp.time', side_effect=[0.0] * 100)

            request = RequestTask(RequestMethod.SEND, name='test', endpoint='/tmp')
            request.source = 'test/file.txt'

            scenario = GrizzlyContextScenario(2)
            scenario.name = 'test'

            request.scenario = scenario

            fire_spy = mocker.spy(user.environment.events.request, 'fire')
            response_event_spy = mocker.spy(user.response_event, 'fire')

            request.scenario.failure_exception = None
            with pytest.raises(StopUser):
                user.request(request)

            assert fire_spy.call_count == 1
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            request.scenario.failure_exception = StopUser
            with pytest.raises(StopUser):
                user.request(request)

            assert fire_spy.call_count == 2
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            request.scenario.failure_exception = RestartScenario
            with pytest.raises(StopUser):
                user.request(request)

            assert fire_spy.call_count == 3
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'SEND'
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, NotImplementedError)
            assert 'SftpUser has not implemented SEND' in str(exception)

            request.method = RequestMethod.GET

            user.request(request)

            assert response_event_spy.call_count == 4
            _, kwargs = response_event_spy.call_args_list[-1]
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('request', None) is request
            assert kwargs.get('context', None) == ({
                'host': user.host,
                'method': 'get',
                'time': 0,
                'path': '/tmp',
            }, 'test/file.txt')
            assert kwargs.get('user', None) is user
            assert kwargs.get('exception', '') is None

            request.method = RequestMethod.PUT
            request.source = None

            with pytest.raises(RestartScenario):
                user.request(request)

            assert response_event_spy.call_count == 5
            _, kwargs = response_event_spy.call_args_list[-1]

            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('request', None) is request
            assert kwargs.get('context', None) == ({
                'host': user.host,
                'method': 'put',
                'time': 0,
                'path': '/tmp',
            }, None)
            assert kwargs.get('user', None) is user
            exception = kwargs.get('exception', None)

            assert exception is not None
            assert isinstance(exception, ValueError)
            assert 'SftpUser: request "002 test" does not have a payload, incorrect method specified' in str(exception)

            request.source = 'foo.bar'

            user.request(request)

            assert response_event_spy.call_count == 6
            _, kwargs = response_event_spy.call_args_list[-1]
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('request', None) is request
            assert kwargs.get('context', None) == ({
                'host': user.host,
                'method': 'put',
                'time': 0,
                'path': '/tmp',
            }, path.join(test_context_root, 'requests', 'foo.bar'))
            assert kwargs.get('user', None) is user
            assert kwargs.get('exception', '') is None

            mocker.patch.object(user.response_event, 'fire', side_effect=[RuntimeError('error error')])

            request.scenario.failure_exception = StopUser

            with pytest.raises(StopUser):
                user.request(request)

            assert fire_spy.call_count == 7
            _, kwargs = fire_spy.call_args_list[-1]

            assert kwargs.get('request_type', None) == 'PUT'
            assert kwargs.get('name', None) == f'{request.scenario.identifier} test'
            assert kwargs.get('response_time', None) == 0
            assert kwargs.get('response_length', None) == 100
            assert kwargs.get('context', None) == user._context
            exception = kwargs.get('exception', '')
            assert exception is not None
            assert isinstance(exception, RuntimeError)
            assert str(exception) == 'error error'
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.skip(reason='needs preconditions outside of pytest, has to be executed explicitly manually')
    def test_real(self, locust_fixture: LocustFixture, tmp_path_factory: TempPathFactory) -> None:
        # first start sftp server:
        #  mkdir /tmp/sftp-upload; \
        #  docker run --rm -it -p 2222:22 -v /tmp/sftp-upload:/home/foo/upload atmoz/sftp:alpine foo:pass:1000:::upload; \
        #  rm -rf /tmp/sftp-upload

        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            with open(path.join(test_context, 'test.txt'), 'w') as fd:
                fd.write('this is a file that is going to be put on the actual sftp server\n')
                fd.flush()

            SftpUser.host = 'sftp://host.docker.internal:2222'
            SftpUser._context['auth'] = {
                'username': 'foo',
                'password': 'pass',
                'key_file': None,
            }
            user = SftpUser(locust_fixture.env)

            request = RequestTask(RequestMethod.PUT, name='test', endpoint='/upload')
            request.source = 'test.txt'

            scenario = GrizzlyContextScenario(1)
            scenario.name = 'test'
            scenario.user.class_name = 'SftpUser'
            scenario.failure_exception = StopUser

            request.scenario = scenario

            user.request(request)

            request = RequestTask(RequestMethod.GET, name='test', endpoint='/upload/test.txt')
            request.scenario = scenario

            user.request(request)

            localpath = path.join(test_context_root, 'requests', 'download', 'test.txt')

            assert path.exists(localpath)

            with open(localpath, 'r') as fd:
                assert fd.read().strip() == 'this is a file that is going to be put on the actual sftp server'
        finally:
            del environ['GRIZZLY_CONTEXT_ROOT']

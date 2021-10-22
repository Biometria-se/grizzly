import shutil

from typing import Callable
from os import path, environ
from jinja2.environment import Template

import pytest

from _pytest.tmpdir import TempdirFactory
from locust.env import Environment
from locust.exception import StopUser

from grizzly.users.sftp import SftpUser
from grizzly.clients import SftpClientSession
from grizzly.types import RequestMethod
from grizzly.context import GrizzlyContextScenario
from grizzly.task import RequestTask

from ..fixtures import locust_environment, paramiko_mocker  # pylint: disable=unused-import
from ..helpers import ResultFailure, ResultSuccess, RequestEvent, RequestSilentFailureEvent


class TestSftpUser:
    @pytest.mark.usefixtures('locust_environment', 'tmpdir_factory')
    def test_create(self, locust_environment: Environment, tmpdir_factory: TempdirFactory) -> None:
        test_context = tmpdir_factory.mktemp('test_context').mkdir('requests')
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            SftpUser.host = 'http://test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_environment)

            SftpUser.host = 'sftp://username:password@test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_environment)

            SftpUser.host = 'sftp://test.nu/pub/test'

            with pytest.raises(ValueError):
                SftpUser(locust_environment)

            SftpUser.host = 'sftp://test.nu'

            with pytest.raises(ValueError):
                SftpUser(locust_environment)

            SftpUser._context['auth']['username'] = 'syrsa'

            with pytest.raises(ValueError):
                SftpUser(locust_environment)

            SftpUser._context['auth']['password'] = 'hemligaarne'

            user = SftpUser(locust_environment)

            assert isinstance(user.client, SftpClientSession)
            assert user.client.port == 22
            assert user.client.host == 'test.nu'

            SftpUser.host = 'sftp://test.nu:1337'
            user = SftpUser(locust_environment)

            assert isinstance(user.client, SftpClientSession)
            assert user.client.port == 1337
            assert user.client.host == 'test.nu'

            SftpUser._context['auth']['key_file'] = '~/.ssh/id_rsa'

            with pytest.raises(NotImplementedError):
                SftpUser(locust_environment)
        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.usefixtures('locust_environment', 'paramiko_mocker', 'tmpdir_factory')
    def test_request(self, locust_environment: Environment, paramiko_mocker: Callable, tmpdir_factory: TempdirFactory) -> None:
        paramiko_mocker()

        test_context = tmpdir_factory.mktemp('test_context').mkdir('requests')
        test_context_root = path.dirname(str(test_context))
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            SftpUser.host = 'sftp://example.org'
            SftpUser._context['auth'] = {
                'username': 'test',
                'password': 'hemligaarne',
                'key_file': None,
            }
            user = SftpUser(locust_environment)

            request = RequestTask(RequestMethod.SEND, name='test', endpoint='/tmp')
            request.source = 'test/file.txt'
            request.template = Template(request.source)

            scenario = GrizzlyContextScenario()
            scenario.name = 'test'

            request.scenario = scenario

            locust_environment.events.request = RequestSilentFailureEvent()

            request.scenario.stop_on_failure = False
            user.request(request)

            request.scenario.stop_on_failure = True
            with pytest.raises(StopUser):
                user.request(request)

            locust_environment.events.request = RequestEvent()

            request.method = RequestMethod.GET

            with pytest.raises(ResultSuccess):
                user.request(request)

            request.method = RequestMethod.PUT
            request.template = None

            with pytest.raises(ResultFailure):
                user.request(request)

            request.source = 'hello world'
            request.template = Template(request.source)

            with pytest.raises(ResultSuccess):
                user.request(request)

        finally:
            shutil.rmtree(test_context_root)
            del environ['GRIZZLY_CONTEXT_ROOT']

    @pytest.mark.skip(reason='needs preconditions outside of pytest, has to be executed explicitly manually')
    @pytest.mark.usefixtures('locust_environment', 'tmpdir_factory')
    def test_real(self, locust_environment: Environment, tmpdir_factory: TempdirFactory) -> None:
        # first start sftp server:
        #  mkdir /tmp/sftp-upload; \
        #  docker run --rm -it -p 2222:22 -v /tmp/sftp-upload:/home/foo/upload atmoz/sftp:alpine foo:pass:1000:::upload; \
        #  rm -rf /tmp/sftp-upload

        test_context = tmpdir_factory.mktemp('test_context').mkdir('requests')
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
            user = SftpUser(locust_environment)

            request = RequestTask(RequestMethod.PUT, name='test', endpoint='/upload')
            request.source = 'test.txt'
            request.template = Template(request.source)

            scenario = GrizzlyContextScenario()
            scenario.name = 'test'
            scenario.user_class_name = 'SftpUser'
            scenario.stop_on_failure = True

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

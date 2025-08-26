"""Tests for grizzly_cli.distributed.build."""

from __future__ import annotations

import sys
from argparse import Namespace
from contextlib import suppress
from inspect import getfile
from os import environ
from pathlib import Path
from socket import gaierror
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.distributed.build import _create_build_command, build, getgid, getuid
from grizzly_cli.utils import RunCommandResult, rm_rf

from test_cli.helpers import cwd

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


@pytest.mark.skipif(sys.platform == 'win32', reason='only run test on non-windows platforms')
def test_getuid_getgid_win32(mocker: MockerFixture) -> None:
    mocker.patch('grizzly_cli.distributed.build.sys.platform', 'win32')

    assert getuid() == 1000
    assert getgid() == 1000

    mocker.patch('grizzly_cli.distributed.build.sys.platform', 'linux')

    assert getuid() >= 0
    assert getgid() >= 0


def test__create_build_command(mocker: MockerFixture) -> None:
    mocker.patch('grizzly_cli.distributed.build.getuid', return_value=1337)
    mocker.patch('grizzly_cli.distributed.build.getgid', return_value=2147483647)
    args = Namespace(container_system='test', local_install=False)

    mocker.patch('grizzly_cli.distributed.build.get_dependency_versions', return_value=(('1.1.1', None), '2.8.4'))

    assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
        'test',
        'image',
        'build',
        '--ssh',
        'default',
        '--build-arg',
        'GRIZZLY_EXTRA=base',
        '--build-arg',
        'GRIZZLY_INSTALL_TYPE=remote',
        '--build-arg',
        'GRIZZLY_UID=1337',
        '--build-arg',
        'GRIZZLY_GID=2147483647',
        '-f',
        'Containerfile.test',
        '-t',
        'grizzly-cli:test',
        '/home/grizzly-cli/',
    ]

    mocker.patch('grizzly_cli.distributed.build.get_dependency_versions', return_value=(('1.1.1', []), '2.8.4'))

    args.local_install = True

    assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
        'test',
        'image',
        'build',
        '--ssh',
        'default',
        '--build-arg',
        'GRIZZLY_EXTRA=base',
        '--build-arg',
        'GRIZZLY_INSTALL_TYPE=local',
        '--build-arg',
        'GRIZZLY_UID=1337',
        '--build-arg',
        'GRIZZLY_GID=2147483647',
        '-f',
        'Containerfile.test',
        '-t',
        'grizzly-cli:test',
        '/home/grizzly-cli/',
    ]

    mocker.patch('grizzly_cli.distributed.build.get_dependency_versions', return_value=(('1.1.1', ['dev', 'ci', 'mq']), '2.8.4'))

    assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
        'test',
        'image',
        'build',
        '--ssh',
        'default',
        '--build-arg',
        'GRIZZLY_EXTRA=mq',
        '--build-arg',
        'GRIZZLY_INSTALL_TYPE=local',
        '--build-arg',
        'GRIZZLY_UID=1337',
        '--build-arg',
        'GRIZZLY_GID=2147483647',
        '-f',
        'Containerfile.test',
        '-t',
        'grizzly-cli:test',
        '/home/grizzly-cli/',
    ]

    try:
        environ['IBM_MQ_LIB_HOST'] = 'https://localhost:8003'
        args.local_install = False

        assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
            'test',
            'image',
            'build',
            '--ssh',
            'default',
            '--build-arg',
            'GRIZZLY_EXTRA=mq',
            '--build-arg',
            'GRIZZLY_INSTALL_TYPE=remote',
            '--build-arg',
            'GRIZZLY_UID=1337',
            '--build-arg',
            'GRIZZLY_GID=2147483647',
            '--build-arg',
            'IBM_MQ_LIB_HOST=https://localhost:8003',
            '-f',
            'Containerfile.test',
            '-t',
            'grizzly-cli:test',
            '/home/grizzly-cli/',
        ]

        environ['IBM_MQ_LIB_HOST'] = 'http://host.docker.internal:8000'

        mocker.patch('grizzly_cli.distributed.build.gethostbyname', return_value='1.2.3.4')
        args.local_install = True

        assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
            'test',
            'image',
            'build',
            '--ssh',
            'default',
            '--build-arg',
            'GRIZZLY_EXTRA=mq',
            '--build-arg',
            'GRIZZLY_INSTALL_TYPE=local',
            '--build-arg',
            'GRIZZLY_UID=1337',
            '--build-arg',
            'GRIZZLY_GID=2147483647',
            '--build-arg',
            'IBM_MQ_LIB_HOST=http://host.docker.internal:8000',
            '--add-host',
            'host.docker.internal:1.2.3.4',
            '-f',
            'Containerfile.test',
            '-t',
            'grizzly-cli:test',
            '/home/grizzly-cli/',
        ]

        mocker.patch('grizzly_cli.distributed.build.gethostbyname', side_effect=[gaierror] * 2)

        assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
            'test',
            'image',
            'build',
            '--ssh',
            'default',
            '--build-arg',
            'GRIZZLY_EXTRA=mq',
            '--build-arg',
            'GRIZZLY_INSTALL_TYPE=local',
            '--build-arg',
            'GRIZZLY_UID=1337',
            '--build-arg',
            'GRIZZLY_GID=2147483647',
            '--build-arg',
            'IBM_MQ_LIB_HOST=http://host.docker.internal:8000',
            '--add-host',
            'host.docker.internal:host-gateway',
            '-f',
            'Containerfile.test',
            '-t',
            'grizzly-cli:test',
            '/home/grizzly-cli/',
        ]

        environ['IBM_MQ_LIB'] = 'mqm.tar.gz'

        args.local_install = False

        assert _create_build_command(args, 'Containerfile.test', 'grizzly-cli:test', '/home/grizzly-cli/') == [
            'test',
            'image',
            'build',
            '--ssh',
            'default',
            '--build-arg',
            'GRIZZLY_EXTRA=mq',
            '--build-arg',
            'GRIZZLY_INSTALL_TYPE=remote',
            '--build-arg',
            'GRIZZLY_UID=1337',
            '--build-arg',
            'GRIZZLY_GID=2147483647',
            '--build-arg',
            'IBM_MQ_LIB_HOST=http://host.docker.internal:8000',
            '--add-host',
            'host.docker.internal:host-gateway',
            '--build-arg',
            'IBM_MQ_LIB=mqm.tar.gz',
            '-f',
            'Containerfile.test',
            '-t',
            'grizzly-cli:test',
            '/home/grizzly-cli/',
        ]

    finally:
        with suppress(KeyError):
            del environ['IBM_MQ_LIB_HOST']

        with suppress(KeyError):
            del environ['IBM_MQ_LIB']


def test_build(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')

    try:
        with cwd(test_context):
            mocker.patch('grizzly_cli.EXECUTION_CONTEXT', test_context.as_posix())
            mocker.patch('grizzly_cli.distributed.build.EXECUTION_CONTEXT', test_context.as_posix())
            mocker.patch('grizzly_cli.distributed.build.PROJECT_NAME', 'grizzly-scenarios')
            mocker.patch('grizzly_cli.distributed.build.getuser', return_value='test-user')
            mocker.patch('grizzly_cli.distributed.build.getuid', return_value=1337)
            mocker.patch('grizzly_cli.distributed.build.getgid', return_value=2147483647)
            run_command = mocker.patch(
                'grizzly_cli.distributed.build.run_command',
                side_effect=[
                    RunCommandResult(return_code=254),
                    RunCommandResult(return_code=133),
                    RunCommandResult(return_code=0),
                    RunCommandResult(return_code=1),
                    RunCommandResult(return_code=0),
                    RunCommandResult(return_code=0),
                    RunCommandResult(return_code=2),
                    RunCommandResult(return_code=0),
                    RunCommandResult(return_code=0),
                    RunCommandResult(return_code=0),
                ],
            )
            setattr(getattr(build, '__wrapped__'), '__value__', test_context.as_posix())  # noqa: B009, B010

            test_args = Namespace(container_system='test', force_build=False, project_name=None, local_install=False, no_progress=False, verbose=False)

            static_context = Path.joinpath(Path(getfile(_create_build_command)).parent, '..', 'static').resolve()

            assert build(test_args) == 254

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == ''
            assert run_command.call_count == 1
            args, kwargs = run_command.call_args_list[-1]

            container_file_actual = args[0].pop(14)
            container_file_expected = Path.joinpath(static_context, 'Containerfile').as_posix()

            if sys.platform == 'win32':
                container_file_actual = container_file_actual.lower()
                container_file_expected = container_file_expected.lower()

            assert args[0] == [
                'test',
                'image',
                'build',
                '--ssh',
                'default',
                '--build-arg',
                'GRIZZLY_EXTRA=base',
                '--build-arg',
                'GRIZZLY_INSTALL_TYPE=remote',
                '--build-arg',
                'GRIZZLY_UID=1337',
                '--build-arg',
                'GRIZZLY_GID=2147483647',
                '-f',
                '-t',
                'grizzly-scenarios:test-user',
                test_context.as_posix(),
            ]

            assert container_file_actual == container_file_expected

            actual_env = kwargs.get('env', None)
            assert actual_env is not None
            assert actual_env.get('DOCKER_BUILDKIT', None) == environ.get('DOCKER_BUILDKIT', None)

            test_args = Namespace(container_system='docker', force_build=True, local_install=True, project_name='foobar', no_progress=False, verbose=False)

            mocker.patch('grizzly_cli.distributed.build.get_dependency_versions', return_value=(('1.1.1', ['mq', 'dev']), '2.8.4'))

            assert build(test_args) == 133
            assert run_command.call_count == 2
            args, kwargs = run_command.call_args_list[-1]

            container_file_actual = args[0].pop(14)
            container_file_expected = Path.joinpath(static_context, 'Containerfile').as_posix()

            if sys.platform == 'win32':
                container_file_actual = container_file_actual.lower()
                container_file_expected = container_file_expected.lower()

            assert args[0] == [
                'docker',
                'image',
                'build',
                '--ssh',
                'default',
                '--build-arg',
                'GRIZZLY_EXTRA=mq',
                '--build-arg',
                'GRIZZLY_INSTALL_TYPE=local',
                '--build-arg',
                'GRIZZLY_UID=1337',
                '--build-arg',
                'GRIZZLY_GID=2147483647',
                '-f',
                '-t',
                'foobar:test-user',
                test_context.as_posix(),
                '--no-cache',
            ]
            assert container_file_actual == container_file_expected

            capsys.readouterr()

            actual_env = kwargs.get('env', None)
            assert actual_env is not None
            assert actual_env.get('DOCKER_BUILDKIT', None) == '1'

            image_name = 'grizzly-scenarios:test-user'
            test_args = Namespace(
                container_system='docker',
                force_build=False,
                local_install=False,
                project_name=None,
                registry='ghcr.io/biometria-se/',
                no_progress=False,
                verbose=False,
            )

            assert build(test_args) == 1

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == (f'\nbuilt image {image_name}\n\n!! failed to tag image {image_name} -> ghcr.io/biometria-se/{image_name}\n')

            assert run_command.call_count == 4

            args, kwargs = run_command.call_args_list[-1]
            assert args[0] == [
                'docker',
                'image',
                'tag',
                image_name,
                f'ghcr.io/biometria-se/{image_name}',
            ]

            actual_env = kwargs.get('env', None)
            assert actual_env.get('DOCKER_BUILDKIT', None) == '1'

            test_args = Namespace(
                container_system='docker',
                force_build=True,
                no_cache=True,
                build=True,
                registry='ghcr.io/biometria-se/',
                project_name='foobar',
                local_install=True,
                no_progress=False,
                verbose=False,
            )

            image_name = 'foobar:test-user'
            assert build(test_args) == 2

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == (
                f'\nbuilt image {image_name}\ntagged image {image_name} -> ghcr.io/biometria-se/{image_name}\n\n!! failed to push image ghcr.io/biometria-se/{image_name}\n'
            )

            assert run_command.call_count == 7

            args, kwargs = run_command.call_args_list[-1]
            assert args[0] == [
                'docker',
                'image',
                'push',
                f'ghcr.io/biometria-se/{image_name}',
            ]

            actual_env = kwargs.get('env', None)
            assert actual_env.get('DOCKER_BUILDKIT', None) == '1'

            assert build(test_args) == 0

            capture = capsys.readouterr()
            assert capture.err == ''
            assert capture.out == (f'\nbuilt image {image_name}\ntagged image {image_name} -> ghcr.io/biometria-se/{image_name}\npushed image ghcr.io/biometria-se/{image_name}\n')
    finally:
        rm_rf(test_context)

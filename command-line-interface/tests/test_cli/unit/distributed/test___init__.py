"""Tests for grizzly_cli.distributed."""

from __future__ import annotations

import json
import sys
from argparse import ArgumentParser, Namespace
from contextlib import suppress
from datetime import datetime, timezone
from os import environ
from tempfile import gettempdir
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.distributed import create_parser, distributed, distributed_run
from grizzly_cli.utils import RunCommandResult, rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test_distributed(mocker: MockerFixture) -> None:
    run_mocked = mocker.patch('grizzly_cli.distributed.run', return_value=0)
    build_mocked = mocker.patch('grizzly_cli.distributed.do_build', return_value=5)
    clean_mocked = mocker.patch('grizzly_cli.distributed.do_clean', return_value=10)

    arguments = Namespace(subcommand='run')
    assert distributed(arguments) == 0

    assert run_mocked.call_count == 1
    args, _ = run_mocked.call_args_list[0]
    assert args[0] is arguments
    assert args[1] is distributed_run

    assert build_mocked.call_count == 0
    assert clean_mocked.call_count == 0

    arguments = Namespace(subcommand='build')
    assert distributed(arguments) == 5

    assert run_mocked.call_count == 1
    assert build_mocked.call_count == 1
    assert clean_mocked.call_count == 0
    args, _ = build_mocked.call_args_list[0]
    assert args[0] is arguments

    arguments = Namespace(subcommand='clean')
    assert distributed(arguments) == 10

    assert run_mocked.call_count == 1
    assert build_mocked.call_count == 1
    assert clean_mocked.call_count == 1
    args, _ = clean_mocked.call_args_list[0]
    assert args[0] is arguments

    arguments = Namespace(subcommand='foo')
    with pytest.raises(ValueError, match='unknown subcommand foo'):
        distributed(arguments)


def test_distributed_run(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    mocker.patch('grizzly_cli.distributed.getuser', return_value='test-user')
    get_default_mtu_mock = mocker.patch('grizzly_cli.distributed.get_default_mtu', return_value=None)
    do_build_mock = mocker.patch('grizzly_cli.distributed.do_build', return_value=None)
    list_images_mock = mocker.patch('grizzly_cli.distributed.list_images', return_value=None)

    import grizzly_cli.distributed

    mocker.patch.object(grizzly_cli.distributed, 'EXECUTION_CONTEXT', '/srv/grizzly/execution-context')
    mocker.patch.object(grizzly_cli.distributed, 'STATIC_CONTEXT', '/srv/grizzly/static-context')
    mocker.patch.object(grizzly_cli.distributed, 'MOUNT_CONTEXT', '/srv/grizzly/mount-context')
    mocker.patch.object(grizzly_cli.distributed, 'PROJECT_NAME', 'grizzly-cli-test-project')

    run_command_result = RunCommandResult(return_code=1)
    run_command_result.abort_timestamp = datetime.now(tz=timezone.utc)

    run_command_mock = mocker.patch('grizzly_cli.distributed.run_command', return_value=None)
    check_output_mock = mocker.patch('grizzly_cli.distributed.subprocess.check_output', return_value=None)

    parser = ArgumentParser()

    sub_parsers = parser.add_subparsers(dest='test')

    create_parser(sub_parsers)

    try:
        run_command_mock.return_value = RunCommandResult(return_code=111)
        check_output_mock.return_value = '{}'
        get_default_mtu_mock.return_value = '1500'
        sys.argv = ['grizzly-cli', 'dist', '--workers', '3', '--tty', 'run', f'{test_context}/test.feature']
        arguments = parser.parse_args()
        setattr(arguments, 'container_system', 'docker')  # noqa: B010
        setattr(arguments, 'file', ' '.join(arguments.file))  # noqa: B010

        # this is set in the devcontainer
        for key in environ:
            if key.startswith('GRIZZLY_'):
                del environ[key]

        assert distributed_run(arguments, {}, {}) == 111
        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == (
            f'!! something in the compose project is not valid, check with:\ngrizzly-cli dist --validate-config --workers 3 --tty run {test_context}/test.feature\n'
        )

        with suppress(KeyError):
            del environ['GRIZZLY_MTU']

        run_command_mock.return_value = RunCommandResult(return_code=0)
        do_build_mock.return_value = 255
        check_output_mock.return_value = '{}'
        get_default_mtu_mock.return_value = None
        list_images_mock.return_value = {}

        assert distributed_run(arguments, {}, {}) == 255
        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == (
            '!! unable to determine MTU, try manually setting GRIZZLY_MTU environment variable if anything other than 1500 is needed\n'
            '!! failed to build grizzly-cli-test-project, rc=255\n'
        )
        assert environ.get('GRIZZLY_MTU', None) == '1500'
        assert environ.get('GRIZZLY_EXECUTION_CONTEXT', None) == '/srv/grizzly/execution-context'
        assert environ.get('GRIZZLY_STATIC_CONTEXT', None) == '/srv/grizzly/static-context'
        assert environ.get('GRIZZLY_MOUNT_CONTEXT', None) == '/srv/grizzly/mount-context'
        assert environ.get('GRIZZLY_PROJECT_NAME', None) == 'grizzly-cli-test-project'
        assert environ.get('GRIZZLY_USER_TAG', None) == 'test-user'
        assert environ.get('GRIZZLY_EXPECTED_WORKERS', None) == '3'
        assert environ.get('GRIZZLY_MASTER_RUN_ARGS', None) is None
        assert environ.get('GRIZZLY_WORKER_RUN_ARGS', None) is None
        assert environ.get('GRIZZLY_COMMON_RUN_ARGS', None) is None
        assert environ.get('GRIZZLY_IMAGE_REGISTRY', None) == ''
        assert environ.get('GRIZZLY_ENVIRONMENT_FILE', '').startswith(gettempdir())
        assert environ.get('GRIZZLY_LIMIT_NOFILE', None) == '10001'
        assert environ.get('GRIZZLY_HEALTH_CHECK_INTERVAL', None) == '5'
        assert environ.get('GRIZZLY_HEALTH_CHECK_TIMEOUT', None) == '3'
        assert environ.get('GRIZZLY_HEALTH_CHECK_RETRIES', None) == '3'
        assert environ.get('GRIZZLY_CONTAINER_TTY', None) == 'true'
        assert environ.get('LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP', None) is None
        assert environ.get('GRIZZLY_MOUNT_PATH', None) == ''

        # this is set in the devcontainer
        for key in environ:
            if key.startswith(('GRIZZLY_', 'LOCUST_')):
                del environ[key]

        arguments = parser.parse_args(
            [
                'dist',
                '--workers',
                '3',
                '--build',
                '--limit-nofile',
                '133700',
                '--health-interval',
                '10',
                '--health-timeout',
                '8',
                '--health-retries',
                '30',
                '--registry',
                'registry.example.com/biometria-se',
                '--wait-for-worker',
                '10000',
                '--project-name',
                'foobar',
                'run',
                f'{test_context}/test.feature',
            ]
        )
        setattr(arguments, 'container_system', 'docker')  # noqa: B010
        setattr(arguments, 'file', ' '.join(arguments.file))  # noqa: B010

        # docker-compose v2
        rcr = RunCommandResult(return_code=1)
        rcr.abort_timestamp = datetime.now(tz=timezone.utc)
        run_command_mock.return_value = None
        run_command_mock.side_effect = [RunCommandResult(return_code=0), rcr, RunCommandResult(return_code=0)]
        do_build_mock.return_value = 0
        check_output_mock.return_value = None
        check_output_mock.side_effect = ['{}', '{}', '<!-- here is the missing logs -->']
        get_default_mtu_mock.return_value = '1400'
        list_images_mock.return_value = {'grizzly-cli-test-project': {'test-user': {}}}

        assert (
            distributed_run(
                arguments,
                {
                    'GRIZZLY_CONFIGURATION_FILE': '/srv/grizzly/execution-context/configuration.yaml',
                    'GRIZZLY_TEST_VAR': 'True',
                },
                {
                    'master': ['--foo', 'bar', '--master'],
                    'worker': ['--bar', 'foo', '--worker'],
                    'common': ['--common', 'true', '-Dcsv-prefix=asdf'],
                },
            )
            == 1
        )
        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == (
            'foobar-test-user-master-1  | <!-- here is the missing logs -->\n'
            '\n!! something went wrong, check full container logs with:\n'
            'docker container logs foobar-test-user-master-1\n'
            'docker container logs foobar-test-user-worker-1\n'
            'docker container logs foobar-test-user-worker-2\n'
            'docker container logs foobar-test-user-worker-3\n'
        )

        assert run_command_mock.call_count == 5
        args, _ = run_command_mock.call_args_list[-3]
        assert args[0] == [
            'docker',
            'compose',
            '-p',
            'foobar-test-user',
            '-f',
            '/srv/grizzly/static-context/compose.yaml',
            'config',
        ]
        args, _ = run_command_mock.call_args_list[-2]
        assert args[0] == [
            'docker',
            'compose',
            '-p',
            'foobar-test-user',
            '-f',
            '/srv/grizzly/static-context/compose.yaml',
            'up',
            '--scale',
            'worker=3',
            '--remove-orphans',
        ]
        args, _ = run_command_mock.call_args_list[-1]
        assert args[0] == [
            'docker',
            'compose',
            '-p',
            'foobar-test-user',
            '-f',
            '/srv/grizzly/static-context/compose.yaml',
            'stop',
        ]

        assert environ.get('GRIZZLY_RUN_FILE', None) == f'{test_context}/test.feature'
        assert environ.get('GRIZZLY_MTU', None) == '1400'
        assert environ.get('GRIZZLY_EXECUTION_CONTEXT', None) == '/srv/grizzly/execution-context'
        assert environ.get('GRIZZLY_STATIC_CONTEXT', None) == '/srv/grizzly/static-context'
        assert environ.get('GRIZZLY_MOUNT_CONTEXT', None) == '/srv/grizzly/mount-context'
        assert environ.get('GRIZZLY_PROJECT_NAME', None) == 'foobar'
        assert environ.get('GRIZZLY_USER_TAG', None) == 'test-user'
        assert environ.get('GRIZZLY_EXPECTED_WORKERS', None) == '3'
        assert environ.get('GRIZZLY_MASTER_RUN_ARGS', None) == '--foo bar --master'
        assert environ.get('GRIZZLY_WORKER_RUN_ARGS', None) == '--bar foo --worker'
        assert environ.get('GRIZZLY_COMMON_RUN_ARGS', None) == '--common true -Dcsv-prefix=asdf'
        assert environ.get('GRIZZLY_ENVIRONMENT_FILE', '').startswith(gettempdir())
        assert environ.get('GRIZZLY_LIMIT_NOFILE', None) == '133700'
        assert environ.get('GRIZZLY_HEALTH_CHECK_INTERVAL', None) == '10'
        assert environ.get('GRIZZLY_HEALTH_CHECK_TIMEOUT', None) == '8'
        assert environ.get('GRIZZLY_HEALTH_CHECK_RETRIES', None) == '30'
        assert environ.get('GRIZZLY_IMAGE_REGISTRY', None) == 'registry.example.com/biometria-se'
        assert environ.get('GRIZZLY_CONTAINER_TTY', None) == 'false'
        assert environ.get('LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP', None) == '10000'
        assert environ.get('GRIZZLY_MOUNT_PATH', None) == ''

        arguments.project_name = None

        # this is set in the devcontainer
        for key in environ:
            if key.startswith(('GRIZZLY_', 'LOCUST_')):
                del environ[key]

        arguments = parser.parse_args(
            [
                'dist',
                '--workers',
                '1',
                '--id',
                'suffix',
                '--validate-config',
                '--limit-nofile',
                '20000',
                '--health-interval',
                '10',
                '--health-timeout',
                '8',
                '--health-retries',
                '30',
                '--wait-for-worker',
                '1.25 * WORKER_REPORT_INTERVAL',
                'run',
                f'{test_context}/test.feature',
            ]
        )
        setattr(arguments, 'container_system', 'docker')  # noqa: B010
        setattr(arguments, 'file', ' '.join(arguments.file))  # noqa: B010

        run_command_mock.return_value = None
        run_command_mock.side_effect = [RunCommandResult(return_code=13)]
        do_build_mock.return_value = 0
        check_output_mock.return_value = None
        check_output_mock.side_effect = [json.dumps([{'Source': '/srv/grizzly/mount-context', 'Destination': '/srv/grizzly'}]), '13']
        get_default_mtu_mock.return_value = '1800'
        list_images_mock.return_value = {'grizzly-cli-test-project': {'test-user': {}}}

        assert (
            distributed_run(
                arguments,
                {
                    'GRIZZLY_CONFIGURATION_FILE': '/srv/grizzly/execution-context/configuration.yaml',
                    'GRIZZLY_TEST_VAR': 'True',
                },
                {
                    'master': ['--foo', 'bar', '--master'],
                    'worker': ['--bar', 'foo', '--worker'],
                    'common': ['--common', 'true'],
                },
            )
            == 13
        )
        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        assert run_command_mock.call_count == 6
        args, _ = run_command_mock.call_args_list[-1]
        assert args[0] == [
            'docker',
            'compose',
            '-p',
            'grizzly-cli-test-project-suffix-test-user',
            '-f',
            '/srv/grizzly/static-context/compose.yaml',
            'config',
        ]

        assert environ.get('GRIZZLY_RUN_FILE', None) == f'{test_context}/test.feature'
        assert environ.get('GRIZZLY_MTU', None) == '1800'
        assert environ.get('GRIZZLY_EXECUTION_CONTEXT', None) == '/srv/grizzly/execution-context'
        assert environ.get('GRIZZLY_STATIC_CONTEXT', None) == '/srv/grizzly/static-context'
        assert environ.get('GRIZZLY_MOUNT_CONTEXT', None) == '/srv/grizzly/mount-context'
        assert environ.get('GRIZZLY_PROJECT_NAME', None) == 'grizzly-cli-test-project'
        assert environ.get('GRIZZLY_USER_TAG', None) == 'test-user'
        assert environ.get('GRIZZLY_EXPECTED_WORKERS', None) == '1'
        assert environ.get('GRIZZLY_MASTER_RUN_ARGS', None) == '--foo bar --master'
        assert environ.get('GRIZZLY_WORKER_RUN_ARGS', None) == '--bar foo --worker'
        assert environ.get('GRIZZLY_COMMON_RUN_ARGS', None) == '--common true'
        assert environ.get('GRIZZLY_ENVIRONMENT_FILE', '').startswith(gettempdir())
        assert environ.get('GRIZZLY_LIMIT_NOFILE', None) == '20000'
        assert environ.get('GRIZZLY_HEALTH_CHECK_INTERVAL', None) == '10'
        assert environ.get('GRIZZLY_HEALTH_CHECK_TIMEOUT', None) == '8'
        assert environ.get('GRIZZLY_HEALTH_CHECK_RETRIES', None) == '30'
        assert environ.get('LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP', None) == '1.25 * WORKER_REPORT_INTERVAL'
        assert environ.get('GRIZZLY_MOUNT_PATH', None) == 'execution-context'

    finally:
        rm_rf(test_context)

        for key in environ:
            if key.startswith('GRIZZLY_'):
                del environ[key]

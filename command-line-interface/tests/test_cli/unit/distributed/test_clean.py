"""Tests for grizzly_cli.distributed.clean."""

from __future__ import annotations

from argparse import Namespace
from typing import TYPE_CHECKING

from grizzly_cli.distributed.clean import clean
from grizzly_cli.utils import RunCommandResult

if TYPE_CHECKING:
    from pytest_mock import MockerFixture


def test_clean(mocker: MockerFixture) -> None:  # noqa: PLR0915
    import grizzly_cli.distributed.clean

    mocker.patch.object(grizzly_cli.distributed.clean, 'STATIC_CONTEXT', '/srv/grizzly/static-context')
    mocker.patch.object(grizzly_cli.distributed.clean, 'PROJECT_NAME', 'grizzly-cli-test-project')

    arguments = Namespace(networks=True, images=True, project_name='foobar', container_system='docker', id=None)
    mocker.patch('grizzly_cli.distributed.clean.getuser', return_value='root')
    mocker.patch('grizzly_cli.distributed.clean.get_terminal_size', return_value=(1024, 1024))

    run_command_spy = mocker.patch(
        'grizzly_cli.distributed.clean.run_command',
        side_effect=[
            RunCommandResult(return_code=0),
            RunCommandResult(return_code=1),
            RunCommandResult(return_code=2),
            RunCommandResult(return_code=3),
            RunCommandResult(return_code=4),
            RunCommandResult(return_code=5),
            RunCommandResult(return_code=6),
            RunCommandResult(return_code=7),
        ],
    )

    assert clean(arguments) == 0

    assert run_command_spy.call_count == 3

    args, kwargs = run_command_spy.call_args_list[0]
    assert args[0] == [
        'docker',
        'compose',
        '-f',
        '/srv/grizzly/static-context/compose.yaml',
        '-p',
        'foobar-root',
        'rm',
        '-f',
        '-s',
        '-v',
    ]

    actual_env = kwargs.get('env', None)
    assert actual_env is not None

    assert actual_env.get('GRIZZLY_ENVIRONMENT_FILE', None) is not None
    assert actual_env.get('GRIZZLY_PROJECT_NAME', None) == 'foobar'
    assert actual_env.get('GRIZZLY_USER_TAG', None) == 'root'
    assert actual_env.get('GRIZZLY_CONTAINER_TTY', None) == 'false'
    assert actual_env.get('GRIZZLY_LIMIT_NOFILE', None) == '1024'
    assert actual_env.get('COLUMNS', None) == '1024'
    assert actual_env.get('LINES', None) == '1024'

    args, kwargs = run_command_spy.call_args_list[1]
    assert args[0] == [
        'docker',
        'image',
        'rm',
        'foobar:root',
    ]

    assert kwargs == {}

    args, kwargs = run_command_spy.call_args_list[2]
    assert args[0] == [
        'docker',
        'network',
        'rm',
        'foobar-root_default',
    ]

    assert kwargs == {}

    run_command_spy.reset_mock()

    # do not remove images
    arguments.project_name = None
    arguments.id = 'foobar'
    arguments.images = False

    assert clean(arguments) == 3

    assert run_command_spy.call_count == 2

    args, kwargs = run_command_spy.call_args_list[0]
    assert args[0] == [
        'docker',
        'compose',
        '-f',
        '/srv/grizzly/static-context/compose.yaml',
        '-p',
        'grizzly-cli-test-project-foobar-root',
        'rm',
        '-f',
        '-s',
        '-v',
    ]

    assert kwargs.get('env', None) is not None

    args, kwargs = run_command_spy.call_args_list[1]
    assert args[0] == [
        'docker',
        'network',
        'rm',
        'grizzly-cli-test-project-foobar-root_default',
    ]

    assert kwargs == {}

    run_command_spy.reset_mock()

    # do not remove networks
    arguments.images = True
    arguments.networks = False
    arguments.id = None

    assert clean(arguments) == 5

    assert run_command_spy.call_count == 2

    args, kwargs = run_command_spy.call_args_list[0]
    assert args[0] == [
        'docker',
        'compose',
        '-f',
        '/srv/grizzly/static-context/compose.yaml',
        '-p',
        'grizzly-cli-test-project-root',
        'rm',
        '-f',
        '-s',
        '-v',
    ]

    assert kwargs.get('env', None) is not None

    args, kwargs = run_command_spy.call_args_list[1]
    assert args[0] == [
        'docker',
        'image',
        'rm',
        'grizzly-cli-test-project:root',
    ]

    assert kwargs == {}

    run_command_spy.reset_mock()

    # do not remove images or networks
    arguments.images = False

    assert clean(arguments) == 7

    assert run_command_spy.call_count == 1

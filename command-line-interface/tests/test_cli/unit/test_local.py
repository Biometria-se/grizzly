"""Tests for grizzly_cli.local."""

from __future__ import annotations

from argparse import ArgumentParser, Namespace
from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.local import create_parser, local, local_run
from grizzly_cli.utils import RunCommandResult, rm_rf

if TYPE_CHECKING:
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test_local(mocker: MockerFixture) -> None:
    run_mocked = mocker.patch('grizzly_cli.local.run', return_value=0)

    arguments = Namespace(subcommand='run')

    assert local(arguments) == 0
    assert run_mocked.call_count == 1
    args, _ = run_mocked.call_args_list[0]
    assert args[0] is arguments
    assert args[1] is local_run

    arguments = Namespace(subcommand='foo')
    with pytest.raises(ValueError, match='unknown subcommand foo'):
        local(arguments)


def test_local_run(mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
    run_command = mocker.patch('grizzly_cli.local.run_command', return_value=RunCommandResult(return_code=0))
    test_context = tmp_path_factory.mktemp('test_context')
    (test_context / 'test.feature').write_text('Feature:')

    parser = ArgumentParser()

    sub_parsers = parser.add_subparsers(dest='test')

    create_parser(sub_parsers)

    try:
        assert environ.get('GRIZZLY_TEST_VAR', None) is None

        arguments = parser.parse_args(
            [
                'local',
                'run',
                f'{test_context}/test.feature',
            ]
        )

        arguments.file = ' '.join(arguments.file)

        assert (
            local_run(
                arguments,
                {
                    'GRIZZLY_TEST_VAR': 'True',
                },
                {
                    'master': ['--foo', 'bar', '--master'],
                    'worker': ['--bar', 'foo', '--worker'],
                    'common': ['--common', 'true'],
                },
            )
            == 0
        )

        assert run_command.call_count == 1
        args, _ = run_command.call_args_list[-1]
        assert args[0] == [
            'behave',
            f'{test_context}/test.feature',
            '--foo',
            'bar',
            '--master',
            '--bar',
            'foo',
            '--worker',
            '--common',
            'true',
        ]

        assert environ.get('GRIZZLY_TEST_VAR', None) == 'True'

        assert (
            local_run(
                arguments,
                {
                    'GRIZZLY_TEST_VAR': 'True',
                },
                {
                    'master': ['--foo', 'bar', '--master'],
                    'worker': ['--bar', 'foo', '--worker'],
                    'common': ['--common', 'true', '-Dcsv-prefix="cool beans"'],
                },
            )
            == 0
        )

        assert run_command.call_count == 2
        args, _ = run_command.call_args_list[-1]
        assert args[0] == [
            'behave',
            f'{test_context}/test.feature',
            '--foo',
            'bar',
            '--master',
            '--bar',
            'foo',
            '--worker',
            '--common',
            'true',
            '-Dcsv-prefix="cool beans"',
        ]

        assert environ.get('GRIZZLY_TEST_VAR', None) == 'True'
    finally:
        rm_rf(test_context)
        with suppress(KeyError):
            del environ['GRIZZLY_TEST_VAR']

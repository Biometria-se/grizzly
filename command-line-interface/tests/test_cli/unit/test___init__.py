"""Tests for grizzly_cli."""

from __future__ import annotations

from contextlib import suppress
from importlib import reload
from inspect import getfile
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

from test_cli.helpers import cwd, rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test___import__(tmp_path_factory: TempPathFactory, mocker: MockerFixture) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    try:
        with cwd(test_context):
            environ['GRIZZLY_MOUNT_CONTEXT'] = '/srv/grizzly'

            import grizzly_cli

            reload(grizzly_cli)
            mocker.patch.object(grizzly_cli, '__version__', '1.2.3')

            static_context = Path.joinpath(Path(getfile(grizzly_cli)).parent, 'static')

            assert grizzly_cli.__version__ == '1.2.3'
            assert test_context.as_posix() == grizzly_cli.EXECUTION_CONTEXT
            assert grizzly_cli.MOUNT_CONTEXT == '/srv/grizzly'
            assert static_context.as_posix() == grizzly_cli.STATIC_CONTEXT
            assert test_context.name == grizzly_cli.PROJECT_NAME
            assert len(grizzly_cli.SCENARIOS) == 0
    finally:
        rm_rf(test_context)

        with suppress(KeyError):
            del environ['GRIZZLY_MOUNT_CONTEXT']

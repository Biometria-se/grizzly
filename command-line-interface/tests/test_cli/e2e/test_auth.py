"""Tests for grizzly-cli auth."""

from __future__ import annotations

import re
from contextlib import contextmanager, suppress
from os import environ
from typing import TYPE_CHECKING

import pytest

from test_cli.helpers import rm_rf, run_command

if TYPE_CHECKING:
    from collections.abc import Generator

    from _pytest.tmpdir import TempPathFactory


@contextmanager
def auth_via(tmp_path_factory: TempPathFactory, method: str) -> Generator[tuple[str | None, str | None], None, None]:
    secret = 'asdfasdf'  # noqa: S105
    test_context = tmp_path_factory.mktemp('test_context')
    argument: str | None = None
    stdin: str | None = None

    if method == 'env':
        environ['OTP_SECRET'] = secret
        argument = None
    elif method == 'stdin':
        argument = '-'
        stdin = secret
    elif method == 'file':
        file = test_context / 'secret.txt'
        file.write_text(f'{secret}\n')
        argument = str(file)

    try:
        yield (argument, stdin)
    finally:
        if method == 'env':
            with suppress(KeyError):
                del environ['OTP_SECRET']

        rm_rf(test_context)


@pytest.mark.parametrize('method', ['env', 'file', 'stdin'])
def test_e2e_auth(tmp_path_factory: TempPathFactory, method: str) -> None:
    with auth_via(tmp_path_factory, method) as context:
        argument, stdin = context
        command = ['grizzly-cli', 'auth']
        if argument is not None:
            command.append(argument)

        rc, output = run_command(command, stdin=stdin)

        assert rc == 0

        result = ''.join(output).strip()

        assert re.match(r'^[0-9]{6}$', result)

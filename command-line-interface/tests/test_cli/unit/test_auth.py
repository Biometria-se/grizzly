"""Tests for grizzly_cli.auth."""

from __future__ import annotations

import sys
from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.__main__ import _parse_arguments
from grizzly_cli.auth import auth

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test_auth_env(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    try:
        sys.argv = ['grizzly-cli', 'auth']
        arguments = _parse_arguments()

        with pytest.raises(ValueError, match='environment variable OTP_SECRET is not set'):
            auth(arguments)

        capsys.readouterr()

        environ['OTP_SECRET'] = 'f00bar='  # noqa: S105

        with pytest.raises(ValueError, match='unable to generate TOTP code: Non-base32 digit found'):
            auth(arguments)

        environ['OTP_SECRET'] = 'asdfasdf'  # noqa: S105
        mocker.patch('grizzly_cli.auth.TOTP.now', return_value=111111)

        assert auth(arguments) == 0

        capture = capsys.readouterr()

        assert capture.err == ''
        assert capture.out == '111111\n'
    finally:
        with suppress(KeyError):
            del environ['OTP_SECRET']


def test_auth_stdin(capsys: CaptureFixture, mocker: MockerFixture) -> None:
    sys.argv = ['grizzly-cli', 'auth', '-']
    arguments = _parse_arguments()

    mocker.patch('sys.stdin.read', return_value=None)

    with pytest.raises(ValueError, match='OTP secret could not be read from stdin'):
        auth(arguments)

    mocker.patch('sys.stdin.read', return_value=' ')

    with pytest.raises(ValueError, match='OTP secret could not be read from stdin'):
        auth(arguments)

    mocker.patch('sys.stdin.read', return_value='f00bar=')

    with pytest.raises(ValueError, match='unable to generate TOTP code: Non-base32 digit found'):
        auth(arguments)

    capsys.readouterr()

    mocker.patch('grizzly_cli.auth.TOTP.now', return_value=222222)
    mocker.patch('sys.stdin.read', return_value='asdfasdf')

    assert auth(arguments) == 0

    capture = capsys.readouterr()

    assert capture.err == ''
    assert capture.out == '222222\n'


def test_auth_file(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    file = test_context / 'secret.txt'

    sys.argv = ['grizzly-cli', 'auth', str(file)]
    arguments = _parse_arguments()

    with pytest.raises(ValueError, match=f'file {file.as_posix()} does not exist'):
        auth(arguments)

    file.write_text(' ')

    with pytest.raises(ValueError, match=f'file {file.as_posix()} does not seem to contain a single line with a valid OTP secret'):
        auth(arguments)

    file.write_text('aasdf\nasdfasdf\n')

    with pytest.raises(ValueError, match=f'file {file.as_posix()} does not seem to contain a single line with a valid OTP secret'):
        auth(arguments)

    file.write_text('hello world\n')

    with pytest.raises(ValueError, match=f'file {file.as_posix()} does not seem to contain a single line with a valid OTP secret'):
        auth(arguments)

    file.write_text('f00bar=\n')

    with pytest.raises(ValueError, match='unable to generate TOTP code: Non-base32 digit found'):
        auth(arguments)

    file.write_text('asdfasdf')
    mocker.patch('grizzly_cli.auth.TOTP.now', return_value=333333)

    assert auth(arguments) == 0

    capture = capsys.readouterr()

    assert capture.err == ''
    assert capture.out == '333333\n'

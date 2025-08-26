"""Functionality for `grizzly-cli auth ...`."""

from __future__ import annotations

import sys
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

from pyotp import TOTP

from grizzly_cli import register_parser

if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace as Arguments

    from grizzly_cli.argparse import ArgumentSubParser


@register_parser()
def create_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli auth
    auth_parser = sub_parser.add_parser('auth', description=('grizzly stateless authenticator application'))

    auth_parser.add_argument(
        'input',
        nargs='?',
        type=str,
        default=None,
        const=None,
        help=('where to read OTP secret, nothing specified means environment variable OTP_SECRET, `-` means stdin and anything else is considered a file'),
    )

    if auth_parser.prog != 'grizzly-cli auth':  # pragma: no cover
        auth_parser.prog = 'grizzly-cli auth'


def auth(args: Arguments) -> int:
    secret: str | None = None

    if args.input is None:
        secret = environ.get('OTP_SECRET', None)
        if secret is None:
            message = 'environment variable OTP_SECRET is not set'
            raise ValueError(message)
    elif args.input == '-':
        try:
            secret = sys.stdin.read().strip()
        except:  # noqa: S110
            pass
        finally:
            if secret is None or len(secret.strip()) < 1:
                message = 'OTP secret could not be read from stdin'
                raise ValueError(message)
    else:
        input_file = Path(args.input)

        if not input_file.exists():
            message = f'file {input_file.as_posix()} does not exist'
            raise ValueError(message)

        secret = input_file.read_text().strip()

        if ' ' in secret or len(secret.split('\n')) > 1 or secret == '':
            message = f'file {input_file.as_posix()} does not seem to contain a single line with a valid OTP secret'
            raise ValueError(message)

    try:
        totp = TOTP(secret)

        print(totp.now())
    except Exception as e:
        message = f'unable to generate TOTP code: {e!s}'
        raise ValueError(message) from e

    return 0

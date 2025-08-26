"""Main entrypoint for grizzly_cli."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from shutil import which
from traceback import format_exc
from typing import TYPE_CHECKING

from grizzly_cli import __common_version__, __version__, register_parser
from grizzly_cli.argparse import ArgumentParser
from grizzly_cli.auth import auth
from grizzly_cli.distributed import distributed
from grizzly_cli.init import init
from grizzly_cli.keyvault import keyvault
from grizzly_cli.local import local
from grizzly_cli.utils import ask_yes_no, get_dependency_versions, get_distributed_system, setup_logging

if TYPE_CHECKING:
    import argparse


def _create_parser() -> ArgumentParser:
    parser = ArgumentParser(
        description=(
            'the command line interface for grizzly, which makes it easer to start a test with all features of grizzly wrapped up nicely.\n\n'
            'installing it is a matter of:\n\n'
            '```bash\n'
            'pip install grizzly-loadtester-cli\n'
            '```\n\n'
            'enable bash completion by adding the following to your shell profile:\n\n'
            '```bash\n'
            'eval "$(grizzly-cli --bash-completion)"\n'
            '```'
        ),
        markdown_help=True,
        bash_completion=True,
    )

    if parser.prog != 'grizzly-cli':
        parser.prog = 'grizzly-cli'

    parser.add_argument(
        '--version',
        nargs='?',
        default=None,
        const=True,
        choices=['all'],
        help='print version of command line interface, and exit. add argument `all` to get versions of dependencies',
    )

    sub_parser = parser.add_subparsers(dest='command')

    for create_parser in register_parser.registered:
        create_parser(sub_parser)

    return parser


def _parse_show_version(args: argparse.Namespace) -> None:
    grizzly_versions: tuple[str | None, list[str] | None] | None = None

    if args.version == 'all':
        grizzly_versions, locust_version = get_dependency_versions(local_install=False)
    else:
        grizzly_versions, locust_version = None, None

    tree_branch = '├' if grizzly_versions is not None else '└'

    print(f'grizzly-cli {__version__}')
    print(f'{tree_branch}── grizzly-common {__common_version__}')
    if grizzly_versions is not None:
        grizzly_version, grizzly_extras = grizzly_versions
        if grizzly_version is not None:
            print(f'└── grizzly {grizzly_version}', end='')
            if grizzly_extras is not None and len(grizzly_extras) > 0:
                print(f' ── extras: {", ".join(grizzly_extras)}', end='')
            print()

    if locust_version is not None:
        print(f'    └── locust {locust_version}')

    raise SystemExit(0)


def _parse_run(parser: ArgumentParser, args: argparse.Namespace) -> None:
    if args.command == 'dist':
        if args.limit_nofile < 10001 and not args.yes:
            print('!! this will cause warning messages from locust later on')
            ask_yes_no('are you sure you know what you are doing?')
    elif args.command == 'local' and which('behave') is None:
        parser.error_no_help('"behave" not found in PATH, needed when running local mode')

    if args.testdata_variable is not None:
        for variable in args.testdata_variable:
            try:
                [name, value] = variable.split('=', 1)
                os.environ[f'TESTDATA_VARIABLE_{name}'] = value
            except ValueError:  # noqa: PERF203
                parser.error_no_help('-T/--testdata-variable needs to be in the format NAME=VALUE')

    if args.csv_prefix is None:
        if args.csv_interval is not None:
            parser.error_no_help('--csv-interval can only be used in combination with --csv-prefix')

        if args.csv_flush_interval is not None:
            parser.error_no_help('--csv-flush-interval can only be used in combination with --csv-prefix')


def _parse_arguments() -> argparse.Namespace:
    parser = _create_parser()
    args = parser.parse_args()

    if hasattr(args, 'file'):
        # needed to support file names with spaces, which is escaped (sh-style)
        args.file = ' '.join(args.file)

    if args.version:
        _parse_show_version(args)

    if args.command is None:
        parser.error('no command specified')

    if getattr(args, 'subcommand', None) is None and args.command not in ['init', 'auth']:
        parser.error_no_help(f'no subcommand for {args.command} specified')

    if args.command == 'dist':
        args.container_system = get_distributed_system()

        if args.container_system is None:
            parser.error_no_help('cannot run distributed')

        if args.registry is not None and not args.registry.endswith('/'):
            args.registry = f'{args.registry}/'
    elif args.command in ['init', 'auth']:
        args.subcommand = None

    if args.subcommand == 'run':
        _parse_run(parser, args)
    elif args.command == 'dist' and args.subcommand == 'build':
        args.force_build = args.no_cache
        args.build = not args.no_cache

    log_file = getattr(args, 'log_file', None)
    setup_logging(log_file)

    return args


def _inject_additional_arguments_from_metadata(args: argparse.Namespace) -> argparse.Namespace:
    with Path(args.file).open() as fd:
        file_metadata = [line.strip().replace('# grizzly-cli ', '').split(' ') for line in fd if line.strip().startswith('# grizzly-cli ')]

    if len(file_metadata) < 1:
        return args

    argv = sys.argv[1:]
    for additional_arguments in file_metadata:
        try:
            if additional_arguments[0].strip().startswith('-'):
                raise ValueError

            index = argv.index(additional_arguments[0]) + 1
            for zindex, additional_argument in enumerate(additional_arguments[1:]):
                argv.insert(index + zindex, additional_argument)
        except ValueError:  # noqa: PERF203
            print('?? ignoring {}'.format(' '.join(additional_arguments)))

    sys.argv = sys.argv[0:1] + argv

    return _parse_arguments()


def main() -> int:
    args: argparse.Namespace | None = None

    try:
        args = _parse_arguments()

        if getattr(args, 'file', None) is not None and args.command not in ['keyvault']:
            args = _inject_additional_arguments_from_metadata(args)

        if args.command == 'local':
            rc = local(args)
        elif args.command == 'dist':
            rc = distributed(args)
        elif args.command == 'init':
            rc = init(args)
        elif args.command == 'auth':
            rc = auth(args)
        elif args.command == 'keyvault':
            rc = keyvault(args)
        else:
            message = f'unknown command {args.command}'
            raise ValueError(message)
    except (KeyboardInterrupt, ValueError) as e:
        print()
        if isinstance(e, ValueError):
            exception = format_exc() if args is not None and getattr(args, 'verbose', False) else str(e)

            print(exception)

        print('\n!! aborted grizzly-cli')
        return 1
    else:
        return rc

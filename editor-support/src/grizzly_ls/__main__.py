from __future__ import annotations

import argparse
import logging
import sys
from os import environ


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog='grizzly-ls')

    parser.add_argument(
        '--version',
        action='store_true',
        required=False,
        default=False,
        help='print version and exit',
    )

    parser.add_argument(
        '--socket',
        action='store_true',
        required=False,
        default=False,
        help='run server in socket mode',
    )

    parser.add_argument(
        '--socket-port',
        type=int,
        default=4444,
        required=False,
        help='port the language server should listen on',
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        required=False,
        default=False,
        help='verbose output from server',
    )

    parser.add_argument(
        '--no-verbose',
        nargs='+',
        type=str,
        default=None,
        help='name of loggers to disable',
    )

    parser.add_argument(
        '--embedded',
        action='store_true',
        default=False,
        required=False,
        help='controlls logging, added when started by editor',
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        required=False,
        default=False,
        help='run server in debug mode',
    )

    parser.add_argument(
        '--debug-port',
        type=int,
        default=5678,
        required=False,
        help='port the language server should listen on for debugging',
    )

    parser.add_argument(
        '--debug-wait',
        action='store_true',
        required=False,
        default=False,
        help='wait for debug client to connect',
    )

    subparsers = parser.add_subparsers(dest='command')

    lint_parser = subparsers.add_parser('lint', help='command line lint files')
    lint_parser.add_argument('files', nargs='+', type=str, help='files to lint')

    render_parser = subparsers.add_parser('render', help='render a feature file')
    render_parser.add_argument('file', type=str, nargs=1, help='feature file')

    args = parser.parse_args()

    if args.version:
        from grizzly_ls import __version__

        print(__version__, file=sys.stderr)

        raise SystemExit(0)

    return args


def setup_logging(args: argparse.Namespace) -> None:
    handlers: list[logging.Handler] = []
    level = logging.INFO if not args.verbose else logging.DEBUG

    if not args.socket and level < logging.INFO:
        file_handler = logging.FileHandler('grizzly-ls.log')
        file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
        handlers.append(file_handler)

    if not args.embedded:
        stream_handler = logging.StreamHandler(sys.stderr)
        stream_handler.setFormatter(logging.Formatter('server/%(levelname)s: %(message)s'))
        handlers.append(stream_handler)

    logging.basicConfig(
        level=level,
        handlers=handlers,
    )

    no_verbose: list[str] | None = args.no_verbose

    if no_verbose is None:
        no_verbose = []

    # always supress these loggers
    no_verbose.append('parse')
    no_verbose.append('pip')
    if not args.verbose:
        no_verbose.append('pygls')

    for logger_name in no_verbose:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.ERROR)


def setup_debugging(args: argparse.Namespace) -> str | None:
    if args.debug:
        try:
            import debugpy

            debugpy.listen(args.debug_port)
            logging.info('debugging enabled, listening on port %d', args.debug_port)
            if args.debug_wait:
                logging.info('waiting for debugger to attach')
                debugpy.wait_for_client()
        except ModuleNotFoundError:
            return 'debugging requires the debugpy package to be installed'
    return None


def main() -> int:
    args = parse_arguments()

    if args.embedded:
        environ.update({'GRIZZLY_RUN_EMBEDDED': 'true'})

    from grizzly_ls.server import server

    if args.command == 'lint':
        from grizzly_ls.cli import lint

        return lint(server, args)
    if args.command == 'render':
        from grizzly_ls.cli import render

        return render(args)

    setup_logging(args)
    err_msg = setup_debugging(args)

    if err_msg:
        server.add_startup_error_message(err_msg)

    server.verbose = args.verbose

    if not args.socket:
        server.start_io(sys.stdin.buffer, sys.stdout.buffer)  # type: ignore[arg-type]
    else:
        server.start_tcp('127.0.0.1', args.socket_port)

    return 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main())

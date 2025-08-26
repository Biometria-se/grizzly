"""Functionality for `grizzly-cli dist build ...`."""

from __future__ import annotations

import os
import sys
from argparse import SUPPRESS
from argparse import Namespace as Arguments
from getpass import getuser
from pathlib import Path
from socket import gaierror, gethostbyname
from typing import TYPE_CHECKING

from grizzly_cli import EXECUTION_CONTEXT, PROJECT_NAME, STATIC_CONTEXT
from grizzly_cli.utils import get_dependency_versions, requirements, run_command

if TYPE_CHECKING:  # pragma: no cover
    from grizzly_cli.argparse import ArgumentSubParser


def create_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli dist build ...
    build_parser = sub_parser.add_parser(
        'build',
        description=(
            'build grizzly compose project container image before running test. if worker nodes runs on different physical '
            'computers, it is mandatory to build the images before hand and push to a registry.'
            '\n\n'
            'if image includes IBM MQ native dependencies, the build time increases due to download times. it is possible '
            'to self-host the archive and override the download host with environment variable `IBM_MQ_LIB_HOST`.'
        ),
    )
    build_parser.add_argument(
        '--no-cache',
        action='store_true',
        required=False,
        help='build container image with out cache (full build)',
    )
    build_parser.add_argument(
        '--registry',
        type=str,
        default=None,
        required=False,
        help='push built image to this registry, if the registry has authentication you need to login first',
    )
    build_parser.add_argument(
        '--no-progress',
        action='store_true',
        default=False,
        required=False,
        help='do not show a progress spinner while building',
    )
    build_parser.add_argument(
        '--verbose',
        action='store_true',
        default=False,
        required=False,
        help='show more information',
    )
    # <!-- used during development, hide from help
    build_parser.add_argument(
        '--local-install',
        nargs='?',
        const=True,
        default=False,
        help=SUPPRESS,
    )
    # used during development, hide from help -->

    if build_parser.prog != 'grizzly-cli dist build':  # pragma: no cover
        build_parser.prog = 'grizzly-cli dist build'


def getuid() -> int:
    if sys.platform == 'win32':
        return 1000
    else:  # noqa: RET505
        return os.getuid()


def getgid() -> int:
    if sys.platform == 'win32':
        return 1000
    else:  # noqa: RET505
        return os.getuid()


def _create_build_command(args: Arguments, containerfile: str, tag: str, context: str) -> list[str]:
    local_install = getattr(args, 'local_install', False)

    install_type = 'local' if local_install else 'remote'

    (_, grizzly_extras), _ = get_dependency_versions(local_install=local_install)

    grizzly_extra = 'mq' if grizzly_extras is not None and 'mq' in grizzly_extras else 'base'

    extra_args: list[str] = []

    ibm_mq_lib_host = os.environ.get('IBM_MQ_LIB_HOST', None)
    if ibm_mq_lib_host is not None:
        extra_args += ['--build-arg', f'IBM_MQ_LIB_HOST={ibm_mq_lib_host}']

        if 'host.docker.internal' in ibm_mq_lib_host:
            try:
                host_docker_internal = gethostbyname('host.docker.internal')
            except gaierror:
                host_docker_internal = 'host-gateway'

            extra_args += ['--add-host', f'host.docker.internal:{host_docker_internal}']

    ibm_mq_lib = os.environ.get('IBM_MQ_LIB', None)
    if ibm_mq_lib is not None:
        extra_args += ['--build-arg', f'IBM_MQ_LIB={ibm_mq_lib}']

    return [
        args.container_system,
        'image',
        'build',
        '--ssh',
        'default',
        '--build-arg',
        f'GRIZZLY_EXTRA={grizzly_extra}',
        '--build-arg',
        f'GRIZZLY_INSTALL_TYPE={install_type}',
        '--build-arg',
        f'GRIZZLY_UID={getuid()}',
        '--build-arg',
        f'GRIZZLY_GID={getgid()}',
        *extra_args,
        '-f',
        containerfile,
        '-t',
        tag,
        context,
    ]


@requirements(EXECUTION_CONTEXT)
def build(args: Arguments) -> int:
    tag = getuser()

    image_name = f'{PROJECT_NAME}:{tag}' if args.project_name is None else f'{args.project_name}:{tag}'

    build_command = _create_build_command(
        args,
        Path.joinpath(Path(STATIC_CONTEXT), 'Containerfile').as_posix(),
        image_name,
        EXECUTION_CONTEXT,
    )

    if args.force_build:
        build_command.append('--no-cache')

    # make sure buildkit is used
    build_env = os.environ.copy()
    if args.container_system == 'docker':
        build_env['DOCKER_BUILDKIT'] = '1'

    spinner = 'building' if not getattr(args, 'no_progress', False) else None

    result = run_command(build_command, env=build_env, spinner=spinner, verbose=args.verbose)

    if result.return_code == 0:
        print(f'\nbuilt image {image_name}')

    if getattr(args, 'registry', None) is None or result.return_code != 0:
        return result.return_code

    tag_command = [
        f'{args.container_system}',
        'image',
        'tag',
        image_name,
        f'{args.registry}{image_name}',
    ]

    result = run_command(tag_command, env=build_env, verbose=args.verbose)

    if result.return_code != 0:
        print(f'\n!! failed to tag image {image_name} -> {args.registry}{image_name}')
        return result.return_code

    print(f'tagged image {image_name} -> {args.registry}{image_name}')

    push_command = [
        f'{args.container_system}',
        'image',
        'push',
        f'{args.registry}{image_name}',
    ]

    spinner = 'pushing' if not args.no_progress else None

    result = run_command(push_command, env=build_env, spinner=spinner, verbose=args.verbose)

    if result.return_code != 0:
        print(f'\n!! failed to push image {args.registry}{image_name}')
    else:
        print(f'pushed image {args.registry}{image_name}')

    return result.return_code

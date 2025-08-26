"""Functionality for `grizzly-cli init ...`."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from packaging.version import Version

from grizzly_cli import EXECUTION_CONTEXT, register_parser
from grizzly_cli.utils import ask_yes_no

if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace as Arguments
    from collections.abc import Generator

    from grizzly_cli.argparse import ArgumentSubParser

# prefix components:
space = '    '
branch = '│   '
# pointers:
tee = '├── '
last = '└── '


@register_parser(order=1)
def create_parser(sub_parser: ArgumentSubParser) -> None:
    # grizzly-cli init
    init_parser = sub_parser.add_parser('init', description=('create a skeleton project with required structure and files.'))

    init_parser.add_argument(
        'project',
        nargs=None,
        type=str,
        help='project name, a directory will be created with this name',
    )

    init_parser.add_argument(
        '--grizzly-version',
        type=str,
        required=False,
        default=None,
        help='specify which grizzly version to use for project, default is latest',
    )

    init_parser.add_argument(
        '--with-mq',
        action='store_true',
        default=False,
        required=False,
        help='if grizzly should be installed with IBM MQ support (external dependencies excluded)',
    )

    init_parser.add_argument(
        '-y',
        '--yes',
        action='store_true',
        default=False,
        required=False,
        help='automagically answer yes on any questions',
    )

    if init_parser.prog != 'grizzly-cli init':  # pragma: no cover
        init_parser.prog = 'grizzly-cli init'


def tree(dir_path: Path, prefix: str = '') -> Generator[str, None, None]:
    """Recursive generator, given a directory Path object
    will yield a visual tree structure line by line
    with each line prefixed by the same characters.

    credit: https://stackoverflow.com/a/59109706

    """
    contents = sorted(dir_path.iterdir())
    # contents each get pointers that are ├── with a final └── :
    pointers = [tee] * (len(contents) - 1) + [last]
    for pointer, sub_path in zip(pointers, contents, strict=False):
        yield prefix + pointer + sub_path.name
        if sub_path.is_dir():  # extend the prefix and recurse:
            extension = branch if pointer == tee else space
            # i.e. space because last, └── , above so no more |
            yield from tree(sub_path, prefix=prefix + extension)


def init(args: Arguments) -> int:
    if Path.joinpath(Path(EXECUTION_CONTEXT), args.project).exists():
        print(f'"{args.project}" already exists in {EXECUTION_CONTEXT}')
        return 1

    if all(Path.joinpath(Path(EXECUTION_CONTEXT), p).exists() for p in ['environments', 'features', 'requirements.txt']):
        print('oops, looks like you are already in a grizzly project directory', end='\n\n')
        print(EXECUTION_CONTEXT)
        for line in tree(Path(EXECUTION_CONTEXT)):
            print(line)
        return 1

    layout = f"""
    {args.project}
    ├── environments
    │   └── {args.project}.yaml
    ├── features
    │   ├── environment.py
    │   ├── steps
    │   │   └── steps.py
    │   ├── {args.project}.feature
    │   └── requests
    └── requirements.txt
"""

    message = f'the following structure will be created:\n{layout}'

    if not args.yes:
        ask_yes_no(f'{message}\ndo you want to create grizzly project "{args.project}"?')
    else:
        print(message)

    # create project root
    structure = Path.joinpath(Path(EXECUTION_CONTEXT), args.project)
    structure.mkdir()

    # create requirements.txt
    grizzly_dependency = 'grizzly-loadtester'

    if args.with_mq:
        grizzly_dependency = f'{grizzly_dependency}[mq]'

    if args.grizzly_version is not None:
        grizzly_dependency = f'{grizzly_dependency}=={args.grizzly_version}'

    (structure / 'requirements.txt').write_text(f'{grizzly_dependency}\n')

    # create environments/
    structure_environments = structure / 'environments'
    structure_environments.mkdir()

    # create environments/<project>.yaml
    (structure_environments / f'{args.project}.yaml').write_text("""configuration:
  template:
    host: https://localhost
""")

    # create features/ directory
    structure_features = structure / 'features'
    structure_features.mkdir()

    # create features/<project>.feature
    (structure_features / f'{args.project}.feature').write_text("""Feature: Template feature file
  Scenario: Template scenario
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
""")

    # create features/environment.py
    if args.grizzly_version is not None:
        version = Version(args.grizzly_version)
        if version < Version('2.6.0'):
            grizzly_behave_module = 'environment'
    else:
        grizzly_behave_module = 'behave'
    (structure_features / 'environment.py').write_text(f'from grizzly.{grizzly_behave_module} import *\n\n')

    # create features/requests directory
    (structure_features / 'requests').mkdir()

    # create features/steps directory
    structure_feature_steps = structure_features / 'steps'
    structure_feature_steps.mkdir()

    # create features/steps/steps.py
    (structure_feature_steps / 'steps.py').write_text('from grizzly.steps import *\n\n')

    print(f'successfully created project "{args.project}", with the following options:')
    print(f'{" " * 2}\u2022 {"with" if args.with_mq else "without"} IBM MQ support')
    if args.grizzly_version is not None:
        print(f'{" " * 2}\u2022 pinned to grizzly version {args.grizzly_version}')
    else:
        print(f'{" " * 2}\u2022 latest grizzly version')

    return 0

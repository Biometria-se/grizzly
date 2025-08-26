"""Functionality for `grizzly-cli ... run ...`."""

from __future__ import annotations

import os
import sys
from contextlib import suppress
from datetime import datetime
from pathlib import Path
from platform import node as get_hostname
from typing import (
    TYPE_CHECKING,
    TextIO,
    cast,
)

from jinja2 import Environment

import grizzly_cli
from grizzly_cli.argparse.bashcompletion import BashCompletionTypes
from grizzly_cli.utils import (
    ask_yes_no,
    distribution_of_users_per_scenario,
    find_metadata_notices,
    find_variable_names_in_questions,
    get_input,
    logger,
    parse_feature_file,
    requirements,
    rm_rf,
)
from grizzly_cli.utils.configuration import ScenarioTag, get_context_root, load_configuration

if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace as Arguments
    from collections.abc import Callable

    from grizzly_cli.argparse import ArgumentSubParser


def create_parser(sub_parser: ArgumentSubParser, parent: str) -> None:
    # grizzly-cli ... run ...
    run_parser = sub_parser.add_parser('run', description='execute load test scenarios specified in a feature file.')
    run_parser.add_argument(
        '--verbose',
        action='store_true',
        required=False,
        help=(
            'changes the log level to `DEBUG`, regardless of what it says in the feature file. gives more verbose logging '
            'that can be useful when troubleshooting a problem with a scenario.'
        ),
    )
    run_parser.add_argument(
        '-T',
        '--testdata-variable',
        action='append',
        type=str,
        required=False,
        help=('specified in the format `<name>=<value>`. avoids being asked for an initial value for a scenario variable.'),
    )
    run_parser.add_argument(
        '-y',
        '--yes',
        action='store_true',
        default=False,
        required=False,
        help='answer yes on any questions that would require confirmation',
    )
    run_parser.add_argument(
        '-e',
        '--environment-file',
        type=BashCompletionTypes.File('*.yaml', '*.yml'),
        required=False,
        default=None,
        help='configuration file with [environment specific information][framework.usage.variables.environment-configuration]',
    )
    run_parser.add_argument(
        '--csv-prefix',
        nargs='?',
        const=True,
        default=None,
        help='write log statistics to CSV files with specified prefix, if no value is specified the description of the gherkin Feature tag will be used, suffixed with timestamp',
    )
    run_parser.add_argument(
        '--csv-interval',
        type=int,
        default=None,
        required=False,
        help='interval that statistics is collected for CSV files, can only be used in combination with `--csv-prefix`',
    )
    run_parser.add_argument(
        '--csv-flush-interval',
        type=int,
        default=None,
        required=False,
        help='interval that CSV statistics is flushed to disk, can only be used in combination with `--csv-prefix`',
    )
    run_parser.add_argument(
        '-l',
        '--log-file',
        type=str,
        default=None,
        required=False,
        help='save all `grizzly-cli` run output in specified log file',
    )
    run_parser.add_argument(
        '--log-dir',
        type=str,
        default=None,
        required=False,
        help='log directory suffix (relative to `requests/logs`) to save log files generated in a scenario',
    )
    run_parser.add_argument(
        '--dump',
        nargs='?',
        default=None,
        const=True,
        help=(
            'Dump parsed contents of file, can be useful when including scenarios from other feature files. If no argument is specified it '
            'will be dumped to stdout, the argument is treated as a filename'
        ),
    )
    run_parser.add_argument(
        '--dry-run',
        action='store_true',
        required=False,
        help='Will setup and run anything up until when locust should start. Useful for debugging feature files when developing new tests',
    )
    run_parser.add_argument(
        '--profile',
        action='store_true',
        required=False,
        help='Enable profiling of grizzly execution, generates a .hprof file upon completion',
    )
    run_parser.add_argument(
        'file',
        nargs='+',
        type=BashCompletionTypes.File('*.feature'),
        help='path to feature file with one or more scenarios',
    )

    if run_parser.prog != f'grizzly-cli {parent} run':  # pragma: no cover
        run_parser.prog = f'grizzly-cli {parent} run'


def should_prompt_questions(args: Arguments, environ: dict) -> None:
    variables = find_variable_names_in_questions(args.file)
    questions = len(variables)
    manual_input = False

    if questions > 0 and not getattr(args, 'validate_config', False):
        logger.info(f'feature file requires values for {questions} variables')

        for variable in variables:
            name = f'TESTDATA_VARIABLE_{variable}'
            value = os.environ.get(name, '')
            while len(value) < 1:
                value = get_input(f'initial value for "{variable}": ')
                manual_input = True

            environ[name] = value

        logger.info('the following values was provided:')
        for key, value in environ.items():
            if not key.startswith('TESTDATA_VARIABLE_'):
                continue
            logger.info(f'{key.replace("TESTDATA_VARIABLE_", "")} = {value}')

        if manual_input:
            ask_yes_no('continue?')


def should_prompt_notices(args: Arguments) -> None:
    notices = find_metadata_notices(args.file)

    if len(notices) > 0:
        output_func = cast('Callable[[str], None]', logger.info) if args.yes else ask_yes_no

        for notice in notices:
            output_func(notice)


def update_grizzly_environment(args: Arguments, environ: dict) -> None:
    if args.environment_file is not None:
        environment_lock_file = load_configuration(Path(args.environment_file).resolve())
        environ.update({'GRIZZLY_CONFIGURATION_FILE': environment_lock_file.as_posix()})

    if args.dry_run:
        environ.update({'GRIZZLY_DRY_RUN': 'true'})

    if args.log_dir is not None:
        environ.update({'GRIZZLY_LOG_DIR': args.log_dir})

    if args.profile:
        environ.update({'GRIZZLY_PROFILE': 'true'})


def build_run_arguments(args: Arguments) -> dict[str, list[str]]:
    run_arguments: dict[str, list[str]] = {
        'master': [],
        'worker': [],
        'common': [],
    }

    if args.verbose:
        run_arguments['common'] += ['--verbose', '--no-logcapture', '--no-capture', '--no-capture-stderr']

    if args.csv_prefix is not None:
        if args.csv_prefix is True:
            parse_feature_file(args.file)
            if grizzly_cli.FEATURE_DESCRIPTION is None:
                message = 'feature file does not seem to have a `Feature:` description to use as --csv-prefix'
                raise ValueError(message)

            csv_prefix = grizzly_cli.FEATURE_DESCRIPTION.replace(' ', '_')
            timestamp = datetime.now().astimezone().strftime('%Y%m%dT%H%M%S')
            args.csv_prefix = f'{csv_prefix}_{timestamp}'

        run_arguments['common'] += [f'-Dcsv-prefix="{args.csv_prefix}"']

        if args.csv_interval is not None:
            run_arguments['common'] += [f'-Dcsv-interval={args.csv_interval}']

        if args.csv_flush_interval is not None:
            run_arguments['common'] += [f'-Dcsv-flush-interval={args.csv_flush_interval}']

    return run_arguments


@requirements(grizzly_cli.EXECUTION_CONTEXT)
def run(args: Arguments, run_func: Callable[[Arguments, dict, dict[str, list[str]]], int]) -> int:
    # always set hostname of host where grizzly-cli was executed, could be useful
    environ: dict = {
        'GRIZZLY_CLI_HOST': get_hostname(),
        'GRIZZLY_EXECUTION_CONTEXT': grizzly_cli.EXECUTION_CONTEXT,
        'GRIZZLY_MOUNT_CONTEXT': grizzly_cli.MOUNT_CONTEXT,
    }

    environment = Environment(autoescape=False, extensions=[ScenarioTag])
    feature_file = Path(args.file)
    environment_lock_file: str | None = None

    # during execution, create a temporary .lock.feature file that will be removed when done
    original_feature_lines = feature_file.read_text().splitlines()
    feature_lock_file = feature_file.parent / f'{feature_file.stem}.lock{feature_file.suffix}'

    try:
        buffer: list[str] = []
        remove_endif = False

        # remove if-statements containing variables (`{$ .. $}`)
        for line in original_feature_lines:
            stripped_line = line.strip()

            if stripped_line[:2] == '{%' and stripped_line[-2:] == '%}':
                if '{$' in stripped_line and '$}' in stripped_line and 'if' in stripped_line:
                    remove_endif = True
                    continue

                if remove_endif and 'endif' in stripped_line:
                    remove_endif = False
                    continue

            buffer.append(line)

        original_feature_content = '\n'.join(buffer)

        template = environment.from_string(original_feature_content)
        environment.extend(feature_file=feature_file, ignore_errors=False)
        feature_content = template.render()
        feature_lock_file.write_text(feature_content)

        if args.dump:
            output: TextIO = Path(args.dump).open('w+') if isinstance(args.dump, str) else sys.stdout  # noqa: SIM115

            try:
                print(feature_content, file=output)
            finally:
                # do not close stdout...
                if output is not sys.stdout:
                    output.close()

            return 0

        args.file = feature_lock_file.as_posix()

        should_prompt_questions(args, environ)
        should_prompt_notices(args)
        update_grizzly_environment(args, environ)

        if not getattr(args, 'validate_config', False):
            distribution_of_users_per_scenario(args, environ)

        run_arguments = build_run_arguments(args)

        return run_func(args, environ, run_arguments)
    finally:
        if environment_lock_file is not None:
            Path(environment_lock_file).unlink(missing_ok=True)

        feature_lock_file.unlink(missing_ok=True)

        with suppress(FileNotFoundError, ValueError):
            tmp_files = get_context_root() / 'files'
            rm_rf(tmp_files)

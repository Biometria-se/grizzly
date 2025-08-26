"""grizzly-cli utils."""

from __future__ import annotations

import logging
import logging.config
import os
import re
import signal as psignal
import stat
import subprocess
import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import wraps
from hashlib import sha1
from importlib.util import find_spec
from json import loads as jsonloads
from math import ceil
from os import environ
from pathlib import Path
from shutil import rmtree, which
from tempfile import mkdtemp
from typing import TYPE_CHECKING, Any, ClassVar, Union, cast

import requests
import tomli
from behave.parser import parse_file as feature_file_parser
from jinja2 import Template
from packaging import version as versioning
from progress.spinner import Spinner
from yaml import Dumper

import grizzly_cli

if TYPE_CHECKING:  # pragma: no cover
    from argparse import Namespace as Arguments
    from types import FrameType, TracebackType

    from behave.model import Scenario

logger = logging.getLogger('grizzly-cli')


class SignalHandler:
    handler: Callable[[int, FrameType | None], None]
    signals: dict[int, Union[Callable[[int, FrameType | None], Any], int, None]]

    def __init__(self, handler: Callable[[int, FrameType | None], None], signal: int, *signals: int) -> None:
        self.handler = handler
        self.signals = {signal: None}

        if signals is not None and len(signals) > 0:
            for sig in signals:
                self.signals.update({sig: None})

    def __enter__(self) -> None:
        for signal in self.signals:
            self.signals.update({signal: psignal.getsignal(signal)})
            psignal.signal(signal, self.handler)

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> bool:
        for signal, handler in self.signals.items():
            psignal.signal(signal, handler)

        return exc is None


@dataclass
class RunCommandResult:
    return_code: int
    abort_timestamp: datetime | None = field(init=False, default=None)
    output: list[bytes] | None = field(init=False, default=None)


def run_command(command: list[str], env: dict[str, str] | None = None, *, silent: bool = False, verbose: bool = False, spinner: str | None = None) -> RunCommandResult:
    if env is None:
        env = environ.copy()

    if verbose:
        logger.info('run_command: %s', ' '.join(command))

    process = subprocess.Popen(
        command,
        env=env,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )

    result = RunCommandResult(return_code=-1)

    if silent:
        result.output = []

    _spinner: Spinner | None = None

    if spinner is not None:  # pragma: no cover
        _spinner = Spinner(f'{spinner} ')

    def sig_handler(*_args: Any, **_kwargs: Any) -> None:  # pragma: no cover
        if result.abort_timestamp is None:
            result.abort_timestamp = datetime.now(timezone.utc)
            process.terminate()

    with SignalHandler(sig_handler, psignal.SIGINT, psignal.SIGTERM):
        try:
            while process.poll() is None:
                stdout = process.stdout
                if stdout is None:
                    break

                if _spinner is not None:  # pragma: no cover
                    _spinner.next()

                output = stdout.readline()
                if not output:
                    break

                if result.output is None:
                    if spinner is None:
                        logger.info(output.decode().rstrip())
                else:
                    result.output.append(output)

            process.terminate()
        except KeyboardInterrupt:
            pass
        finally:
            with suppress(Exception):
                process.kill()

    process.wait()

    if spinner is not None:  # pragma: no cover
        logger.info('')

    result.return_code = process.returncode

    return result


def get_docker_compose_version() -> tuple[int, int, int]:  # pragma: no cover
    output = subprocess.getoutput('docker compose version')  # noqa: S605

    version_line = output.splitlines()[0]

    match = re.match(r'.*version [v]?([1-2]\.[0-9]+\.[0-9]+).*$', version_line)

    return cast('tuple[int, int, int]', tuple([int(part) for part in match.group(1).split('.')])) if match else (0, 0, 0)


def onerror(
    func: Callable,
    path: str,
    exc_info: Union[  # noqa: ARG001
        BaseException,
        tuple[type[BaseException], BaseException, TracebackType | None],
    ],
) -> None:  # pragma: no cover
    """Error handler for shutil.rmtree.

    If the error is due to an access error (read only file)
    it attempts to add write permission and then retries.
    If the error is for another reason it re-raises the error.
    Usage : ``shutil.rmtree(path, onerror=onerror)``
    """
    _path = Path(path)
    # Is the error an access error?
    if not os.access(_path, os.W_OK):
        _path.chmod(stat.S_IWUSR)
        func(path)
    else:
        raise  # noqa: PLE0704


def rm_rf(path: Union[str, Path], *, missing_ok: bool = False) -> None:
    """Remove the path contents recursively, even if some elements
    are read-only.
    """
    p = path.as_posix() if isinstance(path, Path) else path

    try:
        if sys.version_info >= (3, 12):
            rmtree(p, onexc=onerror)
        else:  # pragma: no cover
            rmtree(p, onerror=onerror)
    except FileNotFoundError:  # pragma: no cover
        if not missing_ok:
            raise


def get_dependency_versions(*, local_install: Union[bool, str]) -> tuple[tuple[str | None, list[str] | None], str | None]:  # noqa: C901, PLR0912, PLR0915
    grizzly_requirement: str | None = None
    grizzly_requirement_egg: str
    locust_version: str | None = None
    grizzly_version: str | None = None
    grizzly_extras: list[str] | None = None

    args: tuple[str, ...] = ()
    if isinstance(local_install, str):
        args += (local_install,)
        if not local_install.endswith('requirements.txt'):
            args += ('requirements.txt',)
    else:
        args += ('requirements.txt',)

    project_requirements = Path.joinpath(Path(grizzly_cli.EXECUTION_CONTEXT), *args)

    try:
        with project_requirements.open(encoding='utf-8') as fd:
            for line in fd.readlines():
                if any(pkg in line for pkg in ['grizzly-loadtester', 'grizzly.git'] if not re.match(r'^([\s]+)?#', line)):
                    grizzly_requirement = line.strip()
                    break
    except:
        return (None, None), None

    if grizzly_requirement is None:
        print(f'!! unable to find grizzly dependency in {project_requirements}', file=sys.stderr)
        return ('(unknown)', None), '(unknown)'

    # check if it's a repo or not
    if 'git+' in grizzly_requirement:
        if grizzly_requirement.startswith('git+'):
            url, egg_part = grizzly_requirement.rsplit('#', 1)
            _, grizzly_requirement_egg = egg_part.split('=', 1)
        elif grizzly_requirement.index('@') < grizzly_requirement.index('git+'):
            grizzly_requirement_egg, url = grizzly_requirement.split('@', 1)
            grizzly_requirement_egg = grizzly_requirement_egg.strip()
            url = url.strip()
        else:
            print(f'!! unable to find properly formatted grizzly dependency in {project_requirements}', file=sys.stderr)
            return ('(unknown)', None), '(unknown)'

        url, branch = url.rsplit('@', 1)
        url = url[4:]  # remove git+
        suffix = sha1(grizzly_requirement.encode('utf-8')).hexdigest()  # noqa: S324

        # extras_requirement normalization
        egg = grizzly_requirement_egg.replace('[', '__').replace(']', '__').replace(',', '_')

        tmp_workspace = mkdtemp(prefix='grizzly-cli-')
        repo_destination = Path(tmp_workspace) / f'{egg}_{suffix}'

        try:
            rc = subprocess.check_call(
                [
                    'git',
                    'clone',
                    '--filter=blob:none',
                    '-q',
                    url,
                    repo_destination,
                ],
                shell=False,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            if rc != 0:
                print(f'!! unable to clone git repo {url}', file=sys.stderr)
                raise RuntimeError  # abort

            active_branch = branch

            try:
                active_branch = subprocess.check_output(
                    ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
                    cwd=repo_destination,
                    shell=False,
                    universal_newlines=True,
                ).strip()
                rc = 0
            except subprocess.CalledProcessError as cpe:
                rc = cpe.returncode

            if rc != 0:
                print(f'!! unable to check branch name of HEAD in git repo {url}', file=sys.stderr)
                raise RuntimeError  # abort

            if active_branch != branch:
                try:
                    git_object_type = subprocess.check_output(
                        ['git', 'cat-file', '-t', branch],
                        cwd=repo_destination,
                        shell=False,
                        universal_newlines=True,
                        stderr=subprocess.STDOUT,
                    ).strip()
                except subprocess.CalledProcessError as cpe:  # pragma: no cover
                    if 'Not a valid object name' in cpe.output:
                        git_object_type = 'branch'  # assume remote branch
                    else:
                        print(f'!! unable to determine git object type for {branch}')
                        raise RuntimeError from cpe

                if git_object_type == 'tag':  # pragma: no cover
                    rc += subprocess.check_call(
                        [
                            'git',
                            'checkout',
                            f'tags/{branch}',
                            '-b',
                            branch,
                        ],
                        cwd=repo_destination,
                        shell=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                    if rc != 0:  # pragma: no cover
                        print(f'!! unable to checkout tag {branch} from git repo {url}', file=sys.stderr)
                        raise RuntimeError  # abort
                elif git_object_type == 'commit':
                    rc += subprocess.check_call(
                        [
                            'git',
                            'checkout',
                            branch,
                        ],
                        cwd=repo_destination,
                        shell=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                    if rc != 0:  # pragma: no cover
                        print(f'!! unable to checkout commit {branch} from git repo {url}', file=sys.stderr)
                        raise RuntimeError  # abort
                else:
                    rc += subprocess.check_call(
                        [
                            'git',
                            'checkout',
                            '-b',
                            branch,
                            '--track',
                            f'origin/{branch}',
                        ],
                        cwd=repo_destination,
                        shell=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )

                    if rc != 0:
                        print(f'!! unable to checkout branch {branch} from git repo {url}', file=sys.stderr)
                        raise RuntimeError  # abort

            if not Path.joinpath(repo_destination, 'pyproject.toml').exists():
                with Path.joinpath(repo_destination, 'grizzly', '__init__.py').open(encoding='utf-8') as fd:
                    version_raw = [line.strip() for line in fd.readlines() if line.strip().startswith('__version__ =')]

                if len(version_raw) != 1:
                    print(f'!! unable to find "__version__" declaration in grizzly/__init__.py from {url}', file=sys.stderr)
                    raise RuntimeError  # abort

                _, grizzly_version, _ = version_raw[-1].split("'")
            else:
                try:
                    with Path.joinpath(repo_destination, 'setup.cfg').open(encoding='utf-8') as fd:
                        version_raw = [line.strip() for line in fd.readlines() if line.strip().startswith('version = ')]

                    if len(version_raw) != 1:
                        print(f'!! unable to find "version" declaration in setup.cfg from {url}', file=sys.stderr)
                        raise RuntimeError  # abort

                    _, grizzly_version = version_raw[-1].split(' = ')
                except FileNotFoundError:
                    if find_spec('setuptools_scm') is None:
                        rc = subprocess.check_call(
                            [
                                sys.executable,
                                '-m',
                                'pip',
                                'install',
                                'setuptools_scm',
                            ]
                        )

                    try:
                        grizzly_version = subprocess.check_output(
                            [
                                sys.executable,
                                '-m',
                                'setuptools_scm',
                            ],
                            shell=False,
                            universal_newlines=True,
                            cwd=repo_destination,
                        ).strip()
                    except subprocess.CalledProcessError as e:  # pragma: no cover
                        print(f'!! unable to get setuptools_scm version from {url}', file=sys.stderr)
                        raise RuntimeError from e  # abort

            try:
                with Path.joinpath(repo_destination, 'requirements.txt').open(encoding='utf-8') as fd:
                    version_raw = [line.strip() for line in fd.readlines() if line.strip().startswith('locust')]

                if len(version_raw) != 1:
                    print(f'!! unable to find "locust" dependency in requirements.txt from {url}', file=sys.stderr)
                    raise RuntimeError  # abort

                match = re.match(r'^locust.{2}(.*?)$', version_raw[-1].strip().split(' ')[0])

                if not match:
                    print(f'!! unable to find locust version in "{version_raw[-1].strip()}" specified in requirements.txt from {url}', file=sys.stderr)
                else:
                    locust_version = match.group(1).strip()
            except FileNotFoundError:  # pragma: no cover
                with Path.joinpath(repo_destination, 'pyproject.toml').open('rb') as fdt:
                    toml_dict = tomli.load(fdt)
                    dependencies = toml_dict.get('project', {}).get('dependencies', [])
                    for dependency in dependencies:
                        if not dependency.startswith('locust'):
                            continue

                        _, locust_version = dependency.strip().split(' ', 1)

                        break
        except RuntimeError:
            pass
        finally:
            rm_rf(tmp_workspace)
    else:
        response = requests.get(
            'https://pypi.org/pypi/grizzly-loadtester/json',
            timeout=10,
        )

        if response.status_code != 200:
            print(f'!! unable to get grizzly package information from {response.url} ({response.status_code})', file=sys.stderr)
        else:
            pypi = jsonloads(response.text)

            grizzly_requirement_egg = grizzly_requirement

            # get grizzly version used in requirements.txt
            if re.match(r'^grizzly-loadtester(\[[^\]]*\])?$', grizzly_requirement):  # latest
                grizzly_version = pypi.get('info', {}).get('version', None)
            else:
                conditions: list[Callable[[versioning.Version], bool]] = []

                match = re.match(r'^(grizzly-loadtester(\[[^\]]*\])?)(.*?)$', grizzly_requirement)

                if match:
                    grizzly_requirement_egg = match.group(1)
                    condition_expression = match.group(3)

                    for condition in condition_expression.split(',', 1):
                        version_string = re.sub(r'^[^0-9]{1,2}', '', condition)
                        condition_version = versioning.parse(version_string)

                        if '>' in condition:
                            compare = condition_version.__le__ if '=' in condition else condition_version.__lt__
                        elif '<' in condition:
                            compare = condition_version.__ge__ if '=' in condition else condition_version.__gt__
                        else:
                            compare = condition_version.__eq__

                        conditions.append(compare)

                matched_version = None

                for available_version in pypi.get('releases', {}):
                    with suppress(versioning.InvalidVersion):
                        version = versioning.parse(available_version)
                        if len(conditions) > 0 and all(compare(version) for compare in conditions):
                            matched_version = version

                if matched_version is None:
                    print(f'!! could not resolve {grizzly_requirement} to one specific version available at pypi', file=sys.stderr)
                else:
                    grizzly_version = str(matched_version)

            if grizzly_version is not None:
                # get version from pypi, to be able to get locust version
                response = requests.get(
                    f'https://pypi.org/pypi/grizzly-loadtester/{grizzly_version}/json',
                    timeout=10,
                )

                if response.status_code != 200:
                    print(f'!! unable to get grizzly {grizzly_version} package information from {response.url} ({response.status_code})', file=sys.stderr)
                else:
                    release_info = jsonloads(response.text)

                    for requires_dist in release_info.get('info', {}).get('requires_dist', []):
                        if not requires_dist.startswith('locust'):
                            continue

                        match = re.match(r'^locust \((.*?)\)$', requires_dist.strip())

                        locust_version = cast('str', match.group(1)) if match else requires_dist.replace('locust', '').strip()

                        if locust_version is not None and locust_version.startswith('=='):
                            locust_version = locust_version[2:]

                        if len(locust_version or '') < 1:
                            print(f'!! unable to find locust version in "{requires_dist.strip()}" specified in pypi for grizzly-loadtester {grizzly_version}', file=sys.stderr)
                            locust_version = '(unknown)'
                        break

                    if locust_version is None:
                        print(f'!! could not find "locust" in requires_dist information for grizzly-loadtester {grizzly_version}', file=sys.stderr)

    if grizzly_version is None:
        grizzly_version = '(unknown)'
    else:
        match = re.match(r'^grizzly-loadtester\[([^\]]*)\]$', grizzly_requirement_egg)

        grizzly_extras = [extra.strip() for extra in match.group(1).split(',')] if match else []

    if locust_version is None:
        locust_version = '(unknown)'

    return (grizzly_version, grizzly_extras), locust_version


def list_images(args: Arguments) -> dict[str, dict[str, str]]:
    images: dict[str, dict[str, str]] = {}
    output = subprocess.check_output(
        [
            f'{args.container_system}',
            'image',
            'ls',
            '--format',
            '{"name": "{{.Repository}}", "tag": "{{.Tag}}", "size": "{{.Size}}", "created": "{{.CreatedAt}}", "id": "{{.ID}}"}',
        ]
    ).decode('utf-8')

    for line in output.split('\n'):
        if len(line) < 1:
            continue
        image = jsonloads(line)
        name = image['name']
        tag = image['tag']
        del image['name']
        del image['tag']

        version = {tag: image}

        if name not in images:
            images[name] = {}

        images[name].update(version)

    return images


def get_default_mtu(args: Arguments) -> str | None:
    try:
        output = subprocess.check_output(
            [
                f'{args.container_system}',
                'network',
                'inspect',
                'bridge',
                '--format',
                '{{ json .Options }}',
            ]
        ).decode('utf-8')

        line, _ = output.split('\n', 1)
        network_options: dict[str, str] = jsonloads(line)

        return network_options.get('com.docker.network.driver.mtu', '1500')
    except:
        return None


def requirements(execution_context: str) -> Callable[[Callable[..., int]], Callable[..., int]]:
    def wrapper(func: Callable[..., int]) -> Callable[..., int]:
        @wraps(func)
        def _wrapper(*args: Any, **kwargs: Any) -> int:
            return func(*args, **kwargs)

        # a bit ugly, but needed for testability
        setattr(func, '__value__', execution_context)  # noqa: B010
        setattr(_wrapper, '__wrapped__', func)  # noqa: B010

        return _wrapper

    return wrapper


def get_distributed_system() -> str | None:
    if which('docker') is not None:
        container_system = 'docker'
    elif which('podman') is not None:
        container_system = 'podman'
        print('!! podman might not work due to buildah missing support for `RUN --mount=type=ssh`: https://github.com/containers/buildah/issues/2835')
    else:
        print('neither "podman" nor "docker" found in PATH')
        return None

    rc, _ = subprocess.getstatusoutput(f'{container_system} compose version')  # noqa: S605

    if rc != 0:
        print(f'"{container_system} compose" not found in PATH')
        return None

    return container_system


def get_input(text: str) -> str:  # pragma: no cover
    return input(text).strip()


def ask_yes_no(question: str) -> None:
    answer = 'undefined'
    while answer.lower() not in ['y', 'n']:
        if answer != 'undefined':
            print('you must answer y (yes) or n (no)')
        answer = get_input(f'{question} [y/n]: ')

        if answer == 'n':
            raise KeyboardInterrupt


def parse_feature_file(file: str) -> None:
    if len(grizzly_cli.SCENARIOS) > 0:
        return

    feature = feature_file_parser(file)

    grizzly_cli.FEATURE_DESCRIPTION = feature.name

    for scenario in feature.scenarios:
        grizzly_cli.SCENARIOS.append(scenario)


def find_metadata_notices(file: str) -> list[str]:
    with Path(file).open('r') as fd:
        return [line.strip().replace('# grizzly-cli:notice ', '') for line in fd if line.strip().startswith('# grizzly-cli:notice ')]


def find_variable_names_in_questions(file: str) -> list[str]:
    unique_variables: set[str] = set()

    parse_feature_file(file)

    for scenario in grizzly_cli.SCENARIOS:
        for step in scenario.steps + scenario.background_steps or []:
            if not step.name.startswith('ask for value of variable'):
                continue

            match = re.match(r'ask for value of variable "([^"]*)"', step.name)

            if not match:
                message = f'could not find variable name in "{step.name}"'
                raise ValueError(message)

            unique_variables.add(match.group(1))

    return sorted(unique_variables)


def _guess_datatype(value: str) -> Union[str, int, float, bool]:
    check_value = value.replace('.', '', 1)

    if check_value[0] == '-':
        check_value = check_value[1:]

    if check_value.isdecimal():
        if float(value) % 1 == 0:
            if value.startswith('0'):
                return str(value)

            return int(float(value))

        return float(value)

    if value.lower() in ['true', 'false']:
        return value.lower() == 'true'

    return value


class ScenarioProperties:
    name: str
    index: int
    identifier: str
    user: str | None
    weight: float
    _iterations: int | None
    _user_count: int | None

    def __init__(
        self,
        name: str,
        index: int,
        weight: float | None = None,
        user: str | None = None,
        iterations: int | None = None,
        user_count: int | None = None,
    ) -> None:
        self.name = name
        self.index = index
        self.user = user
        self._iterations = iterations
        self.weight = weight or 1.0
        self.identifier = f'{index:03}'
        self._user_count = user_count

    @property
    def iterations(self) -> int:
        if self._iterations is None:  # pragma: no cover
            message = 'iterations has not been set'
            raise ValueError(message)

        return self._iterations

    @iterations.setter
    def iterations(self, value: int) -> None:
        self._iterations = value

    @property
    def user_count(self) -> int:
        if self._user_count is None:  # pragma: no cover
            message = 'user count has not been set'
            raise ValueError(message)
        return self._user_count

    @user_count.setter
    def user_count(self, value: int) -> None:
        self._user_count = value

    def is_fulfilled(self) -> bool:
        return self.user is not None and self._iterations is not None and self._user_count is not None


def distribution_of_users_per_scenario(args: Arguments, environ: dict) -> None:  # noqa: C901, PLR0912, PLR0915
    distribution: dict[str, ScenarioProperties] = {}
    variables = {key.replace('TESTDATA_VARIABLE_', ''): _guess_datatype(value) for key, value in environ.items() if key.startswith('TESTDATA_VARIABLE_')}

    def _pre_populate_scenario(scenario: Scenario, index: int) -> None:
        if scenario.name not in distribution:
            distribution[scenario.name] = ScenarioProperties(
                name=scenario.name,
                index=index,
                user=None,
                weight=None,
                iterations=None,
            )

    scenario_user_count_total: int | None = None
    use_weights = True

    for index, scenario in enumerate(grizzly_cli.SCENARIOS):
        scenario_variables: dict = {}
        if len(scenario.steps) < 1:
            message = f'scenario "{scenario.name}" does not have any steps'
            raise ValueError(message)

        _pre_populate_scenario(scenario, index=index + 1)

        if index == 0:  # background_steps is only processed for first scenario in grizzly
            for step in scenario.background_steps or []:
                if (step.name.endswith(' users') or step.name.endswith(' user')) and step.keyword == 'Given':
                    match = re.match(r'"([^"]*)" user(s)?', step.name)
                    if match:
                        scenario_user_count_total = int(round(float(Template(match.group(1)).render(**variables)), 0))

        if scenario_user_count_total is None:
            use_weights = False
            scenario_user_count_total = 0

        for step in (scenario.background_steps or []) + scenario.steps:
            if step.name.startswith('value for variable'):  # pragma: no cover
                match = re.match(r'value for variable "([^"]*)" is "([^"]*)"', step.name)
                if match:
                    try:
                        variable_name = match.group(1)
                        variable_value = Template(match.group(2)).render(**variables, **scenario_variables)
                        scenario_variables.update({variable_name: variable_value})
                    except:  # noqa: S112
                        continue
            elif step.name.startswith('a user of type'):
                match = re.match(r'a user of type "([^"]*)" (with weight "([^"]*)")?.*', step.name)
                if match:
                    distribution[scenario.name].user = match.group(1)
                    distribution[scenario.name].weight = int(float(Template(match.group(3) or '1.0').render(**variables, **scenario_variables)))
            elif step.name.startswith('repeat for'):
                match = re.match(r'repeat for "([^"]*)" iteration[s]?', step.name)
                if match:
                    distribution[scenario.name].iterations = int(round(float(Template(match.group(1)).render(**variables, **scenario_variables)), 0))
            elif any(pattern in step.name for pattern in ['users of type', 'user of type']):
                match = re.match(r'"([^"]*)" user[s]? of type "([^"]*)".*', step.name)
                if match:
                    scenario_user_count = int(round(float(Template(match.group(1)).render(**variables, **scenario_variables)), 0))
                    scenario_user_count_total += scenario_user_count

                    distribution[scenario.name].user_count = scenario_user_count
                    distribution[scenario.name].user = match.group(2)

            if distribution[scenario.name].is_fulfilled():
                break

    scenario_count = len(distribution.keys())
    assert scenario_user_count_total is not None
    if scenario_count > scenario_user_count_total:
        message = f'grizzly needs at least {scenario_count} users to run this feature'
        raise ValueError(message)

    total_weight = 0.0
    total_iterations = 0
    for scenario in distribution.values():
        if scenario.user is None:
            message = f'{scenario.name} does not have a user type'
            raise ValueError(message)

        total_weight += scenario.weight
        total_iterations += scenario.iterations

    if use_weights:
        for scenario in distribution.values():
            scenario.user_count = ceil(scenario_user_count_total * (scenario.weight / total_weight))

        # smooth assigned user count based on weight, so that the sum of scenario.user_count == total_user_count
        total_user_count = sum([scenario.user_count for scenario in distribution.values()])
        user_overflow = total_user_count - scenario_user_count_total

        while user_overflow > 0:
            for scenario in dict(sorted(distribution.items(), key=lambda d: d[1].user_count, reverse=True)).values():
                if scenario.user_count <= 1:
                    continue

                scenario.user_count -= 1
                user_overflow -= 1

                if user_overflow < 1:
                    break

    def print_table_lines(max_length_iterations: int, max_length_users: int, max_length_description: int, max_length_errors: int) -> None:
        line = ['-' * 5, '-|-', '-' * 6, '|-', '-' * max_length_iterations, '|-', '-' * max_length_users, '|-', '-' * max_length_description, '-|']
        if not use_weights:
            line = line[:1] + line[3:]
            line[0] = f'{line[0]}-'

        if max_length_errors > 0:
            line += ['-' * (max_length_errors + 1), '-|']
        logger.info(''.join(line))

    rows: list[str] = []
    max_length_description = len('description')
    max_length_iterations = len('#iter')
    max_length_users = len('#user')
    max_length_errors = len('errors')

    message = f'\nfeature file {args.file} will execute in total {total_iterations} iterations divided on {len(grizzly_cli.SCENARIOS)} scenarios'
    if hasattr(args, 'environment_file') and args.environment_file is not None:
        message = f'{message} with environment file {environ["GRIZZLY_CONFIGURATION_FILE"]}'

    logger.info('%s\n', message)

    errors: dict[str, list[str]] = {}

    for scenario in distribution.values():
        # check for errors
        if scenario.user_count < 1:
            if scenario.name not in errors:
                errors.update({scenario.name: []})

            errors[scenario.name].append('no users assigned')

        if scenario.iterations < 1:
            if scenario.name not in errors:
                errors.update({scenario.name: []})

            errors[scenario.name].append('no iterations')

        # calculate max length out of all rows
        max_length_description = max(len(scenario.name), max_length_description)
        max_length_iterations = max(len(str(scenario.iterations)), max_length_iterations)
        max_length_users = max(len(str(scenario.user_count)), max_length_users)
        max_length_errors = max(len(', '.join(errors.get(scenario.name, []))), max_length_errors)

    # there was no errors, so reset max length for that column, so it won't be included
    if len(errors) < 1:
        max_length_errors = 0

    row_format = ['{:5} ', '  {:>6d}', '  {:>{}}', '  {:>{}}', '  {:<{}}']
    if not use_weights:
        row_format.pop(1)

    if len(errors) > 0:
        row_format.append('   {}')

    for scenario in distribution.values():
        row_format_args: list = [
            scenario.identifier,
            scenario.weight,
            scenario.iterations,
            max_length_iterations,
            scenario.user_count,
            max_length_users,
            scenario.name,
            max_length_description,
        ]

        # remove weights column
        if not use_weights:
            row_format_args.pop(1)

        if len(errors) > 0:
            row_format_args.append(
                ', '.join(errors.get(scenario.name, [])),
            )

        rows.append(''.join(row_format).format(*row_format_args))

    logger.info('each scenario will execute accordingly:\n')
    header_row_args: list = [
        'ident',
        'weight',
        '#iter',
        max_length_iterations,
        '#user',
        max_length_users,
        'description',
        max_length_description,
    ]

    header_row_format = ['{:5} ', '  {:>6}', '  {:>{}}', '  {:>{}}', '  {:<{}}']
    if not use_weights:
        header_row_format.pop(1)
        header_row_args.pop(1)

    if len(errors) > 0:
        header_row_format.append('   {}')
        header_row_args.append('errors')

    logger.info(''.join(header_row_format).format(*header_row_args))
    print_table_lines(max_length_iterations, max_length_users, max_length_description, max_length_errors)
    for row in rows:
        logger.info(row)
    print_table_lines(max_length_iterations, max_length_users, max_length_description, max_length_errors)

    if len(errors) > 0:
        arrow_width = len('ident') + 2 + max_length_iterations + 2 + max_length_users + 2 + max_length_description + 2
        if use_weights:
            arrow_width += len('weight') + 2
        message = f"""{' ' * (1 + arrow_width)}^
+{'-' * arrow_width}+
|
+- there were errors when calculating user distribution and iterations per scenario, adjust user "weight", number of users or iterations per scenario
"""

        raise ValueError(message)

    logger.info('')

    for scenario in distribution.values():
        if scenario.iterations < scenario.user_count:
            message = f'{scenario.name} will have {scenario.user_count} users to run {scenario.iterations} iterations, increase iterations or lower user count'
            raise ValueError(message)

    if not args.yes:
        ask_yes_no('continue?')


def setup_logging(logfile: str | None = None) -> None:
    logging_config: dict = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'plain': {
                'format': '%(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'plain',
            },
        },
        'loggers': {
            'grizzly-cli': {
                'handlers': ['console'],
                'level': 'INFO',
                'propagate': False,
            },
        },
        'root': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    }

    if logfile is not None:  # pragma: no cover
        logging_config['handlers']['file'] = {
            'class': 'logging.FileHandler',
            'filename': logfile,
            'formatter': 'plain',
        }

        logging_config['loggers']['grizzly-cli']['handlers'].append('file')
        logging_config['root']['handlers'].append('file')

    logging.config.dictConfig(logging_config)


def unflatten(key: str, value: Any) -> dict:
    paths: list[str] = key.split('.')

    # last node should have the value
    path = paths.pop()
    struct = {path: value}

    # build the struct from the inside out
    paths.reverse()

    for path in paths:
        struct = {path: {**struct}}

    return struct


def flatten(node: dict, parents: list[str] | None = None) -> dict:
    """Flatten a dictionary so each value key is the path down the nested dictionary structure."""
    flat: dict = {}
    if parents is None:
        parents = []

    for key, value in node.items():
        parents.append(key)
        if isinstance(value, dict):
            flat = {**flat, **flatten(value, parents)}
        else:
            flat['.'.join(parents)] = value

        parents.pop()

    return flat


def merge_dicts(merged: dict, source: dict) -> dict:
    """Merge two dicts recursively, where `source` values takes precedance over `merged` values."""
    merged = deepcopy(merged)
    source = deepcopy(source)

    for key in source:
        if key in merged and isinstance(merged[key], dict) and (isinstance(source[key], Mapping) or source[key] is None):
            merged[key] = merge_dicts(merged[key], source[key] or {})
        else:
            value = source[key]
            if isinstance(value, str) and value.lower() == 'none':  # pragma: no cover
                value = None
            merged[key] = value

    return merged


def chunker(value: str, size: int) -> list[str]:
    return [value[x * size : x * size + size] for x in list(range(ceil(len(value) / size)))]


def get_indentation(file: Path) -> int:
    try:
        first_indent_line = file.read_text().splitlines()[1]
        return len(first_indent_line.rstrip()) - len(first_indent_line.strip())
    except IndexError:
        # use 2 as default indentation when it is not possible to detect from file
        return 2


class IndentDumper(Dumper):
    use_indent: ClassVar[int]

    @classmethod
    def use_indentation(cls, target: Path | int) -> type[IndentDumper]:
        cls.use_indent = get_indentation(target) if isinstance(target, Path) else target

        return cls

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.best_indent = self.use_indent

    def increase_indent(self, flow: bool = False, indentless: bool = False) -> None:  # noqa: ARG002, FBT001, FBT002
        return super().increase_indent(flow, False)  # noqa: FBT003

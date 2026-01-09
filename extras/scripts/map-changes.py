#!/usr/bin/env python
#
# /// script
# requires-python = ">=3.13"
# dependencies = [
#    "pyyaml>=6.0.2",
# ]
# ///
"""Map changed directories to package changes and their test configurations.

This script analyzes the repoistory structure to determine which packages have changed
and identifies their corresponding test suites. It supports both Python (uv-managed)
and Node.js (npm-managed) packages.

The script outputs a JSON mapping of changes that can be consumed by CI/CD workflows
to run appropriate tests for affected packages, including reverse dependencies.

Usage:
    python map-changes.py --changes '["framework", "docs"]' --release --force false

Environment Variables:
    GITHUB_OUTPUT: Path to GitHub Actions output file (optional, for CI/CD integration)
"""

import argparse
import json
import sys
from contextlib import suppress
from dataclasses import asdict, dataclass
from operator import itemgetter
from os import environ
from pathlib import Path
from typing import Any, TypedDict

import tomllib
import yaml


@dataclass(eq=True, frozen=True)
class ChangeE2eTests:
    """Configuration for end-to-end tests.

    Attributes:
        local: Path to local e2e tests (run mode: local)
        dist: Path to distributed e2e tests (run mode: distributed)

    """

    local: str
    dist: str


@dataclass(eq=True, frozen=True)
class ChangeTests:
    """Configuration for all test types in a package.

    Attributes:
        unit: Path to unit tests directory
        e2e: Configuration for end-to-end tests

    """

    unit: str
    e2e: ChangeE2eTests


@dataclass(eq=True, frozen=True)
class Change:
    """Represents a detected package change.

    Attributes:
        directory: Relative path to package directory
        package: Package name (e.g., 'grizzly-loadtester')
        tests: Test configuration for this package

    """

    directory: str
    package: str
    tests: ChangeTests


class Changes(TypedDict):
    """Map of package manager types to their respective changes.

    Attributes:
        npm: Set of changes for npm-managed packages
        uv: Set of changes for uv-managed Python packages
        actions: Set of changes for GitHub Actions packages

    """

    npm: set[Change]
    uv: set[Change]
    actions: set[Change]


def _create_python_change(directory: str, package: str) -> Change:
    """Create a Change object for a Python package by detecting its test structure.

    Analyzes the package directory to locate test directories and determines
    whether they contain unit tests, e2e tests, or both.

    Args:
        directory: Path to the package directory
        package: Name of the Python package

    Returns:
        Change object with populated test configuration

    Note:
        - Searches for tests in 'tests/test_*/' directories
        - Unit tests: 'tests/test_*/unit/'
        - E2E tests: 'tests/test_*/e2e/'
        - For 'grizzly-loadtester', both local and dist e2e tests are configured

    """
    directory_path = Path(directory)

    try:
        test_directory = next(iter([test_base for test_base in Path.joinpath(directory_path, 'tests').glob('test_*') if test_base.is_dir()]))
    except StopIteration:  # no tests and/or tests/test_ directories, ergo: no tests
        return Change(directory=directory, package=package, tests=ChangeTests(unit='', e2e=ChangeE2eTests(local='', dist='')))

    test_unit_directory = Path.joinpath(test_directory, 'unit')
    test_e2e_directory = Path.joinpath(test_directory, 'e2e')

    args_unit: str = ''
    args_e2e: str = ''
    args_e2e_dist: str = ''

    if test_unit_directory.exists() and test_e2e_directory.exists():
        args_unit = test_unit_directory.relative_to(directory_path).as_posix()
        args_e2e = test_e2e_directory.relative_to(directory_path).as_posix()

        if package == 'grizzly-loadtester':
            args_e2e_dist = args_e2e
    else:
        args_unit = f'{test_directory.relative_to(directory_path).as_posix()}'

    tests = ChangeTests(unit=args_unit, e2e=ChangeE2eTests(local=args_e2e, dist=args_e2e_dist))

    return Change(directory=directory, package=package, tests=tests)


def python_package(directory: str, uv_lock_package: list[dict[str, Any]], *, release: bool) -> set[Change]:
    """Detect changes in a Python package and its reverse dependencies.

    Analyzes a directory for Python package configuration and identifies
    all packages that depend on it (reverse dependencies) to ensure
    comprehensive test coverage.

    Args:
        directory: Path to the directory to analyze
        uv_lock_package: List of packages from uv.lock file
        release: If True, only include packages with release configuration

    Returns:
        Set of Change objects for the package and its reverse dependencies

    Note:
        - Reads pyproject.toml for package metadata
        - For release mode, requires hatch version configuration with git describe_command
        - Automatically includes workspace packages that depend on this package

    """
    changes: set[Change] = set()

    directory_path = Path(directory)
    pyproject_file = directory_path / 'pyproject.toml'

    if not pyproject_file.exists():
        return changes

    with pyproject_file.open('rb') as pyproject_fd:
        pyproject = tomllib.load(pyproject_fd)
        project = pyproject.get('project', {})

        if release and pyproject.get('tool', {}).get('hatch', {}).get('version', {}).get('raw-options', {}).get('scm', {}).get('git', {}).get('describe_command', None) is None:
            return changes

        package = project.get('name', None)

        changes.add(_create_python_change(directory, package))

        # workspace packages that has dependencies on this package
        for value in uv_lock_package:
            if not (value.get('name', '').startswith('grizzly-') and any(dependency['name'] == package for dependency in value.get('dependencies', []))):
                continue

            reverse_package: str = value['name']
            reverse_directory: str = value['source']['editable']

            changes.add(_create_python_change(reverse_directory, reverse_package))

    return changes


def node_package(directory: str, *, release: bool) -> set[Change]:
    """Detect changes in a Node.js/npm package.

    Analyzes a directory for Node.js package configuration and detects
    available test scripts.

    Args:
        directory: Path to the directory to analyze
        release: If True, only include packages with release configuration

    Returns:
        Set containing a Change object if the package exists and meets criteria

    Note:
        - Reads package.json for package metadata and test scripts
        - For release mode, requires package.local.json with tag.pattern configuration
        - Detects 'test' and 'e2e-test' npm scripts

    """
    changes: set[Change] = set()

    package_json_file = Path(directory) / 'package.json'
    if not package_json_file.exists():
        return changes

    if release:
        package_local_json_file = Path(directory) / 'package.local.json'
        if not package_local_json_file.exists():
            return changes

        with package_local_json_file.open('r') as fd:
            package_local_json = json.loads(fd.read())

            if package_local_json.get('tag', {}).get('pattern', None) is None and release:
                return changes

    with package_json_file.open('r') as fd:
        package_json = json.loads(fd.read())
        package_scripts = package_json.get('scripts', {})

        args_unit: str = 'test' if 'test' in package_scripts else ''
        args_e2e: str = 'test:e2e' if 'test:e2e' in package_scripts else ''

        changes.add(Change(directory=directory, package=package_json['name'], tests=ChangeTests(args_unit, e2e=ChangeE2eTests(local=args_e2e, dist=''))))

    return changes


def main() -> int:
    """Process command-line arguments and map package changes to test configurations.

    Determines which packages have changed and outputs their test configurations
    in JSON format for consumption by CI/CD workflows.

    Returns:
        Exit code: 0 on success, 1 on error

    Outputs:
        - Prints detected changes to stdout in JSON format
        - Writes changes to GITHUB_OUTPUT file if running in GitHub Actions

    Examples:
        Normal mode:
            python map-changes.py --changes '["framework"]' --force false

        Release mode (only packages with release config):
            python map-changes.py --changes '["framework"]' --release --force false

        Force mode (all packages from change-filters.yaml):
            python map-changes.py --changes '[]' --force true

    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--changes', required=True, type=str, help='JSON string of list of directories that had changes')
    parser.add_argument('--release', action='store_true', help='Indicates if this is a release run')
    parser.add_argument('--force', required=True, type=str, help='Force run on all packages')

    args = parser.parse_args()

    if args.force == 'true':
        change_filters_file = Path.joinpath(Path(__file__).parent.parent.parent, '.github', 'changes-filter.yaml')
        with change_filters_file.open('r') as fd:
            change_filters = yaml.safe_load(fd)
            workflow_input = list(change_filters.keys())
    else:
        try:
            workflow_input = json.loads(args.changes)
        except json.JSONDecodeError:
            print(f'invalid json in --changes: "{args.changes}"', file=sys.stderr)
            return 1

    # Fail if workflow files were modified - cannot release when .github/workflows/ changes
    if args.release and any('workflows' in directory for directory in workflow_input):
        print('error: workflow files cannot be part of a release', file=sys.stderr)
        return 1

    changes: Changes = {'uv': set(), 'npm': set(), 'actions': set()}
    uv_lock_file = (Path(__file__).parent / '..' / '..' / 'uv.lock').resolve()

    with uv_lock_file.open('rb') as fd:
        uv_lock = tomllib.load(fd)
        uv_lock_package: list[dict[str, Any]] = uv_lock.get('package', {})

        for directory in workflow_input:
            changes['uv'].update(python_package(directory, uv_lock_package, release=args.release))
            changes['npm'].update(node_package(directory, release=args.release))

        if len(changes) < 1:
            print('no changes detected in known locations', file=sys.stderr)
            return 1

    changes_npm = json.dumps(sorted([asdict(change) for change in changes['npm']], key=itemgetter('package')))
    changes_uv = json.dumps(sorted([asdict(change) for change in changes['uv']], key=itemgetter('package')))
    changes_actions = json.dumps(sorted([asdict(change) for change in changes['actions']], key=itemgetter('package')))

    print(f'detected changes:\nuv={changes_uv}\nnpm={changes_npm}\nactions={changes_actions}')

    with suppress(KeyError), Path(environ['GITHUB_OUTPUT']).open('a') as fd:
        fd.write(f'changes_uv={changes_uv}\n')
        fd.write(f'changes_npm={changes_npm}\n')
        fd.write(f'changes_actions={changes_actions}\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())

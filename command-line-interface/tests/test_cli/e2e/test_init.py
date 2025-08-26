"""Tests for grizzly-cli init."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from packaging.version import Version

from test_cli.helpers import rm_rf, run_command

if TYPE_CHECKING:
    from _pytest.tmpdir import TempPathFactory


@pytest.mark.parametrize(
    ('arguments', 'expected'),
    [
        (
            [],
            {'mq_output': 'without IBM MQ support', 'grizzly_output': 'latest grizzly version', 'grizzly_requirements': 'grizzly-loadtester'},
        ),
        (
            ['--grizzly-version', '2.2.2'],
            {'mq_output': 'without IBM MQ support', 'grizzly_output': 'pinned to grizzly version 2.2.2', 'grizzly_requirements': 'grizzly-loadtester==2.2.2'},
        ),
        (
            ['--with-mq'],
            {'mq_output': 'with IBM MQ support', 'grizzly_output': 'latest grizzly version', 'grizzly_requirements': 'grizzly-loadtester[mq]'},
        ),
        (
            ['--with-mq', '--grizzly-version', '1.1.1'],
            {'mq_output': 'with IBM MQ support', 'grizzly_output': 'pinned to grizzly version 1.1.1', 'grizzly_requirements': 'grizzly-loadtester[mq]==1.1.1'},
        ),
    ],
)
def test_e2e_init(arguments: list[str], expected: dict[str, str], tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    grizzly_version: Version | None = None
    try:
        grizzly_version_index = arguments.index('--grizzly-version') + 1
        grizzly_version = Version(arguments[grizzly_version_index])
    except ValueError:
        grizzly_version = None

    grizzly_behave_module = 'behave' if grizzly_version is None or grizzly_version >= Version('2.6.0') else 'environment'

    try:
        rc, output = run_command(
            ['grizzly-cli', 'init', 'foobar', '--yes', *arguments],
            cwd=test_context,
        )
        try:
            assert rc == 0
        except AssertionError:
            print(''.join(output))
            raise
        assert (
            ''.join(output)
            == f"""the following structure will be created:

    foobar
    ├── environments
    │   └── foobar.yaml
    ├── features
    │   ├── environment.py
    │   ├── steps
    │   │   └── steps.py
    │   ├── foobar.feature
    │   └── requests
    └── requirements.txt

successfully created project "foobar", with the following options:
  • {expected['mq_output']}
  • {expected['grizzly_output']}
"""
        )

        assert (test_context / 'foobar').is_dir()
        assert (test_context / 'foobar' / 'environments').is_dir()
        requirements_file = test_context / 'foobar' / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == f'{expected["grizzly_requirements"]}\n'

        environments_file = test_context / 'foobar' / 'environments' / 'foobar.yaml'
        assert environments_file.is_file()
        assert (
            environments_file.read_text()
            == """configuration:
  template:
    host: https://localhost
"""
        )

        features_dir = test_context / 'foobar' / 'features'
        assert features_dir.is_dir()

        assert (features_dir / 'requests').is_dir()
        assert list((features_dir / 'requests').rglob('**/*')) == []

        assert (features_dir / 'steps').is_dir()
        steps_file = features_dir / 'steps' / 'steps.py'
        assert steps_file.is_file()
        assert steps_file.read_text() == 'from grizzly.steps import *\n\n'

        environment_file = features_dir / 'environment.py'
        assert environment_file.is_file()
        assert environment_file.read_text() == f'from grizzly.{grizzly_behave_module} import *\n\n'

        feature_file = features_dir / 'foobar.feature'
        assert feature_file.is_file()
        assert (
            feature_file.read_text()
            == """Feature: Template feature file
  Scenario: Template scenario
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
"""
        )
    finally:
        rm_rf(test_context)

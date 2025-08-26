"""Tests for grizzly_cli.init."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from grizzly_cli.__main__ import _parse_arguments
from grizzly_cli.init import init, tree

from test_cli.helpers import rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.capture import CaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test_tree(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context')

    (test_context / 'a' / 'b' / 'c').mkdir(parents=True)

    (test_context / 'a' / 'file-a1.txt').touch()
    (test_context / 'a' / 'file-a2.txt').touch()

    (test_context / 'a' / 'b' / 'file-b1.txt').touch()

    (test_context / 'a' / 'b' / 'c' / 'file-c1.txt').touch()
    (test_context / 'a' / 'b' / 'c' / 'file-c2.txt').touch()

    (test_context / 'root.yaml').touch()

    try:
        assert (
            '\n'.join(list(tree(test_context)))
            == """├── a
│   ├── b
│   │   ├── c
│   │   │   ├── file-c1.txt
│   │   │   └── file-c2.txt
│   │   └── file-b1.txt
│   ├── file-a1.txt
│   └── file-a2.txt
└── root.yaml"""
        )
    finally:
        rm_rf(test_context)


def test_init(tmp_path_factory: TempPathFactory, capsys: CaptureFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
    test_context = tmp_path_factory.mktemp('test_context')

    test_existing = test_context / 'foobar'
    test_existing.mkdir()

    mocker.patch('grizzly_cli.init.EXECUTION_CONTEXT', str(test_context))

    try:
        sys.argv = ['grizzly-cli', 'init', 'foobar']
        arguments = _parse_arguments()

        assert init(arguments) == 1

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == f'"foobar" already exists in {test_context}\n'

        (test_existing / 'environments').mkdir()
        (test_existing / 'features').mkdir()
        (test_existing / 'requirements.txt').touch()

        mocker.patch('grizzly_cli.init.EXECUTION_CONTEXT', str(test_existing))

        assert init(arguments) == 1

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == f"""oops, looks like you are already in a grizzly project directory

{test_existing}
├── environments
├── features
└── requirements.txt
"""
        )

        rm_rf(test_existing)

        question_mock = mocker.patch('grizzly_cli.init.ask_yes_no', side_effect=[None] * 4)
        mocker.patch('grizzly_cli.init.EXECUTION_CONTEXT', str(test_context))

        assert init(arguments) == 0

        assert question_mock.call_count == 1
        args, _ = question_mock.call_args_list[-1]
        assert (
            args[0]
            == """the following structure will be created:

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

do you want to create grizzly project "foobar"?"""
        )

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == """successfully created project "foobar", with the following options:
  • without IBM MQ support
  • latest grizzly version
"""
        )

        template_root = test_context / 'foobar'
        assert template_root.is_dir()
        assert (template_root / 'environments').is_dir()
        environments_file = template_root / 'environments' / 'foobar.yaml'
        assert environments_file.is_file()
        assert (
            environments_file.read_text()
            == """configuration:
  template:
    host: https://localhost
"""
        )

        assert (template_root / 'features').is_dir()
        feature_file = template_root / 'features' / 'foobar.feature'
        assert feature_file.is_file()
        assert (
            feature_file.read_text()
            == """Feature: Template feature file
  Scenario: Template scenario
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
"""
        )

        environment_file = template_root / 'features' / 'environment.py'
        assert environment_file.is_file()
        assert environment_file.read_text() == 'from grizzly.behave import *\n\n'

        assert (template_root / 'features' / 'requests').is_dir()

        assert (template_root / 'features' / 'steps').is_dir()

        steps_file = template_root / 'features' / 'steps' / 'steps.py'
        assert steps_file.is_file()
        assert steps_file.read_text() == 'from grizzly.steps import *\n\n'

        requirements_file = template_root / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == 'grizzly-loadtester\n'

        created_structure = '\n'.join(list(tree(template_root)))
        assert (
            created_structure
            == """├── environments
│   └── foobar.yaml
├── features
│   ├── environment.py
│   ├── foobar.feature
│   ├── requests
│   └── steps
│       └── steps.py
└── requirements.txt"""
        )

        rm_rf(template_root)

        capsys.readouterr()

        sys.argv = ['grizzly-cli', 'init', 'foobar', '--with-mq']
        arguments = _parse_arguments()

        assert init(arguments) == 0

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == """successfully created project "foobar", with the following options:
  • with IBM MQ support
  • latest grizzly version
"""
        )
        requirements_file = template_root / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == 'grizzly-loadtester[mq]\n'

        rm_rf(template_root)

        sys.argv = ['grizzly-cli', 'init', 'foobar', '--grizzly-version', '1.2.4']
        arguments = _parse_arguments()

        assert init(arguments) == 0

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == """successfully created project "foobar", with the following options:
  • without IBM MQ support
  • pinned to grizzly version 1.2.4
"""
        )
        requirements_file = template_root / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == 'grizzly-loadtester==1.2.4\n'

        assert (template_root / 'features').is_dir()
        feature_file = template_root / 'features' / 'foobar.feature'
        assert feature_file.is_file()
        assert (
            feature_file.read_text()
            == """Feature: Template feature file
  Scenario: Template scenario
    Given a user of type "RestApi" with weight "1" load testing "$conf::template.host"
"""
        )

        environment_file = template_root / 'features' / 'environment.py'
        assert environment_file.is_file()
        assert environment_file.read_text() == 'from grizzly.environment import *\n\n'

        rm_rf(template_root)

        sys.argv = ['grizzly-cli', 'init', 'foobar', '--with-mq', '--grizzly-version', '1.5.0']
        arguments = _parse_arguments()

        assert init(arguments) == 0

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == """successfully created project "foobar", with the following options:
  • with IBM MQ support
  • pinned to grizzly version 1.5.0
"""
        )
        requirements_file = template_root / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == 'grizzly-loadtester[mq]==1.5.0\n'

        rm_rf(template_root)

        sys.argv = ['grizzly-cli', 'init', 'foobar', '--yes']
        arguments = _parse_arguments()

        assert init(arguments) == 0

        capture = capsys.readouterr()
        assert capture.err == ''
        assert (
            capture.out
            == """the following structure will be created:

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
  • without IBM MQ support
  • latest grizzly version
"""
        )
        requirements_file = template_root / 'requirements.txt'
        assert requirements_file.is_file()
        assert requirements_file.read_text() == 'grizzly-loadtester\n'
    finally:
        rm_rf(test_context)

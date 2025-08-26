"""Tests for grizzly_cli.run."""

from __future__ import annotations

import logging
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from grizzly_cli.run import create_parser, run
from grizzly_cli.utils import setup_logging
from grizzly_cli.utils.configuration import ScenarioTag
from jinja2 import Environment

from test_cli.helpers import CaseInsensitive, rm_rf

if TYPE_CHECKING:
    from _pytest.capture import CaptureFixture
    from _pytest.logging import LogCaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from pytest_mock import MockerFixture


def test_run(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    setup_logging()

    test_context = tmp_path_factory.mktemp('test_context')
    execution_context = test_context / 'execution-context'
    execution_context.mkdir()
    mount_context = test_context / 'mount-context'
    mount_context.mkdir()
    feature_file = execution_context / 'features' / 'test.feature'
    feature_file.parent.mkdir(parents=True, exist_ok=True)
    feature_file.write_text('Feature: this feature is testing something')
    (execution_context / 'configuration.yaml').write_text('configuration:')

    parser = ArgumentParser()

    sub_parsers = parser.add_subparsers(dest='test')

    create_parser(sub_parsers, parent='local')

    try:
        mocker.patch('grizzly_cli.run.grizzly_cli.EXECUTION_CONTEXT', execution_context.as_posix())
        mocker.patch('grizzly_cli.run.grizzly_cli.MOUNT_CONTEXT', mount_context.as_posix())
        mocker.patch('grizzly_cli.run.get_hostname', return_value='localhost')
        mocker.patch('grizzly_cli.run.find_variable_names_in_questions', side_effect=[['foo', 'bar'], [], [], [], [], [], [], []])
        mocker.patch('grizzly_cli.run.find_metadata_notices', side_effect=[[], ['is the event log cleared?'], ['hello world', 'foo bar'], [], [], [], [], []])
        mocker.patch('grizzly_cli.run.distribution_of_users_per_scenario', autospec=True)
        ask_yes_no_mock = mocker.patch('grizzly_cli.run.ask_yes_no', autospec=True)
        distributed_mock = mocker.MagicMock(return_value=0)
        local_mock = mocker.MagicMock(return_value=0)
        get_input_mock = mocker.patch('grizzly_cli.run.get_input', side_effect=['bar', 'foo'])

        setattr(getattr(run, '__wrapped__'), '__value__', execution_context.as_posix())  # noqa: B009, B010

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                f'{execution_context.as_posix()}/features/test.feature',
                '--verbose',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, distributed_mock) == 0

        capture = capsys.readouterr()
        assert capture.out == ''
        assert (
            capture.err
            == """feature file requires values for 2 variables
the following values was provided:
foo = bar
bar = foo
"""
        )

        local_mock.assert_not_called()
        distributed_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
                'TESTDATA_VARIABLE_foo': 'bar',
                'TESTDATA_VARIABLE_bar': 'foo',
            },
            {
                'master': [],
                'worker': [],
                'common': ['--verbose', '--no-logcapture', '--no-capture', '--no-capture-stderr'],
            },
        )
        distributed_mock.reset_mock()

        ask_yes_no_mock.assert_called_once_with('continue?')
        ask_yes_no_mock.reset_mock()
        assert get_input_mock.call_count == 2
        args, kwargs = get_input_mock.call_args_list[0]
        assert kwargs == {}
        assert args[0] == 'initial value for "foo": '
        args, kwargs = get_input_mock.call_args_list[1]
        assert kwargs == {}
        assert args[0] == 'initial value for "bar": '
        get_input_mock.reset_mock()

        assert capture.out == ''
        assert capture.err == ('feature file requires values for 2 variables\nthe following values was provided:\nfoo = bar\nbar = foo\n')

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                f'{execution_context.as_posix()}/features/test.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        capture = capsys.readouterr()

        distributed_mock.assert_not_called()
        local_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
            },
            {
                'master': [],
                'worker': [],
                'common': [],
            },
        )
        local_mock.reset_mock()

        ask_yes_no_mock.assert_called_once_with('is the event log cleared?')
        ask_yes_no_mock.reset_mock()
        get_input_mock.assert_not_called()

        assert capture.err == ''
        assert capture.out == ''

        # with --yes, notices should only be printed, and not needed to be confirmed via ask_yes_no
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()

        local_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
            },
            {
                'master': [],
                'worker': [],
                'common': [],
            },
        )
        local_mock.reset_mock()

        ask_yes_no_mock.assert_not_called()
        get_input_mock.assert_not_called()

        assert capture.err == ''
        assert capture.out == ''

        # no `csv_prefix` nothing should be added
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--csv-interval',
                '20',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        local_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
            },
            {
                'master': [],
                'worker': [],
                'common': [],
            },
        )
        local_mock.reset_mock()
        distributed_mock.assert_not_called()

        # static csv-prefix
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--csv-interval',
                '20',
                '--csv-prefix',
                'test test',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
            },
            {
                'master': [],
                'worker': [],
                'common': ['-Dcsv-prefix="test test"', '-Dcsv-interval=20'],
            },
        )
        local_mock.reset_mock()

        # dynamic csv-prefix
        datetime_mock = mocker.patch(
            'grizzly_cli.run.datetime',
            side_effect=lambda *args, **kwargs: datetime(*args, **kwargs),  # noqa: DTZ001
        )
        datetime_mock.now.return_value = datetime(2022, 12, 6, 13, 1, 13)  # noqa: DTZ001
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--csv-prefix',
                '--csv-interval',
                '20',
                '--csv-flush-interval',
                '60',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, distributed_mock) == 0

        local_mock.assert_not_called()
        distributed_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
            },
            {
                'master': [],
                'worker': [],
                'common': ['-Dcsv-prefix="this_feature_is_testing_something_20221206T130113"', '-Dcsv-interval=20', '-Dcsv-flush-interval=60'],
            },
        )
        distributed_mock.reset_mock()

        arguments.csv_prefix = None
        arguments.csv_flush_interval = None

        # --log-dir
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                '--log-dir',
                'foobar',
                f'{execution_context.as_posix()}/features/test.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, distributed_mock) == 0

        local_mock.assert_not_called()
        distributed_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
                'GRIZZLY_LOG_DIR': 'foobar',
            },
            {
                'master': [],
                'worker': [],
                'common': [],
            },
        )
        distributed_mock.reset_mock()

        capsys.readouterr()

        # --dry-run
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                '--log-dir',
                'foobar',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dry-run',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, distributed_mock) == 0

        local_mock.assert_not_called()
        distributed_mock.assert_called_once_with(
            arguments,
            {
                'GRIZZLY_CLI_HOST': 'localhost',
                'GRIZZLY_EXECUTION_CONTEXT': execution_context.as_posix(),
                'GRIZZLY_MOUNT_CONTEXT': mount_context.as_posix(),
                'GRIZZLY_CONFIGURATION_FILE': CaseInsensitive(Path.joinpath(execution_context, 'configuration.lock.yaml').as_posix()),
                'GRIZZLY_LOG_DIR': 'foobar',
                'GRIZZLY_DRY_RUN': 'true',
            },
            {
                'master': [],
                'worker': [],
                'common': [],
            },
        )
        distributed_mock.reset_mock()

        capsys.readouterr()

        # --dump
        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dump',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, distributed_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()

        assert capture.out == 'Feature: this feature is testing something\n'
        assert capture.err == ''
    finally:
        rm_rf(test_context)


def test_run_dump(capsys: CaptureFixture, mocker: MockerFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
    setup_logging()

    test_context = tmp_path_factory.mktemp('test_context')
    execution_context = test_context / 'execution-context'
    execution_context.mkdir()
    mount_context = test_context / 'mount-context'
    mount_context.mkdir()
    feature_file = execution_context / 'features' / 'test.feature'
    feature_file.parent.mkdir(parents=True, exist_ok=True)
    feature_file.write_text('Feature: this feature is testing something')
    (execution_context / 'configuration.yaml').write_text('configuration:')

    parser = ArgumentParser()

    sub_parsers = parser.add_subparsers(dest='test')

    create_parser(sub_parsers, parent='local')

    try:
        mocker.patch('grizzly_cli.run.grizzly_cli.EXECUTION_CONTEXT', execution_context.as_posix())
        mocker.patch('grizzly_cli.run.grizzly_cli.MOUNT_CONTEXT', mount_context.as_posix())
        mocker.patch('grizzly_cli.run.get_hostname', return_value='localhost')
        mocker.patch('grizzly_cli.run.find_variable_names_in_questions', side_effect=[['foo', 'bar'], [], [], [], [], [], [], []])
        mocker.patch('grizzly_cli.run.find_metadata_notices', side_effect=[[], ['is the event log cleared?'], ['hello world', 'foo bar'], [], [], [], [], []])
        mocker.patch('grizzly_cli.run.distribution_of_users_per_scenario', autospec=True)
        distributed_mock = mocker.MagicMock(return_value=0)
        local_mock = mocker.MagicMock(return_value=0)

        setattr(getattr(run, '__wrapped__'), '__value__', execution_context.as_posix())  # noqa: B009, B010

        # --dump output.feature
        feature_file.write_text("""Feature: a feature
    Background: common
        Given a common step

    Scenario: first
        Given a variable with value "{{foo * 0.25 | int }}" and another value " {{ bar |int + 12}}"
        And a variable with value "{{ hello }}"
        And a variable with value "{{ thisshouldwork | upper }}"
        And a variable with value "{{thisshouldalsowork |bigtime}}"
        And a variable with value "{{andthis|too}}"

    Scenario: second
        {% scenario "second", feature="../second.feature" %}

    Scenario: third
        Given a variable with value "{{ some*0.25 | more}}" and another value "{{yes|box }}"

    Scenario: fourth
        {% scenario "fourth", feature="./fourth.feature", foo="bar" %}
""")
        feature_file_2 = execution_context / 'second.feature'
        feature_file_2.write_text("""Feature: a second feature
    Background: common
        Given a common step

    Scenario: second
        Given a variable with value "{{ foobar }}"
        Then run a bloody test
""")
        feature_file_3 = execution_context / 'features' / 'fourth.feature'
        feature_file_3.write_text("""Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Given a variable with value "{{ {$ foo $}_barfoo }}"
        Then get "{{ bar{$ foo $}foo }}" from "{{ {$ foo $}_barfoo }}"
        Then run a bloody test
""")

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context}/configuration.yaml',
                '--yes',
                f'{execution_context}/features/test.feature',
                '--dump',
                f'{execution_context}/output.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        output_file = execution_context / 'output.feature'
        assert (
            output_file.read_text()
            == """Feature: a feature
    Background: common
        Given a common step

    Scenario: first
        Given a variable with value "{{foo * 0.25 | int }}" and another value " {{ bar |int + 12}}"
        And a variable with value "{{ hello }}"
        And a variable with value "{{ thisshouldwork | upper }}"
        And a variable with value "{{thisshouldalsowork |bigtime}}"
        And a variable with value "{{andthis|too}}"

    Scenario: second
        Given a variable with value "{{ foobar }}"
        Then run a bloody test

    Scenario: third
        Given a variable with value "{{ some*0.25 | more}}" and another value "{{yes|box }}"

    Scenario: fourth
        Given a variable with value "{{ bar_barfoo }}"
        Then get "{{ barbarfoo }}" from "{{ bar_barfoo }}"
        Then run a bloody test
"""
        )

        feature_file.write_text("""Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        {% scenario "second", feature="../second.feature", prefix="s1" %}

    # Scenario: third
    #     {% scenario "inactive-second", feature="./second.feature", prefix="s1" %}

    Scenario: third
        {% scenario "third", feature="../second.feature", prefix="s1" %}
""")
        feature_file_2 = execution_context / 'second.feature'
        feature_file_2.write_text("""Feature: a second feature
    Background: common
        Given a common step

    Scenario: second
        Given a variable with value "{{ {$ prefix $}foobar }}"
        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"

    Scenario: third
        Given a variable with value "{{ {$ prefix $}value }}"
        Then run a bloody test, with table
          | hello | world |
          | foo   | bar   |
          | bar   |       |
          |       | foo   |
""")

        output_file.unlink(missing_ok=True)

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dump',
                f'{execution_context.as_posix()}/output.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        output_file = execution_context / 'output.feature'
        assert (
            output_file.read_text()
            == """Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        Given a variable with value "{{ s1foobar }}"
        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"

    # Scenario: third
    #     {% scenario "inactive-second", feature="./second.feature", prefix="s1" %}

    Scenario: third
        Given a variable with value "{{ s1value }}"
        Then run a bloody test, with table
          | hello | world |
          | foo   | bar   |
          | bar   |       |
          |       | foo   |
"""
        )
        assert (
            feature_file.read_text()
            == """Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        {% scenario "second", feature="../second.feature", prefix="s1" %}

    # Scenario: third
    #     {% scenario "inactive-second", feature="./second.feature", prefix="s1" %}

    Scenario: third
        {% scenario "third", feature="../second.feature", prefix="s1" %}
"""
        )

        feature_file.write_text("""Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        {% scenario "second", feature="../second.feature", foo="bar" %}
""")
        feature_file_2 = execution_context / 'second.feature'
        feature_file_2.write_text("""Feature: a second feature
    Background: common
        Given a common step

    Scenario: second
        Given a variable with value "{{ {$ bar $}foobar }}"
        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
""")

        output_file.unlink(missing_ok=True)

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dump',
                f'{execution_context.as_posix()}/output.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        with pytest.raises(ValueError) as ve:  # noqa: PT011
            run(arguments, local_mock)

        assert (
            str(ve.value)
            == """the following variables has been declared in scenario tag but not used in ../second.feature#second:
  foo

the following variables was used in ../second.feature#second but was not declared in scenario tag:
  bar
"""
        )

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''

        feature_file.write_text("""Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        {% scenario "second", feature="../second.feature", foo="bar" %}
""")
        feature_file_2 = execution_context / 'second.feature'
        feature_file_2.write_text("""Feature: a second feature
    Background: common
        Given a common step

    Scenario: second
        Given a variable with value "{{ {$ foo $}foobar }}"
        {% scenario "fourth", feature="./features/fourth.feature", foo="{$ foo $}", bar="foo", condition=True %}

        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
""")

        feature_file_3.write_text("""Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"

        # <!-- this step is conditional -->
        {%- if {$ condition $} %}
        Then alert me!
        {%- endif %}
""")

        output_file.unlink(missing_ok=True)

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dump',
                f'{execution_context.as_posix()}/output.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''
        assert (
            output_file.read_text()
            == """Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        Given a variable with value "{{ barfoobar }}"
        Then could it be "bar" and "foo"

        # <!-- this step is conditional -->
        Then alert me!

        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
"""
        )

        feature_file_2.write_text("""Feature: a second feature
    Background: common
        Given a common step

    Scenario: second
        Given a variable with value "{{ {$ foo $}foobar }}"
        {% scenario "fourth", feature="./features/fourth.feature", foo="{$ foo $}", bar="foo", condition=False %}

        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
""")

        output_file.unlink(missing_ok=True)

        arguments = parser.parse_args(
            [
                'run',
                '-e',
                f'{execution_context.as_posix()}/configuration.yaml',
                '--yes',
                f'{execution_context.as_posix()}/features/test.feature',
                '--dump',
                f'{execution_context.as_posix()}/output.feature',
            ]
        )
        arguments.file = ' '.join(arguments.file)

        assert run(arguments, local_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''
        assert (
            output_file.read_text()
            == """Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        Given a variable with value "{{ barfoobar }}"
        Then could it be "bar" and "foo"

        # <!-- this step is conditional -->

        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
"""
        )

        feature_file_3.write_text("""Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"
        \"\"\"
        hello world
        \"\"\"

        # <!-- this step is conditional -->
        {%- if {$ condition $} %}
        Then alert me!
        {%- endif %}

    Scenario: fifth
        Given a scenario after the included scenario
""")

        output_file.unlink(missing_ok=True)

        assert run(arguments, distributed_mock) == 0

        distributed_mock.assert_not_called()
        local_mock.assert_not_called()

        capture = capsys.readouterr()
        assert capture.err == ''
        assert capture.out == ''
        assert (
            output_file.read_text()
            == """Feature: a feature
    Scenario: first
        Given a variable with value "{{ hello }}"

    Scenario: second
        Given a variable with value "{{ barfoobar }}"
        Then could it be "bar" and "foo"
        \"\"\"
        hello world
        \"\"\"

        # <!-- this step is conditional -->

        Then run a bloody test
            \"\"\"
            with step text
            that spans
            more than
            one line
            \"\"\"
"""
        )
    finally:
        rm_rf(test_context)


def test_if_condition_with_scenario_tag_ext(caplog: LogCaptureFixture) -> None:
    environment = Environment(autoescape=False, extensions=[ScenarioTag])

    template = environment.from_string('{% if False %}hello {{ name }}!{% endif %}')
    with caplog.at_level(logging.DEBUG):
        assert template.render() == ''

    template = environment.from_string('{% if True %}hello {{ name }}!{% endif %}')
    assert template.render() == 'hello {{ name }}!'

    template = environment.from_string("""foobar

{%- if True %}
hello {{ name }}!
{%- endif %}
world""")
    assert template.render() == 'foobar\nhello {{ name }}!\nworld'

    template = environment.from_string("""Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"

        # <!-- this step is conditional -->
        {%- if True %}
        Then show me the "{{ money }}"
        {%- endif %}""")

    assert (
        template.render()
        == """Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"

        # <!-- this step is conditional -->
        Then show me the "{{ money }}"
        """.rstrip()
    )

    template = environment.from_string("""Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"

        # <!-- this step is conditional -->
        {%- if False %}
        Then show me the "{{ money }}"
        {%- endif %}""")

    assert (
        template.render()
        == """Feature: a fourth feature
    Background: common
        Given a common step

    Scenario: fourth
        Then could it be "{$ foo $}" and "{$ bar $}"

        # <!-- this step is conditional -->"""
    )

"""End-to-end tests of grizzly.steps.setup."""

from __future__ import annotations

from os.path import sep as os_path_sep
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.context import GrizzlyContext
    from grizzly.types.behave import Context, Feature

    from test_framework.fixtures import End2EndFixture


def test_e2e_step_setup_variable_value_ask(e2e_fixture: End2EndFixture) -> None:
    def validate_variables(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)

        assert grizzly.scenario.variables['background_variable'] == 'foo-background-value'
        assert grizzly.scenario.variables['scenario_variable'] == 'bar-scenario-value'

    e2e_fixture.add_validator(validate_variables)

    feature_file = e2e_fixture.test_steps(
        background=[
            'And ask for value of variable "background_variable"',
        ],
        scenario=[
            'Then ask for value of variable "scenario_variable"',
            'Then log message "{{ background_variable }}={{ scenario_variable }}"',
        ],
    )

    assert feature_file == f'features{os_path_sep}test_e2e_step_setup_variable_value_ask.feature'

    rc, output = e2e_fixture.execute(
        feature_file,
        testdata={
            'background_variable': 'foo-background-value',
            'scenario_variable': 'bar-scenario-value',
        },
    )

    try:
        assert rc == 0
    except AssertionError:
        print(''.join(output))

        raise


def test_e2e_step_setup_variable_value(e2e_fixture: End2EndFixture) -> None:
    def validate_variables(context: Context) -> None:
        grizzly = cast('GrizzlyContext', context.grizzly)

        assert grizzly.scenario.variables['AtomicCsvWriter.output'] == "output.csv | headers='foo, bar'"
        assert grizzly.scenario.variables['leveranser'] == 10
        assert len(grizzly.scenario.tasks()) == 3 + 1

    e2e_fixture.add_validator(validate_variables)

    feature_file = e2e_fixture.test_steps(
        background=[
            'Given value for variable "leveranser" is "10"',
        ],
        scenario=[
            'And value for variable "AtomicCsvWriter.output" is "output.csv | headers=\'foo, bar\'"',
            'Then log message "dummy={{ leveranser }}"',
            'Given value for variable "AtomicCsvWriter.output" is "bar, foo"',
            'And value for variable "foobar" is "foobar"',
            'And value for variable "foobar" is "foobaz"',
        ],
    )

    assert e2e_fixture._root is not None

    output_file = e2e_fixture._root / 'features' / 'requests' / 'output.csv'

    assert not output_file.exists()

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0

    assert 'dummy=10' in result
    assert 'registered callback for message type "atomiccsvwriter"' in result

    assert output_file.exists()
    assert output_file.read_text() == 'foo,bar\nbar,foo\n'


def test_e2e_step_scenario_variable_value(e2e_fixture: End2EndFixture) -> None:
    def validate_variable_value(context: Context) -> None:
        from os import environ
        from pathlib import Path

        grizzly = cast('GrizzlyContext', context.grizzly)

        assert grizzly.scenario.variables.get('testdata_variable', None) == 'hello world!'
        assert grizzly.scenario.variables.get('int_value', None) == 10
        assert grizzly.scenario.variables.get('float_value', None) == 1.0
        assert grizzly.scenario.variables.get('bool_value', False)
        assert grizzly.scenario.variables.get('wildcard', None) == 'foobar'
        assert grizzly.scenario.variables.get('nested_value', None) == 'hello world!'
        assert grizzly.scenario.variables.get('AtomicIntegerIncrementer.persistent', None) == '10 | step=13, persist=True'

        feature_file = environ.get('GRIZZLY_FEATURE_FILE', None)
        assert feature_file is not None, 'environment variable GRIZZLY_FEATURE_FILE was not set'
        persist_file = Path(context.config.base_dir) / 'persistent' / f'{Path(feature_file).stem}.json'

        assert persist_file.exists(), f'{persist_file} does not exist'

        persist_file.unlink()

    e2e_fixture.add_validator(validate_variable_value)

    def before_feature(context: Context, feature: Feature) -> None:
        from json import dumps as jsondumps
        from pathlib import Path

        context_root = Path(context.config.base_dir)
        persist_root = context_root / 'persistent'
        persist_root.mkdir(exist_ok=True)
        persist_file = persist_root / f'{Path(feature.filename).stem}.json'
        persist_file.write_text(
            jsondumps(
                {
                    'IteratorScenario_001': {
                        'AtomicIntegerIncrementer.persistent': '10 | step=13, persist=True',
                    },
                },
            ),
        )

    e2e_fixture.add_before_feature(before_feature)

    def after_feature(context: Context, feature: Feature) -> None:
        from json import loads as jsonloads
        from pathlib import Path

        context_root = Path(context.config.base_dir)

        persist_file = context_root / 'persistent' / f'{Path(feature.filename).stem}.json'

        assert persist_file.exists(), f'{persist_file} does not exist'
        contents = persist_file.read_text()
        assert jsonloads(contents) == {
            'IteratorScenario_001': {
                'AtomicIntegerIncrementer.persistent': '23 | step=13, persist=True',
            },
        }, f'"{contents}" is not expected value'

    e2e_fixture.add_after_feature(after_feature)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "testdata_variable" is "hello world!"',
            'And value for variable "int_value" is "10"',
            'And value for variable "float_value" is "1.0"',
            'And value for variable "bool_value" is "True"',
            'And value for variable "wildcard" is "foobar"',
            'And value for variable "nested_value" is "{{ testdata_variable }}"',
            'And value for variable "AtomicIntegerIncrementer.persistent" is "1 | step=1, persist=True"',
            (
                'Then log message "testdata_variable={{ testdata_variable }}, int_value={{ int_value }}, '
                'float_value={{ float_value }}, bool_value={{ bool_value }}, wildcard={{ wildcard }}, '
                'nested_value={{ nested_value }}"'
            ),
            'Then log message "persistent={{ AtomicIntegerIncrementer.persistent }}"',
        ],
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 0
    assert 'persistent=10' in result

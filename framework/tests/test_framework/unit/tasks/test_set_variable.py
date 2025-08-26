"""Unit tests of grizzly.tasks.set_variable."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from grizzly.tasks import SetVariableTask
from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.variables import AtomicCsvWriter
from grizzly.types import VariableType

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture, MockerFixture


class TestSetVariableTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            # non-Atomic variable
            task_factory = SetVariableTask('foobar', '{{ hello }}', VariableType.VARIABLES)

            assert task_factory.variable == 'foobar'
            assert task_factory.variable_template == '{{ foobar }}'
            assert task_factory.value == '{{ hello }}'
            assert task_factory._variable_instance is None
            assert task_factory._variable_key == task_factory.variable
            assert task_factory.__template_attributes__ == {'variable_template', 'value'}
            assert sorted(task_factory.get_templates()) == sorted(['{{ foobar }}', '{{ hello }}'])

            # Atomic variable, not settable
            with pytest.raises(AttributeError, match=r'grizzly\.testdata\.variables\.AtomicIntegerIncrementer is not settable'):
                SetVariableTask('AtomicIntegerIncrementer.id', '{{ value }}', VariableType.VARIABLES)

            grizzly_fixture.grizzly.scenario.variables.update({'AtomicIntegerIncrementer.id': 1})
            GrizzlyVariables.initialize_variable(grizzly_fixture.grizzly.scenario, 'AtomicIntegerIncrementer.id')

            with pytest.raises(AttributeError, match=r'grizzly\.testdata\.variables\.AtomicIntegerIncrementer is not settable'):
                SetVariableTask('AtomicIntegerIncrementer.id', '{{ value }}', VariableType.VARIABLES)

            # Atomic variable, settable
            grizzly_fixture.grizzly.scenario.variables.update({'AtomicCsvWriter.output': 'output.csv | headers="foo,bar"'})
            GrizzlyVariables.initialize_variable(grizzly_fixture.grizzly.scenario, 'AtomicCsvWriter.output')

            task_factory = SetVariableTask('AtomicCsvWriter.output.foo', '{{ value }}', VariableType.VARIABLES)

            assert task_factory.variable == 'AtomicCsvWriter.output.foo'
            assert task_factory.variable_template == '{{ AtomicCsvWriter.output.foo }}'
            assert task_factory.value == '{{ value }}'
            assert task_factory._variable_key == 'output.foo'
            assert task_factory._variable_instance is None
            assert task_factory._variable_instance_type is AtomicCsvWriter
        finally:
            cleanup()

    def test_variable_template(self) -> None:
        task_factory = SetVariableTask('foobar', '{{ world }}', VariableType.VARIABLES)
        assert task_factory.variable_template == '{{ foobar }}'

        task_factory = SetVariableTask('{{ foobar }}', '{{ world }}', VariableType.VARIABLES)
        assert task_factory.variable_template == '{{ foobar }}'

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        parent = grizzly_fixture()

        try:
            # non-Atomic variable
            task_factory = SetVariableTask('foobar', '{{ value }}', VariableType.VARIABLES)

            task = task_factory()

            assert 'foobar' not in parent.user.variables

            parent.user._scenario.variables.update({'value': 'none', 'AtomicCsvWriter.output': 'output.csv | headers="foo,bar"', 'bar': 'none'})
            parent.user.set_variable('value', 'hello world!')

            task(parent)

            assert parent.user.variables.get('foobar', None) == 'hello world!'

            parent.user.variables = GrizzlyVariables()

            # settable Atomic variable
            set_value_mock = mocker.patch('grizzly.testdata.variables.csv_writer.AtomicCsvWriter.__setitem__', return_value=None)

            parent.user.set_variable('AtomicCsvWriter.output', 'output.csv | headers="foo,bar"')
            GrizzlyVariables.initialize_variable(parent.user._scenario, 'AtomicCsvWriter.output')
            task_factory_foo = SetVariableTask('AtomicCsvWriter.output', '{{ value }} | headers="foo,bar"', VariableType.VARIABLES)
            parent.user.set_variable('value', 'file.csv')

            task = task_factory_foo()
            task(parent)

            set_value_mock.assert_called_once_with('output', 'file.csv | headers="foo,bar"')
            assert 'AtomicCsvWriter.output.foo' not in parent.user.variables

            # set value from file runtime, and render file contents
            test_file = grizzly_fixture.test_context / 'requests' / 'test' / 'hello.foo.txt'
            test_file.parent.mkdir(exist_ok=True, parents=True)
            test_file.write_text('{{ value }}')

            parent.user.set_variable('bar', 'foo')
            task_factory = SetVariableTask('foobar', 'test/hello.{{ bar }}.txt', VariableType.VARIABLES)
            task = task_factory()

            task(parent)

            assert parent.user.variables['foobar'] == 'file.csv'
        finally:
            cleanup()

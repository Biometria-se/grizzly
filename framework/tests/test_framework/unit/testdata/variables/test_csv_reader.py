"""Unit tests of grizzly.testdata.variables.csv_reader."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata.variables import AtomicCsvReader
from grizzly.testdata.variables.csv_reader import _atomiccsvreader

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


def test__atomiccsvreader(grizzly_fixture: GrizzlyFixture) -> None:
    test_context = grizzly_fixture.test_context / 'requests'
    test_context.mkdir(exist_ok=True)

    with pytest.raises(ValueError, match=r'must be a CSV file with file extension \.csv'):
        _atomiccsvreader('file1.txt')

    (test_context / 'a-directory.csv').mkdir()

    with pytest.raises(ValueError, match='is not a file in'):
        _atomiccsvreader('a-directory.csv')

    with pytest.raises(ValueError, match='is not a file in'):
        _atomiccsvreader('file1.csv')

    test_file = test_context / 'file1.csv'
    test_file.write_text('\n')

    assert _atomiccsvreader('file1.csv') == 'file1.csv'

    with pytest.raises(ValueError, match='is not allowed'):
        _atomiccsvreader('file1.csv | arg1=test')

    assert _atomiccsvreader('file1.csv|random=True') == 'file1.csv | random=True'


class TestAtomicCsvReader:
    def test_variable(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:  # noqa: PLR0915
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)

        for count in range(1, 4):
            file = f'{count}.csv'
            with (test_context / file).open('w') as fd:
                for column in range(1, count + 1):
                    fd.write(f'header{column}{count}')
                    if column < count:
                        fd.write(',')
                fd.write('\n')

                for row in range(1, count + 1):
                    for column in range(1, count + 1):
                        fd.write(f'value{column}{row}{count}')
                        if column < count:
                            fd.write(',')
                    fd.write('\n')

                fd.flush()

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            instance1 = AtomicCsvReader(scenario=scenario1, variable='test1', value='1.csv')
            instance2 = AtomicCsvReader(scenario=scenario2, variable='test1', value='1.csv')

            for instance in [instance1, instance2]:
                assert len(instance._rows['test1']) == 1

                csvrow = instance['test1']
                assert csvrow is not None
                assert isinstance(csvrow, dict)
                assert 'header11' in csvrow
                assert csvrow['header11'] == 'value111'
                assert len(instance._rows['test1']) == 0

                csvrow = instance['test1']
                assert csvrow is None

            instance = AtomicCsvReader(scenario=scenario1, variable='test2', value='2.csv')
            assert len(instance._rows['test2']) == 2

            csvrow = instance['test2.header22']
            assert csvrow is not None
            assert len(csvrow.keys()) == 1
            assert len(instance._rows['test2']) == 1
            assert 'header22' in csvrow
            assert csvrow['header22'] == 'value212'

            csvrow = instance['test2']
            assert csvrow is not None
            assert len(csvrow.keys()) == 2
            assert len(instance._rows['test2']) == 0
            assert csvrow == {'header12': 'value122', 'header22': 'value222'}

            csvrow = instance['test2']
            assert csvrow is None

            instance = AtomicCsvReader(scenario=scenario2, variable='test3', value='3.csv')
            assert len(instance._rows['test3']) == 3

            with pytest.raises(ValueError, match='AtomicCsvReader.test3: headerXX does not exists'):
                instance['test3.headerXX']

            assert instance['test3'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['test3'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['test3'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance.__getitem__('test3') is None

            with pytest.raises(NotImplementedError, match='AtomicCsvReader has not implemented "__setitem__"'):
                instance['test4'] = {'test': 'value'}
            assert 'test4' not in instance._rows

            instance = AtomicCsvReader(scenario=scenario1, variable='test5', value='3.csv')
            assert 'test5' in instance._rows

            del instance['test5']
            assert 'test5' not in instance._rows

            del instance['test5']
            del instance['test5.header13']

            instance = AtomicCsvReader(scenario=scenario2, variable='infinite', value='3.csv | repeat=True')
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}

            instance = AtomicCsvReader(scenario=scenario1, variable='random', value='3.csv | random=True')
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['random'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance.__getitem__('random') is None

            instance = AtomicCsvReader(scenario=scenario2, variable='randomrepeat', value='3.csv | random=True, repeat=True')
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
            assert instance['randomrepeat'] in [
                {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'},
                {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'},
                {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'},
            ]
        finally:
            cleanup()

    def test_clear_and_destroy(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        test_context = grizzly_fixture.test_context / 'requests'
        test_context.mkdir(exist_ok=True)

        with (test_context / 'test.csv').open('w') as fd:
            fd.write('header1\n')
            fd.write('value1\n')
            fd.flush()

        grizzly = grizzly_fixture.grizzly
        scenario1 = grizzly.scenario
        scenario2 = grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('second'))

        try:
            with suppress(Exception):
                AtomicCsvReader.destroy()

            with pytest.raises(ValueError, match='AtomicCsvReader is not instantiated'):
                AtomicCsvReader.destroy()

            with pytest.raises(ValueError, match='AtomicCsvReader is not instantiated'):
                AtomicCsvReader.clear()

            instance1 = AtomicCsvReader(scenario=scenario1, variable='test', value='test.csv')
            instance2 = AtomicCsvReader(scenario=scenario2, variable='test', value='test.csv')

            for instance in [instance1, instance2]:
                assert instance['test'] == {'header1': 'value1'}

                assert len(instance._values.keys()) == 1
                assert len(instance._rows.keys()) == 1

            AtomicCsvReader.clear()

            for instance in [instance1, instance2]:
                assert len(instance._values.keys()) == 0
                assert len(instance._rows.keys()) == 0

            AtomicCsvReader.destroy()
        finally:
            cleanup()

    def test___init___error(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = grizzly_fixture.grizzly

            with pytest.raises(ValueError, match='is not a valid CSV source name, must be'):
                AtomicCsvReader(scenario=grizzly.scenario, variable='test.test', value='file1.csv')
        finally:
            cleanup()

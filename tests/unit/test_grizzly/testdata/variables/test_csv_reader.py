"""Unit tests of grizzly.testdata.variables.csv_reader."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from shutil import rmtree
from typing import TYPE_CHECKING

import pytest

from grizzly.testdata.variables import AtomicCsvReader
from grizzly.testdata.variables.csv_reader import atomiccsvreader__base_type__

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory

    from tests.fixtures import AtomicVariableCleanupFixture


def test_atomiccsvreader__base_type__(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = test_context.parent.as_posix()

    try:
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        with pytest.raises(ValueError, match=r'must be a CSV file with file extension \.csv'):
            atomiccsvreader__base_type__('file1.txt')

        (test_context / 'a-directory.csv').mkdir()

        with pytest.raises(ValueError, match='is not a file in'):
            atomiccsvreader__base_type__('a-directory.csv')

        with pytest.raises(ValueError, match='is not a file in'):
            atomiccsvreader__base_type__('file1.csv')

        test_file = test_context / 'file1.csv'
        test_file.write_text('\n')

        assert atomiccsvreader__base_type__('file1.csv') == 'file1.csv'

        with pytest.raises(ValueError, match='is not allowed'):
            atomiccsvreader__base_type__('file1.csv | arg1=test')

        assert atomiccsvreader__base_type__('file1.csv|random=True') == 'file1.csv | random=True'
    finally:
        rmtree(test_context_root)

        with suppress(KeyError):
            del environ['GRIZZLY_CONTEXT_ROOT']


class TestAtomicCsvReader:
    def test_variable(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:  # noqa: PLR0915
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = test_context.parent.as_posix()

        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

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

        try:
            instance = AtomicCsvReader('test1', '1.csv')
            assert len(instance._rows['test1']) == 1

            csvrow = instance['test1']
            assert csvrow is not None
            assert isinstance(csvrow, dict)
            assert 'header11' in csvrow
            assert csvrow['header11'] == 'value111'
            assert len(instance._rows['test1']) == 0

            csvrow = instance['test1']
            assert csvrow is None

            instance = AtomicCsvReader('test2', '2.csv')
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

            instance = AtomicCsvReader('test3', '3.csv')
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

            instance = AtomicCsvReader('test5', '3.csv')
            assert 'test5' in instance._rows

            del instance['test5']
            assert 'test5' not in instance._rows

            del instance['test5']
            del instance['test5.header13']

            instance = AtomicCsvReader('infinite', '3.csv | repeat=True')
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance['infinite'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['infinite'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['infinite'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}

            instance = AtomicCsvReader('random', '3.csv | random=True')
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

            instance = AtomicCsvReader('randomrepeat', '3.csv | random=True, repeat=True')
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
            rmtree(test_context_root)

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

            cleanup()

    def test_clear_and_destroy(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = test_context.parent.as_posix()

        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        with (test_context / 'test.csv').open('w') as fd:
            fd.write('header1\n')
            fd.write('value1\n')
            fd.flush()

        try:
            with suppress(Exception):
                AtomicCsvReader.destroy()

            with pytest.raises(ValueError, match='AtomicCsvReader is not instantiated'):
                AtomicCsvReader.destroy()

            with pytest.raises(ValueError, match='AtomicCsvReader is not instantiated'):
                AtomicCsvReader.clear()

            instance = AtomicCsvReader('test', 'test.csv')

            assert instance['test'] == {'header1': 'value1'}

            assert len(instance._values.keys()) == 1
            assert len(instance._rows.keys()) == 1

            AtomicCsvReader.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._rows.keys()) == 0

            AtomicCsvReader.destroy()
        finally:
            rmtree(test_context_root)

            with suppress(KeyError):
                del environ['GRIZZLY_CONTEXT_ROOT']

            cleanup()

    def test___init___error(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with pytest.raises(ValueError, match='is not a valid CSV source name, must be'):
                AtomicCsvReader('test.test', 'file1.csv')
        finally:
            cleanup()

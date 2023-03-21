import os
import shutil

import pytest

from _pytest.tmpdir import TempPathFactory

from grizzly.testdata.variables import AtomicCsvReader
from grizzly.testdata.variables.csv_reader import atomiccsvreader__base_type__

from tests.fixtures import AtomicVariableCleanupFixture


def test_atomiccsvreader__base_type__(tmp_path_factory: TempPathFactory) -> None:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context.mkdir()
    test_context_root = os.path.dirname(str(test_context))

    try:
        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
        with pytest.raises(ValueError) as ve:
            atomiccsvreader__base_type__('file1.txt')
        assert 'must be a CSV file with file extension .csv' in str(ve)

        os.mkdir(os.path.join(str(test_context), 'a-directory.csv'))

        with pytest.raises(ValueError) as ve:
            atomiccsvreader__base_type__('a-directory.csv')
        assert 'is not a file in' in str(ve)

        with pytest.raises(ValueError) as ve:
            atomiccsvreader__base_type__('file1.csv')
        assert 'is not a file in' in str(ve)

        test_file = test_context / 'file1.csv'
        test_file.write_text('\n')

        assert atomiccsvreader__base_type__('file1.csv') == 'file1.csv'

        with pytest.raises(ValueError) as ve:
            atomiccsvreader__base_type__('file1.csv | arg1=test')
        assert 'is not allowed' in str(ve)

        assert atomiccsvreader__base_type__('file1.csv|random=True') == 'file1.csv | random=True'
    finally:
        shutil.rmtree(test_context_root)

        try:
            del os.environ['GRIZZLY_CONTEXT_ROOT']
        except:
            pass


class TestAtomicCsvReader:
    def test(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = os.path.dirname(test_context)

        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        for count in range(1, 4):
            file = f'{count}.csv'
            with open(os.path.join(test_context, file), 'w') as fd:
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
            assert 'header11' in csvrow  # pylint: disable=unsupported-membership-test
            assert csvrow['header11'] == 'value111'  # pylint: disable=unsubscriptable-object
            assert len(instance._rows['test1']) == 0

            csvrow = instance['test1']
            assert csvrow is None

            instance = AtomicCsvReader('test2', '2.csv')
            assert len(instance._rows['test2']) == 2

            csvrow = instance['test2.header22']
            assert csvrow is not None
            assert len(csvrow.keys()) == 1
            assert len(instance._rows['test2']) == 1
            assert 'header22' in csvrow  # pylint: disable=unsupported-membership-test
            assert csvrow['header22'] == 'value212'  # pylint: disable=unsubscriptable-object

            csvrow = instance['test2']
            assert csvrow is not None
            assert len(csvrow.keys()) == 2
            assert len(instance._rows['test2']) == 0
            assert csvrow == {'header12': 'value122', 'header22': 'value222'}

            csvrow = instance['test2']
            assert csvrow is None

            instance = AtomicCsvReader('test3', '3.csv')
            assert len(instance._rows['test3']) == 3

            with pytest.raises(ValueError):
                instance['test3.headerXX']

            assert instance['test3'] == {'header13': 'value113', 'header23': 'value213', 'header33': 'value313'}
            assert instance['test3'] == {'header13': 'value123', 'header23': 'value223', 'header33': 'value323'}
            assert instance['test3'] == {'header13': 'value133', 'header23': 'value233', 'header33': 'value333'}
            assert instance.__getitem__('test3') is None

            with pytest.raises(NotImplementedError) as nie:
                instance['test4'] = {'test': 'value'}
            assert str(nie.value) == 'AtomicCsvReader has not implemented "__setitem__"'
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
            shutil.rmtree(test_context_root)

            try:
                del os.environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            cleanup()

    def test_clear_and_destroy(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = os.path.dirname(test_context)

        os.environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        with open(os.path.join(test_context, 'test.csv'), 'w') as fd:
            fd.write('header1\n')
            fd.write('value1\n')
            fd.flush()

        try:
            try:
                AtomicCsvReader.destroy()
            except Exception:
                pass

            with pytest.raises(ValueError):
                AtomicCsvReader.destroy()

            with pytest.raises(ValueError):
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
            shutil.rmtree(test_context_root)

            try:
                del os.environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            cleanup()

    def test___init___error(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            with pytest.raises(ValueError) as ve:
                AtomicCsvReader('test.test', 'file1.csv')
            assert 'is not a valid CSV source name, must be' in str(ve)
        finally:
            cleanup()

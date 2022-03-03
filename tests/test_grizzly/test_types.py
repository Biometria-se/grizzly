from typing import Callable
from os import path, environ, mkdir
from shutil import rmtree

import pytest

from _pytest.tmpdir import TempPathFactory

from grizzly.types import RequestDirection, RequestMethod, bool_typed, int_rounded_float_typed, AtomicVariable, GrizzlyDict

from .testdata.fixtures import cleanup  # pylint: disable=unused-import


class TestRequestDirection:
    def test_from_string(self) -> None:
        for direction in RequestDirection:
            assert RequestDirection.from_string(direction.name.lower()) == direction

        with pytest.raises(ValueError):
            RequestDirection.from_string('asdf')

    def test_methods(self) -> None:
        for method in RequestMethod:
            if method.direction == RequestDirection.FROM:
                assert method in RequestDirection.FROM.methods
            elif method.direction == RequestDirection.TO:
                assert method in RequestDirection.TO.methods
            else:
                pytest.fail(f'{method.name} does not have a direction registered')


class TestRequestMethod:
    def test_from_string(self) -> None:
        for method in RequestMethod:
            assert RequestMethod.from_string(method.name.lower()) == method

        with pytest.raises(ValueError):
            RequestMethod.from_string('asdf')

    def test_direction(self) -> None:
        for method in RequestMethod:
            assert method.value == method.direction
            assert method in method.direction.methods




def test_bool_typed() -> None:
    assert bool_typed('True')
    assert not bool_typed('False')

    with pytest.raises(ValueError):
        bool_typed('asdf')


def test_int_rounded_float_typed() -> None:
    assert int_rounded_float_typed('1') == 1
    assert int_rounded_float_typed('1.337') == 1
    assert int_rounded_float_typed('1.51') == 2

    with pytest.raises(ValueError):
        int_rounded_float_typed('asdf')

    with pytest.raises(ValueError):
        int_rounded_float_typed('0xbeef')

    with pytest.raises(ValueError):
        int_rounded_float_typed('1,5')


class TestGrizzlyDict:
    def test_static_str(self) -> None:
        t = GrizzlyDict()

        t['test1'] = 'hallo'
        assert isinstance(t['test1'], str)

        t['test7'] = '"true"'
        assert isinstance(t['test7'], str)

        t['test4'] = "'1337'"
        assert isinstance(t['test4'], str)

        t['test5'] = '"1337"'
        assert isinstance(t['test5'], str)
        assert t['test5'] == '1337'

        t['test6'] = '"True"'
        assert isinstance(t['test6'], str)
        assert t['test6'] == 'True'

        t['test7'] = '00004302'
        assert isinstance(t['test7'], str)
        assert t['test7'] == '00004302'

        t['test8'] = '02002-00000'
        assert isinstance(t['test8'], str)
        assert t['test8'] == '02002-00000'

    def test_static_float(self) -> None:
        t = GrizzlyDict()

        t['test2'] = 1.337
        assert isinstance(t['test2'], float)

        t['test2.1'] = -1.337
        assert isinstance(t['test2.1'], float)

        t['test2.2'] = '1.337'
        assert isinstance(t['test2.2'], float)
        assert t['test2.2'] == 1.337

        t['test2.3'] = '-1.337'
        assert isinstance(t['test2.3'], float)
        assert t['test2.3'] == -1.337

        t['test2.4'] = '0.01'
        assert isinstance(t['test2.4'], float)
        assert t['test2.4'] == 0.01

    def test_static_int(self) -> None:
        t = GrizzlyDict()
        t['test3'] = 1337
        assert isinstance(t['test3'], int)

        t['test3.1'] = 1337
        assert isinstance(t['test3.1'], int)

        t['test3.2'] = '1337'
        assert isinstance(t['test3.2'], int)
        assert t['test3.2'] == 1337

        t['test3.3'] = '-1337'
        assert isinstance(t['test3.3'], int)
        assert t['test3.3'] == -1337

    def test_static_bool(self) -> None:
        t: GrizzlyDict = GrizzlyDict()

        t['test6'] = True
        assert isinstance(t['test6'], bool)

        t['test8'] = 'True'
        assert isinstance(t['test8'], bool)

        t['test9'] = 'False'
        assert isinstance(t['test9'], bool)

        t['test10'] = 'true'
        assert isinstance(t['test10'], bool)
        assert t['test10'] == True

        t['test11'] = 'FaLsE'
        assert isinstance(t['test11'], bool)
        assert t['test11'] == False


    @pytest.mark.usefixtures('cleanup')
    def test_AtomicIntegerIncrementer(self, cleanup: Callable) -> None:
        try:
            t = GrizzlyDict()
            t['AtomicIntegerIncrementer.test1'] = 1337
            assert isinstance(t['AtomicIntegerIncrementer.test1'], str)

            t['AtomicIntegerIncrementer.test2'] = -1337
            assert isinstance(t['AtomicIntegerIncrementer.test2'], str)

            t['AtomicIntegerIncrementer.test3'] = '1337'
            assert isinstance(t['AtomicIntegerIncrementer.test3'], str)
            assert t['AtomicIntegerIncrementer.test3'] == '1337'

            t['AtomicIntegerIncrementer.test4'] = '-1337'
            assert isinstance(t['AtomicIntegerIncrementer.test4'], str)
            assert t['AtomicIntegerIncrementer.test4'] == '-1337'

            t['AtomicIntegerIncrementer.test5'] = '1.337'
            assert t['AtomicIntegerIncrementer.test5'] == '1'

            with pytest.raises(ValueError):
                t['AtomicIntegerIncrementer.test6'] = 'hello'

            t['AtomicIntegerIncrementer.test7'] = '1337 | step=10'
            assert t['AtomicIntegerIncrementer.test7'] == '1337 | step=10'

            t['AtomicIntegerIncrementer.test8'] = '1337|step=1'
            assert t['AtomicIntegerIncrementer.test8'] == '1337 | step=1'

            t['AtomicIntegerIncrementer.test9'] = '-1337|step=-10'
            assert isinstance(t['AtomicIntegerIncrementer.test9'], str)
            assert t['AtomicIntegerIncrementer.test9'] == '-1337 | step=-10'

            t['AtomicIntegerIncrementer.test10'] = '1.337 | step=-1'
            assert t['AtomicIntegerIncrementer.test10'] == '1 | step=-1'
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_AtomicDirectoryContents(self, cleanup: Callable, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(test_context)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            t = GrizzlyDict()

            with pytest.raises(ValueError):
                t['AtomicDirectoryContents.test1'] = 'doesnotexist/'

            with open(path.join(test_context, 'notadirectory'), 'w') as fd:
                fd.write('test')
                fd.flush()

            with pytest.raises(ValueError):
                t['AtomicDirectoryContents.test2'] = 'notadirectory'

            mkdir(path.join(test_context, 'adirectory'))

            t['AtomicDirectoryContents.test3'] = 'adirectory'

            with pytest.raises(ValueError):
                t['AtomicDirectoryContents.test4'] = 'adirectory | repeat=asdf'

            with pytest.raises(ValueError):
                t['AtomicDirectoryContents.test4'] = 'adirectory | repeat=True, prefix="test-"'

            t['AtomicDirectoryContents.test4'] = 'adirectory | repeat=True'

            t['AtomicDirectoryContents.test5'] = 'adirectory|repeat=True'
            assert t['AtomicDirectoryContents.test5'] == 'adirectory | repeat=True'

            t['AtomicDirectoryContents.test6'] = 'adirectory| random=True'
            assert t['AtomicDirectoryContents.test6'] == 'adirectory | random=True'

            t['AtomicDirectoryContents.test7'] = 'adirectory|repeat=True, random=True'
            assert t['AtomicDirectoryContents.test7'] == 'adirectory | repeat=True, random=True'
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            rmtree(test_context_root)
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_AtomicCsvRow(self, cleanup: Callable, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(test_context)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        with open(path.join(test_context, 'test.csv'), 'w') as fd:
            fd.write('header1,header2\n')
            fd.write('value1,value2\n')
            fd.flush()

        try:
            t = GrizzlyDict()

            with pytest.raises(ValueError):
                t['AtomicCsvRow.test'] = 'doesnotexist.csv'

            t['AtomicCsvRow.test'] = 'test.csv'

            with pytest.raises(ValueError):
                t['AtomicCsvRow.test2'] = 'test.csv | repeat=asdf'

            with pytest.raises(ValueError):
                t['AtomicCsvRow.test2'] = 'test.csv | repeat=True, suffix=True'

            t['AtomicCsvRow.test2'] = 'test.csv|repeat=True'
            assert t['AtomicCsvRow.test2'] == 'test.csv | repeat=True'
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            rmtree(test_context_root)
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_AtomicDate(self, cleanup: Callable) -> None:
        try:
            t = GrizzlyDict()
            with pytest.raises(ValueError):
                t['AtomicDate.test1'] = 1337

            with pytest.raises(ValueError):
                t['AtomicDate.test6'] = 'hello'

            t['AtomicDate.test2'] = '2021-03-29'
            assert isinstance(t['AtomicDate.test2'], str)
            assert t['AtomicDate.test2'] == '2021-03-29'

            t['AtomicDate.test3'] = '2021-03-29 16:43:49'
            assert isinstance(t['AtomicDate.test3'], str)
            assert t['AtomicDate.test3'] == '2021-03-29 16:43:49'

            t['AtomicDate.test4'] = 'now|format="%Y-%m-%d"'
            assert isinstance(t['AtomicDate.test4'], str)
            assert t['AtomicDate.test4'] == 'now | format="%Y-%m-%d"'

            with pytest.raises(ValueError):
                t['AtomicDate.test5'] = 'asdf|format="%Y-%m-%d"'

            with pytest.raises(ValueError):
                t['AtomicDate.test6'] = 'now|'

            t['AtomicDate.test7'] = 'now | format="%Y-%m-%dT%H:%M:%S.000Z"'
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_AtomicRandomInteger(self, cleanup: Callable) -> None:
        try:
            t = GrizzlyDict()

            with pytest.raises(ValueError):
                t['AtomicRandomInteger.test1'] = '10'

            with pytest.raises(ValueError):
                t['AtomicRandomInteger.test2'] = '1.17..5.0'

            with pytest.raises(ValueError):
                t['AtomicRandomInteger.test5'] = '1.0..3.5'

            with pytest.raises(ValueError):
                t['AtomicRandomInteger.test3'] = '100..10'

            t['AtomicRandomInteger.test4'] = '1..10'
            assert isinstance(t['AtomicRandomInteger.test4'], str)
            assert t['AtomicRandomInteger.test4'] == '1..10'
        finally:
            cleanup()


class TestAtomicVariable:
    @pytest.mark.usefixtures('cleanup')
    def test_dont_instantiate(self, cleanup: Callable) -> None:
        try:
            with pytest.raises(TypeError):
                AtomicVariable('dummy')

            with pytest.raises(TypeError):
                AtomicVariable[int]('dummy')
        finally:
            cleanup()

    def test_get(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.get()
        assert 'is not instantiated' in str(ve)

    def test_destroy(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.destroy()
        assert 'is not instantiated' in str(ve)

    def test_clear(self) -> None:
        with pytest.raises(ValueError) as ve:
            AtomicVariable.clear()
        assert 'is not instantiated' in str(ve)


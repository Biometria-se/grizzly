from typing import Tuple, Optional
from os import environ, path, mkdir
from shutil import rmtree

import pytest

from _pytest.tmpdir import TempPathFactory

from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.variables import AtomicCsvRow, AtomicIntegerIncrementer, AtomicServiceBus
from grizzly.context import GrizzlyContext

from ...fixtures import AtomicVariableCleanupFixture, NoopZmqFixture


class TestGrizzlyVariables:
    def test_static_str(self) -> None:
        t = GrizzlyVariables()

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
        t = GrizzlyVariables()

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
        t = GrizzlyVariables()
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
        t: GrizzlyVariables = GrizzlyVariables()

        t['test6'] = True
        assert isinstance(t['test6'], bool)

        t['test8'] = 'True'
        assert isinstance(t['test8'], bool)

        t['test9'] = 'False'
        assert isinstance(t['test9'], bool)

        t['test10'] = 'true'
        assert isinstance(t['test10'], bool)
        assert t.__getitem__('test10') is True

        t['test11'] = 'FaLsE'
        assert isinstance(t['test11'], bool)
        assert t.__getitem__('test11') is False

    def test_AtomicIntegerIncrementer(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = GrizzlyVariables()
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

    def test_AtomicDirectoryContents(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(test_context)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        try:
            t = GrizzlyVariables()

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

    def test_AtomicCsvRow(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(test_context)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        with open(path.join(test_context, 'test.csv'), 'w') as fd:
            fd.write('header1,header2\n')
            fd.write('value1,value2\n')
            fd.flush()

        try:
            t = GrizzlyVariables()

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

    def test_AtomicDate(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = GrizzlyVariables()
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

    def test_AtomicRandomInteger(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = GrizzlyVariables()

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

    @pytest.mark.parametrize('input,expected', [
        ('variable', (None, None, 'variable',),),
        ('AtomicIntegerIncrementer.foo', ('grizzly.testdata.variables', 'AtomicIntegerIncrementer', 'foo',),),
        ('AtomicCsvRow.users.username', ('grizzly.testdata.variables', 'AtomicCsvRow', 'users',),),
        ('tests.helpers.AtomicCustomVariable.hello', ('tests.helpers', 'AtomicCustomVariable', 'hello',),),
        ('tests.helpers.AtomicCustomVariable.foo.bar', ('tests.helpers', 'AtomicCustomVariable', 'foo',),),
        ('a.custom.struct', (None, None, 'a.custom.struct',),),
    ])
    def test_get_variable_spec(self, input: str, expected: Tuple[Optional[str], Optional[str], str]) -> None:
        assert GrizzlyVariables.get_variable_spec(input) == expected

    def test__get_variable_value_static(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = GrizzlyContext()
            variable_name = 'test'

            grizzly.state.variables[variable_name] = 1337
            value, _ = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert value == 1337

            grizzly.state.variables[variable_name] = '1337'
            value, _ = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert value == 1337

            grizzly.state.variables[variable_name] = "'1337'"
            value, _ = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert value == '1337'
        finally:
            cleanup()

    def test__get_variable_value_AtomicIntegerIncrementer(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = GrizzlyContext()

            variable_name = 'AtomicIntegerIncrementer.test'
            grizzly.state.variables[variable_name] = 1337
            value, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert external_dependencies == set()
            assert value['test'] == 1337
            assert value['test'] == 1338
            AtomicIntegerIncrementer.destroy()

            grizzly.state.variables[variable_name] = '1337'
            value, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert external_dependencies == set()
            assert value['test'] == 1337
            assert value['test'] == 1338
            AtomicIntegerIncrementer.destroy()
        finally:
            cleanup()

    def test__get_variable_value_custom_variable(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            grizzly = GrizzlyContext()

            variable_name = 'tests.helpers.AtomicCustomVariable.hello'
            grizzly.state.variables[variable_name] = 'world'

            value, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert external_dependencies == set()
            assert value['hello'] == 'world'

            variable_name = 'tests.helpers.AtomicCustomVariable.foo'
            grizzly.state.variables[variable_name] = 'bar'

            _, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert value['foo'] == 'bar'
        finally:
            cleanup()

    def test__get_variable_value_AtomicServiceBus(self, noop_zmq: NoopZmqFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        noop_zmq('grizzly.testdata.variables.servicebus')

        try:
            grizzly = GrizzlyContext()
            variable_name = 'AtomicServiceBus.test'
            grizzly.state.variables[variable_name] = (
                'queue:documents-in | url="sb://sb.example.com/;SharedAccessKeyName=name;SharedAccessKey=key"'
            )
            value, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)
            assert external_dependencies == set(['async-messaged'])
            assert not isinstance(value, AtomicServiceBus)
            assert value == '__on_consumer__'

            with pytest.raises(ValueError) as ve:
                AtomicServiceBus.destroy()
            assert "'AtomicServiceBus' is not instantiated" in str(ve)
        finally:
            cleanup()

    def test__get_variable_value_AtomicCsvRow(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'requests'
        test_context.mkdir()
        test_context_root = path.dirname(test_context)
        environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

        with open(path.join(test_context, 'test.csv'), 'w') as fd:
            fd.write('header1,header2\n')
            fd.write('value1,value2\n')
            fd.write('value3,value4\n')
            fd.flush()
        try:
            grizzly = GrizzlyContext()
            variable_name = 'AtomicCsvRow.test'
            grizzly.state.variables['AtomicCsvRow.test'] = 'test.csv'
            value, external_dependencies = GrizzlyVariables.get_variable_value(grizzly, variable_name)

            assert isinstance(value, AtomicCsvRow)
            assert external_dependencies == set()
            assert 'test' in value._values
            assert 'test' in value._rows
            assert value['test'] == {'header1': 'value1', 'header2': 'value2'}
            assert value['test'] == {'header1': 'value3', 'header2': 'value4'}
            assert value['test'] is None
        finally:
            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            rmtree(test_context_root)
            cleanup()

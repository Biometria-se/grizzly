"""Unit tests of grizzly.testdata."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, Optional

import pytest

from grizzly.context import GrizzlyContext
from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.variables import AtomicCsvReader, AtomicIntegerIncrementer
from tests.helpers import SOME, rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.tmpdir import TempPathFactory

    from tests.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


class TestGrizzlyVariables:
    def test_static_str(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        t = GrizzlyVariables(scenarios=grizzly.scenarios)

        t['test1'] = 'hallo'
        assert isinstance(t['test1'], str)

        t['test2'] = '"true"'
        assert isinstance(t['test2'], str)

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

        for scenario in grizzly.scenarios:
            assert scenario.jinja2.globals == SOME(dict, test1='hallo', test2='true', test4='1337', test5='1337', test6='True', test7='00004302', test8='02002-00000')

    def test_static_float(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        t = GrizzlyVariables(scenarios=grizzly.scenarios)

        t['test1'] = 1.337
        assert isinstance(t['test1'], float)

        t['test2'] = -1.337
        assert isinstance(t['test2'], float)

        t['test3'] = '1.337'
        assert isinstance(t['test3'], float)
        assert t['test3'] == 1.337

        t['test4'] = '-1.337'
        assert isinstance(t['test4'], float)
        assert t['test4'] == -1.337

        t['test5'] = '0.01'
        assert isinstance(t['test5'], float)
        assert t['test5'] == 0.01

        for scenario in grizzly.scenarios:
            assert scenario.jinja2.globals == SOME(dict, test1=1.337, test2=-1.337, test3=1.337, test4=-1.337, test5=0.01)

    def test_static_int(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        t = GrizzlyVariables(scenarios=grizzly.scenarios)
        t['test1'] = 1337
        assert isinstance(t['test1'], int)

        t['test2'] = -1337
        assert isinstance(t['test2'], int)

        t['test3'] = '1337'
        assert isinstance(t['test3'], int)
        assert t['test3'] == 1337

        t['test4'] = '-1337'
        assert isinstance(t['test4'], int)
        assert t['test4'] == -1337

        for scenario in grizzly.scenarios:
            assert scenario.jinja2.globals == SOME(dict, test1=1337, test2=-1337, test3=1337, test4=-1337)

    def test_static_bool(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        t: GrizzlyVariables = GrizzlyVariables(scenarios=grizzly.scenarios)

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

        for scenario in grizzly.scenarios:
            assert scenario.jinja2.globals == SOME(dict, test6=True, test8=True, test9=False, test10=True, test11=False)

    def test_atomic_integer_incrementer(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)
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

            with pytest.raises(ValueError, match='"hello" is not a valid initial value'):
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

            for scenario in grizzly.scenarios:
                assert 'AtomicIntegerIncrementer.test6' not in scenario.jinja2.globals
                assert scenario.jinja2.globals == SOME(dict, {
                    'AtomicIntegerIncrementer.test1': '1337',
                    'AtomicIntegerIncrementer.test2': '-1337',
                    'AtomicIntegerIncrementer.test3': '1337',
                    'AtomicIntegerIncrementer.test4': '-1337',
                    'AtomicIntegerIncrementer.test5': '1',
                    'AtomicIntegerIncrementer.test7': '1337 | step=10',
                    'AtomicIntegerIncrementer.test8': '1337 | step=1',
                    'AtomicIntegerIncrementer.test9': '-1337 | step=-10',
                    'AtomicIntegerIncrementer.test10': '1 | step=-1',
                })
        finally:
            cleanup()

    def test_atomic_directory_contents(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)

            with pytest.raises(ValueError, match='is not a directory in'):
                t['AtomicDirectoryContents.test1'] = 'doesnotexist/'

            (grizzly_fixture.test_context / 'requests' / 'notadirectory').write_text('test')

            with pytest.raises(ValueError, match='is not a directory in'):
                t['AtomicDirectoryContents.test2'] = 'notadirectory'

            (grizzly_fixture.test_context / 'requests' / 'adirectory').mkdir()

            t['AtomicDirectoryContents.test3'] = 'adirectory'

            with pytest.raises(ValueError, match='asdf is not a valid boolean'):
                t['AtomicDirectoryContents.test4'] = 'adirectory | repeat=asdf'

            with pytest.raises(ValueError, match='argument prefix is not allowed'):
                t['AtomicDirectoryContents.test5'] = 'adirectory | repeat=True, prefix="test-"'

            t['AtomicDirectoryContents.test6'] = 'adirectory | repeat=True'

            t['AtomicDirectoryContents.test7'] = 'adirectory|repeat=True'
            assert t['AtomicDirectoryContents.test7'] == 'adirectory | repeat=True'

            t['AtomicDirectoryContents.test8'] = 'adirectory| random=True'
            assert t['AtomicDirectoryContents.test8'] == 'adirectory | random=True'

            t['AtomicDirectoryContents.test9'] = 'adirectory|repeat=True, random=True'
            assert t['AtomicDirectoryContents.test9'] == 'adirectory | repeat=True, random=True'

            for scenario in grizzly.scenarios:
                assert all(f'AtomicDirectoryContents.{name}' not in scenario.jinja2.globals for name in ['test1', 'test2', 'test4', 'test5'])
                assert scenario.jinja2.globals == SOME(dict, {
                    'AtomicDirectoryContents.test3': 'adirectory',
                    'AtomicDirectoryContents.test6': 'adirectory | repeat=True',
                    'AtomicDirectoryContents.test7': 'adirectory | repeat=True',
                    'AtomicDirectoryContents.test8': 'adirectory | random=True',
                    'AtomicDirectoryContents.test9': 'adirectory | repeat=True, random=True',
                })
        finally:
            cleanup()

    def test_atomic_csv_reader(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))
        test_context = grizzly_fixture.test_context / 'requests'

        (test_context / 'test.csv').write_text("""header1,header2
header1,header2
value1,value2
""")

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)

            with pytest.raises(ValueError, match='is not a file in'):
                t['AtomicCsvReader.test1'] = 'doesnotexist.csv'

            t['AtomicCsvReader.test2'] = 'test.csv'

            with pytest.raises(ValueError, match='asdf is not a valid boolean'):
                t['AtomicCsvReader.test3'] = 'test.csv | repeat=asdf'

            with pytest.raises(ValueError, match='argument suffix is not allowed'):
                t['AtomicCsvReader.test4'] = 'test.csv | repeat=True, suffix=True'

            t['AtomicCsvReader.test5'] = 'test.csv|repeat=True'
            assert t['AtomicCsvReader.test5'] == 'test.csv | repeat=True'

            for scenario in grizzly.scenarios:
                assert all(f'AtomicCsvReader.{name}' not in scenario.jinja2.globals for name in ['test1', 'test3', 'test4'])
                assert scenario.jinja2.globals == SOME(dict, {
                    'AtomicCsvReader.test2': 'test.csv',
                    'AtomicCsvReader.test5': 'test.csv | repeat=True',
                })
        finally:
            cleanup()

    def test_atomic_date(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)
            with pytest.raises(TypeError, match='is not a string'):
                t['AtomicDate.test1'] = 1337

            with pytest.raises(ValueError, match='Unknown string format: hello'):
                t['AtomicDate.test2'] = 'hello'

            t['AtomicDate.test3'] = '2021-03-29'
            assert t['AtomicDate.test3'] == '2021-03-29'

            t['AtomicDate.test4'] = '2021-03-29 16:43:49'
            assert t['AtomicDate.test4'] == '2021-03-29 16:43:49'

            t['AtomicDate.test5'] = 'now|format="%Y-%m-%d"'
            assert t['AtomicDate.test5'] == 'now | format="%Y-%m-%d"'

            with pytest.raises(ValueError, match='Unknown string format: asdf'):
                t['AtomicDate.test6'] = 'asdf|format="%Y-%m-%d"'

            with pytest.raises(ValueError, match='incorrect format in arguments: ""'):
                t['AtomicDate.test7'] = 'now|'

            t['AtomicDate.test8'] = 'now | format="%Y-%m-%dT%H:%M:%S.000Z"'

            for scenario in grizzly.scenarios:
                assert all(f'AtomicDate.{name}' not in scenario.jinja2.globals for name in ['test1', 'test2', 'test6', 'test7'])
                assert scenario.jinja2.globals == SOME(dict, {
                    'AtomicDate.test3': '2021-03-29',
                    'AtomicDate.test4': '2021-03-29 16:43:49',
                    'AtomicDate.test5': 'now | format="%Y-%m-%d"',
                    'AtomicDate.test8': 'now | format="%Y-%m-%dT%H:%M:%S.000Z"',
                })
        finally:
            cleanup()

    def test_atomic_random_integer(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)

            with pytest.raises(ValueError, match='10 is not a valid value format, must be: "a..b"'):
                t['AtomicRandomInteger.test1'] = '10'

            with pytest.raises(ValueError, match='1.17 is not a valid integer'):
                t['AtomicRandomInteger.test2'] = '1.17..5.0'

            with pytest.raises(ValueError, match='1.0 is not a valid integer'):
                t['AtomicRandomInteger.test3'] = '1.0..3.5'

            with pytest.raises(ValueError, match='first value needs to be less than second value'):
                t['AtomicRandomInteger.test4'] = '100..10'

            t['AtomicRandomInteger.test5'] = '1..10'
            assert t['AtomicRandomInteger.test5'] == '1..10'

            for scenario in grizzly.scenarios:
                assert all(f'AtomicRandomInteger.{name}' not in scenario.jinja2.globals for name in ['test1', 'test2', 'test3', 'test4'])
                assert scenario.jinja2.globals == SOME(dict, {
                    'AtomicRandomInteger.test5': '1..10',
                })
        finally:
            cleanup()

    @pytest.mark.parametrize(('value', 'expected_spec', 'expected_init_value'), [
        ('variable', (None, None, 'variable', None), 'variable'),
        ('AtomicIntegerIncrementer.foo', ('grizzly.testdata.variables', 'AtomicIntegerIncrementer', 'foo', None), 'AtomicIntegerIncrementer.foo'),
        ('AtomicCsvReader.users.username', ('grizzly.testdata.variables', 'AtomicCsvReader', 'users', 'username'), 'AtomicCsvReader.users'),
        ('tests.helpers.AtomicCustomVariable.hello', ('tests.helpers', 'AtomicCustomVariable', 'hello', None), 'tests.helpers.AtomicCustomVariable.hello'),
        ('tests.helpers.AtomicCustomVariable.foo.bar', ('tests.helpers', 'AtomicCustomVariable', 'foo', 'bar'), 'tests.helpers.AtomicCustomVariable.foo'),
        ('a.custom.struct', (None, None, 'a.custom.struct', None), 'a.custom.struct'),
    ])
    def test_get_variable_spec_and_initialization_value(self, value: str, expected_spec: tuple[Optional[str], Optional[str], str, Optional[str]], expected_init_value: str) -> None:
        assert GrizzlyVariables.get_variable_spec(value) == expected_spec
        assert GrizzlyVariables.get_initialization_value(value) == expected_init_value

    class Test_initialize_variable:
        """Unit tests of grizzly.testdata.initialize_variable."""

        def test_static(self, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = GrizzlyContext()
                variable_name = 'test'

                with pytest.raises(ValueError, match=f'variable "{variable_name}" has not been declared'):
                    GrizzlyVariables.initialize_variable(grizzly, variable_name)

                grizzly.state.variables[variable_name] = 1337
                assert GrizzlyVariables.initialize_variable(grizzly, variable_name) == (1337, set(), {})

                grizzly.state.variables[variable_name] = '1337'
                assert GrizzlyVariables.initialize_variable(grizzly, variable_name) == (1337, set(), {})

                grizzly.state.variables[variable_name] = "'1337'"
                assert GrizzlyVariables.initialize_variable(grizzly, variable_name) == ('1337', set(), {})
            finally:
                cleanup()

        def test_atomic_integer_incrementer(self, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = GrizzlyContext()

                variable_name = 'AtomicIntegerIncrementer.test'
                with pytest.raises(ValueError, match=f'variable "{variable_name}" has not been declared'):
                    GrizzlyVariables.initialize_variable(grizzly, variable_name)

                grizzly.state.variables[variable_name] = 1337
                value, external_dependencies, message_handlers = GrizzlyVariables.initialize_variable(grizzly, variable_name)
                assert external_dependencies == set()
                assert message_handlers == {}
                assert value['test'] == 1337
                assert value['test'] == 1338
                AtomicIntegerIncrementer.destroy()

                grizzly.state.variables[variable_name] = '1337'
                value, external_dependencies, message_handlers = GrizzlyVariables.initialize_variable(grizzly, variable_name)
                assert external_dependencies == set()
                assert message_handlers == {}
                assert value['test'] == 1337
                assert value['test'] == 1338
                AtomicIntegerIncrementer.destroy()
            finally:
                cleanup()

        def test_custom_variable(self, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = GrizzlyContext()

                variable_name = 'tests.helpers.AtomicCustomVariable.hello'
                grizzly.state.variables[variable_name] = 'world'

                value, external_dependencies, message_handlers = GrizzlyVariables.initialize_variable(grizzly, variable_name)
                assert external_dependencies == set()
                assert message_handlers == {}
                assert value['hello'] == 'world'

                variable_name = 'tests.helpers.AtomicCustomVariable.foo'
                grizzly.state.variables[variable_name] = 'bar'

                _, external_dependencies, message_handlers = GrizzlyVariables.initialize_variable(grizzly, variable_name)
                assert value['foo'] == 'bar'
                assert external_dependencies == set()
                assert message_handlers == {}
            finally:
                cleanup()

        def test_atomic_csv_reader(self, cleanup: AtomicVariableCleanupFixture, tmp_path_factory: TempPathFactory) -> None:
            test_context = tmp_path_factory.mktemp('test_context') / 'requests'
            test_context.mkdir()
            test_context_root = test_context.parent.as_posix()
            environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

            (test_context / 'test.csv').write_text("""header1,header2
value1,value2
value3,value4
""")
            try:
                grizzly = GrizzlyContext()
                variable_name = 'AtomicCsvReader.test'
                grizzly.state.variables['AtomicCsvReader.test'] = 'test.csv'
                value, external_dependencies, message_handlers = GrizzlyVariables.initialize_variable(grizzly, variable_name)

                assert isinstance(value, AtomicCsvReader)
                assert external_dependencies == set()
                assert message_handlers == {}
                assert 'test' in value._values
                assert 'test' in value._rows
                assert value['test'] == {'header1': 'value1', 'header2': 'value2'}
                assert value['test'] == {'header1': 'value3', 'header2': 'value4'}
                assert value['test'] is None
            finally:
                with suppress(KeyError):
                    del environ['GRIZZLY_CONTEXT_ROOT']

                rm_rf(test_context_root)
                cleanup()

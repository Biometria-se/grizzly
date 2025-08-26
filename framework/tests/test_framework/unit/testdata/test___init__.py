"""Unit tests of grizzly.testdata."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from grizzly.testdata import GrizzlyVariables
from grizzly.testdata.variables import AtomicCsvReader, AtomicIntegerIncrementer

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import AtomicVariableCleanupFixture, GrizzlyFixture


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

        t['test5'] = 0
        assert t['test5'] == 0

        t['test6'] = '0'
        assert t['test6'] == 0

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
        finally:
            cleanup()

    def test_atomic_json_reader(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
        grizzly = grizzly_fixture.grizzly
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-1'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-2'))
        grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test-3'))
        test_context = grizzly_fixture.test_context / 'requests'

        test_file = test_context / 'test.json'
        with test_file.open('w') as fd:
            json.dump([{'hello': 'world'}, {'hello': 'foo'}, {'hello': 'bar'}], fd)

        try:
            t = GrizzlyVariables(scenarios=grizzly.scenarios)

            with pytest.raises(ValueError, match='is not a file in'):
                t['AtomicJsonReader.test1'] = 'doesnotexist.json'

            t['AtomicJsonReader.test2'] = 'test.json'

            with pytest.raises(ValueError, match='asdf is not a valid boolean'):
                t['AtomicJsonReader.test3'] = 'test.json | repeat=asdf'

            with pytest.raises(ValueError, match='argument suffix is not allowed'):
                t['AtomicJsonReader.test4'] = 'test.json | repeat=True, suffix=True'

            t['AtomicJsonReader.test5'] = 'test.json|repeat=True'
            assert t['AtomicJsonReader.test5'] == 'test.json | repeat=True'
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
        finally:
            cleanup()

    @pytest.mark.parametrize(
        ('value', 'expected_spec', 'expected_init_value'),
        [
            ('variable', (None, None, 'variable', None), 'variable'),
            ('AtomicIntegerIncrementer.foo', ('grizzly.testdata.variables', 'AtomicIntegerIncrementer', 'foo', None), 'AtomicIntegerIncrementer.foo'),
            ('AtomicCsvReader.users.username', ('grizzly.testdata.variables', 'AtomicCsvReader', 'users', 'username'), 'AtomicCsvReader.users'),
            ('tests.helpers.AtomicCustomVariable.hello', ('tests.helpers', 'AtomicCustomVariable', 'hello', None), 'tests.helpers.AtomicCustomVariable.hello'),
            ('tests.helpers.AtomicCustomVariable.foo.bar', ('tests.helpers', 'AtomicCustomVariable', 'foo', 'bar'), 'tests.helpers.AtomicCustomVariable.foo'),
            ('a.custom.struct', (None, None, 'a.custom.struct', None), 'a.custom.struct'),
        ],
    )
    def test_get_variable_spec_and_initialization_value(self, value: str, expected_spec: tuple[str | None, str | None, str, str | None], expected_init_value: str) -> None:
        assert GrizzlyVariables.get_variable_spec(value) == expected_spec
        assert GrizzlyVariables.get_initialization_value(value) == expected_init_value

    class Test_initialize_variable:
        """Unit tests of grizzly.testdata.initialize_variable."""

        def test_static(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = grizzly_fixture.grizzly
                variable_name = 'test'

                with pytest.raises(ValueError, match=f'variable "{variable_name}" has not been declared'):
                    GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)

                grizzly.scenario.variables[variable_name] = 1337
                assert GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name) == (1337, set())

                grizzly.scenario.variables[variable_name] = '1337'
                assert GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name) == (1337, set())

                grizzly.scenario.variables[variable_name] = "'1337'"
                assert GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name) == ('1337', set())
            finally:
                cleanup()

        def test_atomic_integer_incrementer(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = grizzly_fixture.grizzly

                variable_name = 'AtomicIntegerIncrementer.test'
                with pytest.raises(ValueError, match=f'variable "{variable_name}" has not been declared'):
                    GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)

                grizzly.scenario.variables[variable_name] = 1337
                value, dependencies = GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)
                assert dependencies == set()
                assert value['test'] == 1337
                assert value['test'] == 1338
                AtomicIntegerIncrementer.destroy()

                grizzly.scenario.variables[variable_name] = '1337'
                value, dependencies = GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)
                assert dependencies == set()
                assert value['test'] == 1337
                assert value['test'] == 1338
                AtomicIntegerIncrementer.destroy()
            finally:
                cleanup()

        def test_custom_variable(self, grizzly_fixture: GrizzlyFixture, cleanup: AtomicVariableCleanupFixture) -> None:
            try:
                grizzly = grizzly_fixture.grizzly

                variable_name = 'test_framework.helpers.AtomicCustomVariable.hello'
                grizzly.scenario.variables[variable_name] = 'world'

                value, dependencies = GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)
                assert dependencies == set()
                assert value['hello'] == 'world'

                variable_name = 'test_framework.helpers.AtomicCustomVariable.foo'
                grizzly.scenario.variables[variable_name] = 'bar'

                _, dependencies = GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)
                assert value['foo'] == 'bar'
                assert dependencies == set()
            finally:
                cleanup()

        def test_atomic_csv_reader(self, cleanup: AtomicVariableCleanupFixture, grizzly_fixture: GrizzlyFixture) -> None:
            test_context = grizzly_fixture.test_context / 'requests'
            test_context.mkdir(exist_ok=True)

            (test_context / 'test.csv').write_text("""header1,header2
value1,value2
value3,value4
""")
            try:
                grizzly = grizzly_fixture.grizzly
                variable_name = 'AtomicCsvReader.test'
                grizzly.scenario.variables['AtomicCsvReader.test'] = 'test.csv'
                value, dependencies = GrizzlyVariables.initialize_variable(grizzly.scenario, variable_name)

                assert isinstance(value, AtomicCsvReader)
                assert dependencies == set()
                assert 'test' in value._values
                assert 'test' in value._rows
                assert value['test'] == {'header1': 'value1', 'header2': 'value2'}
                assert value['test'] == {'header1': 'value3', 'header2': 'value4'}
                assert value['test'] is None
            finally:
                cleanup()

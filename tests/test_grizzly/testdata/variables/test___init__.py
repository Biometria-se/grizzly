from typing import Callable

import pytest

from grizzly.testdata.variables import AtomicVariable, AtomicInteger, AtomicIntegerIncrementer, load_variable, destroy_variables, parse_arguments

from ..fixtures import cleanup  # pylint: disable=unused-import


def test_load_variable_non_existent() -> None:
    with pytest.raises(AttributeError):
        load_variable('AtomicIntegerWithAnAbsurdNameThatReallyReallyShouldNotExist')


@pytest.mark.usefixtures('cleanup')
def test_load_variable_Atomic_types(cleanup: Callable) -> None:
    try:
        for name in ['AtomicInteger', 'AtomicDate', 'AtomicIntegerIncrementer']:
            v = load_variable(name)
            assert callable(v)
    finally:
        cleanup()


class AtomicFakeClass:
    pass


@pytest.mark.usefixtures('cleanup')
def test_destroy_variables(cleanup: Callable) -> None:
    try:
        t1 = AtomicInteger('test', 2)
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test'] == 2
        assert t2['test2'] == 1337

        with pytest.raises(ValueError):
            AtomicInteger('test', 3)

        with pytest.raises(ValueError):
            AtomicIntegerIncrementer('test2', 1338)

        destroy_variables()

        t1 = AtomicInteger('test', 2)
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test'] == 2
        assert t2['test2'] == 1337
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


@pytest.mark.usefixtures('cleanup')
def test_parse_arguments(cleanup: Callable) -> None:
    try:
        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'argument')
        assert 'incorrect format in arguments:' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'arg1=test arg2=value')
        assert 'incorrect format in arguments:' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'args1=test,')
        assert 'incorrect format for arguments:' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'args1=test,arg2')
        assert 'incorrect format for argument:' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, '"test"=value')
        assert 'no quotes or spaces allowed in argument names' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, "'test'=value")
        assert 'no quotes or spaces allowed in argument names' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'test variable=value')
        assert 'no quotes or spaces allowed in argument names' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'arg="value\'')
        assert 'value is incorrectly quoted' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, "arg='value\"")
        assert 'value is incorrectly quoted' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'arg=value"')
        assert 'value is incorrectly quoted' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, "arg=value'")
        assert 'value is incorrectly quoted' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'arg=test value')
        assert 'value needs to be quoted' in str(ve)

        arguments = parse_arguments(AtomicVariable, 'arg1=testvalue1, arg2="test value 2"')

        assert arguments == {
            'arg1': 'testvalue1',
            'arg2': 'test value 2',
        }

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'url=http://www.example.com?query_string=value')
        assert 'incorrect format in arguments: ' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, 'url="http://www.example.com?query_string=value')
        assert 'incorrect format in arguments: ' in str(ve)

        with pytest.raises(ValueError) as ve:
            parse_arguments(AtomicVariable, "url='http://www.example.com?query_string=value")
        assert 'incorrect format in arguments: ' in str(ve)

        arguments = parse_arguments(AtomicVariable, "url='http://www.example.com?query_string=value', argument=False")
        assert arguments == {
            'url': 'http://www.example.com?query_string=value',
            'argument': 'False',
        }
    finally:
        cleanup()


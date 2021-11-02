from typing import Callable

import pytest

from grizzly.testdata.variables import AtomicIntegerIncrementer, AtomicRandomString, load_variable, destroy_variables

from ..fixtures import cleanup  # pylint: disable=unused-import


def test_load_variable_non_existent() -> None:
    with pytest.raises(AttributeError):
        load_variable('AtomicIntegerWithAnAbsurdNameThatReallyReallyShouldNotExist')


@pytest.mark.usefixtures('cleanup')
def test_load_variable_Atomic_types(cleanup: Callable) -> None:
    try:
        for name in ['AtomicRandomInteger', 'AtomicDate', 'AtomicIntegerIncrementer']:
            v = load_variable(name)
            assert callable(v)
    finally:
        cleanup()


@pytest.mark.usefixtures('cleanup')
def test_destroy_variables(cleanup: Callable) -> None:
    try:
        t1 = AtomicRandomString('test1', '%s%s%d%d')
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test1'] is not None
        assert t2['test2'] == 1337

        with pytest.raises(ValueError):
            AtomicRandomString('test1', '%s%d')

        with pytest.raises(ValueError):
            AtomicIntegerIncrementer('test2', 1338)

        destroy_variables()

        t1 = AtomicRandomString('test1', '%s%s%d%d')
        t2 = AtomicIntegerIncrementer('test2', 1337)
        assert t1['test1'] is not None
        assert t2['test2'] == 1337
    finally:
        cleanup()


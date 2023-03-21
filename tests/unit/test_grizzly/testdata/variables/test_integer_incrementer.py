import threading

from typing import List, Set
from json import dumps as jsondumps

import pytest
import gevent

from gevent.greenlet import Greenlet

from grizzly.testdata.variables.integer_incrementer import atomicintegerincrementer__base_type__
from grizzly.testdata.variables import AtomicIntegerIncrementer

from tests.fixtures import AtomicVariableCleanupFixture


def test_atomicintegerincrementer__base_type__() -> None:
    assert atomicintegerincrementer__base_type__(10) == '10'

    assert atomicintegerincrementer__base_type__('10') == '10'
    assert atomicintegerincrementer__base_type__('10 | step=2') == '10 | step=2'
    assert atomicintegerincrementer__base_type__('10|step=35') == '10 | step=35'
    assert atomicintegerincrementer__base_type__('1| step=20') == '1 | step=20'

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('10 |')

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('10 | asdf')

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('|')

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('asdf|')

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('asdf| step=asdf')

    with pytest.raises(ValueError):
        atomicintegerincrementer__base_type__('10 | step=asdf')

    with pytest.raises(ValueError) as ve:
        atomicintegerincrementer__base_type__('10 | step=1, iterations=10')
    assert 'argument iterations is not allowed'

    with pytest.raises(ValueError) as ve:
        atomicintegerincrementer__base_type__('10 | iterations=10')
    assert 'step is not specified' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicintegerincrementer__base_type__('asdf')
    assert 'is not a valid initial value' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicintegerincrementer__base_type__('5 | step=2, persist=asdf')
    assert str(ve.value) == 'asdf is not a valid boolean'


class TestAtomicIntegerIncrementer:
    def test_increments_on_access(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', 1)
            assert t['message_id'] == 1
            assert t['message_id'] == 2

            t = AtomicIntegerIncrementer('test', '0 | step=10, persist=True')
            assert len(t._steps.keys()) == 2
            assert 'message_id' in t._steps
            assert 'test' in t._steps

            assert t['test'] == 0
            assert t['test'] == 10
            assert t['test'] == 20

            with pytest.raises(ValueError) as ve:
                t.generate_initial_value('message_id')
            assert str(ve.value) == 'AtomicIntegerIncrementer.message_id should not be persisted'

            assert t.generate_initial_value('test') == '30 | step=10, persist=True'
        finally:
            cleanup()

    def test_clear_and_destory(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            try:
                AtomicIntegerIncrementer.destroy()
            except Exception:
                pass

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer.destroy()

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer.clear()

            instance = AtomicIntegerIncrementer('dummy', '1|step=10')

            assert len(instance._values.keys()) == 1
            assert len(instance._steps.keys()) == 1

            AtomicIntegerIncrementer.clear()

            assert len(instance._values.keys()) == 0
            assert len(instance._steps.keys()) == 0

            AtomicIntegerIncrementer.destroy()

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer.destroy()
        finally:
            cleanup()

    def test_no_redefine_value(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', 3)
            with pytest.raises(NotImplementedError) as nie:
                t['message_id'] = 1
            assert str(nie.value) == 'AtomicIntegerIncrementer has not implemented "__setitem__"'

            assert t['message_id'] == 3

            del t['message_id']
            del t['message_id']
        finally:
            cleanup()

    def test_increments_with_step(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', '4 | step=10, persist=True')
            t = AtomicIntegerIncrementer('test', '10 | step=20')
            assert t['message_id'] == 4
            assert t['message_id'] == 14
            assert t['test'] == 10
            assert t['test'] == 30

            with pytest.raises(ValueError) as ve:
                t.generate_initial_value('test')
            assert str(ve.value) == 'AtomicIntegerIncrementer.test should not be persisted'

            assert t.generate_initial_value('message_id') == '24 | step=10, persist=True'

            del t['message_id']
            del t['test']

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer('test', '| step=10')

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer('test', 'asdf | step=10')

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer('test', '10 | step=asdf')

            with pytest.raises(ValueError):
                AtomicIntegerIncrementer('test', '0xFF | step=0x01')
        finally:
            cleanup()

    def test_json_serializable(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', 1)
            jsondumps(t['message_id'])
        finally:
            cleanup()

    def test_multi_thread(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            start_value: int = 2
            num_threads: int = 20
            num_iterations: int = 1001
            expected_value = start_value + num_threads * num_iterations

            t = AtomicIntegerIncrementer('thread_var', start_value)

            values: Set[int] = set()

            def func1() -> None:
                for _ in range(num_iterations):
                    value = t.__getitem__('thread_var')
                    assert value is not None
                    assert value > start_value - 1
                    assert value not in values
                    values.add(value)

            threads: List[threading.Thread] = []
            for _ in range(num_threads):
                thread = threading.Thread(target=func1)
                threads.append(thread)

            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()

            assert t['thread_var'] == expected_value
        finally:
            cleanup()

    def test_multi_greenlet(self, cleanup: AtomicVariableCleanupFixture) -> None:
        try:
            start_value: int = 2
            num_threads: int = 20
            num_iterations: int = 1001
            expected_value = start_value + num_threads * num_iterations

            t = AtomicIntegerIncrementer('greenlet_var', start_value)

            values: Set[int] = set()

            def exception_handler(greenlet: gevent.Greenlet) -> None:
                raise RuntimeError(f'func1 did not validate for {greenlet}')

            def func1() -> None:
                for _ in range(num_iterations):
                    value = t.__getitem__('greenlet_var')
                    assert value is not None
                    assert value > start_value - 1
                    assert value not in values
                    values.add(value)

            greenlets: List[Greenlet] = []
            for _ in range(num_threads):
                greenlet = gevent.spawn(func1)
                greenlet.link_exception(exception_handler)
                greenlets.append(greenlet)

            try:
                gevent.joinall(greenlets)

                for greenlet in greenlets:
                    greenlet.get()
            except RuntimeError as e:
                pytest.fail(str(e))

            assert len(values) == num_threads * num_iterations
            assert t['greenlet_var'] == expected_value
        finally:
            cleanup()

import threading

from typing import Callable, List, Set, cast
from json import dumps as jsondumps

import pytest
import gevent

from gevent.greenlet import Greenlet

from grizzly.testdata.variables import atomicintegerincrementer__base_type__, AtomicIntegerIncrementer, AtomicInteger

from ..fixtures import cleanup  # pylint: disable=unused-import


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


class TestAtomicIntegerIncrementer:
    @pytest.mark.usefixtures('cleanup')
    def test_increments_on_access(self, cleanup: Callable) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', 1)
            assert t['message_id'] == 1
            assert t['message_id'] == 2

            t = AtomicIntegerIncrementer('test', '0 | step=10')
            assert len(t._steps.keys()) == 2
            assert 'message_id' in t._steps
            assert 'test' in t._steps

            assert t['test'] == 0
            assert t['test'] == 10
            assert t['test'] == 20
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_clear_and_destory(self, cleanup: Callable) -> None:
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

    @pytest.mark.usefixtures('cleanup')
    def test_no_redefine_value(self, cleanup: Callable) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', 3)
            t['message_id'] = 1

            assert t['message_id'] == 3

            del t['message_id']
            del t['message_id']
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_increments_with_step(self, cleanup: Callable) -> None:
        try:
            t = AtomicIntegerIncrementer('message_id', '4 | step=10')
            t = AtomicIntegerIncrementer('test', '10 | step=20')
            assert t['message_id'] == 4
            assert t['message_id'] == 14
            assert t['test'] == 10
            assert t['test'] == 30

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

    @pytest.mark.usefixtures('cleanup')
    def test_json_serializable(self, cleanup: Callable) -> None:
        try:
            t = AtomicInteger('message_id', 1)
            jsondumps(t['message_id'])
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_multi_thread(self, cleanup: Callable) -> None:
        try:
            start_value: int = 2
            num_threads: int = 20
            num_iterations: int = 1001
            expected_value = start_value + num_threads * num_iterations

            t = AtomicIntegerIncrementer('thread_var', start_value)

            def func1() -> None:
                for _ in range(num_iterations):
                    # pylint: disable=pointless-statement
                    t['thread_var']

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

    @pytest.mark.usefixtures('cleanup')
    def test_multi_greenlet(self, cleanup: Callable) -> None:
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
                    # pylint: disable=pointless-statement
                    value = t['greenlet_var']
                    assert value != None
                    v = cast(int, value)
                    assert v > start_value - 1
                    assert v not in values
                    values.add(v)

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

from typing import Callable

import pytest

from grizzly.testdata.variables import AtomicInteger

from ..fixtures import cleanup  # pylint: disable=unused-import


class TestAtomicInteger:
    @pytest.mark.usefixtures('cleanup')
    def test_is_singleton(self, cleanup: Callable) -> None:
        try:
            try:
                AtomicInteger.destroy()
            except Exception:
                pass

            with pytest.raises(ValueError):
                AtomicInteger.get()

            with pytest.raises(ValueError):
                AtomicInteger.destroy()

            t1 = AtomicInteger('dummy1')
            t2 = AtomicInteger('dummy2')

            assert t1 is t2

            t3 = AtomicInteger.get()
            assert t3 is t1
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_clear_and_destroy(self) -> None:
        try:
            AtomicInteger.destroy()
        except Exception:
            pass

        with pytest.raises(ValueError):
            AtomicInteger.destroy()

        with pytest.raises(ValueError):
            AtomicInteger.get()

        instance = AtomicInteger('dummy', 1)
        assert instance['dummy'] == 1
        assert len(instance._values.keys()) == 1

        AtomicInteger.clear()

        assert len(instance._values.keys()) == 0

        with pytest.raises(AttributeError):
            instance['dummy']

        AtomicInteger.destroy()

    @pytest.mark.usefixtures('cleanup')
    def test_missing_attribute(self, cleanup: Callable) -> None:
        try:
            t = AtomicInteger('dummy')

            with pytest.raises(AttributeError):
                t['message_id'] = 1

            del t['dummy']

            with pytest.raises(AttributeError):
                # pylint: disable=pointless-statement
                t['dummy']
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_redefine_variable(self, cleanup: Callable) -> None:
        try:
            t = AtomicInteger('message_id')
            assert t['message_id'] is None

            with pytest.raises(ValueError):
                t = AtomicInteger('message_id', 10)
            assert t['message_id'] is None
        finally:
            cleanup()

    @pytest.mark.usefixtures('cleanup')
    def test_set_start_value(self, cleanup: Callable) -> None:
        try:
            t = AtomicInteger('message_id')
            assert t['message_id'] is None

            t['message_id'] = 1
            assert t['message_id'] == 1
            assert t['message_id'] != 2
        finally:
            cleanup()

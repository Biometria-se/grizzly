"""Unit tests of grizzly.gevent."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pytest
from gevent import getcurrent
from greenlet import greenlet
from grizzly.gevent import GreenletFactory

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from grizzly.scenarios import GrizzlyScenario

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


def func(a: str, i: int) -> None:
    msg = f'func error, {a=}, {i=}'
    raise RuntimeError(msg)


class TestGreenletFactory:
    def test___init__(self) -> None:
        g = GreenletFactory(logger=logging.getLogger())

        assert g.started_from == getcurrent()
        assert isinstance(g, GreenletFactory)
        assert isinstance(g.started_from, greenlet)

    def test_handle_exception(self) -> None:
        g = GreenletFactory(logger=logging.getLogger())

        with pytest.raises(RuntimeError, match='error'):
            g.handle_exception(RuntimeError('error'))

    def test_spawn(self, mocker: MockerFixture) -> None:
        g = GreenletFactory(logger=logging.getLogger())
        wrap_exceptions_spy = mocker.spy(g, 'wrap_exceptions')

        with pytest.raises(RuntimeError, match="func error, a='hello', i=1"):  # noqa: PT012
            func_g = g.spawn(func, 'hello', i=1)
            wrap_exceptions_spy.assert_called_once_with(func)
            wrap_exceptions_spy.reset_mock()
            func_g.join()

        wrap_exceptions_spy.assert_called_once_with(g.handle_exception)

    def test_spawn_task(self, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        parent = grizzly_fixture()

        def fail(_p: GrizzlyScenario) -> None:
            msg = 'foobar'
            raise RuntimeError(msg)

        def ok(_p: GrizzlyScenario) -> None:
            pass

        factory = GreenletFactory(logger=parent.logger)

        with caplog.at_level(logging.ERROR), pytest.raises(RuntimeError, match='foobar'), factory.spawn_task(parent, fail, 1, 10, 'Then fail'):
            pass

        assert caplog.messages == ['task 1 of 10 failed: Then fail']
        caplog.clear()

        with caplog.at_level(logging.DEBUG), factory.spawn_task(parent, ok, 3, 11, 'Then succeed'):
            pass

        assert [message for message in caplog.messages if not any(ignore in message for ignore in ['checking if heartbeat has been'])] == ['task 3 of 11 executed: Then succeed']
        caplog.clear()

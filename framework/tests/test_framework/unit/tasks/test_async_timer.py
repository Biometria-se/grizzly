"""Unit tests for grizzly.tasks.async_timer."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dateutil.parser import parse as dateparser
from grizzly.tasks import AsyncTimerTask
from grizzly.testdata.communication import TestdataConsumer

from test_framework.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestAsyncTimerTask:
    def test___init__(self) -> None:
        task_factory = AsyncTimerTask('timer-1', 'foobar', '1', 'start')
        assert task_factory == SOME(AsyncTimerTask, tname='timer-1', tid='foobar', version='1', action='start')

        task_factory = AsyncTimerTask('timer-2', 'foobar', '1', 'stop')
        assert task_factory == SOME(AsyncTimerTask, tname='timer-2', tid='foobar', version='1', action='stop')

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        assert parent is not None

        parent.__class__._consumer = TestdataConsumer(grizzly_fixture.behave.locust.runner, parent)
        toggle_mock = mocker.patch.object(parent.consumer.async_timers, 'toggle', return_value=None)

        datetime_mock = mocker.patch('grizzly.tasks.async_timer.datetime', side_effect=lambda *args, **kwargs: datetime(*args, **kwargs))  # noqa: DTZ001

        expected_datetime = dateparser('2024-12-03 10:02:00.123456+0100')
        datetime_mock.now.return_value = expected_datetime
        datetime_mock.strptime.side_effect = lambda *args, **kwargs: datetime.strptime(*args, **kwargs)  # noqa: DTZ007

        # <!-- start
        task_factory = AsyncTimerTask('timer-1', 'foobar', '1', 'start')
        task = task_factory()

        task(parent)

        toggle_mock.assert_called_once_with('start', 'timer-1', 'foobar', '1', expected_datetime)
        toggle_mock.reset_mock()
        # // -->

        # <!-- stop, default timestamp
        expected_datetime = dateparser('2024-12-03 10:09:00.123456+0100')
        datetime_mock.now.return_value = expected_datetime

        assert not any(var in parent.user.variables for var in ['PutDate', 'PutTime'])

        task_factory = AsyncTimerTask('timer-1', 'foobar', '1', 'stop')
        task = task_factory()

        task(parent)

        toggle_mock.assert_called_once_with('stop', 'timer-1', 'foobar', '1', expected_datetime)
        toggle_mock.reset_mock()
        # // -->

        # <!-- stop, timestamp from MQ message properties
        expected_datetime = dateparser('2024-12-03 10:12:00.123456Z').replace(tzinfo=timezone.utc)

        parent.user.variables.update(
            {
                'PutDate': '20241203',
                'PutTime': '101200123456',
            },
        )

        task_factory = AsyncTimerTask('timer-2', 'foobar', '1', 'stop')
        task = task_factory()

        task(parent)

        toggle_mock.assert_called_once_with('stop', 'timer-2', 'foobar', '1', expected_datetime)
        toggle_mock.reset_mock()
        # // -->

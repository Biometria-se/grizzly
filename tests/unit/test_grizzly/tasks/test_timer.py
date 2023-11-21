"""Unit tests of grizzly.tasks.timer."""
from __future__ import annotations

from hashlib import sha1
from typing import TYPE_CHECKING

from grizzly.tasks import LogMessageTask, TimerTask

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import GrizzlyFixture


class TestTimerTask:
    def test___init__(self, mocker: MockerFixture) -> None:
        sha1_patch = mocker.patch('grizzly.tasks.timer.sha1', return_value=mocker.MagicMock())
        sha1_patch.return_value.hexdigest.return_value = 'aaaabbbbccccdddd'
        task_factory = TimerTask(name='test-timer-1')

        assert task_factory.name == 'test-timer-1'
        assert task_factory.variable == 'aaaabbbb::test-timer-1'
        assert task_factory.__template_attributes__ == set()

    def test__call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        parent = grizzly_fixture()

        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')
        dummy_task = LogMessageTask(message='dummy')()

        # flat
        mocker.patch('grizzly.tasks.timer.perf_counter', side_effect=[2.0, 12.0])

        expected_variable_prefix = sha1(b'timer-test-timer-1').hexdigest()[:8]  # noqa: S324

        task_factory = TimerTask(name='test-timer-1')

        task = task_factory()
        parent.tasks = [dummy_task, task, dummy_task, task]
        parent.tasks += [dummy_task] * 7

        request_fire_spy.assert_not_called()
        assert parent.user._context['variables'] == {}

        parent._task_index = 1
        task(parent)

        request_fire_spy.assert_not_called()
        assert parent.user._context['variables'].get(f'{expected_variable_prefix}::test-timer-1', None) == {
            'start': 2.0,
            'task-index': 1,
        }

        parent._task_index = 9
        task(parent)

        assert parent.user._context['variables'] == {}

        request_fire_spy.assert_called_once_with(
            request_type='TIMR',
            name=f'{parent.user._scenario.identifier} test-timer-1',
            response_time=10000,
            response_length=9,
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()

        # nested
        mocker.patch('grizzly.tasks.timer.perf_counter', side_effect=[2.0, 3.0, 5.0, 12.0])

        expected_variable_prefix_1 = expected_variable_prefix
        expected_variable_prefix_2 = sha1(b'timer-test-timer-2').hexdigest()[:8]  # noqa: S324

        task_1 = task
        task_2 = TimerTask(name='test-timer-2')()

        parent.tasks = [task_1, dummy_task, task_2]
        parent.tasks += [dummy_task] * 7
        parent.tasks += [task_2, task_1]

        request_fire_spy.assert_not_called()
        assert parent.user._context['variables'] == {}

        parent._task_index = 1
        task_1(parent)

        request_fire_spy.assert_not_called()
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            },
        }

        parent._task_index = 3
        task_2(parent)

        request_fire_spy.assert_not_called()
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            },
            f'{expected_variable_prefix_2}::test-timer-2': {
                'start': 3.0,
                'task-index': 3,
            },
        }

        parent._task_index = 10
        task_2(parent)

        request_fire_spy.assert_called_once_with(
            request_type='TIMR',
            name=f'{parent.user._scenario.identifier} test-timer-2',
            response_time=2000,
            response_length=8,
            context=parent.user._context,
            exception=None,
        )
        request_fire_spy.reset_mock()
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            },
        }

        parent._task_index = 11
        task_1(parent)

        request_fire_spy.assert_called_once_with(
            request_type='TIMR',
            name=f'{parent.user._scenario.identifier} test-timer-1',
            response_time=10000,
            response_length=11,
            context=parent.user._context,
            exception=None,
        )
        assert parent.user._context['variables'] == {}

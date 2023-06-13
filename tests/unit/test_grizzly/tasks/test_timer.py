from hashlib import sha1

from pytest_mock import MockerFixture

from grizzly.tasks import TimerTask, LogMessageTask

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

        expected_variable_prefix = sha1('timer-test-timer-1'.encode('utf-8')).hexdigest()[:8]

        task_factory = TimerTask(name='test-timer-1')

        task = task_factory()
        parent.tasks = [dummy_task, task, dummy_task, task]
        parent.tasks += [dummy_task] * 7

        assert request_fire_spy.call_count == 0
        assert parent.user._context['variables'] == {}

        parent._task_index = 1
        task(parent)

        assert request_fire_spy.call_count == 0
        assert parent.user._context['variables'].get(f'{expected_variable_prefix}::test-timer-1', None) == {
            'start': 2.0,
            'task-index': 1,
        }

        parent._task_index = 9
        task(parent)

        assert parent.user._context['variables'] == {}

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-timer-1'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 9
        assert kwargs.get('context', None) == parent.user._context
        assert kwargs.get('exception', RuntimeError) is None

        request_fire_spy.reset_mock()

        # nested
        mocker.patch('grizzly.tasks.timer.perf_counter', side_effect=[2.0, 3.0, 5.0, 12.0])

        expected_variable_prefix_1 = expected_variable_prefix
        expected_variable_prefix_2 = sha1('timer-test-timer-2'.encode('utf-8')).hexdigest()[:8]

        task_1 = task
        task_2 = TimerTask(name='test-timer-2')()

        parent.tasks = [task_1, dummy_task, task_2]
        parent.tasks += [dummy_task] * 7
        parent.tasks += [task_2, task_1]

        assert request_fire_spy.call_count == 0
        assert parent.user._context['variables'] == {}

        parent._task_index = 1
        task_1(parent)

        assert request_fire_spy.call_count == 0
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            }
        }

        parent._task_index = 3
        task_2(parent)

        assert request_fire_spy.call_count == 0
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            },
            f'{expected_variable_prefix_2}::test-timer-2': {
                'start': 3.0,
                'task-index': 3,
            }
        }

        parent._task_index = 10
        task_2(parent)

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-timer-2'
        assert kwargs.get('response_time', None) == 2000
        assert kwargs.get('response_length', None) == 8
        assert kwargs.get('context', None) == parent.user._context
        assert kwargs.get('exception', RuntimeError) is None
        assert parent.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            }
        }

        parent._task_index = 11
        task_1(parent)

        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} test-timer-1'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 11
        assert kwargs.get('context', None) == parent.user._context
        assert kwargs.get('exception', RuntimeError) is None
        assert parent.user._context['variables'] == {}

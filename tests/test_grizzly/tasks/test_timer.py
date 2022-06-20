from hashlib import sha1

from pytest_mock import MockerFixture

from grizzly.tasks import TimerTask, LogMessageTask

from ...fixtures import GrizzlyFixture


class TestTimerTask:
    def test___init__(self) -> None:
        task_factory = TimerTask(name='test-timer-1')

        expected_variable_prefix = sha1('timer-test-timer-1'.encode('utf-8')).hexdigest()[:8]

        assert task_factory.name == 'test-timer-1'
        assert task_factory.variable == f'{expected_variable_prefix}::test-timer-1'

    def test__call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        scenario_context = grizzly_fixture.request_task.request.scenario
        request_fire_spy = mocker.spy(scenario.user.environment.events.request, 'fire')
        dummy_task = LogMessageTask(message='dummy')()

        # flat
        mocker.patch('grizzly.tasks.timer.perf_counter', side_effect=[2.0, 12.0])

        expected_variable_prefix = sha1('timer-test-timer-1'.encode('utf-8')).hexdigest()[:8]

        task_factory = TimerTask(name='test-timer-1', scenario=scenario_context)

        task = task_factory()
        scenario.tasks = [dummy_task, task, dummy_task, task]
        scenario.tasks += [dummy_task] * 7

        assert request_fire_spy.call_count == 0
        assert scenario.user._context['variables'] == {}

        scenario._task_index = 1
        task(scenario)

        assert request_fire_spy.call_count == 0
        assert scenario.user._context['variables'].get(f'{expected_variable_prefix}::test-timer-1', None) == {
            'start': 2.0,
            'task-index': 1,
        }

        scenario._task_index = 9
        task(scenario)

        assert scenario.user._context['variables'] == {}

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-timer-1'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 9
        assert kwargs.get('context', None) == scenario.user._context
        assert kwargs.get('exception', RuntimeError) is None

        request_fire_spy.reset_mock()

        # nested
        mocker.patch('grizzly.tasks.timer.perf_counter', side_effect=[2.0, 3.0, 5.0, 12.0])

        expected_variable_prefix_1 = expected_variable_prefix
        expected_variable_prefix_2 = sha1('timer-test-timer-2'.encode('utf-8')).hexdigest()[:8]

        task_1 = task
        task_2 = TimerTask(name='test-timer-2', scenario=scenario_context)()

        scenario.tasks = [task_1, dummy_task, task_2]
        scenario.tasks += [dummy_task] * 7
        scenario.tasks += [task_2, task_1]

        assert request_fire_spy.call_count == 0
        assert scenario.user._context['variables'] == {}

        scenario._task_index = 1
        task_1(scenario)

        assert request_fire_spy.call_count == 0
        assert scenario.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            }
        }

        scenario._task_index = 3
        task_2(scenario)

        assert request_fire_spy.call_count == 0
        assert scenario.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            },
            f'{expected_variable_prefix_2}::test-timer-2': {
                'start': 3.0,
                'task-index': 3,
            }
        }

        scenario._task_index = 10
        task_2(scenario)

        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-timer-2'
        assert kwargs.get('response_time', None) == 2000
        assert kwargs.get('response_length', None) == 8
        assert kwargs.get('context', None) == scenario.user._context
        assert kwargs.get('exception', RuntimeError) is None
        assert scenario.user._context['variables'] == {
            f'{expected_variable_prefix_1}::test-timer-1': {
                'start': 2.0,
                'task-index': 1,
            }
        }

        scenario._task_index = 11
        task_1(scenario)

        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'TIMR'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-timer-1'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 11
        assert kwargs.get('context', None) == scenario.user._context
        assert kwargs.get('exception', RuntimeError) is None
        assert scenario.user._context['variables'] == {}

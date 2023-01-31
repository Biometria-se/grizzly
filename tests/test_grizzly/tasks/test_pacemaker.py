import pytest

from pytest_mock import MockerFixture
from grizzly.tasks import PacemakerTask
from grizzly.exceptions import StopUser

from ...fixtures import GrizzlyFixture


class TestPacemakerTask:
    def test___init__(self, mocker: MockerFixture) -> None:
        sha1_patch = mocker.patch('grizzly.tasks.pacemaker.sha1', return_value=mocker.MagicMock())
        sha1_patch.return_value.hexdigest.return_value = 'aaaabbbbccccdddd'
        task_factory = PacemakerTask(name='test-pace-maker-1', value='1337.1337')

        assert task_factory.name == 'test-pace-maker-1'
        assert task_factory.variable == 'aaaabbbb::test-pace-maker-1'
        assert task_factory.value == '1337.1337'

    def test___call__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        sha1_patch = mocker.patch('grizzly.tasks.pacemaker.sha1', return_value=mocker.MagicMock())
        sha1_patch.return_value.hexdigest.return_value = 'aaaabbbbccccdddd'

        gsleep_spy = mocker.patch('grizzly.tasks.pacemaker.gsleep', return_value=None)

        _, _, scenario = grizzly_fixture()
        scenario_context = grizzly_fixture.request_task.request.scenario

        assert scenario is not None

        request_spy = mocker.spy(grizzly_fixture.locust_env.events.request, 'fire')

        # iteration time < specified value
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00, 13.37, 14.48])

        task_factory = PacemakerTask(name='test-pace-maker-1', value='20000', scenario=scenario_context)

        task = task_factory()

        task(scenario)

        assert scenario.user._context['variables'].get('aaaabbbb::test-pace-maker-1', None) == 10.00
        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-1'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('exception', RuntimeError) is None

        task(scenario)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[-1]
        assert len(args) == 1
        assert args[0] == pytest.approx(20000.0 - (13.37 - 10.00) * 1000, 0.1)
        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-1'
        assert kwargs.get('response_time', None) == 1110  # 14.48 - 13.37 * 1000
        assert kwargs.get('response_length', None) == 1
        assert kwargs.get('exception', RuntimeError) is None

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

        # iteration time > specified value
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00, 13.37, 14.48])

        task_factory = PacemakerTask(name='test-pace-maker-2', value='2000', scenario=scenario_context)

        task = task_factory()

        task(scenario)

        assert scenario.user._context['variables'].get('aaaabbbb::test-pace-maker-2', None) == 10.00
        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-2'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('exception', RuntimeError) is None

        task(scenario)

        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-2'
        assert kwargs.get('response_time', None) == 1110  # 14.48 - 13.37 * 1000
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == 'pace falling behind'

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

        # non-numeric value
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00])
        task_factory = PacemakerTask(name='test-pace-maker-3', value='asdf', scenario=scenario_context)

        task = task_factory()

        with pytest.raises(StopUser):
            task(scenario)

        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-3'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == 'asdf does not render to a number'

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

        # templating, no variable value
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00])
        task_factory = PacemakerTask(name='test-pace-maker-4', value='{{ foobar }}', scenario=scenario_context)

        task = task_factory()

        with pytest.raises(StopUser):
            task(scenario)

        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-4'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == '{{ foobar }} does not render to a number'

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

        # templating, variable value float
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00, 13.37, 14.48])

        scenario.user._context['variables']['foobar'] = 20000

        task_factory = PacemakerTask(name='test-pace-maker-5', value='{{ foobar }}', scenario=scenario_context)

        task = task_factory()

        task(scenario)

        assert scenario.user._context['variables'].get('aaaabbbb::test-pace-maker-5', None) == 10.00
        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-5'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('exception', RuntimeError) is None

        task(scenario)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[-1]
        assert len(args) == 1
        assert args[0] == pytest.approx(20000.0 - (13.37 - 10.00) * 1000, 0.1)
        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-5'
        assert kwargs.get('response_time', None) == 1110  # 14.48 - 13.37 * 1000
        assert kwargs.get('response_length', None) == 1
        assert kwargs.get('exception', RuntimeError) is None

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

        # templating, variable value str
        mocker.patch('grizzly.tasks.pacemaker.perf_counter', side_effect=[0, 10.00, 13.37, 14.48])

        scenario.user._context['variables']['foobar'] = '20000'

        task_factory = PacemakerTask(name='test-pace-maker-6', value='{{ foobar }}', scenario=scenario_context)

        task = task_factory()

        task(scenario)

        assert scenario.user._context['variables'].get('aaaabbbb::test-pace-maker-6', None) == 10.00
        assert gsleep_spy.call_count == 0
        assert request_spy.call_count == 1
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-6'
        assert kwargs.get('response_time', None) == 10000
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('exception', RuntimeError) is None

        task(scenario)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[-1]
        assert len(args) == 1
        assert args[0] == pytest.approx(20000.0 - (13.37 - 10.00) * 1000, 0.1)
        assert request_spy.call_count == 2
        _, kwargs = request_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'PACE'
        assert kwargs.get('name', None) == f'{scenario_context.identifier} test-pace-maker-6'
        assert kwargs.get('response_time', None) == 1110  # 14.48 - 13.37 * 1000
        assert kwargs.get('response_length', None) == 1
        assert kwargs.get('exception', RuntimeError) is None

        gsleep_spy.reset_mock()
        request_spy.reset_mock()

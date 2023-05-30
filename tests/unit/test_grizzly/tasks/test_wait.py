import pytest

from pytest_mock import MockerFixture

from grizzly.tasks import WaitTask
from grizzly.exceptions import StopUser

from tests.fixtures import GrizzlyFixture


class TestWaitTask:
    def test(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        task_factory = WaitTask(time_expression='1.0')

        assert task_factory.time_expression == '1.0'
        assert task_factory.__template_attributes__ == {'time_expression'}
        task = task_factory()

        assert callable(task)

        import grizzly.tasks.wait
        gsleep_spy = mocker.patch.object(grizzly.tasks.wait, 'gsleep', autospec=True)
        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task(parent)

        assert gsleep_spy.call_count == 1
        assert request_fire_spy.call_count == 0
        args, _ = gsleep_spy.call_args_list[-1]
        assert args[0] == 1.0

        task_factory.time_expression = '{{ wait_time }}'
        parent.user._context['variables']['wait_time'] = 126

        task(parent)

        assert gsleep_spy.call_count == 2
        assert request_fire_spy.call_count == 0
        args, _ = gsleep_spy.call_args_list[-1]
        assert args[0] == 126

        task_factory.time_expression = 'foobar'

        with pytest.raises(StopUser):
            task(parent)

        assert gsleep_spy.call_count == 2
        assert request_fire_spy.call_count == 1
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'WAIT'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} WaitTask=>foobar'
        assert kwargs.get('response_time', None) == 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == "could not convert string to float: 'foobar'"

        task_factory.time_expression = '{{ foobar }}'

        assert task_factory.get_templates() == ['{{ foobar }}']

        parent.user._context['variables']['foobar'] = 'foobar'

        with pytest.raises(StopUser):
            task(parent)

        assert gsleep_spy.call_count == 2
        assert request_fire_spy.call_count == 2
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'WAIT'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} WaitTask=>{{{{ foobar }}}}'
        assert kwargs.get('response_time', None) == 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, ValueError)
        assert str(exception) == "could not convert string to float: 'foobar'"

        task_factory.time_expression = '{{ undefined_variable }}'

        assert task_factory.get_templates() == ['{{ undefined_variable }}']

        with pytest.raises(StopUser):
            task(parent)

        assert gsleep_spy.call_count == 2
        assert request_fire_spy.call_count == 3
        _, kwargs = request_fire_spy.call_args_list[-1]
        assert kwargs.get('request_type', None) == 'WAIT'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} WaitTask=>{{{{ undefined_variable }}}}'
        assert kwargs.get('response_time', None) == 0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) is parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"{{ undefined_variable }}" rendered into "" which is not valid'

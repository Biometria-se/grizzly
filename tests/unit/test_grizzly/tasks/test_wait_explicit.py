"""Unit tests of grizzly.tasks.wait_explicit."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from grizzly.exceptions import StopUser
from grizzly.tasks import ExplicitWaitTask
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from tests.fixtures import GrizzlyFixture


class TestExplicitWaitTask:
    def test_task(self, mocker: MockerFixture, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        task_factory = ExplicitWaitTask(time_expression='1.0')

        assert task_factory.time_expression == '1.0'
        assert task_factory.__template_attributes__ == {'time_expression'}
        task = task_factory()

        assert callable(task)

        import grizzly.tasks.wait_explicit
        gsleep_spy = mocker.patch.object(grizzly.tasks.wait_explicit, 'gsleep', autospec=True)
        request_fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        task(parent)

        gsleep_spy.assert_called_once_with(1.0)
        gsleep_spy.reset_mock()
        request_fire_spy.assert_not_called()

        task_factory.time_expression = '{{ wait_time }}'
        parent.user._context['variables']['wait_time'] = 126

        task(parent)

        gsleep_spy.assert_called_once_with(126)
        gsleep_spy.reset_mock()
        request_fire_spy.assert_not_called()

        task_factory.time_expression = 'foobar'

        with pytest.raises(StopUser):
            task(parent)

        gsleep_spy.assert_not_called()
        request_fire_spy.assert_called_once_with(
            request_type='WAIT',
            name=f'{parent.user._scenario.identifier} WaitTask=>foobar',
            response_time=0,
            response_length=0,
            context=parent.user._context,
            exception=ANY(ValueError, message="could not convert string to float: 'foobar'"),
        )
        request_fire_spy.reset_mock()

        task_factory.time_expression = '{{ foobar }}'

        assert task_factory.get_templates() == ['{{ foobar }}']

        parent.user._context['variables']['foobar'] = 'foobar'

        with pytest.raises(StopUser):
            task(parent)

        gsleep_spy.assert_not_called()
        request_fire_spy.assert_called_once_with(
            request_type='WAIT',
            name=f'{parent.user._scenario.identifier} WaitTask=>{{{{ foobar }}}}',
            response_time=0,
            response_length=0,
            context=parent.user._context,
            exception=ANY(ValueError, message="could not convert string to float: 'foobar'"),
        )
        request_fire_spy.reset_mock()

        task_factory.time_expression = '{{ undefined_variable }}'

        assert task_factory.get_templates() == ['{{ undefined_variable }}']

        with pytest.raises(StopUser):
            task(parent)

        gsleep_spy.assert_not_called()
        request_fire_spy.assert_called_once_with(
            request_type='WAIT',
            name=f'{parent.user._scenario.identifier} WaitTask=>{{{{ undefined_variable }}}}',
            response_time=0,
            response_length=0,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='"{{ undefined_variable }}" rendered into "" which is not valid'),
        )

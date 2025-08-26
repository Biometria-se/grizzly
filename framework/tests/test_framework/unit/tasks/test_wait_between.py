"""Unit tests of grizzly.tasks.wait_between."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from grizzly.tasks import WaitBetweenTask
from locust.exception import MissingWaitTimeError

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture


class TestWaitBetweenTask:
    def test___init__(self) -> None:
        task_factory = WaitBetweenTask('1.0')

        assert task_factory.min_time == '1.0'
        assert task_factory.max_time is None
        assert task_factory.__template_attributes__ == {'min_time', 'max_time'}

        task_factory = WaitBetweenTask('2.0', '13.0')
        assert task_factory.min_time == '2.0'
        assert task_factory.max_time == '13.0'

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        # force the scenario user to not have a wait_time method
        parent.user.wait_time = None

        with pytest.raises(MissingWaitTimeError):
            parent.wait_time()

        task = WaitBetweenTask('1.0', '12.0')()
        task(parent)

        wait_time = parent.wait_time()
        assert wait_time >= 1.0
        assert wait_time <= 12.0

        task = WaitBetweenTask('13.0')()
        task(parent)

        assert parent.wait_time() == 13.0

        parent.user.set_variable('wait_time', 14.0)

        task = WaitBetweenTask('{{ wait_time }}')()
        task(parent)

        assert parent.wait_time() == 14.0

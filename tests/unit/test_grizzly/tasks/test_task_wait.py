import pytest

from locust.exception import MissingWaitTimeError
from grizzly.tasks import TaskWaitTask

from tests.fixtures import GrizzlyFixture


class TestTaskWaitTask:
    def test___init__(self) -> None:
        task_factory = TaskWaitTask(1.0)

        assert task_factory.min_time == 1.0
        assert task_factory.max_time is None
        assert task_factory.__template_attributes__ == set()

        task_factory = TaskWaitTask(2.0, 13.0)
        assert task_factory.min_time == 2.0
        assert task_factory.max_time == 13.0

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:
        parent = grizzly_fixture()

        # force the scenario user to not have a wait_time method
        parent.user.wait_time = None

        with pytest.raises(MissingWaitTimeError):
            parent.wait_time()

        task = TaskWaitTask(1.0, 12.0)()

        task(parent)

        wait_time = parent.wait_time()
        assert wait_time >= 1.0 and wait_time <= 12.0

        task = TaskWaitTask(13.0)()

        task(parent)

        assert parent.wait_time() == 13.0

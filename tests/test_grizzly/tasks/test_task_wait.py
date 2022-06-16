import pytest

from locust.exception import MissingWaitTimeError
from grizzly.tasks import TaskWaitTask

from ...fixtures import GrizzlyFixture


class TestTaskWaitTask:
    def test___init__(self) -> None:
        task_factory = TaskWaitTask(1.0)

        assert task_factory.min_time == 1.0
        assert task_factory.max_time is None

        task_factory = TaskWaitTask(2.0, 13.0)
        assert task_factory.min_time == 2.0
        assert task_factory.max_time == 13.0

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:
        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        # force the scenario user to not have a wait_time method
        scenario.user.wait_time = None

        with pytest.raises(MissingWaitTimeError):
            scenario.wait_time()

        task = TaskWaitTask(1.0, 12.0)()

        task(scenario)

        wait_time = scenario.wait_time()
        assert wait_time >= 1.0 and wait_time <= 12.0

        task = TaskWaitTask(13.0)()

        task(scenario)

        assert scenario.wait_time() == 13.0

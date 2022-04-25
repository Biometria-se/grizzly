import pytest

from grizzly.tasks import RequestTask, ParallellRequestTask
from grizzly.types import RequestMethod
from grizzly.users import RestApiUser
from grizzly.scenarios import IteratorScenario

from ...fixtures import GrizzlyFixture


class TestParallellTask:
    def test__init__(self) -> None:
        task = ParallellRequestTask(name='test')

        assert isinstance(task.requests, list)
        assert len(task.requests) == 0
        assert task.name == 'test'

    def test___call__(self, grizzly_fixture: GrizzlyFixture) -> None:
        _, user, scenario = grizzly_fixture(host='http://host.docker.internal', user_type=RestApiUser, scenario_type=IteratorScenario)

        assert scenario is not None
        assert user is not None

        task_factory = ParallellRequestTask(name='test')

        task_factory.add(RequestTask(RequestMethod.GET, name='test-3', endpoint='/api/sleep/3'))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-5', endpoint='/api/sleep/5'))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-1', endpoint='/api/sleep/1'))

        task = task_factory()

        task(scenario)

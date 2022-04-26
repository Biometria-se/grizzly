import pytest

from pytest_mock import MockerFixture

from grizzly.tasks import RequestTask, ParallellRequestTask
from grizzly.types import RequestMethod
from grizzly.users import RestApiUser
from grizzly.scenarios import IteratorScenario
from grizzly.context import GrizzlyContextScenario

from ...fixtures import GrizzlyFixture


class TestParallellTask:
    def test__init__(self) -> None:
        task = ParallellRequestTask(name='test')

        assert isinstance(task.requests, list)
        assert len(task.requests) == 0
        assert task.name == 'test'

    @pytest.mark.skip(reason='needs a webservice that sleeps')
    def test___call___real(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        _, user, scenario = grizzly_fixture(host='http://host.docker.internal:8002', user_type=RestApiUser, scenario_type=IteratorScenario)

        assert scenario is not None
        assert user is not None

        request_spy = mocker.spy(user.environment.events.request, 'fire')

        assert user.host == 'http://host.docker.internal:8002'

        context_scenario = GrizzlyContextScenario()
        context_scenario.name = context_scenario.description = 'test scenario'

        task_factory = ParallellRequestTask(name='test', scenario=context_scenario)

        task_factory.add(RequestTask(RequestMethod.GET, name='test-2', endpoint='/api/sleep/2', scenario=context_scenario))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-6', endpoint='/api/sleep/6', scenario=context_scenario))
        task_factory.add(RequestTask(RequestMethod.GET, name='test-1', endpoint='/api/sleep/1', scenario=context_scenario))

        assert len(task_factory.requests) == 3

        task = task_factory()

        task(scenario)

        assert request_spy.call_count == 4

from grizzly.users.base import GrizzlyUser
from grizzly.users import DummyUser
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod

from tests.fixtures import GrizzlyFixture


class TestDummyUser:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly_fixture()
        environment = grizzly_fixture.behave.locust.environment
        DummyUser.__scenario__ = grizzly_fixture.grizzly.scenario
        DummyUser.host = ''
        assert isinstance(DummyUser(environment), GrizzlyUser)

    def test_request(self, grizzly_fixture: GrizzlyFixture) -> None:
        DummyUser.__scenario__ = grizzly_fixture.grizzly.scenario
        DummyUser.host = '/dev/null'
        user = DummyUser(grizzly_fixture.behave.locust.environment)

        for method in RequestMethod:
            assert user.request(RequestTask(method, 'dummy', '/api/what/ever')) == (None, None,)

        assert user._scenario is not DummyUser.__scenario__

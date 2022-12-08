from grizzly.users.base import GrizzlyUser
from grizzly.users import DummyUser
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod

from ...fixtures import GrizzlyFixture


class TestDummyUser:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly_fixture()
        environment = grizzly_fixture.locust_env
        DummyUser.host = ''
        assert isinstance(DummyUser(environment), GrizzlyUser)

    def test_request(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly_fixture()
        DummyUser.host = '/dev/null'
        user = DummyUser(grizzly_fixture.locust_env)

        for method in RequestMethod:
            assert user.request(RequestTask(method, 'dummy', '/api/what/ever')) == (None, None,)

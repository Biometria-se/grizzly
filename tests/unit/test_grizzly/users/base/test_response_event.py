import pytest

from typing import Optional, Union, Tuple, Dict, Any

from locust.clients import ResponseContextManager
from locust.user.users import User
from requests.models import Response

from grizzly.clients import ResponseEventSession
from grizzly.users.base import ResponseEvent, HttpRequests
from grizzly.types import RequestMethod
from grizzly.types.locust import LocustError
from grizzly.tasks import RequestTask

from tests.fixtures import LocustFixture
from tests.helpers import TestUser


class TestResponseEvent:
    def test_create(self, locust_fixture: LocustFixture) -> None:
        assert ResponseEvent.host is None

        with pytest.raises(LocustError):
            ResponseEvent(locust_fixture.environment)

        ResponseEvent.host = 'http://example.org'

        user = ResponseEvent(locust_fixture.environment)
        assert getattr(user, 'client', '') is None
        assert len(user.response_event._handlers) == 0

        fake_user_type = type('FakeResponseEventUser', (ResponseEvent, HttpRequests,), {
            'host': 'https://example.org'
        })

        user = fake_user_type(locust_fixture.environment)
        assert isinstance(user.client, ResponseEventSession)
        assert len(user.response_event._handlers) == 0

    def test_add_listener(self, locust_fixture: LocustFixture) -> None:
        class Called(Exception):
            pass

        ResponseEvent.host = TestUser.host = 'http://example.com'
        user = ResponseEvent(locust_fixture.environment)

        def handler(name: str, request: Optional[RequestTask], context: Union[ResponseContextManager, Tuple[Dict[str, Any], str]], user: User) -> None:
            raise Called()

        assert len(user.response_event._handlers) == 0

        user.response_event.add_listener(handler)

        assert len(user.response_event._handlers) == 1

        with pytest.raises(Called):
            user.response_event._handlers[0](
                '',
                RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test'),
                ResponseContextManager(Response(), None, None),
                TestUser(environment=locust_fixture.environment),
            )

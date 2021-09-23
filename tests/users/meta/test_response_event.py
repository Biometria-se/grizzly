import pytest

from typing import Optional, Union, Tuple, Dict, Any

from locust.env import Environment
from locust.exception import LocustError
from locust.clients import ResponseContextManager
from locust.user.users import User
from requests.models import Response

from grizzly.clients import ResponseEventSession
from grizzly.users.meta import ResponseEvent, HttpRequests
from grizzly.types import RequestMethod
from grizzly.context import RequestContext

from ...fixtures import locust_environment
from ...helpers import TestUser


class TestResponseEvent:
    @pytest.mark.usefixtures('locust_environment')
    def test_create(self, locust_environment: Environment) -> None:
        assert ResponseEvent.host is None

        with pytest.raises(LocustError):
            ResponseEvent(locust_environment)

        ResponseEvent.host = 'http://example.org'

        user = ResponseEvent(locust_environment)
        assert user.client == None
        assert len(user.response_event._handlers) == 0

        fake_user_type = type('FakeResponseEventUser', (ResponseEvent, HttpRequests,), {
            'host': 'https://example.org'
        })

        user = fake_user_type(locust_environment)
        assert isinstance(user.client, ResponseEventSession)
        assert len(user.response_event._handlers) == 0

    @pytest.mark.usefixtures('locust_environment')
    def test_add_listener(self, locust_environment: Environment) -> None:
        class Called(Exception):
            pass

        ResponseEvent.host = 'http://example.com'
        user = ResponseEvent(locust_environment)

        def handler(name: str, request: Optional[RequestContext], context: Union[ResponseContextManager, Tuple[Dict[str, Any], str]], user: User) -> None:
            raise Called()

        assert len(user.response_event._handlers) == 0

        user.response_event.add_listener(handler)

        assert len(user.response_event._handlers) == 1

        with pytest.raises(Called):
            user.response_event._handlers[0](
                '',
                RequestContext(RequestMethod.POST, name='test-request', endpoint='/api/test'),
                ResponseContextManager(Response(), None, None),
                TestUser(environment=locust_environment),
            )

"""Unit tests for grizzly.users.base.response_event."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple, Union

import pytest
from locust.clients import ResponseContextManager
from requests.models import Response

from grizzly.clients import ResponseEventSession
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.types.locust import LocustError
from grizzly.users.base import HttpRequests, ResponseEvent
from tests.helpers import TestUser

if TYPE_CHECKING:  # pragma: no cover
    from locust.user.users import User

    from tests.fixtures import LocustFixture, GrizzlyFixture


class TestResponseEvent:
    def test_create(self, locust_fixture: LocustFixture) -> None:
        test_cls = type('ResponseEventTest', (ResponseEvent, ), {'host': None})
        assert issubclass(test_cls, ResponseEvent)
        assert test_cls.host is None

        with pytest.raises(LocustError, match='You must specify the base host'):
            test_cls(locust_fixture.environment)

        test_cls.host = 'http://example.org'

        user = test_cls(locust_fixture.environment)
        assert getattr(user, 'client', '') is None
        assert len(user.response_event._handlers) == 0

        fake_user_type = type('FakeResponseEventUser', (ResponseEvent, HttpRequests,), {
            'host': 'https://example.org'
        })

        user = fake_user_type(locust_fixture.environment)
        assert isinstance(user.client, ResponseEventSession)
        assert len(user.response_event._handlers) == 0

    def test_add_listener(self, grizzly_fixture: GrizzlyFixture) -> None:
        class Called(Exception):  # noqa: N818
            pass

        test_cls = type('ResponseEventTest', (ResponseEvent, ), {'host': None})
        assert issubclass(test_cls, ResponseEvent)

        environment = grizzly_fixture.grizzly.state.locust.environment

        test_cls.host = TestUser.host = 'http://example.com'
        TestUser.__scenario__ = grizzly_fixture.grizzly.scenario
        user = test_cls(environment)

        def handler(name: str, request: Optional[RequestTask], context: Union[ResponseContextManager, Tuple[Dict[str, Any], str]], user: User) -> None:  # noqa: ARG001
            raise Called

        assert len(user.response_event._handlers) == 0

        user.response_event.add_listener(handler)

        assert len(user.response_event._handlers) == 1

        with pytest.raises(Called):
            user.response_event._handlers[0](
                '',
                RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test'),
                ResponseContextManager(Response(), None, None),
                TestUser(environment=environment),
            )

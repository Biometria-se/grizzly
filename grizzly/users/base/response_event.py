"""Load users that maps response_even to the correct event so that response handlers are triggered."""
from __future__ import annotations

from typing import Any

from locust.event import EventHook
from locust.user.users import User

from grizzly.clients import ResponseEventSession
from grizzly.types.locust import Environment, LocustError

from . import HttpRequests


class ResponseEvent(User):
    abstract = True

    client: ResponseEventSession
    response_event: EventHook

    def __init__(self, environment: Environment, *args: Any, **kwargs: Any) -> None:
        super().__init__(environment, *args, **kwargs)

        if self.host is None:
            message = (
                'You must specify the base host. Either in the host attribute in the User class, '
                'or on the command line using the --host option.'
            )
            raise LocustError(message)

        if issubclass(self.__class__, (HttpRequests, )):
            session = ResponseEventSession(
                base_url=self.host,
                request_event=self.environment.events.request,
                user=self,
            )
            session.trust_env = False
            self.client = session
            self.response_event = self.client.event_hook
        else:
            self.response_event = EventHook()
            setattr(self, 'client', None)  # noqa: B010

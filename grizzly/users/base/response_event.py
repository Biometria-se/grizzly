from typing import Any, Dict, Tuple
from locust.event import EventHook

from locust.exception import LocustError
from locust.user.users import User
from locust.env import Environment

from ...clients import ResponseEventSession
from . import HttpRequests


class ResponseEvent(User):
    abstract = True

    client: ResponseEventSession
    response_event: EventHook

    def __init__(self, environment: Environment, *args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        super().__init__(environment, *args, **kwargs)

        if self.host is None:
            raise LocustError(
                'You must specify the base host. Either in the host attribute in the User class, '
                'or on the command line using the --host option.'
            )

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
            setattr(self, 'client', None)

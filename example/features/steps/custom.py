# noqa: D100, INP001
from __future__ import annotations

import logging
from json import loads as jsonloads
from typing import TYPE_CHECKING, Any, Dict

from grizzly.tasks import GrizzlyTask, RequestTask, grizzlytask
from grizzly.testdata.variables import AtomicVariable
from grizzly.types.locust import Environment, LocalRunner, Message, WorkerRunner
from grizzly.users import RestApiUser

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.scenarios import GrizzlyScenario
    from grizzly.types import GrizzlyResponse


class User(RestApiUser):
    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        self.logger.info('executing custom.User.request for %s and %s', request.name, request.endpoint)

        return super().request_impl(request)


class Task(GrizzlyTask):
    data: Dict[str, str]
    logger = logging.getLogger(__name__)

    def __init__(self, data: str) -> None:
        self.data = jsonloads(data.replace("'", '"'))

    def __call__(self) -> grizzlytask:
        @grizzlytask
        def implementation(parent: GrizzlyScenario) -> Any:
            if isinstance(parent.grizzly.state.locust, (LocalRunner,)) and self.data.get('server', None) is not None:
                self.logger.info('sending "server_client" from SERVER')
                parent.grizzly.state.locust.send_message('server_client', self.data)

            if isinstance(parent.grizzly.state.locust, (WorkerRunner, LocalRunner)) and self.data.get('client', None) is not None:
                self.logger.info('sending "client_server" from CLIENT')
                parent.grizzly.state.locust.send_message('client_server', self.data)

        @implementation.on_start
        def on_start(_parent: GrizzlyScenario) -> None:
            self.logger.info('%s on_start called before test', self.__class__.__name__)

        @implementation.on_stop
        def on_stop(_parent: GrizzlyScenario) -> None:
            self.logger.info('%s on_stop called after test', self.__class__.__name__)

        return implementation


def callback_server_client(environment: Environment, msg: Message, **_kwargs: Any) -> None:  # noqa: ARG001
    import logging
    logging.info('received from SERVER: msg.node_id=%r, msg.data=%r', msg.node_id, msg.data)


def callback_client_server(environment: Environment, msg: Message) -> None:  # noqa: ARG001
    import logging
    logging.info('received from CLIENT: msg.node_id=%r, msg.data=%r', msg.node_id, msg.data)


class AtomicCustomVariable(AtomicVariable[str]):
    pass

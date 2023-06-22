import logging

from json import loads as jsonloads
from typing import Any, Dict

from grizzly.scenarios import GrizzlyScenario
from grizzly.users import RestApiUser
from grizzly.tasks import RequestTask, GrizzlyTask, grizzlytask
from grizzly.types import GrizzlyResponse
from grizzly.types.locust import Message, Environment, WorkerRunner, LocalRunner
from grizzly.testdata.variables import AtomicVariable


class User(RestApiUser):
    def request_impl(self, request: RequestTask) -> GrizzlyResponse:
        self.logger.info(f'executing custom.User.request for {request.name} and {request.endpoint}')

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

            if isinstance(parent.grizzly.state.locust, (WorkerRunner, LocalRunner,)) and self.data.get('client', None) is not None:
                self.logger.info('sending "client_server" from CLIENT')
                parent.grizzly.state.locust.send_message('client_server', self.data)

        @implementation.on_start
        def on_start(parent: GrizzlyScenario) -> None:
            self.logger.info(f'{self.__class__.__name__} on_start called before test')

        @implementation.on_stop
        def on_stop(parent: GrizzlyScenario) -> None:
            self.logger.info(f'{self.__class__.__name__} on_stop called after test')

        return implementation


def callback_server_client(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
    import logging
    logging.info(f'received from SERVER: {msg.node_id=}, {msg.data=}')


def callback_client_server(environment: Environment, msg: Message) -> None:
    import logging
    logging.info(f'received from CLIENT: {msg.node_id=}, {msg.data=}')


class AtomicCustomVariable(AtomicVariable[str]):
    pass

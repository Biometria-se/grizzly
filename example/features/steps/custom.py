from json import loads as jsonloads
from typing import Any, Callable, Dict

from grizzly.scenarios import GrizzlyScenario
from grizzly.users import RestApiUser
from grizzly.tasks import RequestTask, GrizzlyTask
from grizzly.types import GrizzlyResponse, Message, Environment, WorkerRunner, LocalRunner
from grizzly.testdata.variables import AtomicVariable


class User(RestApiUser):
    def request(self, request: RequestTask) -> GrizzlyResponse:
        self.logger.info(f'executing custom.User.request for {request.name} and {request.endpoint}')

        return super().request(request)


class Task(GrizzlyTask):
    data: Dict[str, str]

    def __init__(self, data: str) -> None:
        self.data = jsonloads(data.replace("'", '"'))

    def __call__(self) -> Callable[[GrizzlyScenario], Any]:
        def implementation(parent: GrizzlyScenario) -> Any:
            if isinstance(parent.grizzly.state.locust, (LocalRunner,)) and self.data.get('server', None) is not None:
                parent.logger.info('sending "server_client" from SERVER')
                parent.grizzly.state.locust.send_message('server_client', self.data)

            if isinstance(parent.grizzly.state.locust, (WorkerRunner, LocalRunner,)) and self.data.get('client', None) is not None:
                parent.logger.info('sending "client_server" from CLIENT')
                parent.grizzly.state.locust.send_message('client_server', self.data)

        return implementation


def callback_server_client(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
    import logging
    logging.info(f'received from SERVER: {msg.node_id=}, {msg.data=}')


def callback_client_server(environment: Environment, msg: Message) -> None:
    import logging
    logging.info(f'received from CLIENT: {msg.node_id=}, {msg.data=}')


class AtomicCustomVariable(AtomicVariable[str]):
    pass

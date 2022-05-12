from typing import Any, Callable, Dict

from grizzly.scenarios import GrizzlyScenario
from grizzly.users import RestApiUser
from grizzly.tasks import RequestTask, GrizzlyTask
from grizzly.types import GrizzlyResponse, Message, Environment, MasterRunner, LocalRunner


class User(RestApiUser):
    def request(self, request: RequestTask) -> GrizzlyResponse:
        self.logger.info(f'executing custom.User.request for {request.name} and {request.endpoint}')

        return super().request(request)


class Task(GrizzlyTask):
    def __call__(self) -> Callable[[GrizzlyScenario], Any]:
        def implementation(parent: GrizzlyScenario) -> Any:
            if isinstance(parent.grizzly.state.locust, (MasterRunner, LocalRunner,)):
                parent.logger.info('sending "example_message" from SERVER')
                parent.grizzly.state.locust.send_message('example_message', {'hello': 'world'})

        return implementation


def callback_example_message(environment: Environment, msg: Message, **kwargs: Dict[str, Any]) -> None:
    import logging
    logging.info(f'received from SERVER: {msg.node_id=}, {msg.data=}')

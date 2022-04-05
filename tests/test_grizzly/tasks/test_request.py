from typing import Any, Tuple, Optional

from pytest_mock import MockerFixture
from locust.clients import ResponseContextManager

from grizzly.users.base.grizzly_user import GrizzlyUser
from grizzly.users.base.response_handler import ResponseHandlerAction

from grizzly_extras.transformer import TransformerContentType
from grizzly.tasks import (
    RequestTask,
    RequestTaskHandlers,
    RequestTaskResponse,
)
from grizzly.types import RequestMethod

from ..fixtures import GrizzlyFixture


class TestRequestTaskHandlers:
    def test(self) -> None:
        handlers = RequestTaskHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        class TestResponseHandlerAction(ResponseHandlerAction):
            def __call__(self, input_context: Tuple[TransformerContentType, Any], user: GrizzlyUser, response: Optional[ResponseContextManager] = None) -> None:
                super().__call__(input_context, user, response)

        handler = TestResponseHandlerAction(expression='', match_with='')

        handlers.add_metadata(handler)
        handlers.add_payload(handler)

        assert len(handlers.metadata) == 1
        assert len(handlers.payload) == 1


class TestRequestTaskResponse:
    def test(self) -> None:
        response_task = RequestTaskResponse()
        assert response_task.content_type == TransformerContentType.UNDEFINED

        assert isinstance(response_task.handlers, RequestTaskHandlers)

        assert 200 in response_task.status_codes

        response_task.add_status_code(-200)
        assert 200 not in response_task.status_codes

        response_task.add_status_code(200)
        response_task.add_status_code(302)
        assert [200, 302] == response_task.status_codes

        response_task.add_status_code(200)
        assert [200, 302] == response_task.status_codes

        response_task.add_status_code(-302)
        response_task.add_status_code(400)
        assert [200, 400] == response_task.status_codes


class TestRequestTask:
    def test(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        task_factory = RequestTask(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert task_factory.method == RequestMethod.POST
        assert task_factory.name == 'test-name'
        assert task_factory.endpoint == '/api/test'

        assert not hasattr(task_factory, 'scenario')

        assert task_factory.template is None
        assert task_factory.source is None

        task = task_factory()
        assert callable(task)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        mocker.patch.object(scenario.user, 'request', autospec=True)
        request_spy = mocker.spy(scenario.user, 'request')

        task(scenario)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[0]
        assert args[0] is task_factory

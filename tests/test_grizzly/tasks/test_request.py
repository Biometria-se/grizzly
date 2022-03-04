from typing import Any, Tuple, Optional, Dict, Callable

import pytest

from pytest_mock import mocker, MockerFixture  # pylint: disable=unused-import
from locust.clients import ResponseContextManager
from locust.user.users import User

from grizzly_extras.transformer import TransformerContentType
from grizzly.tasks import (
    RequestTask,
    RequestTaskHandlers,
    RequestTaskResponse,
)
from grizzly.types import RequestMethod

from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestRequestTaskHandlers:
    def test(self) -> None:
        handlers = RequestTaskHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        def handler(input: Tuple[TransformerContentType, Any], user: User, manager: Optional[ResponseContextManager]) -> None:
            pass

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
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        task = RequestTask(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert task.method == RequestMethod.POST
        assert task.name == 'test-name'
        assert task.endpoint == '/api/test'

        assert not hasattr(task, 'scenario')

        assert task.template is None
        assert task.source is None

        implementation = task.implementation()
        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        mocker.patch.object(tasks.user, 'request', noop)
        request_spy = mocker.spy(tasks.user, 'request')

        implementation(tasks)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[0]
        assert args[0] is task

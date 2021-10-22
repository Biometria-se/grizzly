from typing import Any, Tuple, Optional

from locust.clients import ResponseContextManager
from locust.user.users import User

from grizzly.task import (
    RequestTask,
    RequestTaskHandlers,
    RequestTaskResponse,
)

from grizzly.types import ResponseContentType, RequestMethod

class TestRequestTaskHandlers:
    def tests(self) -> None:
        handlers = RequestTaskHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        def handler(input: Tuple[ResponseContentType, Any], user: User, manager: Optional[ResponseContextManager]) -> None:
            pass

        handlers.add_metadata(handler)
        handlers.add_payload(handler)

        assert len(handlers.metadata) == 1
        assert len(handlers.payload) == 1


class TestRequestTaskResponse:
    def test(self) -> None:
        response_task = RequestTaskResponse()
        assert response_task.content_type == ResponseContentType.GUESS

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
    def test(self) -> None:
        request_task = RequestTask(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert request_task.method == RequestMethod.POST
        assert request_task.name == 'test-name'
        assert request_task.endpoint == '/api/test'

        assert not hasattr(request_task, 'scenario')

        assert request_task.template is None
        assert request_task.source is None
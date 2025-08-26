"""Unit tests of grizzly.tasks.request."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from grizzly.events.response_handler import ResponseHandlerAction
from grizzly.tasks import (
    RequestTask,
    RequestTaskHandlers,
    RequestTaskResponse,
)
from grizzly.types import RequestMethod
from grizzly_common.transformer import TransformerContentType

if TYPE_CHECKING:  # pragma: no cover
    from grizzly.types import HandlerContextType
    from grizzly.users import GrizzlyUser
    from pytest_mock import MockerFixture

    from test_framework.fixtures import GrizzlyFixture


class TestRequestTaskHandlers:
    def test(self) -> None:
        handlers = RequestTaskHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        class TestResponseHandlerAction(ResponseHandlerAction):
            def __call__(self, input_context: tuple[TransformerContentType, HandlerContextType], user: GrizzlyUser) -> None:
                super().__call__(input_context, user)

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
        assert response_task.status_codes == [200, 302]

        response_task.add_status_code(200)
        assert response_task.status_codes == [200, 302]

        response_task.add_status_code(-302)
        response_task.add_status_code(400)
        assert response_task.status_codes == [200, 400]


class TestRequestTask:
    def test(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        task_factory = RequestTask(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert task_factory.method == RequestMethod.POST
        assert task_factory.name == 'test-name'
        assert task_factory.endpoint == '/api/test'
        assert task_factory.response.content_type == TransformerContentType.UNDEFINED
        assert task_factory.__template_attributes__ == {'name', 'endpoint', 'source', 'arguments', 'metadata'}

        assert not hasattr(task_factory, 'scenario')

        assert task_factory.source is None

        task = task_factory()
        assert callable(task)

        parent = grizzly_fixture()

        mocker.patch.object(parent.user, 'request')
        request_spy = mocker.spy(parent.user, 'request')

        task(parent)

        request_spy.assert_called_once_with(task_factory)

        # automagically create template if not set
        task_factory.source = 'hello {{ world }}'

    def test_arguments(self) -> None:
        task_factory = RequestTask(RequestMethod.GET, 'test-name', endpoint='/api/test | content_type="application/json", foo=bar')
        assert task_factory.endpoint == '/api/test'
        assert task_factory.response.content_type == TransformerContentType.JSON
        assert task_factory.arguments is not None
        assert 'foo' in task_factory.arguments
        assert task_factory.arguments['foo'] == 'bar'

        task_factory = RequestTask(RequestMethod.GET, 'test-name', endpoint='/api/test | content_type="application/xml"')
        assert task_factory.endpoint == '/api/test'
        assert task_factory.arguments == {}
        assert task_factory.response.content_type == TransformerContentType.XML

        # test missing required arguments for multipart/form-data
        with pytest.raises(AssertionError, match=r'Content type multipart\/form-data requires endpoint arguments multipart_form_data_name and multipart_form_data_filename'):
            RequestTask(RequestMethod.POST, 'test-name', endpoint='/api/test | content_type="multipart/form-data"')

        # test required arguments for multipart/form-data
        task_factory = RequestTask(
            RequestMethod.POST,
            'test-name',
            endpoint='/api/test | content_type="multipart/form-data", multipart_form_data_filename="foo", multipart_form_data_name="bar"',
        )
        assert task_factory.endpoint == '/api/test'
        assert task_factory.arguments == {'multipart_form_data_filename': 'foo', 'multipart_form_data_name': 'bar'}
        assert task_factory.response.content_type == TransformerContentType.MULTIPART_FORM_DATA

    def test_add_metadata(self) -> None:
        task_factory = RequestTask(RequestMethod.GET, 'test-name', endpoint='/api/test | content_type="application/json", foo=bar')
        assert getattr(task_factory, 'metadata', None) == {}

        task_factory.add_metadata('foo', 'bar')
        task_factory.add_metadata('alice', 'bob')

        assert task_factory.metadata is not None
        assert task_factory.metadata['foo'] == 'bar'
        assert task_factory.metadata['alice'] == 'bob'

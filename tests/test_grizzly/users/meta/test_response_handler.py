from json import dumps as jsondumps

import pytest

from lxml import etree as XML
from requests.models import Response
from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from locust.env import Environment
from locust.event import EventHook
from locust.clients import ResponseContextManager
from locust.exception import LocustError, CatchResponseError

from grizzly.clients import ResponseEventSession
from grizzly.users.meta import HttpRequests, ResponseEvent, ResponseHandler
from grizzly.exceptions import ResponseHandlerError
from grizzly.types import RequestMethod, ResponseContentType
from grizzly.task import RequestTask

from ...fixtures import locust_environment  # pylint: disable=unused-import
from ...helpers import RequestEvent, TestUser


class TestResponseHandler:
    @pytest.mark.usefixtures('mocker', 'locust_environment')
    def test___init__(self, locust_environment: Environment) -> None:
        ResponseHandler.host = None

        with pytest.raises(TypeError):
            ResponseHandler()

        with pytest.raises(LocustError):
            ResponseHandler(locust_environment)

        fake_user_type = type('FakeResponseHandlerUser', (ResponseHandler, HttpRequests, ), {
            'host': '',
        })

        user = fake_user_type(locust_environment)

        assert issubclass(user.__class__, ResponseEvent)
        assert isinstance(user.client, ResponseEventSession)
        assert isinstance(user.response_event, EventHook)
        assert len(user.response_event._handlers) == 1

        ResponseHandler.host = ''
        user = ResponseHandler(locust_environment)
        assert user.client is None
        assert isinstance(user.response_event, EventHook)
        assert len(user.response_event._handlers) == 1

    @pytest.mark.usefixtures('mocker', 'locust_environment')
    def test_response_handler_response_context(self, mocker: MockerFixture, locust_environment: Environment) -> None:
        ResponseHandler.host = 'http://example.com'
        user = ResponseHandler(locust_environment)
        test_user = TestUser(locust_environment)

        response = Response()
        response._content = jsondumps({}).encode('utf-8')
        response.status_code = 200
        response_context_manager = ResponseContextManager(response, RequestEvent(), {})

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/v2/test')

        # edge scenario -- from RestApiUser and *_token calls, they don't have a RequestTask
        original_response = request.response
        setattr(request, 'response', None)

        user.response_handler('test', response_context_manager, request, test_user)

        request.response = original_response

        payload_handler = mocker.MagicMock()
        metadata_handler = mocker.MagicMock()

        # no handler called
        user.response_handler('test', response_context_manager, request, test_user)

        # payload handler called
        request.response.handlers.add_payload(payload_handler)
        user.response_handler('test', response_context_manager, request, test_user)

        assert metadata_handler.call_count == 0
        payload_handler.assert_called_once_with((ResponseContentType.JSON, {}), test_user, response_context_manager)
        request.response.handlers.payload.clear()
        payload_handler.reset_mock()

        # metadata handler called
        request.response.handlers.add_metadata(metadata_handler)
        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 0
        metadata_handler.assert_called_once_with((ResponseContentType.JSON, {}), test_user, response_context_manager)
        metadata_handler.reset_mock()
        request.response.handlers.metadata.clear()

        # invalid json content in payload
        response._content = '{"test: "value"}'.encode('utf-8')
        response_context_manager = ResponseContextManager(response, RequestEvent(), {})
        request.response.handlers.add_payload(payload_handler)

        assert response_context_manager._manual_result is None

        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 0
        assert metadata_handler.call_count == 0
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        assert 'failed to transform' in str(response_context_manager._manual_result)
        request.response.handlers.payload.clear()

        # XML in response
        response._content = '''<?xml version="1.0" encoding="UTF-8"?>
        <test>
            value
        </test>'''.encode('utf-8')

        request.response.content_type = ResponseContentType.GUESS
        request.response.handlers.add_payload(payload_handler)
        user.response_handler('test', response_context_manager, request, test_user)

        assert payload_handler.call_count == 1
        ((call_content_type, call_payload), call_user, call_context, ), _ = payload_handler.call_args_list[0]
        assert call_content_type == ResponseContentType.XML
        assert isinstance(call_payload, XML._Element)
        assert call_user is test_user
        assert call_context is response_context_manager

        request.response.content_type = ResponseContentType.XML
        response._content = '''<?xml encoding="UTF-8"?>
        <test>
            value
        </test>'''.encode('utf-8')

        user.response_handler('test', response_context_manager, request, test_user)
        assert payload_handler.call_count == 1
        assert isinstance(response_context_manager._manual_result, CatchResponseError)
        assert 'failed to transform' in str(response_context_manager._manual_result)


    @pytest.mark.usefixtures('mocker', 'locust_environment')
    def test_response_handler_custom_response(self, mocker: MockerFixture, locust_environment: Environment) -> None:
        ResponseHandler.host = 'http://example.com'
        user = ResponseHandler(locust_environment)
        test_user = TestUser(locust_environment)

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/v2/test')

        payload_handler = mocker.MagicMock()
        metadata_handler = mocker.MagicMock()

        # no handler called
        user.response_handler('test', (None, ''), request, test_user)

        # payload handler called
        request.response.handlers.add_payload(payload_handler)
        user.response_handler('test', (None, '{}'), request, test_user)

        payload_handler.assert_called_once_with((ResponseContentType.JSON, {}), test_user, None)
        payload_handler.reset_mock()
        request.response.handlers.payload.clear()

        # metadata handler called
        request.response.handlers.add_metadata(metadata_handler)
        user.response_handler('test', ({}, None), request, test_user)

        metadata_handler.assert_called_once_with((ResponseContentType.JSON, {}), test_user, None)
        metadata_handler.reset_mock()
        request.response.handlers.metadata.clear()

        # invalid json content in payload
        request.response.handlers.add_payload(payload_handler)

        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', (None, '{"test: "value"'), request, test_user)
        assert 'failed to transform' in str(e)
        assert payload_handler.call_count == 0

        request.response.content_type = ResponseContentType.JSON
        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', (None, '{"test: "value"'), request, test_user)
        assert 'failed to transform input as JSON' in str(e)

        request.response.content_type = ResponseContentType.XML
        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', ({}, '{"test": "value"}'), request, test_user)
        assert 'failed to transform input as XML' in str(e)

        request.response.content_type = ResponseContentType.PLAIN
        with pytest.raises(ResponseHandlerError) as e:
            user.response_handler('test', ({}, '{"test": "value"}'), request, test_user)
        assert 'failed to transform input as PLAIN' in str(e)

        # XML input
        request.response.content_type = ResponseContentType.GUESS
        user.response_handler(
            'test',
            (
                None,
                '''<?xml version="1.0" encoding="UTF-8"?>
                <test>
                    value
                </test>''',
            ),
            request,
            test_user,
        )

        assert payload_handler.call_count == 1
        ((call_content_type, call_payload), call_user, call_context, ), _ = payload_handler.call_args_list[0]
        assert call_content_type == ResponseContentType.XML
        assert isinstance(call_payload, XML._Element)
        assert call_user is test_user
        assert call_context is None

        with pytest.raises(ResponseHandlerError):
            user.response_handler(
                'test',
                (
                    None,
                    '''<?xml encoding="UTF-8"?>
                    <test>
                        value
                    </test>''',
                ),
                request,
                test_user,
            )

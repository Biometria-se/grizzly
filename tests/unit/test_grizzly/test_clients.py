"""Unit tests of grizzly.clients."""
from __future__ import annotations

import gevent.monkey

gevent.monkey.patch_all()

from json import dumps as jsondumps
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple, Union

import pytest
from behave.model import Scenario
from locust.clients import ResponseContextManager
from locust.event import EventHook
from paramiko.sftp_client import SFTPClient
from paramiko.transport import Transport
from requests.models import Response

from grizzly.clients import ResponseEventSession, SftpClientSession
from grizzly.context import GrizzlyContextScenario
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.types.locust import StopUser
from tests.helpers import RequestEvent

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture

    from grizzly.users.base import GrizzlyUser
    from tests.fixtures import ParamikoFixture


class TestResponseEventSession:
    def test___init__(self) -> None:
        session = ResponseEventSession(base_url='', request_event=RequestEvent())

        assert isinstance(session.event_hook, EventHook)
        assert len(session.event_hook._handlers) == 0

    def test_request(self, mocker: MockerFixture) -> None:
        def mock_request(payload: Dict[str, Any], status_code: int = 200) -> None:
            def request(
                _: ResponseEventSession,
                *_args: Any,
                **_kwargs: Any,
            ) -> ResponseContextManager:
                response = Response()
                response._content = jsondumps(payload).encode('utf-8')
                response.status_code = status_code
                return ResponseContextManager(response, RequestEvent(), None)

            mocker.patch(
                'locust.clients.HttpSession.request',
                request,
            )

        class HandlerCalled(StopUser):
            pass

        mock_request({}, 200)

        session = ResponseEventSession(base_url='', request_event=RequestEvent())
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        scenario = GrizzlyContextScenario(1, behave=Scenario('<stdin>', 0, 'Any', 'TestScenario'))
        scenario.context['host'] = 'test'

        def handler(expected_request: Optional[RequestTask] = None) -> Callable[
            [str, Union[ResponseContextManager, Tuple[Dict[str, Any], str]], Optional[RequestTask], GrizzlyUser],
            None,
        ]:
            def wrapped(name: str, context: Union[ResponseContextManager, Tuple[Dict[str, Any], str]], request: Optional[RequestTask], user: GrizzlyUser) -> None:  # noqa: ARG001
                if expected_request is request:
                    raise HandlerCalled  # one of few exceptions which event handler lets through

            return wrapped

        assert len(session.event_hook._handlers) == 0

        session.event_hook.add_listener(handler())

        assert len(session.event_hook._handlers) == 1

        session.request(method='GET', url='http://example.org', name='test-name', catch_response=False, request=request, context={})

        session.event_hook._handlers = []
        session.event_hook.add_listener(handler(request))

        # handler should be called, which raises StopUser
        with pytest.raises(HandlerCalled):
            session.request(method='GET', url='http://example.org', name='test-name', catch_response=False, request=request)

        second_request = RequestTask(RequestMethod.GET, name='test-request', endpoint='/api/test')

        # handler is called, but request is not the same
        session.request(method='GET', url='http://example.org', name='test-name', catch_response=False, request=second_request)


class TestSftpClientSession:
    def test(self, paramiko_fixture: ParamikoFixture, mocker: MockerFixture) -> None:
        # ruff: noqa: B009
        paramiko_fixture()

        context = SftpClientSession('example.org', 1337)

        assert context.host == 'example.org'
        assert context.port == 1337
        assert getattr(context, 'username') is None
        assert getattr(context, 'key') is None
        assert getattr(context, 'key_file') is None
        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None

        with pytest.raises(NotImplementedError, match='private key authentication is not supported'), context.session('username', 'password', '~/.ssh/id_rsa'):
            pass

        assert context.host == 'example.org'
        assert context.port == 1337
        assert getattr(context, 'username') is None
        assert getattr(context, 'key') is None
        assert getattr(context, 'key_file') is None
        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None

        username_transport: Optional[Transport] = None
        username_client: Optional[SFTPClient] = None

        # start session for user username
        with context.session('username', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert getattr(context, 'username') is None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            username_transport = context._transport
            username_client = context._client
        assert context.username == 'username'

        # change username, and hence start a new client
        with context.session('test-user', 'password') as session:
            assert isinstance(session, SFTPClient)
            assert getattr(context, 'username') is None
            assert isinstance(context._transport, Transport)
            assert isinstance(context._client, SFTPClient)
            assert username_client is not context._client
            assert username_transport is not context._transport

        context.close()

        assert getattr(context, '_client') is None
        assert getattr(context, '_transport') is None
        assert getattr(context, 'username') is None

        def _from_transport(transport: Transport, window_size: Optional[int] = None, max_packet_size: Optional[int] = None) -> Optional[SFTPClient]:  # noqa: ARG001
            return None

        mocker.patch(
            'paramiko.sftp_client.SFTPClient.from_transport',
            _from_transport,
        )

        with pytest.raises(RuntimeError, match='there is no client'), context.session('test-user', 'password') as session:
            pass

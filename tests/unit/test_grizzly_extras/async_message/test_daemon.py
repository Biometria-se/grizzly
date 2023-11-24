"""Unit tests of grizzly_extras.async_message.daemon."""
from __future__ import annotations

from importlib import reload
from itertools import cycle
from json import dumps as jsondumps
from signal import SIGINT
from typing import TYPE_CHECKING, Any, List, cast

import pytest
import zmq.green as zmq

from grizzly_extras.async_message.daemon import main, router, signal_handler, worker
from tests.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from pytest_mock.plugin import MockerFixture

    from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse


def test_signal_handler() -> None:
    from grizzly_extras.async_message import daemon

    assert not getattr(daemon, 'abort')  # noqa: B009

    signal_handler(SIGINT, None)

    assert daemon.abort

    reload(daemon)


@pytest.mark.parametrize(('scheme', 'implementation'), [
    ('mq', 'AsyncMessageQueueHandler'),
    ('sb', 'AsyncServiceBusHandler'),
])
def test_worker(mocker: MockerFixture, caplog: LogCaptureFixture, scheme: str, implementation: str) -> None:
    context_mock = mocker.MagicMock()
    worker_mock = mocker.MagicMock()
    worker_mock.send_multipart.side_effect = cycle([StopAsyncIteration])

    context_mock.socket.return_value = worker_mock

    def mock_recv_multipart(message: AsyncMessageRequest) -> None:
        def build_zmq_message(_message: AsyncMessageRequest) -> List[bytes]:
            worker = cast(str, _message.get('worker', ''))
            return [
                worker.encode(),
                b'',
                jsondumps(_message).encode(),
            ]

        worker_mock.recv_multipart.side_effect = [
            zmq.Again,
            None,
            build_zmq_message(message),
        ]

    def mock_handle_response(response: AsyncMessageResponse) -> None:
        mocker.patch(
            'grizzly_extras.async_message.AsyncMessageHandler.handle',
            side_effect=[response],
        )

    mock_recv_multipart({'worker': 'ID-54321', 'context': {'url': f'{scheme}://dummy'}})

    with pytest.raises(StopAsyncIteration):
        worker(context_mock, 'ID-12345')

    worker_mock.send_multipart.assert_called_once_with([
        b'ID-54321',
        b'',
        jsondumps({
            'worker': 'ID-12345',
            'response_time': 0,
            'success': False,
            'message': 'got ID-54321, expected ID-12345',
        }).encode(),
    ])
    worker_mock.send_multipart.reset_mock()

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': 'http://www.example.com'}})

    with pytest.raises(StopAsyncIteration):
        worker(context_mock, 'ID-12345')

    worker_mock.send_multipart.assert_called_once_with([
        b'ID-12345',
        b'',
        jsondumps({
            'worker': 'ID-12345',
            'response_time': 0,
            'success': False,
            'message': 'integration for http:// is not implemented',
        }).encode(),
    ])
    worker_mock.send_multipart.reset_mock()

    integration_spy = mocker.patch(
        f'grizzly_extras.async_message.{scheme}.{implementation}.__init__',
        return_value=None,
    )

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': f'{scheme}://example.com'}})
    mock_handle_response({
        'worker': 'ID-12345',
        'success': True,
        'payload': 'hello world',
        'metadata': {
            'some': 'metadata',
        },
        'response_time': 439,
    })

    with pytest.raises(StopAsyncIteration):
        worker(context_mock, 'ID-12345')

    integration_spy.assert_called_once_with('ID-12345')
    integration_spy.reset_mock()

    worker_mock.send_multipart.assert_called_once_with([
        b'ID-12345',
        b'',
        jsondumps({
            'worker': 'ID-12345',
            'success': True,
            'payload': 'hello world',
            'metadata': {
                'some': 'metadata',
            },
            'response_time': 439,
        }).encode(),
    ])
    worker_mock.send_multipart.reset_mock()

    from grizzly_extras.async_message import daemon

    def hack(*_args: Any, **_kwargs: Any) -> None:
        daemon.abort = True

    worker_mock.send_multipart = hack

    mock_recv_multipart({'worker': 'F00B4R', 'context': {'url': f'{scheme}://example.com'}})
    mock_handle_response({
        'worker': 'F00B4R',
        'success': True,
        'payload': 'foo bar',
        'metadata': {
            'some': 'metadata',
        },
        'response_time': 1337,
    })
    caplog.clear()

    integration_close_spy = mocker.patch(
        f'grizzly_extras.async_message.{scheme}.{implementation}.close',
        return_value=None,
    )

    worker(context_mock, 'F00B4R')

    reload(daemon)

    integration_close_spy.assert_called_once_with()

    assert caplog.messages[-1] == 'stopping'


def test_router(mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    from grizzly_extras.async_message.daemon import worker
    context_mock = mocker.MagicMock()
    create_context_mock = mocker.patch('zmq.green.Context.__new__', return_value=context_mock)

    frontend_mock = mocker.MagicMock()
    backend_mock = mocker.MagicMock()
    context_mock.socket.side_effect = cycle([frontend_mock, backend_mock])

    poller_mock = mocker.MagicMock()
    create_poller_mock = mocker.patch('zmq.green.Poller.__new__', return_value=poller_mock)
    poller_mock.poll.side_effect = [RuntimeError]
    thread_mock = mocker.patch('grizzly_extras.async_message.daemon.Thread')

    mocker.patch('grizzly_extras.async_message.daemon.uuid4', return_value='foobar')

    with pytest.raises(RuntimeError):
        router()

    assert 'spawned worker foobar' in caplog.messages[0]
    caplog.clear()

    create_context_mock.assert_called_once_with(
        ANY(),
        1,
    )

    assert context_mock.socket.call_count == 2
    context_mock.socket.assert_called_with(zmq.ROUTER)
    create_context_mock.reset_mock()

    frontend_mock.bind.assert_called_once_with('tcp://127.0.0.1:5554')
    backend_mock.bind.assert_called_once_with('inproc://workers')

    create_poller_mock.assert_called_once_with(ANY())
    poller_mock.poll.assert_called_once_with(timeout=1000)

    poller_mock.register.assert_has_calls([mocker.call(frontend_mock, zmq.POLLIN), mocker.call(backend_mock, zmq.POLLIN)])

    thread_mock.assert_called_once_with(target=worker, args=(context_mock, 'foobar'))


def test_main(mocker: MockerFixture) -> None:
    mocker.patch(
        'grizzly_extras.async_message.daemon.router',
        side_effect=[None, KeyboardInterrupt],
    )

    assert main() == 0
    assert main() == 1

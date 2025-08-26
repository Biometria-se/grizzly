"""Unit tests of async_messaged.daemon."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
from json import dumps as jsondumps
from multiprocessing import Process
from threading import Event
from typing import TYPE_CHECKING, cast

import pytest
import zmq.green as zmq
from async_messaged.daemon import Worker, main, router

from test_async_messaged.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from async_messaged import AsyncMessageRequest, AsyncMessageResponse
    from pytest_mock.plugin import MockerFixture


@pytest.mark.parametrize(
    ('scheme', 'implementation'),
    [
        ('mq', 'AsyncMessageQueueHandler'),
        ('sb', 'AsyncServiceBusHandler'),
    ],
)
def test_worker(mocker: MockerFixture, caplog: LogCaptureFixture, scheme: str, implementation: str) -> None:
    context_mock = mocker.MagicMock()
    worker_mock = mocker.MagicMock()
    worker_mock.send_multipart.side_effect = cycle([StopAsyncIteration])

    context_mock.socket.return_value = worker_mock

    def mock_recv_multipart(message: AsyncMessageRequest) -> None:
        def build_zmq_message(_message: AsyncMessageRequest) -> list[bytes]:
            worker = cast('str', _message.get('worker', ''))
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
            'async_messaged.AsyncMessageHandler.handle',
            side_effect=[response],
        )

    mock_recv_multipart({'worker': 'ID-54321', 'context': {'url': f'{scheme}://dummy'}})

    event_mock = mocker.MagicMock()
    event_mock.is_set.side_effect = [False, False, False, False, False, True]
    Worker(context_mock, 'ID-12345', event=event_mock).run()

    worker_mock.send_multipart.assert_called_once_with(
        [
            b'ID-54321',
            b'',
            jsondumps(
                {
                    'request_id': 'None',
                    'worker': 'ID-12345',
                    'response_time': 0,
                    'success': False,
                    'message': 'got ID-54321, expected ID-12345',
                },
            ).encode(),
        ],
    )
    worker_mock.send_multipart.reset_mock()

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': 'http://www.example.com'}})

    event_mock.is_set.side_effect = [False, False, False, False, False, True]
    Worker(context_mock, 'ID-12345', event=event_mock).run()

    worker_mock.send_multipart.assert_called_once_with(
        [
            b'ID-12345',
            b'',
            jsondumps(
                {
                    'request_id': 'None',
                    'worker': 'ID-12345',
                    'response_time': 0,
                    'success': False,
                    'message': 'integration for http:// is not implemented',
                },
            ).encode(),
        ],
    )
    worker_mock.send_multipart.reset_mock()

    integration_spy = mocker.patch(
        f'async_messaged.{scheme}.{implementation}.__init__',
        return_value=None,
    )

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': f'{scheme}://example.com'}})
    mock_handle_response(
        {
            'request_id': 'None',
            'worker': 'ID-12345',
            'success': True,
            'payload': 'hello world',
            'metadata': {
                'some': 'metadata',
            },
            'response_time': 439,
        },
    )

    event_mock.is_set.side_effect = [False, False, False, False, False, False, True]
    Worker(context_mock, 'ID-12345', event=event_mock).run()

    integration_spy.assert_called_once_with('ID-12345', event=event_mock)
    integration_spy.reset_mock()

    worker_mock.send_multipart.assert_called_once_with(
        [
            b'ID-12345',
            b'',
            jsondumps(
                {
                    'request_id': 'None',
                    'worker': 'ID-12345',
                    'success': True,
                    'payload': 'hello world',
                    'metadata': {
                        'some': 'metadata',
                    },
                    'response_time': 439,
                },
            ).encode(),
        ],
    )
    worker_mock.send_multipart.reset_mock()

    worker = Worker(context_mock, 'F00B4R')

    mock_recv_multipart({'worker': 'F00B4R', 'context': {'url': f'{scheme}://example.com'}})
    mock_handle_response(
        {
            'request_id': 'None',
            'worker': 'F00B4R',
            'success': True,
            'payload': 'foo bar',
            'metadata': {
                'some': 'metadata',
            },
            'response_time': 1337,
        },
    )
    caplog.clear()

    worker.run()


def test_router(mocker: MockerFixture, caplog: LogCaptureFixture) -> None:
    logger = logging.getLogger('router')
    context_mock = mocker.MagicMock()
    create_context_mock = mocker.patch('zmq.green.Context.__new__', return_value=context_mock)

    frontend_mock = mocker.MagicMock()
    backend_mock = mocker.MagicMock()
    context_mock.socket.side_effect = cycle([frontend_mock, backend_mock])

    poller_mock = mocker.MagicMock()
    create_poller_mock = mocker.patch('zmq.green.Poller.__new__', return_value=poller_mock)

    thread_pool_executor_mock = mocker.MagicMock(spec=ThreadPoolExecutor)
    mocker.patch('async_messaged.daemon.ThreadPoolExecutor.__new__', return_value=thread_pool_executor_mock)

    worker_mock = mocker.MagicMock(spec=Worker)
    worker_mock.integration = mocker.PropertyMock()
    worker_mock.logger = logging.getLogger('worker')
    worker_mock.socket = mocker.PropertyMock()
    mocker.patch('async_messaged.daemon.Worker.__new__', side_effect=[worker_mock, mocker.MagicMock(spec=Worker)])

    mocker.patch('async_messaged.daemon.uuid4', return_value='foobar')

    run_daemon = mocker.MagicMock(spec=Event)

    mocker.patch.object(run_daemon, 'is_set', side_effect=[False, True, False])

    with caplog.at_level(logging.DEBUG):
        router(run_daemon, logger)

    print(caplog.messages)

    assert [*caplog.messages[:2], caplog.messages[-1]] == ['starting', 'spawned worker foobar', 'stopped']
    caplog.clear()

    create_context_mock.assert_called_once_with(
        ANY(),
    )

    assert context_mock.socket.call_count == 2
    context_mock.socket.assert_called_with(zmq.ROUTER)
    create_context_mock.reset_mock()

    frontend_mock.bind.assert_called_once_with('tcp://127.0.0.1:5554')
    backend_mock.bind.assert_called_once_with('inproc://workers')

    create_poller_mock.assert_called_once_with(ANY())
    poller_mock.poll.assert_called_once_with(timeout=1000)

    poller_mock.register.assert_has_calls([mocker.call(frontend_mock, zmq.POLLIN), mocker.call(backend_mock, zmq.POLLIN)])

    worker_mock.integration.close.assert_called_once_with()
    worker_mock.socket.close.assert_called_once_with()
    run_daemon.set.assert_called_once_with()
    thread_pool_executor_mock.__enter__.return_value.submit.assert_called_once_with(worker_mock.run)
    thread_pool_executor_mock.__exit__.assert_called_once_with(None, None, None)


def test_main(mocker: MockerFixture) -> None:
    run_daemon_mock = mocker.patch('async_messaged.daemon.Event', spec=Event)
    logger_mock = mocker.patch('async_messaged.daemon.logging.getLogger', spec=logging.Logger)

    setproctitle_mock = mocker.patch('async_messaged.daemon.proc.setproctitle', return_value=None)

    process_mock = mocker.patch('async_messaged.daemon.Process', spec=Process)
    process_mock.return_value.exitcode = None
    process_mock.return_value.is_alive.return_value = True

    router_mock = mocker.patch('async_messaged.daemon.router', return_value=None)

    assert main() == 0
    setproctitle_mock.assert_called_once_with('grizzly-async-messaged')
    setproctitle_mock.reset_mock()
    process_mock.assert_called_once_with(target=router_mock, args=(run_daemon_mock.return_value, logger_mock.return_value))
    process_mock.return_value.start.assert_called_once_with()
    run_daemon_mock.return_value.wait.assert_called_once_with()
    process_mock.return_value.terminate.assert_called_once_with()
    process_mock.return_value.join.assert_called_once_with(timeout=3.0)
    process_mock.return_value.is_alive.assert_called_once_with()
    process_mock.return_value.kill.assert_called_once_with()
    process_mock.reset_mock()
    run_daemon_mock.reset_mock()

    process_mock.return_value.start.side_effect = [KeyboardInterrupt]

    assert main() == 1
    run_daemon_mock.return_value.wait.assert_not_called()

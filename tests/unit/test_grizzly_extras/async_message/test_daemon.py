from typing import List, Any, Dict, Tuple, cast
from json import dumps as jsondumps
from signal import SIGINT
from importlib import reload
from itertools import cycle

import pytest
import zmq

from _pytest.capture import CaptureFixture
from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.async_message.daemon import router, worker, main, signal_handler


def test_signal_handler() -> None:
    import grizzly_extras.async_message.daemon as daemon

    assert not getattr(daemon, 'abort')

    signal_handler(SIGINT, None)

    assert getattr(daemon, 'abort')

    reload(daemon)


@pytest.mark.parametrize('scheme,implementation', [
    ('mq', 'AsyncMessageQueueHandler',),
    ('sb', 'AsyncServiceBusHandler',),
])
def test_worker(mocker: MockerFixture, capsys: CaptureFixture, scheme: str, implementation: str) -> None:
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

    assert worker_mock.send_multipart.call_count == 1
    args, _ = worker_mock.send_multipart.call_args_list[0]
    actual_response_proto = args[0]
    assert actual_response_proto[0] == b'ID-54321'
    assert actual_response_proto[-1] == jsondumps({
        'worker': 'ID-12345',
        'response_time': 0,
        'success': False,
        'message': 'got ID-54321, expected ID-12345',
    }).encode()

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': 'http://www.example.com'}})

    with pytest.raises(StopAsyncIteration):
        worker(context_mock, 'ID-12345')

    assert worker_mock.send_multipart.call_count == 2
    args, _ = worker_mock.send_multipart.call_args_list[1]
    actual_response_proto = args[0]
    assert actual_response_proto[0] == b'ID-12345'
    assert actual_response_proto[-1] == jsondumps({
        'worker': 'ID-12345',
        'response_time': 0,
        'success': False,
        'message': 'integration for http:// is not implemented',
    }).encode()

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

    assert integration_spy.call_count == 1
    args, _ = integration_spy.call_args_list[0]
    assert args[0] == 'ID-12345'

    assert worker_mock.send_multipart.call_count == 3
    args, _ = worker_mock.send_multipart.call_args_list[2]
    actual_response_proto = args[0]
    assert actual_response_proto[0] == b'ID-12345'
    assert actual_response_proto[-1] == jsondumps({
        'worker': 'ID-12345',
        'success': True,
        'payload': 'hello world',
        'metadata': {
            'some': 'metadata',
        },
        'response_time': 439,
    }).encode()

    import grizzly_extras.async_message.daemon as daemon

    def hack(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
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
    capsys.readouterr()

    integration_close_spy = mocker.patch(
        f'grizzly_extras.async_message.{scheme}.{implementation}.close',
        return_value=None,
    )

    worker(context_mock, 'F00B4R')

    reload(daemon)

    assert integration_close_spy.call_count == 1

    capture = capsys.readouterr()

    assert capture.out == ''
    assert 'worker::F00B4R: stopping' in capture.err


def test_router(mocker: MockerFixture, capsys: CaptureFixture) -> None:
    from grizzly_extras.async_message.daemon import worker
    context_mock = mocker.MagicMock()
    create_context_mock = mocker.patch('zmq.green.Context.__new__', return_value=context_mock)

    frontend_mock = mocker.MagicMock()
    backend_mock = mocker.MagicMock()
    context_mock.socket.side_effect = cycle([frontend_mock, backend_mock])

    poller_mock = mocker.MagicMock()
    create_poller_mock = mocker.patch('zmq.green.Poller.__new__', return_value=poller_mock)
    # poller_mock.poll.return_value = {frontend_mock: zmq.POLLIN, backend_mock: zmq.POLLIN}
    poller_mock.poll.side_effect = [RuntimeError]
    thread_mock = mocker.patch('grizzly_extras.async_message.daemon.Thread')

    mocker.patch('grizzly_extras.async_message.daemon.uuid4', return_value='foobar')

    with pytest.raises(RuntimeError):
        router()

    capture = capsys.readouterr()

    assert 'router: spawned worker' in capture.err
    assert capture.out == ''

    assert create_context_mock.call_count == 1
    args, kwargs = create_context_mock.call_args_list[0]
    assert kwargs == {}
    assert len(args) == 2
    assert args[1] == 1
    assert context_mock.socket.call_count == 2
    args, kwargs = context_mock.socket.call_args_list[0]
    assert kwargs == {}
    assert args == (zmq.ROUTER,)
    args, kwargs = context_mock.socket.call_args_list[1]
    assert kwargs == {}
    assert args == (zmq.ROUTER,)

    frontend_mock.bind.assert_called_once_with('tcp://127.0.0.1:5554')
    backend_mock.bind.assert_called_once_with('inproc://workers')

    assert create_poller_mock.call_count == 1
    args, kwargs = create_poller_mock.call_args_list[0]
    assert kwargs == {}
    assert len(args) == 1
    poller_mock.poll.assert_called_once_with(timeout=1000)
    assert poller_mock.register.call_count == 2
    args, kwargs = poller_mock.register.call_args_list[0]
    assert kwargs == {}
    assert args == (frontend_mock, zmq.POLLIN,)
    args, kwargs = poller_mock.register.call_args_list[1]
    assert kwargs == {}
    assert args == (backend_mock, zmq.POLLIN,)

    thread_mock.assert_called_once_with(target=worker, args=(context_mock, 'foobar',))


def test_main(mocker: MockerFixture) -> None:
    mocker.patch(
        'grizzly_extras.async_message.daemon.router',
        side_effect=[None, KeyboardInterrupt],
    )

    assert main() == 0
    assert main() == 1

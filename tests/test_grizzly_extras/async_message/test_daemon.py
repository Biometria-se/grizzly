from typing import List, cast
from json import dumps as jsondumps

import pytest
import zmq

from _pytest.capture import CaptureFixture
from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.async_message.daemon import router, worker, main

from ...fixtures import NoopZmqFixture


@pytest.mark.parametrize('scheme,implementation', [
    ('mq', 'AsyncMessageQueueHandler',),
    ('sb', 'AsyncServiceBusHandler',),
])
def test_worker(mocker: MockerFixture, noop_zmq: NoopZmqFixture, scheme: str, implementation: str) -> None:
    prefix = 'grizzly_extras.async_message.daemon'

    noop_zmq(prefix)

    class BreakLoop(Exception):
        pass

    spy = mocker.patch(
        f'{prefix}.zmq.sugar.socket.Socket.send_multipart',
        side_effect=[BreakLoop] * 10,
    )

    def mock_recv_multipart(message: AsyncMessageRequest) -> None:
        def build_zmq_message(_message: AsyncMessageRequest) -> List[bytes]:
            worker = cast(str, _message.get('worker', ''))
            return [
                worker.encode(),
                b'',
                jsondumps(_message).encode(),
            ]

        mocker.patch(
            f'{prefix}.zmq.sugar.socket.Socket.recv_multipart',
            side_effect=[
                None,
                build_zmq_message({'worker': 'ID-54321'}),
                build_zmq_message(message),
            ],
        )

    def mock_handle_response(response: AsyncMessageResponse) -> None:
        mocker.patch(
            'grizzly_extras.async_message.AsyncMessageHandler.handle',
            side_effect=[response],
        )

    mock_recv_multipart({'worker': 'ID-12345'})

    zmq_context = zmq.Context()
    try:
        with pytest.raises(BreakLoop):
            worker(zmq_context, 'ID-12345')

        assert spy.call_count == 1
        args, _ = spy.call_args_list[0]
        actual_response_proto = args[0]
        assert actual_response_proto[0] == b'ID-12345'
        assert actual_response_proto[-1] == jsondumps({
            'worker': 'ID-12345',
            'response_time': 0,
            'success': False,
            'message': 'no url found in request context',
        }).encode()

        mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': 'http://www.example.com'}})

        with pytest.raises(BreakLoop):
            worker(zmq_context, 'ID-12345')

        assert spy.call_count == 2
        args, _ = spy.call_args_list[1]
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
            side_effect=[None],
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

        with pytest.raises(BreakLoop):
            worker(zmq_context, 'ID-12345')

        assert integration_spy.call_count == 1
        args, _ = integration_spy.call_args_list[0]
        assert args[0] == 'ID-12345'

        assert spy.call_count == 3
        args, _ = spy.call_args_list[2]
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
    finally:
        zmq_context.destroy()


def test_router(mocker: MockerFixture, capsys: CaptureFixture, noop_zmq: NoopZmqFixture) -> None:
    prefix = 'grizzly_extras.async_message.daemon'

    noop_zmq(prefix)

    bind_mock = noop_zmq.get_mock('bind')
    poller_register_mock = noop_zmq.get_mock('register')
    thread_mock = mocker.patch(f'{prefix}.Thread', mocker.MagicMock())

    # recv_multipart_mock = noop_zmq.get_mock('recv_multipart')
    # recv_multipart_mock.side_effect = [False, False]

    zmq_context = zmq.Context(1)

    poller_poll_mock = noop_zmq.get_mock('poll')

    # @ TODO: should have a side effect that is a dict, and that returns zmq.POLLIN for any key
    poller_poll_mock.side_effect = [RuntimeError]

    mocker.patch(f'{prefix}.zmq.Context.__new__', return_value=zmq_context)

    try:
        with pytest.raises(RuntimeError):
            router()

        capture = capsys.readouterr()

        assert 'router: spawned worker' in capture.err
        assert capture.out == ''

        assert bind_mock.call_count == 2
        args, _ = bind_mock.call_args_list[0]
        assert args[-1] == 'tcp://127.0.0.1:5554'
        args, _ = bind_mock.call_args_list[1]
        assert args[-1] == 'inproc://workers'

        assert poller_register_mock.call_count == 2
        args, _ = poller_register_mock.call_args_list[0]
        assert len(args) == 3
        assert args[-1] == zmq.POLLIN

        args, _ = poller_register_mock.call_args_list[1]
        assert len(args) == 3
        assert args[-1] == zmq.POLLIN

        assert len(thread_mock.mock_calls) == 3
        assert poller_poll_mock.call_count == 1
        # assert recv_multipart_mock.call_count == 2
    finally:
        zmq_context.destroy()


def test_main(mocker: MockerFixture) -> None:
    mocker.patch(
        'grizzly_extras.async_message.daemon.router',
        side_effect=[None, KeyboardInterrupt],
    )

    assert main() == 0
    assert main() == 1

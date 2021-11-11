from typing import Any, Dict, Tuple, List, cast
from json import dumps as jsondumps

import pytest
import zmq

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.async_message.daemon import worker, main


def test_worker(mocker: MockerFixture) -> None:
    def mocked_noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> None:
        pass

    prefix = 'grizzly_extras.async_message.daemon'

    targets = [
        'zmq.sugar.context.Context.term',
        'zmq.sugar.context.Context.__del__',
        'zmq.sugar.socket.Socket.bind',
        'zmq.sugar.socket.Socket.connect',
        'zmq.sugar.socket.Socket.send_string',
    ]

    for target in targets:
        mocker.patch(
            f'{prefix}.{target}',
            mocked_noop,
        )

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
        'grizzly_extras.async_message.mq.AsyncMessageQueueHandler.__init__',
        side_effect=[None],
    )

    mock_recv_multipart({'worker': 'ID-12345', 'context': {'url': 'mq://mq.example.com'}})
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


def test_main(mocker: MockerFixture) -> None:
    mocker.patch(
        'grizzly_extras.async_message.daemon.router',
        side_effect=[None, KeyboardInterrupt],
    )

    assert main() == 0
    assert main() == 1

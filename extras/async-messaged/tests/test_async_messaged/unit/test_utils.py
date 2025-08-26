"""Unit tests of async_messaged.utils."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
import zmq.green as zmq
from async_messaged import AsyncMessageError, AsyncMessageRequest
from async_messaged.utils import async_message_request, tohex
from zmq.error import Again as ZMQAgain

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock.plugin import MockerFixture


class Test_tohex:
    def test_unsupported(self) -> None:
        with pytest.raises(ValueError, match='has an unsupported type'):
            tohex(['deadbeef'])

    def test_int(self) -> None:
        assert tohex(3735928559) == 'deadbeef'

    def test_str(self) -> None:
        assert tohex('Þ­¾ï') == 'deadbeef'

    def test_bytes(self) -> None:
        assert tohex(b'\xde\xad\xbe\xef') == 'deadbeef'

    def test_bytearray(self) -> None:
        assert tohex(bytearray(b'\xde\xad\xbe\xef')) == 'deadbeef'


def test_async_message_request(mocker: MockerFixture) -> None:
    client_mock = mocker.MagicMock()
    sleep_mock = mocker.patch('async_messaged.utils.sleep', return_value=None)

    # no valid response
    client_mock.recv_json.side_effect = [ZMQAgain, None]

    request: AsyncMessageRequest = {
        'worker': None,
        'action': 'HELLO',
    }

    with pytest.raises(AsyncMessageError, match='no response'):
        async_message_request(client_mock, request)

    sleep_mock.assert_called_once_with(0.1)
    client_mock.send_json.assert_called_once_with(request)

    assert client_mock.recv_json.call_count == 2
    for i in range(2):
        args, kwargs = client_mock.recv_json.call_args_list[i]
        assert args == ()
        assert kwargs == {'flags': zmq.NOBLOCK}

    sleep_mock.reset_mock()
    client_mock.reset_mock()

    # unsuccessful response
    client_mock.recv_json.side_effect = None
    client_mock.recv_json.return_value = {'success': False, 'message': 'error! error! error!'}

    with pytest.raises(AsyncMessageError, match='error! error! error!'):
        async_message_request(client_mock, request)

    sleep_mock.assert_not_called()
    client_mock.send_json.assert_called_once_with(request)
    client_mock.recv_json.assert_called_once_with(flags=zmq.NOBLOCK)

    # valid response
    client_mock.recv_json.return_value = {'success': True, 'worker': 'foo-bar-baz-foo', 'payload': 'yes'}

    assert async_message_request(client_mock, request) == {'success': True, 'worker': 'foo-bar-baz-foo', 'payload': 'yes'}

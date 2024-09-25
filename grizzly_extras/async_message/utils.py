"""Utilities used by grizzly_extras.async_message."""
from __future__ import annotations

import logging
from contextlib import suppress
from time import perf_counter, sleep
from typing import Any, Optional, Union, cast

import zmq.green as zmq
from zmq.error import Again as ZMQAgain

from grizzly_extras.async_message import AsyncMessageError, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.exceptions import StopScenario

logger = logging.getLogger(__name__)


def tohex(value: Union[int, str, bytes, bytearray, Any]) -> str:
    if isinstance(value, str):
        return ''.join(f'{ord(c):02x}' for c in value)

    if isinstance(value, (bytes, bytearray)):
        return value.hex()

    if isinstance(value, int):
        return hex(value)[2:]

    message = f'{value} has an unsupported type {type(value)}'
    raise ValueError(message)


def async_message_request(client: zmq.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    client.send_json(request)

    response: Optional[AsyncMessageResponse] = None

    while True:
        start = perf_counter()
        try:
            response = cast(Optional[AsyncMessageResponse], client.recv_json(flags=zmq.NOBLOCK))
            break
        except ZMQAgain:
            # with suppress(Exception):
            try:
                sleep(0.1)
            except StopScenario:
                if response is None:
                    response = {}

                response.update({'success': False, 'message': 'abort'})
                break
            except:  # noqa: S110
                pass

        delta = perf_counter() - start
        if delta > 1.0:
            logger.debug('async_message_request::recv_json took %f seconds', delta)

    if response is None:
        msg = 'no response'
        raise AsyncMessageError(msg)

    message = response.get('message', None)

    if not response['success']:
        if message == 'abort':
            logger.warning('received abort message')
            raise StopScenario

        raise AsyncMessageError(message)

    return response

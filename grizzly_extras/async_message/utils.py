"""Utilities used by grizzly_extras.async_message."""
from __future__ import annotations

import logging
from time import perf_counter, sleep
from typing import TYPE_CHECKING, Any, Optional, Union, cast
from uuid import uuid4

import zmq.green as zmq
from zmq.error import Again as ZMQAgain

from grizzly_extras.async_message import AsyncMessageError, AsyncMessageRequest, AsyncMessageResponse
from grizzly_extras.exceptions import StopScenario

if TYPE_CHECKING:
    from zmq import sugar as ztypes

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


def async_message_request(client: ztypes.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    request.update({'request_id': str(uuid4())})

    client.send_json(request)

    logger.debug('async_message_request::send_json: sent %r', request)

    response: Optional[AsyncMessageResponse] = None

    while True:
        start = perf_counter()
        try:
            response = cast(Optional[AsyncMessageResponse], client.recv_json(flags=zmq.NOBLOCK))
            break
        except ZMQAgain:
            exception: Exception | None = None

            try:
                sleep(0.1)
            except StopScenario as e:
                exception = e
            except Exception as e:
                exception = e

            if exception is not None:
                msg = 'abort' if isinstance(exception, StopScenario) else str(exception)

                if response is None:
                    response = {}

                response.update({'success': False, 'message': msg})
                break
        finally:
            delta = perf_counter() - start
            if delta > 1.0:
                logger.debug('async_message_request::recv_json took %f seconds for request_id %s', delta, request['request_id'])

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

"""Utilities used by grizzly_common.async_message."""

from __future__ import annotations

import logging
from contextlib import suppress
from time import perf_counter, sleep
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

import zmq.green as zmq
from grizzly_common.exceptions import StopScenario
from zmq.error import Again as ZMQAgain

from async_messaged import (
    AsyncMessageError,
    AsyncMessageRequest,
    AsyncMessageResponse,
)

if TYPE_CHECKING:  # pragma: no cover
    from zmq import sugar as ztypes

logger = logging.getLogger(__name__)


def tohex(value: Any) -> str:
    if isinstance(value, str):
        return ''.join(f'{ord(c):02x}' for c in value)

    if isinstance(value, bytes | bytearray):
        return value.hex()

    if isinstance(value, int):
        return hex(value)[2:]

    message = f'{value} has an unsupported type {type(value)}'
    raise ValueError(message)


def async_message_request(client: ztypes.Socket, request: AsyncMessageRequest) -> AsyncMessageResponse:
    request.update({'request_id': str(uuid4())})

    client.send_json(request)

    request_metadata = {**request}

    with suppress(Exception):
        del request_metadata['payload']

    logger.debug('async_message_request::send_json: sent %r', request_metadata)

    response: AsyncMessageResponse | None = None
    count = 0

    start = perf_counter()
    while True:
        count += 1
        try:
            response = cast('AsyncMessageResponse | None', client.recv_json(flags=zmq.NOBLOCK))
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

    delta = perf_counter() - start
    logger.debug(
        'async_message_request::recv_json: took %f seconds for request_id %s, after %d retries',
        delta,
        request['request_id'],
        count,
    )

    if response is None:
        msg = 'no response'
        raise AsyncMessageError(msg)

    message = response.get('message')

    if not response['success']:
        if message == 'abort':
            logger.warning('received abort message')
            raise StopScenario

        raise AsyncMessageError(message)

    return response

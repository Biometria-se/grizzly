"""Unit tests of async_messaged."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, cast

import pytest
from async_messaged import (
    AsyncMessageHandler,
    AsyncMessageRequest,
    AsyncMessageRequestHandler,
    AsyncMessageResponse,
    register,
)

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock import MockerFixture


class TestAsyncMessageHandler:
    def test_get_handler(self) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def get_handler(self, action: str) -> AsyncMessageRequestHandler | None:
                return super().get_handler(action)

            def close(self) -> None:
                pass

        handler = AsyncMessageTest('ID-12345', None)

        assert handler.worker == 'ID-12345'

        with pytest.raises(NotImplementedError, match='get_handler is not implemented'):
            handler.get_handler('TEST')

    def test_handle(self, mocker: MockerFixture) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def a_handler(self, request: AsyncMessageRequest) -> AsyncMessageResponse:  # noqa: ARG002
                return {}

            def get_handler(self, action: str) -> AsyncMessageRequestHandler | None:
                if action == 'NONE':
                    return None

                return cast('AsyncMessageRequestHandler', self.a_handler)

            def close(self) -> None:
                pass

        handler = AsyncMessageTest(worker='asdf-asdf-asdf', event=None)

        request: AsyncMessageRequest = {
            'action': 'NONE',
        }

        response = handler.handle(request)

        assert response.get('success', True) is False
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) == 'NONE: AsyncMessageError="no implementation for NONE"'
        assert response.get('response_time', None) is not None

        mocker.patch.object(
            handler,
            'a_handler',
            side_effect=[
                {
                    'payload': 'test payload',
                    'metadata': {'value': 'hello world'},
                    'response_length': len('test payload'),
                },
            ],
        )

        request.update(
            {
                'action': 'GET',
                'context': {
                    'endpoint': 'TEST.QUEUE',
                },
            },
        )

        response = handler.handle(request)

        assert response.get('success', False) is True
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) is None
        assert response.get('response_time', None) is not None
        assert response.get('response_length') == len('test payload')
        assert response.get('payload') == 'test payload'


def test_register() -> None:
    def handler_a(_: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:  # noqa: ARG001
        return {}

    def handler_b(_: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:  # noqa: ARG001
        return {}

    try:
        from async_messaged.mq import handlers

        actual = list(handlers.keys())
        actual.sort()

        expected = ['CONN', 'RECEIVE', 'SEND', 'PUT', 'GET', 'DISC']
        expected.sort()

        assert actual == expected

        register(handlers, 'TEST')(handler_a)
        register(handlers, 'TEST')(handler_b)

        from async_messaged.mq import handlers

        assert handlers['TEST'] is not handler_b
        assert handlers['TEST'] is handler_a
    finally:
        with suppress(KeyError):
            del handlers['TEST']

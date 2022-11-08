from typing import Optional, cast
from os import environ, path, listdir
from shutil import rmtree
from platform import node as hostname

import pytest

from pytest_mock import MockerFixture
from _pytest.tmpdir import TempPathFactory
from _pytest.capture import CaptureFixture

from grizzly_extras.async_message import (
    AsyncMessageRequestHandler,
    AsyncMessageResponse,
    AsyncMessageRequest,
    AsyncMessageHandler,
    ThreadLogger,
    register,
)

from ...helpers import onerror


class TestAsyncMessageHandler:
    def test_get_handler(self) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
                return super().get_handler(action)

        handler = AsyncMessageTest('ID-12345')

        assert handler.worker == 'ID-12345'

        with pytest.raises(NotImplementedError):
            handler.get_handler('TEST')

    def test_handle(self, mocker: MockerFixture) -> None:
        class AsyncMessageTest(AsyncMessageHandler):
            def a_handler(self, request: AsyncMessageRequest) -> AsyncMessageResponse:
                return {}

            def get_handler(self, action: str) -> Optional[AsyncMessageRequestHandler]:
                if action == 'NONE':
                    return None
                else:
                    return cast(AsyncMessageRequestHandler, self.a_handler)

        handler = AsyncMessageTest(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'NONE',
        }

        response = handler.handle(request)

        assert response.get('success', True) is False
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) == 'NONE: AsyncMessageError="no implementation for NONE"'
        assert response.get('response_time', None) is not None

        mocker.patch.object(handler, 'a_handler', side_effect=[{
            'payload': 'test payload',
            'metadata': {'value': 'hello world'},
            'response_length': len('test payload'),
        }])

        request.update({
            'action': 'GET',
            'context': {
                'endpoint': 'TEST.QUEUE',
            }
        })

        response = handler.handle(request)

        assert response.get('success', False) is True
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) is None
        assert response.get('response_time', None) is not None
        assert response.get('response_length') == len('test payload')
        assert response.get('payload') == 'test payload'


def test_register() -> None:
    def handler_a(i: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:
        return {}

    def handler_b(i: AsyncMessageHandler, request: AsyncMessageRequest) -> AsyncMessageResponse:
        return {}

    try:
        from grizzly_extras.async_message.mq import handlers

        actual = list(handlers.keys())
        actual.sort()

        expected = ['CONN', 'RECEIVE', 'SEND', 'PUT', 'GET']
        expected.sort()

        assert actual == expected

        register(handlers, 'TEST')(handler_a)
        register(handlers, 'TEST')(handler_b)

        from grizzly_extras.async_message.mq import handlers

        assert handlers['TEST'] is not handler_b
        assert handlers['TEST'] is handler_a
    finally:
        try:
            del handlers['TEST']
        except KeyError:
            pass


class TestThreadLogger:
    def test_logger(self, tmp_path_factory: TempPathFactory, capsys: CaptureFixture) -> None:
        test_context = tmp_path_factory.mktemp('test_context') / 'logs'
        test_context.mkdir()
        test_context_root = path.dirname(str(test_context))

        try:
            logger = ThreadLogger('test.logger')
            logger.info('info')
            logger.warning('warning')
            logger.error('error')
            logger.debug('debug')

            std = capsys.readouterr()
            assert '] INFO : test.logger: info\n' in std.err
            assert '] ERROR: test.logger: error\n' in std.err
            assert '] WARNING: test.logger: warning\n' in std.err
            assert '] DEBUG: test.logger: debug\n' not in std.err

            log_files = listdir(str(test_context))
            assert len(log_files) == 0

            environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root
            environ['GRIZZLY_EXTRAS_LOGLEVEL'] = 'DEBUG'

            logger = ThreadLogger('test.logger')
            logger.info('info')
            logger.error('error')
            logger.debug('debug')
            logger.warning('warning')

            std = capsys.readouterr()
            log_files = listdir(str(test_context))
            assert len(log_files) == 1
            log_file = log_files[0]
            assert log_file.startswith(f'async-messaged.{hostname()}')

            with open(path.join(str(test_context), log_file)) as fd:
                file = fd.read()

            for sink in [std.err, file]:
                assert '] INFO : test.logger: info\n' in sink
                assert '] ERROR: test.logger: error\n' in sink
                assert '] WARNING: test.logger: warning\n' in sink
                assert '] DEBUG: test.logger: debug\n' in sink
        finally:
            logger._logger.handlers = []  # force StreamHandler to close log file

            try:
                del environ['GRIZZLY_CONTEXT_ROOT']
            except:
                pass

            try:
                del environ['GRIZZLY_EXTRAS_LOGLEVEL']
            except:
                pass

            rmtree(test_context_root, onerror=onerror)

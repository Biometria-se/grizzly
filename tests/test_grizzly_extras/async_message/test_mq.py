import subprocess

from typing import Any, Optional, Tuple, Dict, cast
from os import environ

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageError
from grizzly_extras.async_message.mq import AsyncMessageQueueHandler


try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi


def test_no_pymqi_dependencies() -> None:
    env = environ.copy()
    del env['LD_LIBRARY_PATH']
    env['PYTHONPATH'] = '.'

    process = subprocess.Popen(
        [
            '/usr/bin/env',
            'python3',
            '-c',
            'import grizzly_extras.async_message.mq as mq; print(f"{mq.pymqi.__name__=}"); mq.AsyncMessageQueueHandler(worker="asdf-asdf-asdf")'
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, _ = process.communicate()
    output = out.decode()
    assert process.returncode == 1
    assert "mq.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
    assert 'NotImplementedError: AsyncMessageQueueHandler could not import pymqi, have you installed IBM MQ dependencies?' in output


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestAsyncMessageQueueHandler:
    def test___init__(self) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        assert handler.worker == 'asdf-asdf-asdf'

    def test_queue_context(self, mocker: MockerFixture) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        def mocked_pymqi_close(p: pymqi.Queue, options: Optional[Any] = None) -> None:
            pass

        mocker.patch.object(
            pymqi.Queue,
            'close',
            mocked_pymqi_close,
        )

        pymqi_queue_spy = mocker.spy(pymqi.Queue, '__init__')
        pymqi_queue_close_spy = mocker.spy(pymqi.Queue, 'close')

        with handler.queue_context('TEST.QUEUE'):
            assert pymqi_queue_spy.call_count == 1
            args, _ = pymqi_queue_spy.call_args_list[0]
            assert args[1] is handler.qmgr
            assert args[2] == 'TEST.QUEUE'

        assert pymqi_queue_close_spy.call_count == 1

        try:
            with handler.queue_context('TEST.QUEUE'):
                assert pymqi_queue_spy.call_count == 2
                args, _ = pymqi_queue_spy.call_args_list[1]
                assert args[1] is handler.qmgr
                assert args[2] == 'TEST.QUEUE'
                # simulate error, but make sure close is called anyway
                raise RuntimeError()
        except RuntimeError:
            pass
        finally:
            assert pymqi_queue_close_spy.call_count == 2


    def test_connect(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.mq import handlers

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        request: AsyncMessageRequest = {
            'action': 'CONN',
        }

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](handler, request)
        assert 'already connected' in str(mqe)

        handler.qmgr = None

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](handler, request)
        assert 'no context' in str(mqe)

        def mocked_connect(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            return pymqi.QueueManager(None)

        mocker.patch.object(
            pymqi,
            'connect',
            mocked_connect,
        )

        pymqi_connect_spy = mocker.spy(pymqi, 'connect')

        request.update({
            'context': {
                'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SYS.CONN',
                'username': 'bob',
                'password': 'secret',
                'channel': 'SYS.CONN',
                'connection': 'mq.example.com(1414)',
                'queue_manager': 'QM1',
            }
        })

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 0
        assert handler.qmgr is not None

        assert pymqi_connect_spy.call_count == 1
        args, _ = pymqi_connect_spy.call_args_list[0]
        assert tuple(args) == ('QM1', 'SYS.CONN', 'mq.example.com(1414)', 'bob', 'secret', )

        request['context'].update({
            'key_file': '/test/key',
            'message_wait': 10,
        })

        mocker.patch.object(
            pymqi.QueueManager,
            'connect_with_options',
            mocked_connect,
        )

        pymqi_sco_spy = mocker.spy(pymqi.SCO, '__init__')
        pymqi_cd_spy = mocker.spy(pymqi.CD, '__init__')
        pymqi_connect_with_options_spy = mocker.spy(pymqi.QueueManager, 'connect_with_options')

        handler.qmgr = None

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 10
        assert handler.qmgr is not None

        assert pymqi_sco_spy.call_count == 1
        _, kwargs = pymqi_sco_spy.call_args_list[0]
        assert kwargs.get('KeyRepository', b'').decode().strip() == '/test/key'
        assert kwargs.get('CertificateLabel', b'').decode().strip() == 'bob'

        assert pymqi_cd_spy.call_count == 1
        _, kwargs = pymqi_cd_spy.call_args_list[0]
        assert kwargs.get('ChannelName', b'').decode().strip() == 'SYS.CONN'
        assert kwargs.get('ConnectionName', b'').decode().strip() == 'mq.example.com(1414)'
        assert kwargs.get('ChannelType', None) == pymqi.CMQC.MQCHT_CLNTCONN
        assert kwargs.get('TransportType', None) == pymqi.CMQC.MQXPT_TCP
        assert kwargs.get('SSLCipherSpec', b'').decode().strip() == 'ECDHE_RSA_AES_256_GCM_SHA384'

        assert pymqi_connect_with_options_spy.call_count == 1
        args, kwargs = pymqi_connect_with_options_spy.call_args_list[0]

        assert args[0] is handler.qmgr
        assert kwargs.get('user', b'').decode().strip() == 'bob'
        assert kwargs.get('password', b'').decode().strip() == 'secret'

        pymqi_sco_spy.reset_mock()
        pymqi_cd_spy.reset_mock()
        pymqi_connect_with_options_spy.reset_mock()

        request['context'].update({
            'ssl_cipher': 'rot13',
            'cert_label': 'test_certificate_label'
        })

        handler.qmgr = None

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 10
        assert handler.qmgr is not None

        assert pymqi_sco_spy.call_count == 1
        _, kwargs = pymqi_sco_spy.call_args_list[0]
        assert kwargs.get('CertificateLabel', b'').decode().strip() == 'test_certificate_label'

        assert pymqi_cd_spy.call_count == 1
        _, kwargs = pymqi_cd_spy.call_args_list[0]
        assert kwargs.get('SSLCipherSpec', b'').decode().strip() == 'rot13'

        assert pymqi_connect_with_options_spy.call_count == 1
        args, kwargs = pymqi_connect_with_options_spy.call_args_list[0]

        assert args[0] is handler.qmgr
        assert kwargs.get('user', b'').decode().strip() == 'bob'
        assert kwargs.get('password', b'').decode().strip() == 'secret'

    def test__create_gmo(self, mocker: MockerFixture) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        pymqi_gmo_spy = mocker.spy(pymqi.GMO, '__init__')

        gmo = handler._create_gmo(11)

        assert pymqi_gmo_spy.call_count == 1
        _, kwargs = pymqi_gmo_spy.call_args_list[0]
        assert kwargs.get('WaitInterval', None) == 11*1000
        assert kwargs.get('Options', None) == pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        assert isinstance(gmo, pymqi.GMO)

    def test__request(self, mocker: MockerFixture) -> None:
        def mocked_pymqi_close(p: pymqi.Queue, options: Optional[Any] = None) -> None:
            pass

        def mocked_pymqi_put(p: pymqi.Queue, payload: Any, md: pymqi.MD) -> None:
            assert payload == 'test payload'

        def mocked_pymqi_get(p: pymqi.Queue, *args: Tuple[Any, ...]) -> bytes:
            return b'test payload'

        mocker.patch.object(
            pymqi.Queue,
            'put',
            mocked_pymqi_put,
        )

        mocker.patch.object(
            pymqi.Queue,
            'get',
            mocked_pymqi_get,
        )

        mocker.patch.object(
            pymqi.Queue,
            'close',
            mocked_pymqi_close,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {}

        with pytest.raises(AsyncMessageError) as mqe:
            handler._request(request)
        assert 'not connected' in str(mqe)

        handler.qmgr = pymqi.QueueManager(None)

        with pytest.raises(AsyncMessageError) as mqe:
            handler._request(request)
        assert 'no queue specified' in str(mqe)

        request.update({
            'action': 'PUT',
            'context': {
                'endpoint': 'TEST.QUEUE',
            },
            'payload': 'test payload'
        })

        response = handler._request(request)
        assert response.get('payload', None) == 'test payload'
        assert response.get('metadata', None) == pymqi.MD().get()
        assert response.get('response_length', 0) == len('test payload')

        handler.message_wait = 0

        request.update({
            'action': 'GET',
            'context': {
                'endpoint': 'TEST.QUEUE',
                'message_wait': 10,
            },
        })

        create_gmo_spy = mocker.spy(handler, '_create_gmo')

        response = handler._request(request)

        assert response.get('payload', None) == 'test payload'
        assert response.get('metadata', None) == pymqi.MD().get()
        assert response.get('response_length', None) == len('test payload')

        assert create_gmo_spy.call_count == 1
        args, _ = create_gmo_spy.call_args_list[0]
        assert args[0] == 10

        del request['context']['message_wait']

        handler.message_wait = 13

        response = handler._request(request)
        assert create_gmo_spy.call_count == 2
        args, _ = create_gmo_spy.call_args_list[1]
        assert args[0] == 13


    def test_put(self, mocker: MockerFixture) -> None:
        def mocked_request(i: AsyncMessageQueueHandler, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'grizzly_extras.async_message.mq.AsyncMessageQueueHandler._request',
            mocked_request,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'SEND',
            'payload': None,
        }

        from grizzly_extras.async_message.mq import handlers

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](handler, request)
        assert 'no payload' in str(mqe)

        request['payload'] = 'test'

        response = cast(AsyncMessageRequest, handlers[request['action']](handler, request))

        assert response['action'] != 'SEND'
        assert response['action'] == 'PUT'

    def test_get(self, mocker: MockerFixture) -> None:
        def mocked_request(i: AsyncMessageQueueHandler, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'grizzly_extras.async_message.mq.AsyncMessageQueueHandler._request',
            mocked_request,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'payload': 'test',
        }

        from grizzly_extras.async_message.mq import handlers

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](handler, request)
        assert 'payload not allowed' in str(mqe)

        request['payload'] = None

        response = cast(AsyncMessageRequest, handlers[request['action']](handler, request))

        assert response['action'] != 'RECEIVE'
        assert response['action'] == 'GET'

    def test_get_handler(self) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        assert handler.get_handler('NONE') is None
        assert handler.get_handler('CONN') is AsyncMessageQueueHandler.connect
        assert handler.get_handler('PUT') is AsyncMessageQueueHandler.put
        assert handler.get_handler('SEND') is AsyncMessageQueueHandler.put
        assert handler.get_handler('GET') is AsyncMessageQueueHandler.get
        assert handler.get_handler('RECEIVE') is AsyncMessageQueueHandler.get


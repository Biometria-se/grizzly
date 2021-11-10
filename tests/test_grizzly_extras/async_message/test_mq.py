import subprocess

from typing import Any, Optional, Tuple, Dict, cast
from os import environ

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture

from grizzly_extras.async_message import AsyncMessageRequest, AsyncMessageResponse, AsyncMessageError
from grizzly_extras.async_message.mq import AsyncMessageQueue, register


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
            'import grizzly_extras.async_message.mq as mq; print(f"{mq.pymqi.__name__=}"); mq.AsyncMessageQueue(worker="asdf-asdf-asdf")'
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, _ = process.communicate()
    output = out.decode()
    assert process.returncode == 1
    assert "mq.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
    assert 'NotImplementedError: AsyncMessageQueue could not import pymqi, have you installed IBM MQ dependencies?' in output


def test_register() -> None:
    def handler_a(i: AsyncMessageQueue, request: AsyncMessageRequest) -> AsyncMessageResponse:
        pass

    def handler_b(i: AsyncMessageQueue, request: AsyncMessageRequest) -> AsyncMessageResponse:
        pass

    try:
        from grizzly_extras.async_message.mq import handlers

        actual = list(handlers.keys())
        actual.sort()

        expected = ['CONN', 'RECEIVE', 'SEND', 'PUT', 'GET']
        expected.sort()

        assert actual == expected

        register('TEST')(handler_a)
        register('TEST')(handler_b)

        from grizzly_extras.async_message.mq import handlers

        assert handlers['TEST'] is not handler_b
        assert handlers['TEST'] is handler_a
    finally:
        try:
            del handlers['TEST']
        except KeyError:
            pass


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestAsyncMessageQueue:
    def test___init__(self) -> None:
        client = AsyncMessageQueue(worker='asdf-asdf-asdf')
        assert client.worker == 'asdf-asdf-asdf'

    def test_queue_context(self, mocker: MockerFixture) -> None:
        client = AsyncMessageQueue(worker='asdf-asdf-asdf')
        client.qmgr = pymqi.QueueManager(None)

        def mocked_pymqi_close(p: pymqi.Queue, options: Optional[Any] = None) -> None:
            pass

        mocker.patch.object(
            pymqi.Queue,
            'close',
            mocked_pymqi_close,
        )

        pymqi_queue_spy = mocker.spy(pymqi.Queue, '__init__')
        pymqi_queue_close_spy = mocker.spy(pymqi.Queue, 'close')

        with client.queue_context('TEST.QUEUE'):
            assert pymqi_queue_spy.call_count == 1
            args, _ = pymqi_queue_spy.call_args_list[0]
            assert args[1] is client.qmgr
            assert args[2] == 'TEST.QUEUE'

        assert pymqi_queue_close_spy.call_count == 1

        try:
            with client.queue_context('TEST.QUEUE'):
                assert pymqi_queue_spy.call_count == 2
                args, _ = pymqi_queue_spy.call_args_list[1]
                assert args[1] is client.qmgr
                assert args[2] == 'TEST.QUEUE'
                # simulate error, but make sure close is called anyway
                raise RuntimeError()
        except RuntimeError:
            pass
        finally:
            assert pymqi_queue_close_spy.call_count == 2


    def test_connect(self, mocker: MockerFixture) -> None:
        from grizzly_extras.async_message.mq import handlers

        client = AsyncMessageQueue(worker='asdf-asdf-asdf')
        client.qmgr = pymqi.QueueManager(None)

        request: AsyncMessageRequest = {
            'action': 'CONN',
        }

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](client, request)
        assert 'already connected' in str(mqe)

        client.qmgr = None

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](client, request)
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

        response = handlers[request['action']](client, request)

        assert response['message'] == 'connected'
        assert client.message_wait_global == 0
        assert client.qmgr is not None

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

        client.qmgr = None

        response = handlers[request['action']](client, request)

        assert response['message'] == 'connected'
        assert client.message_wait_global == 10
        assert client.qmgr is not None

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

        assert args[0] is client.qmgr
        assert kwargs.get('user', b'').decode().strip() == 'bob'
        assert kwargs.get('password', b'').decode().strip() == 'secret'

        pymqi_sco_spy.reset_mock()
        pymqi_cd_spy.reset_mock()
        pymqi_connect_with_options_spy.reset_mock()

        request['context'].update({
            'ssl_cipher': 'rot13',
            'cert_label': 'test_certificate_label'
        })

        client.qmgr = None

        response = handlers[request['action']](client, request)

        assert response['message'] == 'connected'
        assert client.message_wait_global == 10
        assert client.qmgr is not None

        assert pymqi_sco_spy.call_count == 1
        _, kwargs = pymqi_sco_spy.call_args_list[0]
        assert kwargs.get('CertificateLabel', b'').decode().strip() == 'test_certificate_label'

        assert pymqi_cd_spy.call_count == 1
        _, kwargs = pymqi_cd_spy.call_args_list[0]
        assert kwargs.get('SSLCipherSpec', b'').decode().strip() == 'rot13'

        assert pymqi_connect_with_options_spy.call_count == 1
        args, kwargs = pymqi_connect_with_options_spy.call_args_list[0]

        assert args[0] is client.qmgr
        assert kwargs.get('user', b'').decode().strip() == 'bob'
        assert kwargs.get('password', b'').decode().strip() == 'secret'

    def test__create_gmo(self, mocker: MockerFixture) -> None:
        client = AsyncMessageQueue(worker='asdf-asdf-asdf')

        pymqi_gmo_spy = mocker.spy(pymqi.GMO, '__init__')

        gmo = client._create_gmo(11)

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

        client = AsyncMessageQueue(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {}

        with pytest.raises(AsyncMessageError) as mqe:
            client._request(request)
        assert 'not connected' in str(mqe)

        client.qmgr = pymqi.QueueManager(None)

        with pytest.raises(AsyncMessageError) as mqe:
            client._request(request)
        assert 'no queue specified' in str(mqe)

        request.update({
            'action': 'PUT',
            'context': {
                'queue': 'TEST.QUEUE',
            },
            'payload': 'test payload'
        })

        response = client._request(request)
        assert response.get('payload', None) == 'test payload'
        assert response.get('metadata', None) == pymqi.MD().get()
        assert response.get('response_length', 0) == len('test payload')

        client.message_wait_global = 0

        request.update({
            'action': 'GET',
            'context': {
                'queue': 'TEST.QUEUE',
                'message_wait': 10,
            },
        })

        create_gmo_spy = mocker.spy(client, '_create_gmo')

        response = client._request(request)

        assert response.get('payload', None) == 'test payload'
        assert response.get('metadata', None) == pymqi.MD().get()
        assert response.get('response_length', None) == len('test payload')

        assert create_gmo_spy.call_count == 1
        args, _ = create_gmo_spy.call_args_list[0]
        assert args[0] == 10

        del request['context']['message_wait']

        client.message_wait_global = 13

        response = client._request(request)
        assert create_gmo_spy.call_count == 2
        args, _ = create_gmo_spy.call_args_list[1]
        assert args[0] == 13


    def test_put(self, mocker: MockerFixture) -> None:
        def mocked_request(i: AsyncMessageQueue, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'grizzly_extras.async_message.mq.AsyncMessageQueue._request',
            mocked_request,
        )

        client = AsyncMessageQueue(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'SEND',
            'payload': None,
        }

        from grizzly_extras.async_message.mq import handlers

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](client, request)
        assert 'no payload' in str(mqe)

        request['payload'] = 'test'

        response = cast(AsyncMessageRequest, handlers[request['action']](client, request))

        assert response['action'] != 'SEND'
        assert response['action'] == 'PUT'

    def test_get(self, mocker: MockerFixture) -> None:
        def mocked_request(i: AsyncMessageQueue, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'grizzly_extras.async_message.mq.AsyncMessageQueue._request',
            mocked_request,
        )

        client = AsyncMessageQueue(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'payload': 'test',
        }

        from grizzly_extras.async_message.mq import handlers

        with pytest.raises(AsyncMessageError) as mqe:
            handlers[request['action']](client, request)
        assert 'payload not allowed' in str(mqe)

        request['payload'] = None

        response = cast(AsyncMessageRequest, handlers[request['action']](client, request))

        assert response['action'] != 'RECEIVE'
        assert response['action'] == 'GET'

    def test_handler(self, mocker: MockerFixture) -> None:
        client = AsyncMessageQueue(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'NONE',
        }

        response = client.handler(request)

        assert response.get('success', True) == False
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) == 'NONE: AsyncMessageError="no implementation for NONE"'
        assert response.get('response_time', None) is not None

        def mocked_request(i: AsyncMessageQueue, request: AsyncMessageRequest) -> AsyncMessageResponse:
            return {
                'payload': 'test payload',
                'metadata': pymqi.MD().get(),
                'response_length': len('test payload'),
            }

        mocker.patch(
            'grizzly_extras.async_message.mq.AsyncMessageQueue._request',
            mocked_request,
        )

        request.update({
            'action': 'GET',
            'context': {
                'queue': 'TEST.QUEUE',
            }
        })

        response = client.handler(request)

        assert response.get('success', False) == True
        assert response.get('worker', None) == 'asdf-asdf-asdf'
        assert response.get('message', None) is None
        assert response.get('response_time', None) is not None
        assert response.get('response_length') == len('test payload')
        assert response.get('payload') == 'test payload'

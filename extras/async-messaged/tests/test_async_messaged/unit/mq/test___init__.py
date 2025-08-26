"""Unit test of async_messaged.mq."""

from __future__ import annotations

import subprocess
import sys
from contextlib import suppress
from os import environ
from typing import TYPE_CHECKING, Any, ClassVar, cast

try:
    import pymqi
except (ModuleNotFoundError, ImportError):
    from grizzly_common import dummy_pymqi as pymqi

import pytest
from async_messaged import AsyncMessageError, AsyncMessageRequest
from async_messaged.mq import AsyncMessageQueueHandler
from async_messaged.mq.rfh2 import Rfh2Encoder
from async_messaged.utils import tohex
from grizzly_common.transformer import TransformerContentType, transformer

from test_async_messaged.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from pytest_mock.plugin import MockerFixture


def test_no_pymqi_dependencies() -> None:
    env = environ.copy()
    with suppress(KeyError):
        del env['LD_LIBRARY_PATH']
    env['PYTHONPATH'] = '.'

    with pytest.raises(subprocess.CalledProcessError) as e:
        subprocess.check_output(
            [
                sys.executable,
                '-c',
                'import async_messaged.mq as mq; print(f"{mq.pymqi.__name__=}"); mq.AsyncMessageQueueHandler(worker="asdf-asdf-asdf")',
            ],
            env=env,
            stderr=subprocess.STDOUT,
        )
    assert e.value.returncode == 1
    output = e.value.output.decode()
    assert "mq.pymqi.__name__='grizzly_common.dummy_pymqi'" in output
    assert 'NotImplementedError: AsyncMessageQueueHandler could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?' in output

    """
    process = subprocess.Popen(
        [
            sys.executable,
            '-c',
            'import async_messaged.mq as mq; print(f"{mq.pymqi.__name__=}"); mq.AsyncMessageQueueHandler(worker="asdf-asdf-asdf")',
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )

    out, _ = process.communicate()
    output = out.decode()
    assert process.returncode == 1
    assert "mq.pymqi.__name__='grizzly_common.dummy_pymqi'" in output
    assert 'NotImplementedError: AsyncMessageQueueHandler could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?' in output
    """


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_common.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestAsyncMessageQueueHandler:
    def test___init__(self) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        assert handler.worker == 'asdf-asdf-asdf'

        from grizzly_common.dummy_pymqi import raise_for_error

        # pymqi check
        tmp = pymqi.__name__
        pymqi.raise_for_error = raise_for_error
        try:
            pymqi.__name__ = 'grizzly_common.dummy_pymqi'
            with pytest.raises(NotImplementedError, match='could not import pymqi, have you installed IBM MQ dependencies'):
                handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        finally:
            delattr(pymqi, 'raise_for_error')
            pymqi.__name__ = tmp

    def test_close(self, mocker: MockerFixture) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        pymqi_qmgr_disconnect_spy = mocker.patch.object(
            handler.qmgr,
            'disconnect',
            return_value=None,
        )

        handler.close()

        assert pymqi_qmgr_disconnect_spy.call_count == 1

    def test_disconnect(self, mocker: MockerFixture) -> None:
        from async_messaged.mq import handlers

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        pymqi_qmgr_disconnect_spy = mocker.patch.object(
            handler.qmgr,
            'disconnect',
            return_value=None,
        )

        request: AsyncMessageRequest = {
            'action': 'DISC',
        }

        assert handlers[request['action']](handler, request) == {
            'message': 'disconnected',
        }

        assert pymqi_qmgr_disconnect_spy.call_count == 1
        assert handler.qmgr is None

    def test_queue_context(self, mocker: MockerFixture) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        pymqi_queue_close_spy = mocker.patch.object(
            pymqi.Queue,
            'close',
            return_value=None,
        )

        pymqi_queue_spy = mocker.spy(pymqi.Queue, '__init__')

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
                raise RuntimeError
        except RuntimeError:
            pass
        finally:
            assert pymqi_queue_close_spy.call_count == 2

    def test_connect(self, mocker: MockerFixture) -> None:
        from async_messaged.mq import handlers

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')
        handler.qmgr = pymqi.QueueManager(None)

        request: AsyncMessageRequest = {
            'action': 'CONN',
        }

        with pytest.raises(AsyncMessageError, match='no context'):
            handlers[request['action']](handler, request)

        request.update(
            {
                'context': {
                    'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SYS.CONN',
                    'username': 'bob',
                    'password': 'secret',
                    'channel': 'SYS.CONN',
                    'connection': 'mq.example.com(1414)',
                    'queue_manager': 'QM1',
                },
            },
        )

        response = handlers[request['action']](handler, request)
        assert response.get('message', None) == 're-used connection'

        handler.qmgr = None

        pymqi_connect_with_options_spy = mocker.patch.object(
            pymqi.QueueManager,
            'connect_with_options',
            autospec=True,
        )

        pymqi_sco_spy = mocker.spy(pymqi.SCO, '__init__')
        pymqi_cd_spy = mocker.spy(pymqi.CD, '__init__')

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 0
        assert handler.qmgr == ANY(pymqi.QueueManager)

        pymqi_sco_spy.assert_called_once_with(ANY(pymqi.SCO))

        pymqi_cd_spy.assert_called_once_with(
            ANY(pymqi.CD),
            ChannelName=b'SYS.CONN',
            ConnectionName=b'mq.example.com(1414)',
            ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
            TransportType=pymqi.CMQC.MQXPT_TCP,
            HeartbeatInterval=300,
        )

        pymqi_connect_with_options_spy.assert_called_once_with(
            handler.qmgr,
            'QM1',
            cd=ANY(pymqi.CD),
            sco=ANY(pymqi.SCO),
            user=b'bob',
            password=b'secret',
            opts=ANY(int),
        )
        pymqi_sco_spy.reset_mock()
        pymqi_cd_spy.reset_mock()
        pymqi_connect_with_options_spy.reset_mock()
        pymqi_cd_setitem_spy = mocker.spy(pymqi.CD, '__setitem__')

        request['context'].update(
            {
                'key_file': '/test/key',
                'message_wait': 10,
            },
        )

        handler.qmgr = None

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 10
        assert handler.qmgr == ANY(pymqi.QueueManager)

        pymqi_sco_spy.assert_called_once_with(
            ANY(pymqi.SCO),
            KeyRepository=b'/test/key',
            CertificateLabel=b'bob',
        )

        pymqi_cd_spy.assert_called_once_with(
            ANY(pymqi.CD),
            ChannelName=b'SYS.CONN',
            ConnectionName=b'mq.example.com(1414)',
            ChannelType=pymqi.CMQC.MQCHT_CLNTCONN,
            TransportType=pymqi.CMQC.MQXPT_TCP,
            HeartbeatInterval=300,
        )

        pymqi_cd_setitem_spy.assert_called_once_with(ANY(pymqi.CD), 'SSLCipherSpec', b'ECDHE_RSA_AES_256_GCM_SHA384')

        pymqi_connect_with_options_spy.assert_called_once_with(
            handler.qmgr,
            'QM1',
            cd=ANY(pymqi.CD),
            sco=ANY(pymqi.SCO),
            user=b'bob',
            password=b'secret',
            opts=ANY(int),
        )

        pymqi_sco_spy.reset_mock()
        pymqi_cd_spy.reset_mock()
        pymqi_cd_setitem_spy.reset_mock()
        pymqi_connect_with_options_spy.reset_mock()

        request['context'].update(
            {
                'ssl_cipher': 'rot13',
                'cert_label': 'test_certificate_label',
            },
        )

        handler.qmgr = None

        response = handlers[request['action']](handler, request)

        assert response['message'] == 'connected'
        assert handler.message_wait == 10
        assert handler.qmgr == ANY(pymqi.QueueManager)

        pymqi_sco_spy.assert_called_once_with(
            ANY(pymqi.SCO),
            KeyRepository=b'/test/key',
            CertificateLabel=b'test_certificate_label',
        )

        pymqi_cd_spy.assert_called_once()
        pymqi_cd_setitem_spy.assert_called_once_with(ANY(pymqi.CD), 'SSLCipherSpec', b'rot13')

        pymqi_connect_with_options_spy.assert_called_once_with(
            handler.qmgr,
            'QM1',
            cd=ANY(pymqi.CD),
            sco=ANY(pymqi.SCO),
            user=b'bob',
            password=b'secret',
            opts=ANY(int),
        )

    def test__create_gmo(self, mocker: MockerFixture) -> None:
        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        pymqi_gmo_spy = mocker.spy(pymqi.GMO, '__init__')

        gmo = handler._create_gmo(11)

        assert pymqi_gmo_spy.call_count == 1
        _, kwargs = pymqi_gmo_spy.call_args_list[0]
        assert kwargs.get('WaitInterval', None) == (11 * 1000)
        assert kwargs.get('Options', None) == pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        assert isinstance(gmo, pymqi.GMO)

    def test__request(self, mocker: MockerFixture) -> None:  # noqa: PLR0915
        def mocked_pymqi_put(_: pymqi.Queue, payload: Any, _md: pymqi.MD) -> None:
            assert payload == 'test payload'

        def mocked_pymqi_put_rfh2(_: pymqi.Queue, payload: Any, _md: pymqi.MD) -> None:
            assert len(payload) == 284

        mocker.patch.object(
            pymqi.Queue,
            'put',
            mocked_pymqi_put,
        )

        mocked_pymqi_get = mocker.patch.object(
            pymqi.Queue,
            'get',
            return_value=b'test payload',
        )

        mocker.patch.object(
            pymqi.Queue,
            'close',
            return_value=None,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        disconnect_mock = mocker.patch.object(handler, 'disconnect', return_value={})
        connect_mock = mocker.patch.object(handler, 'connect', return_value={})

        handler_queue_context = mocker.spy(handler, 'queue_context')

        request: AsyncMessageRequest = {}

        with pytest.raises(AsyncMessageError, match='not connected'):
            handler._request(request)

        handler.qmgr = pymqi.QueueManager(None)

        mocker.patch.object(handler.qmgr, 'backout', return_value=None)
        mocked_qmgr_backout = mocker.spy(handler.qmgr, 'backout')
        mocker.patch.object(handler.qmgr, 'commit', return_value=None)
        mocked_qmgr_commit = mocker.spy(handler.qmgr, 'commit')

        with pytest.raises(AsyncMessageError, match='no endpoint specified'):
            handler._request(request)

        request.update(
            {
                'action': 'PUT',
                'context': {
                    'endpoint': 'queue:TEST.QUEUE',
                },
                'payload': 'test payload',
            },
        )

        handler.header_type = None
        response = handler._request(request)
        assert response.get('payload', None) == 'test payload'
        actual_metadata = pymqi.MD().get()
        actual_metadata['MsgId'] = tohex(actual_metadata['MsgId'])
        assert response.get('metadata', None) == actual_metadata
        assert response.get('response_length', 0) == len(b'test payload')

        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        mocker.patch.object(
            pymqi.Queue,
            'put',
            mocked_pymqi_put_rfh2,
        )

        handler.header_type = 'rfh2'
        response = handler._request(request)

        assert mocked_qmgr_commit.call_count == 0
        assert mocked_qmgr_backout.call_count == 0
        actual_metadata = Rfh2Encoder.create_md().get()
        actual_metadata['MsgId'] = tohex(actual_metadata['MsgId'])
        assert response.get('metadata', None) == actual_metadata
        assert response.get('response_length', 0) == 284

        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        handler.header_type = 'somethingWeird'
        with pytest.raises(AsyncMessageError, match='Invalid header_type: somethingWeird'):
            handler._request(request)

        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_not_called()
        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        handler.message_wait = 0

        request.update(
            {
                'action': 'GET',
                'context': {
                    'endpoint': 'queue:TEST.QUEUE',
                    'message_wait': 10,
                },
            },
        )

        create_gmo_spy = mocker.spy(handler, '_create_gmo')

        handler.header_type = None
        response = handler._request(request)
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()
        mocked_qmgr_backout.assert_not_called()

        assert response.get('payload', None) == 'test payload'
        actual_metadata = pymqi.MD().get()
        actual_metadata['MsgId'] = tohex(actual_metadata['MsgId'])
        assert response.get('metadata', None) == actual_metadata
        assert response.get('response_length', None) == len('test payload')

        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        mocked_pymqi_get.assert_called_once_with(
            None,
            ANY(pymqi.MD),
            ANY(pymqi.GMO),
        )
        mocked_pymqi_get.reset_mock()

        create_gmo_spy.assert_called_once_with(10)
        create_gmo_spy.reset_mock()

        mocked_pymqi_get = mocker.patch.object(
            pymqi.Queue,
            'get',
            return_value=(
                b'RFH \x02\x00\x00\x00\xfc\x00\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> '
                b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>   \\\x00\x00\x00<usr><ContentEncoding>gzip</ContentEncoding>'
                b"<ContentLength dt='i8'>32</ContentLength></usr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00"
            ),
        )

        request['context'].update({'endpoint': 'queue:TEST.QUEUE, max_message_size:13337'})

        handler.header_type = 'rfh2'
        response = handler._request(request)

        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()
        mocked_qmgr_backout.assert_not_called()
        assert response.get('payload', None) == 'test payload'
        actual_metadata = Rfh2Encoder.create_md().get()
        actual_metadata['MsgId'] = tohex(actual_metadata['MsgId'])
        assert response.get('metadata', None) == actual_metadata
        assert response.get('response_length', None) == len(b'test payload')

        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        mocked_pymqi_get.assert_called_once_with(
            13337,
            ANY(pymqi.MD),
            ANY(pymqi.GMO),
        )
        mocked_pymqi_get.reset_mock()

        create_gmo_spy.assert_called_once_with(10)
        create_gmo_spy.reset_mock()

        del request['context']['message_wait']

        handler.message_wait = 13

        response = handler._request(request)
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()
        mocked_qmgr_backout.assert_not_called()

        handler_queue_context.assert_called_once_with(
            endpoint='TEST.QUEUE',
        )
        handler_queue_context.reset_mock()

        create_gmo_spy.assert_called_once_with(13)
        create_gmo_spy.reset_mock()

        mocker.patch.object(
            pymqi.Queue,
            'get',
            side_effect=[pymqi.MQMIError(pymqi.CMQC.MQCC_FAILED, pymqi.CMQC.MQRC_UNEXPECTED_ERROR)],
        )
        with pytest.raises(AsyncMessageError, match='MQI Error. Comp: 2, Reason 2195: FAILED: MQRC_UNEXPECTED_ERROR'):
            response = handler._request(request)

        mocked_qmgr_commit.assert_not_called()
        mocked_qmgr_backout.assert_called_once_with()
        mocked_qmgr_backout.reset_mock()

        disconnect_mock.assert_not_called()
        connect_mock.assert_not_called()

        # throwing PYIFError, with non-matching error message
        mocker.patch.object(
            pymqi.Queue,
            'get',
            side_effect=[pymqi.PYIFError('foobar')],
        )

        with pytest.raises(AsyncMessageError, match='PYMQI Error: foobar'):
            handler._request(request)

        disconnect_mock.assert_not_called()
        connect_mock.assert_not_called()

        # disconnected during GET
        mocker.patch.object(
            pymqi.Queue,
            'get',
            side_effect=[
                pymqi.PYIFError('not open'),
                (
                    b'RFH \x02\x00\x00\x00\xfc\x00\x00\x00"\x02\x00\x00\xb8\x04\x00\x00        \x00\x00\x00\x00\xb8\x04\x00\x00 \x00\x00\x00<mcd><Msd>jms_bytes</Msd></mcd> '
                    b'P\x00\x00\x00<jms><Dst>queue:///TEST.QUEUE</Dst><Tms>1655406556138</Tms><Dlv>2</Dlv></jms>   \\\x00\x00\x00<usr><ContentEncoding>gzip</ContentEncoding>'
                    b"<ContentLength dt='i8'>32</ContentLength></usr> \x1f\x8b\x08\x00\xdc\x7f\xabb\x02\xff+I-.Q(H\xac\xcc\xc9OL\x01\x00\xe1=\x1d\xeb\x0c\x00\x00\x00"
                ),
            ],
        )

        response = handler._request(request)
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()
        assert mocked_qmgr_backout.call_count == 2
        mocked_qmgr_backout.reset_mock()
        assert response.get('payload', None) == 'test payload'
        actual_metadata = Rfh2Encoder.create_md().get()
        actual_metadata['MsgId'] = tohex(actual_metadata['MsgId'])
        assert response.get('metadata', None) == actual_metadata
        assert response.get('response_length', None) == len(b'test payload')

        disconnect_mock.assert_called_once_with({})
        connect_mock.assert_called_once_with(request)

        disconnect_mock.reset_mock()
        connect_mock.reset_mock()

    def test__request_with_expressions(self, mocker: MockerFixture) -> None:  # noqa: PLR0915
        # Mocked representation of an pymqi Queue message
        class DummyMessage:
            def __init__(self, payload: str) -> None:
                self.payload = payload

            def decode(self) -> str:
                return self.payload

        queue_messages = {
            'id1': DummyMessage(
                """
                <root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
                <actors>
                    <actor id="1">Christian Bale</actor>
                    <actor id="2">Liam Neeson</actor>
                    <actor id="3">Michael Caine</actor>
                </actors>
                <foo:singers>
                    <foo:singer id="4">Tom Waits</foo:singer>
                    <foo:singer id="5">B.B. King</foo:singer>
                    <foo:singer id="6">Ray Charles</foo:singer>
                </foo:singers>
                </root>
                """,
            ),
            'id2': DummyMessage(
                """
                <root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
                <actors>
                    <actor id="4">Christian Bale</actor>
                    <actor id="5">Liam Neeson</actor>
                    <actor id="6">Michael Caine</actor>
                </actors>
                <foo:singers>
                    <foo:singer id="7">Tom Waits</foo:singer>
                    <foo:singer id="8">B.B. King</foo:singer>
                    <foo:singer id="9">Ray Charles</foo:singer>
                </foo:singers>
                </root>
                """,
            ),
        }

        queue_msg_ids = [
            'id1',
            'id2',
        ]

        # Mocked representation of pymqi Queue
        class DummyQueue:
            # List with (comp, reason) errors to raise, -1 in comp means skip
            error_list: ClassVar[list[tuple[int, int]]] = []

            def __init__(self) -> None:
                self.msg_ix = 0

            def close(self) -> None:
                pass

            def get(self, _: Any, md: pymqi.MD, gmo: pymqi.GMO) -> DummyMessage:
                if len(DummyQueue.error_list) > 0:
                    next_error = DummyQueue.error_list.pop(0)
                    if next_error[0] != -1:
                        raise pymqi.MQMIError(next_error[0], next_error[1])

                if gmo['MatchOptions'] == pymqi.CMQC.MQMO_MATCH_MSG_ID:
                    # Request for getting specific message
                    return queue_messages[md['MsgId'].decode()]

                # Normal request - return next message
                if self.msg_ix >= len(queue_msg_ids):
                    raise pymqi.MQMIError(pymqi.CMQC.MQCC_FAILED, pymqi.CMQC.MQRC_NO_MSG_AVAILABLE)
                msg = queue_messages[queue_msg_ids[self.msg_ix]]
                md['MsgId'] = bytearray(queue_msg_ids[self.msg_ix].encode())
                self.msg_ix += 1
                return msg

        def mocked_queue(*_args: Any, **_kvargs: Any) -> DummyQueue:
            return DummyQueue()

        mocker.patch('pymqi.Queue', mocked_queue)

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        handler.qmgr = pymqi.QueueManager(None)

        mocker.patch.object(handler.qmgr, 'backout', return_value=None)
        mocked_qmgr_backout = mocker.spy(handler.qmgr, 'backout')
        mocker.patch.object(handler.qmgr, 'commit', return_value=None)
        mocked_qmgr_commit = mocker.spy(handler.qmgr, 'commit')

        request: AsyncMessageRequest = {
            'action': 'GET',
            'payload': None,
            'context': {
                'url': 'mq://hej',
                'queue_manager': 'theqmanager',
                'connection': 'theconnection',
                'channel': 'thechannel',
                'username': 'theusername',
                'password': 'thepassword',
                'key_file': 'thekeyfile',
                'cert_label': 'thecertlabel',
                'ssl_cipher': 'thecipher',
                'message_wait': 1,
                'endpoint': "queue:theendpoint, expression: //actor[@id='3']",
                'content_type': 'xml',
            },
        }

        from async_messaged.mq import handlers

        # Match first message
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id1'].payload
        assert response['response_length'] == len(queue_messages['id1'].payload)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()

        # Match second message
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='9']"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id2'].payload
        assert response['response_length'] == len(queue_messages['id2'].payload)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()

        # Match no message = timeout
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='NOTEXIST']"
        with pytest.raises(AsyncMessageError, match='timeout while waiting for matching message'):
            response = handlers[request['action']](handler, request)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_not_called()

        # Match no message, and no wait time = exception
        tmp_wait = request['context']['message_wait']
        del request['context']['message_wait']
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='NOTEXIST']"
        with pytest.raises(AsyncMessageError, match='no matching message found'):
            response = handlers[request['action']](handler, request)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_not_called()
        request['context']['message_wait'] = tmp_wait

        # Invalid XML
        tmp_xml = queue_messages['id1'].payload
        queue_messages['id1'].payload = 'xxx'
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='3']"
        with pytest.raises(AsyncMessageError, match='failed to transform input as XML'):
            response = handlers[request['action']](handler, request)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_not_called()

        queue_messages['id1'].payload = tmp_xml

        # Queue.get returning unexpected error
        DummyQueue.error_list.append((pymqi.CMQC.MQCC_FAILED, pymqi.CMQC.MQRC_SSL_INITIALIZATION_ERROR))
        with pytest.raises(AsyncMessageError, match='MQI Error. Comp: 2, Reason 2393: FAILED: MQRC_SSL_INITIALIZATION_ERROR'):
            response = handlers[request['action']](handler, request)
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_not_called()

        # Match second message, but fail with MQRC_TRUNCATED_MSG_FAILED at first attempt (yields retry)
        DummyQueue.error_list.append((pymqi.CMQC.MQCC_WARNING, pymqi.CMQC.MQRC_TRUNCATED_MSG_FAILED))
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='9']"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id2'].payload
        assert response['response_length'] == len(queue_messages['id2'].payload)
        assert len(DummyQueue.error_list) == 0
        mocked_qmgr_backout.assert_not_called()
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()

        # Match second message, but fail with MQRC_TRUNCATED_MSG_FAILED when trying to fetch it by id (yields retry)
        # No error for _find_message get #1
        DummyQueue.error_list.append((-1, -1))
        # No error for _find_message get #2
        DummyQueue.error_list.append((-1, -1))
        # Error thrown for _request get with message id --> retries and finally gets the message
        DummyQueue.error_list.append((pymqi.CMQC.MQCC_WARNING, pymqi.CMQC.MQRC_TRUNCATED_MSG_FAILED))
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='9']"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id2'].payload
        assert response['response_length'] == len(queue_messages['id2'].payload)
        mocked_qmgr_backout.assert_called_once_with()
        mocked_qmgr_backout.reset_mock()
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()

        # Match second message, but fail with MQRC_NO_MSG_AVAILABLE when trying to fetch it by id (yields retry)
        # No error for _find_message get #1
        DummyQueue.error_list.append((-1, -1))
        # No error for _find_message get #2
        DummyQueue.error_list.append((-1, -1))
        # Error thrown for _request get with message id --> retries and finally gets the message
        DummyQueue.error_list.append((pymqi.CMQC.MQCC_FAILED, pymqi.CMQC.MQRC_NO_MSG_AVAILABLE))
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='9']"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id2'].payload
        assert response['response_length'] == len(queue_messages['id2'].payload)
        mocked_qmgr_backout.assert_called_once_with()
        mocked_qmgr_backout.reset_mock()
        mocked_qmgr_commit.assert_called_once_with()
        mocked_qmgr_commit.reset_mock()

        # Match second message, but fail with unexpected error when trying to fetch it by id (throws error)
        # No error for _find_message get #1
        DummyQueue.error_list.append((-1, -1))
        # No error for _find_message get #2
        DummyQueue.error_list.append((-1, -1))
        # Error thrown for _request get with message id --> retries and finally gets the message
        DummyQueue.error_list.append((pymqi.CMQC.MQCC_FAILED, pymqi.CMQC.MQRC_UNEXPECTED_ERROR))
        request['context']['endpoint'] = "queue:theendpoint, expression: //singer[@id='9']"
        with pytest.raises(AsyncMessageError, match='MQI Error. Comp: 2, Reason 2195: FAILED: MQRC_UNEXPECTED_ERROR'):
            response = handlers[request['action']](handler, request)
        mocked_qmgr_backout.assert_called_once_with()
        mocked_qmgr_backout.reset_mock()
        mocked_qmgr_commit.assert_not_called()

        # Test Json
        queue_messages = {
            'id1': DummyMessage(
                """
                {
                    "actors": [
                        { "id": "1", "name": "Peter Stormare" },
                        { "id": "2", "name": "Pernilla August" },
                        { "id": "3", "name": "Stellan Skarsgård" }
                    ],
                    "singers": [
                        { "id": "4", "name": "Tom Waits" },
                        { "id": "5", "name": "B.B. King" },
                        { "id": "6", "name": "Ray Charles" }
                    ]
                }
                """,
            ),
            'id2': DummyMessage(
                """
                {
                    "actors": [
                        { "id": "4", "name": "Christian Bale" },
                        { "id": "5", "name": "Liam Neeson" },
                        { "id": "6", "name": "Michael Caine" }
                    ],
                    "singers": [
                        { "id": "7", "name": "Tom Waits" },
                        { "id": "8", "name": "B.B. King" },
                        { "id": "9", "name": "Ray Charles" }
                    ]
                }
                """,
            ),
        }

        request['context']['content_type'] = 'json'

        # Match second message
        request['context']['endpoint'] = "queue:theendpoint, expression: $.singers[?(@.id='9')]"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id2'].payload
        assert response['response_length'] == len(queue_messages['id2'].payload)

        # Match first message
        request['context']['endpoint'] = "queue:theendpoint, expression: '$.actors[?(@.name='Pernilla August')]'"
        response = handlers[request['action']](handler, request)
        assert response['payload'] == queue_messages['id1'].payload
        assert response['response_length'] == len(queue_messages['id1'].payload.encode())

        # Match no message = timeout
        request['context']['endpoint'] = "queue:theendpoint, expression: $.singers[?(@.id='NOTEXIST')]"
        with pytest.raises(AsyncMessageError, match='timeout while waiting for matching message'):
            handlers[request['action']](handler, request)

        # unsupported arguments
        request['context']['endpoint'] = "queue:theendpoint, expression: $.singers[?(@.id='NOTEXIST')], argument=False"
        with pytest.raises(AsyncMessageError, match='incorrect format for argument: "argument=False"'):
            handlers[request['action']](handler, request)

        request['context']['endpoint'] = "queue:theendpoint, expression: $.singers[?(@.id='NOTEXIST')], argument:False"
        with pytest.raises(AsyncMessageError, match='arguments argument is not supported'):
            handlers[request['action']](handler, request)

        # invalid content type
        request['context']['endpoint'] = "queue:theendpoint, expression: $.singers[?(@.id='NOTEXIST')]"
        request['context']['content_type'] = 'garbage'
        with pytest.raises(ValueError, match='is an unknown response content type'):
            handlers[request['action']](handler, request)

        request['context']['content_type'] = 'json'
        json_transformer = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        with pytest.raises(AsyncMessageError, match='could not find a transformer for JSON'):
            handlers[request['action']](handler, request)

        transformer.available.update({TransformerContentType.JSON: json_transformer})

        # invalid expression
        request['context']['endpoint'] = 'queue:theendpoint, expression: json_blah'
        with pytest.raises(AsyncMessageError, match='unable to parse'):
            handlers[request['action']](handler, request)

        # expression with wrong action
        request = {
            'action': 'PUT',
            'context': {
                'endpoint': 'queue:TEST.QUEUE, expression: $.singers',
            },
            'payload': 'test payload',
        }
        with pytest.raises(AsyncMessageError, match='argument expression is not allowed for action'):
            handler._request(request)

    def test_put(self, mocker: MockerFixture) -> None:
        def mocked_request(_: AsyncMessageQueueHandler, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'async_messaged.mq.AsyncMessageQueueHandler._request',
            mocked_request,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'SEND',
            'payload': None,
        }

        from async_messaged.mq import handlers

        with pytest.raises(AsyncMessageError, match='no payload'):
            handlers[request['action']](handler, request)

        request['payload'] = 'test'

        response = cast('AsyncMessageRequest', handlers[request['action']](handler, request))

        assert response['action'] != 'SEND'
        assert response['action'] == 'PUT'

    def test_get(self, mocker: MockerFixture) -> None:
        def mocked_request(_: AsyncMessageQueueHandler, request: AsyncMessageRequest) -> AsyncMessageRequest:
            return request

        mocker.patch(
            'async_messaged.mq.AsyncMessageQueueHandler._request',
            mocked_request,
        )

        handler = AsyncMessageQueueHandler(worker='asdf-asdf-asdf')

        request: AsyncMessageRequest = {
            'action': 'RECEIVE',
            'payload': 'test',
        }

        from async_messaged.mq import handlers

        with pytest.raises(AsyncMessageError, match='payload not allowed'):
            handlers[request['action']](handler, request)

        request['payload'] = None

        response = cast('AsyncMessageRequest', handlers[request['action']](handler, request))

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

import subprocess

from os import environ
from typing import Optional, Callable, cast
from json import dumps as jsondumps

import pytest
import zmq

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from behave.runner import Context

from grizzly.testdata.variables import AtomicMessageQueue
from grizzly.testdata.variables.messagequeue import atomicmessagequeue__base_type__
from grizzly.context import GrizzlyContext
from grizzly_extras.async_message import AsyncMessageResponse

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from ...fixtures import behave_context, locust_environment, noop_zmq  # pylint: disable=unused-import


def test_atomicmessagequeue__base_type__() -> None:
    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__('TEST.QUEUE')
    assert 'AtomicMessageQueue: initial value must contain arguments' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__('|')
    assert 'AtomicMessageQueue: incorrect format in arguments: ""' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__('| url=""')
    assert 'AtomicMessageQueue: incorrect format in arguments: ""' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__('queue: | url=""')
    assert 'AtomicMessageQueue: invalid value for argument "queue"' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__('queue:TEST.QUEUE | argument=False')
    assert 'AtomicMessageQueue: url parameter must be specified' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__(
            'queue:TEST.QUEUE|url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", argument=False',
        )
    assert 'AtomicMessageQueue: argument argument is not allowed' in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__(
            'queue:TEST.QUEUE|url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", repeat=True, wait=asdf',
        )
    assert "invalid literal for int() with base 10: 'asdf'" in str(ve)

    with pytest.raises(ValueError) as ve:
        atomicmessagequeue__base_type__(
            'kurt:TEST.QUEUE|url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", repeat=True, wait=20',
        )
    assert 'AtomicMessageQueue: queue name must be prefixed with queue:' in str(ve)

    safe_value = atomicmessagequeue__base_type__(
        'queue:TEST.QUEUE|url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", repeat=True, wait=20',
    )
    assert safe_value == 'queue:TEST.QUEUE | url="mq://mq.example.com/?QueueManager=QM1&Channel=SRV.CONN", repeat=True, wait=20'

class TestAtomicMessageQueueNoPymqi:
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        del env['LD_LIBRARY_PATH']
        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            [
                '/usr/bin/env',
                'python3',
                '-c'
                'import grizzly.testdata.variables.messagequeue as mq; print(f"{mq.pymqi.__name__=}"); mq.AtomicMessageQueue("test", "test");',
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, _ = process.communicate()
        output = out.decode()

        assert process.returncode == 1
        assert "mq.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
        assert 'NotImplementedError: AtomicMessageQueue could not import pymqi, have you installed IBM MQ dependencies?' in output

@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestAtomicMessageQueue:
    def test___init__(self, mocker: MockerFixture) -> None:
        mocker.patch(
            'grizzly.testdata.variables.messagequeue.AtomicMessageQueue.create_client',
            side_effect=[{'client': True}] * 3,
        )

        try:
            v = AtomicMessageQueue(
                'test1',
                'queue:TEST1.QUEUE | url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )

            assert v._initialized
            assert 'test1' in v._values
            assert v._values.get('test1', None) == 'queue:TEST1.QUEUE'
            assert v._endpoint_messages.get('test1', None) == []
            assert v._settings.get('test1', None) == {
                'repeat': False,
                'wait': None,
                'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN',
                'context': None,
                'worker': None,
            }
            assert v._endpoint_clients.get('test1', None) is not None
            assert isinstance(v._zmq_context, zmq.Context)

            t = AtomicMessageQueue(
                'test2',
                'queue:TEST2.QUEUE | url="mq://mq.example.com?QueueManager=QM2&Channel=SRV.CONN", wait=15',
            )

            assert v is t
            assert len(v._values.keys()) == 2
            assert len(v._endpoint_messages.keys()) == 2
            assert len(v._settings.keys()) == 2
            assert len(v._endpoint_clients.keys()) == 2
            assert 'test2' in v._values
            assert v._values.get('test2', None) == 'queue:TEST2.QUEUE'
            assert v._endpoint_messages.get('test2', None) == []
            assert v._settings.get('test2', None) == {
                'repeat': False,
                'wait': 15,
                'url': 'mq://mq.example.com?QueueManager=QM2&Channel=SRV.CONN',
                'context': None,
                'worker': None,
            }
            assert v._endpoint_clients.get('test2', None) is not None
        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

    @pytest.mark.usefixtures('behave_context')
    def test_create_context(self, behave_context: Context) -> None:
        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        grizzly.state.configuration.update({
            'mq.username': 'mq_test',
            'mq.password': 'password',
            'mq.queue_manager': 'QM1',
            'mq.channel': 'SRV.CONN',
            'mq.host': 'mq.example.com',
        })

        settings = {
            'url': 'mq://$conf::mq.username:$conf::mq.password@$conf::mq.host?QueueManager=$conf::mq.queue_manager&Channel=$conf::mq.channel',
            'wait': 13,
        }

        try:
            context = AtomicMessageQueue.create_context(settings)

            assert context == {
                'url': settings['url'],
                'connection': 'mq.example.com(1414)',
                'queue_manager': 'QM1',
                'channel': 'SRV.CONN',
                'username': 'mq_test',
                'password': 'password',
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
                'message_wait': 13,
            }

            settings = {
                'url': 'http://mq.example.com',
            }

            with pytest.raises(ValueError) as ve:
                AtomicMessageQueue.create_context(settings)
            assert 'AtomicMessageQueue: "http" is not a supported scheme for url' in str(ve)

            settings = {
                'url': 'mq:///',
            }

            with pytest.raises(ValueError) as ve:
                AtomicMessageQueue.create_context(settings)
            assert 'AtomicMessageQueue: hostname is not specified in "mq:///"' in str(ve)

            settings = {
                'url': 'mq://mq.example.com',
            }

            with pytest.raises(ValueError) as ve:
                AtomicMessageQueue.create_context(settings)
            assert 'AtomicMessageQueue: QueueManager and Channel must be specified in the query string of "mq://mq.example.com"' in str(ve)

            settings = {
                'url': 'mq://mq.example.com:1415?Channel=SRV.CONN',
            }

            with pytest.raises(ValueError) as ve:
                AtomicMessageQueue.create_context(settings)
            assert 'AtomicMessageQueue: QueueManager must be specified in the query string' in str(ve)

            settings = {
                'url': 'mq://mq.example.com:1415?QueueManager=QM1',
            }

            with pytest.raises(ValueError) as ve:
                AtomicMessageQueue.create_context(settings)
            assert 'AtomicMessageQueue: Channel must be specified in the query string' in str(ve)

            settings = {
                'url': 'mq://mq.example.com:1415?QueueManager=QM1&Channel=SRV.CONN&KeyFile=mq_test',
            }

            context = AtomicMessageQueue.create_context(settings)

            assert context == {
                'url': settings['url'],
                'connection': 'mq.example.com(1415)',
                'queue_manager': 'QM1',
                'channel': 'SRV.CONN',
                'username': None,
                'password': None,
                'key_file': 'mq_test',
                'cert_label': None,
                'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': None,
            }

            settings = {
                'url': 'mqs://mq_test:password@mq.example.com:1415?QueueManager=QM1&Channel=SRV.CONN&SslCipher=rot13&CertLabel=ibmmqmmq_test',
                'wait': 18,
            }

            context = AtomicMessageQueue.create_context(settings)

            assert context == {
                'url': settings['url'],
                'connection': 'mq.example.com(1415)',
                'queue_manager': 'QM1',
                'channel': 'SRV.CONN',
                'username': 'mq_test',
                'password': 'password',
                'key_file': 'mq_test',
                'cert_label': 'ibmmqmmq_test',
                'ssl_cipher': 'rot13',
                'message_wait': 18,
            }
        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass



    @pytest.mark.usefixtures('noop_zmq')
    def test_create_client(self, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        try:
            v = AtomicMessageQueue(
                'test',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )
            assert isinstance(v._endpoint_clients.get('test', None), zmq.Socket)
            assert v._settings.get('test', None) == {
                'repeat': True,
                'wait': None,
                'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN',
                'worker': None,
                'context': {
                    'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN',
                    'connection': 'mq.example.com(1414)',
                    'queue_manager': 'QM1',
                    'channel': 'SRV.CONN',
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None,
                    'message_wait': None,
                }
            }

        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

    @pytest.mark.usefixtures('noop_zmq')
    def test_clear(self, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        try:
            v = AtomicMessageQueue(
                'test1',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )
            v = AtomicMessageQueue(
                'test2',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )

            assert len(v._settings.keys()) == 2
            assert len(v._endpoint_messages.keys()) == 2
            assert len(v._endpoint_clients.keys()) == 2
            assert len(v._values.keys()) == 2

            AtomicMessageQueue.clear()

            assert len(v._settings.keys()) == 0
            assert len(v._endpoint_messages.keys()) == 0
            assert len(v._endpoint_clients.keys()) == 0
            assert len(v._values.keys()) == 0
        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

    def test___getitem__(self, mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        def mock_response(response: Optional[AsyncMessageResponse], repeat: int = 1) -> None:
            mocker.patch(
                'grizzly.testdata.variables.messagequeue.zmq.sugar.socket.Socket.recv_json',
                side_effect=[zmq.Again(), response] * repeat
            )

        gsleep_spy = mocker.patch(
            'grizzly.testdata.variables.messagequeue.gsleep',
            autospec=True,
        )

        try:
            mock_response(None)

            v = AtomicMessageQueue(
                'test',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )

            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'AtomicMessageQueue.test: no response when trying to connect' in str(re)
            assert gsleep_spy.call_count == 1

            AtomicMessageQueue.destroy()

            mock_response({
                'success': False,
                'message': 'testing testing',
            })

            v = AtomicMessageQueue(
                'test',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )
            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'testing testing' in str(re)
            assert gsleep_spy.call_count == 2
            assert v._settings['test'].get('worker', None) is None

            # simulate that we have connected
            v._settings['test']['worker'] = '1337-aaaabbbb-beef'

            mock_response(None)

            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'AtomicMessageQueue.test: unknown error, no response' in str(re)
            assert gsleep_spy.call_count == 3
            assert v._settings.get('test', None) == {
                'repeat': True,
                'wait': None,
                'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN',
                'worker': '1337-aaaabbbb-beef',
                'context': {
                    'url': 'mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN',
                    'connection': 'mq.example.com(1414)',
                    'queue_manager': 'QM1',
                    'channel': 'SRV.CONN',
                    'username': None,
                    'password': None,
                    'key_file': None,
                    'cert_label': None,
                    'ssl_cipher': None,
                    'message_wait': None,
                }
            }

            mock_response({
                'success': False,
                'message': 'something something MQRC_NO_MSG_AVAILABLE something something',
            }, 6)

            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'something something MQRC_NO_MSG_AVAILABLE something something' in str(re)

            v._endpoint_messages['test'].append('hello world')
            v._endpoint_messages['test'].append('world hello')

            assert v['test'] == 'hello world'
            assert v['test'] == 'world hello'
            assert v['test'] == 'hello world'
            assert v['test'] == 'world hello'

            v._endpoint_messages['test'].clear()

            v._settings['test']['repeat'] = False

            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'something something MQRC_NO_MSG_AVAILABLE something something' in str(re)

            mock_response({
                'success': True,
                'payload': None,
            })

            with pytest.raises(RuntimeError) as re:
                v['test']
            assert 'AtomicMessageQueue.test: payload in response was None' in str(re)

            mock_response({
                'success': True,
                'payload': jsondumps({
                    'test': {
                        'result': 'hello world',
                    },
                }),
            }, 3)

            v._settings['test']['repeat'] = False

            assert len(v._endpoint_messages['test']) == 0
            assert v['test'] == jsondumps({'test': {'result': 'hello world'}})
            assert len(v._endpoint_messages['test']) == 0

            v._settings['test']['repeat'] = True
            assert v['test'] == jsondumps({'test': {'result': 'hello world'}})
            assert len(v._endpoint_messages['test']) == 1
            assert v._endpoint_messages['test'][0] == jsondumps({'test': {'result': 'hello world'}})

            send_json_spy = mocker.spy(zmq.sugar.socket.Socket, 'send_json')
            v._settings['test']['content_type'] = 'xml'
            v['test']
            send_json_spy.assert_called_once()
            arg_in = send_json_spy.call_args_list[0][0][1]
            assert 'context' in arg_in
            assert 'content_type' in arg_in['context']
            assert arg_in['context']['content_type'] == 'xml'
            send_json_spy.reset_mock()

        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

    @pytest.mark.usefixtures('noop_zmq')
    def test___setitem__(self, mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        def mocked___getitem__(i: AtomicMessageQueue, variable: str) -> Optional[str]:
            return i._get_value(variable)

        mocker.patch(
            'grizzly.testdata.variables.messagequeue.AtomicMessageQueue.__getitem__',
            mocked___getitem__,
        )

        try:
            v = AtomicMessageQueue(
                'test1',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )

            assert v['test1'] == 'queue:TEST.QUEUE'
            v['test1'] = 'we <3 ibm mq'
            assert v['test1'] == 'queue:TEST.QUEUE'
        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

    @pytest.mark.usefixtures('noop_zmq')
    def test___delitem__(self, mocker: MockerFixture, noop_zmq: Callable[[str], None]) -> None:
        noop_zmq('grizzly.testdata.variables.messagequeue')

        def mocked___getitem__(i: AtomicMessageQueue, variable: str) -> Optional[str]:
            return i._get_value(variable)

        mocker.patch(
            'grizzly.testdata.variables.messagequeue.AtomicMessageQueue.__getitem__',
            mocked___getitem__,
        )

        try:
            v = AtomicMessageQueue(
                'test1',
                'queue:TEST.QUEUE | repeat=True, url="mq://mq.example.com?QueueManager=QM1&Channel=SRV.CONN"',
            )

            assert v['test1'] == 'queue:TEST.QUEUE'
            assert len(v._values.keys()) == 1
            del v['test1']
            assert len(v._values.keys()) == 0
            del v['test1']
            del v['asdf']
        finally:
            try:
                AtomicMessageQueue.destroy()
            except:
                pass

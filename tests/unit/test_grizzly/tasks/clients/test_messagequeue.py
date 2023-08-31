import subprocess
import sys
import logging

from os import environ
from typing import Optional, Dict, Any, List
from pathlib import Path

import pytest

from _pytest.tmpdir import TempPathFactory
from _pytest.logging import LogCaptureFixture
from pytest_mock import MockerFixture

import zmq.green as zmq
from zmq.error import Again as ZMQAgain

from grizzly.tasks.clients import MessageQueueClientTask
from grizzly.types import RequestDirection
from grizzly.scenarios import IteratorScenario
from grizzly.exceptions import RestartScenario
from grizzly_extras.async_message import AsyncMessageError

try:
    import pymqi
except:
    from grizzly_extras import dummy_pymqi as pymqi

from tests.fixtures import GrizzlyFixture, NoopZmqFixture


class TestMessageQueueClientTaskNoPymqi:
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        try:
            del env['LD_LIBRARY_PATH']
        except KeyError:
            pass

        env['PYTHONPATH'] = '.'

        process = subprocess.Popen(
            [
                sys.executable,
                '-c',
                (
                    'from grizzly.types import RequestDirection; import grizzly.tasks.clients.messagequeue as mq; '
                    'print(f"{mq.pymqi.__name__=}"); mq.MessageQueueClientTask(RequestDirection.FROM, "mqs://localhost:1");'
                ),
            ],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        out, _ = process.communicate()
        output = out.decode()

        assert process.returncode == 1
        assert "mq.pymqi.__name__='grizzly_extras.dummy_pymqi'" in output
        assert 'NotImplementedError: MessageQueueClientTask could not import pymqi, have you installed IBM MQ dependencies?' in output


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_extras.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestMessageQueueClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')

        create_client_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_client', return_value=None)
        create_context_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_context')

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(RequestDirection.FROM, 'mqs://localhost:1')
            zmq_context = task_factory._zmq_context

            assert create_context_mocked.call_count == 1
            assert create_client_mocked.call_count == 0

            assert isinstance(task_factory._zmq_context, zmq.Context)
            assert task_factory._zmq_url == 'tcp://127.0.0.1:5554'
            assert task_factory._worker == {}
            assert task_factory.endpoint == 'mqs://localhost:1'
            assert task_factory.name is None
            assert task_factory.payload_variable is None
            assert task_factory.metadata_variable is None
            assert task_factory.destination is None
            assert task_factory.source is None
            assert not hasattr(task_factory, 'scenario')
            assert task_factory.__template_attributes__ == {'endpoint', 'destination', 'source', 'name', 'variable_template'}
        finally:
            if zmq_context is not None:
                zmq_context.destroy()
                zmq_context = None

        try:
            task_factory = MessageQueueClientTask(RequestDirection.FROM, 'mqs://localhost:1', 'messagequeue-request')
            zmq_context = task_factory._zmq_context

            assert create_context_mocked.call_count == 2
            assert create_client_mocked.call_count == 0

            assert isinstance(task_factory._zmq_context, zmq.Context)
            assert task_factory._zmq_url == 'tcp://127.0.0.1:5554'
            assert task_factory._worker == {}
            assert task_factory.endpoint == 'mqs://localhost:1'
            assert task_factory.name == 'messagequeue-request'
            assert task_factory.payload_variable is None
            assert task_factory.metadata_variable is None
            assert task_factory.destination is None
            assert task_factory.source is None
            assert not hasattr(task_factory, 'scenario')

            with pytest.raises(NotImplementedError) as nie:
                MessageQueueClientTask(RequestDirection.FROM, 'mqs://localhost:1', 'messagequeue-request', text='foobar')
            assert str(nie.value) == 'MessageQueueClientTask has not implemented support for step text'
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_create_context(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')
        create_client_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_client', return_value=None)

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(RequestDirection.FROM, (
                'mqs://mq_username:mq_password@mq.example.com:1415/queue:INCOMING.MESSAGES?QueueManager=QM01&Channel=IN.CHAN'
                '&wait=133&heartbeat=432&KeyFile=/tmp/mq_keys&SslCipher=NUL&CertLabel=something'
            ))
            zmq_context = task_factory._zmq_context

            assert create_client_mocked.call_count == 0

            assert task_factory.endpoint_path == 'queue:INCOMING.MESSAGES'
            assert task_factory.context == {
                'url': task_factory.endpoint,
                'connection': 'mq.example.com(1415)',
                'queue_manager': 'QM01',
                'channel': 'IN.CHAN',
                'username': 'mq_username',
                'password': 'mq_password',
                'key_file': '/tmp/mq_keys',
                'cert_label': 'something',
                'ssl_cipher': 'NUL',
                'message_wait': 133,
                'heartbeat_interval': 432,
                'header_type': None,
            }

            task_factory.endpoint = 'https://mq.example.com'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: "https" is not a supported scheme for endpoint'

            task_factory.endpoint = 'mqs:///'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: hostname not specified in "mqs:///"'

            task_factory.endpoint = 'mq://mq.example.io'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: no valid path component found in "mq://mq.example.io"'

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: QueueManager and Channel must be specified in the query string of "mqs://mq.example.io/topic:INCOMING.MSG"'

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG?Channel=TCP.IN'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: QueueManager must be specified in the query string'

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG?QueueManager=QM01'
            with pytest.raises(ValueError) as ve:
                task_factory.create_context()
            assert str(ve.value) == 'MessageQueueClientTask: Channel must be specified in the query string'

            task_factory.endpoint = 'mq://mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN'
            task_factory.create_context()
            assert task_factory.context == {
                'url': task_factory.endpoint,
                'connection': 'mq.example.io(1414)',
                'queue_manager': 'QM01',
                'channel': 'TCP.IN',
                'username': None,
                'password': None,
                'key_file': None,
                'cert_label': None,
                'ssl_cipher': None,
                'message_wait': None,
                'heartbeat_interval': None,
                'header_type': None,
            }

            task_factory.endpoint = 'mq://mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN&KeyFile=/tmp/mq_keys'
            task_factory.create_context()
            assert task_factory.context == {
                'url': task_factory.endpoint,
                'connection': 'mq.example.io(1414)',
                'queue_manager': 'QM01',
                'channel': 'TCP.IN',
                'username': None,
                'password': None,
                'key_file': '/tmp/mq_keys',
                'cert_label': None,
                'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': None,
                'heartbeat_interval': None,
                'header_type': None,
            }

            task_factory.endpoint = 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN'
            task_factory.create_context()
            assert task_factory.context == {
                'url': task_factory.endpoint,
                'connection': 'mq.example.io(1414)',
                'queue_manager': 'QM01',
                'channel': 'TCP.IN',
                'username': 'mq_username',
                'password': 'mq_password',
                'key_file': 'mq_username',
                'cert_label': 'mq_username',
                'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': None,
                'heartbeat_interval': None,
                'header_type': None,
            }

            task_factory.endpoint = 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN&HeaderType=RFH2'
            task_factory.create_context()
            assert task_factory.context == {
                'url': task_factory.endpoint,
                'connection': 'mq.example.io(1414)',
                'queue_manager': 'QM01',
                'channel': 'TCP.IN',
                'username': 'mq_username',
                'password': 'mq_password',
                'key_file': 'mq_username',
                'cert_label': 'mq_username',
                'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': None,
                'heartbeat_interval': None,
                'header_type': 'rfh2',
            }

            task_factory.endpoint = 'mqs://$conf::mq.username$:$conf::mq.password$@$conf::mq.host$/$conf::mq.endpoint$?QueueManager=$conf::mq.qm$&Channel=$conf::mq.channel$'
            task_factory.grizzly.state.configuration = {
                'mq.username': 'mq_conf_username',
                'mq.password': 'mq_conf_password',
                'mq.host': 'mq.example.com:1444',
                'mq.endpoint': 'topic:INCOMING.MSG',
                'mq.qm': 'QM99',
                'mq.channel': 'UDP.CHAN',
            }

            task_factory.create_context()
            assert task_factory.context == {
                'url': 'mqs://mq_conf_username:mq_conf_password@mq.example.com:1444/topic:INCOMING.MSG?QueueManager=QM99&Channel=UDP.CHAN',
                'connection': 'mq.example.com(1444)',
                'queue_manager': 'QM99',
                'channel': 'UDP.CHAN',
                'username': 'mq_conf_username',
                'password': 'mq_conf_password',
                'key_file': 'mq_conf_username',
                'cert_label': 'mq_conf_username',
                'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                'message_wait': None,
                'heartbeat_interval': None,
                'header_type': None,
            }
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_create_client(self, grizzly_fixture: GrizzlyFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')
        connect_mock = noop_zmq.get_mock('zmq.Socket.connect')
        setsockopt_mock = noop_zmq.get_mock('zmq.Socket.setsockopt')
        close_mock = noop_zmq.get_mock('zmq.Socket.close')

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
            )
            zmq_context = task_factory._zmq_context

            with task_factory.create_client() as client:
                assert connect_mock.call_count == 1
                args, kwargs = connect_mock.call_args_list[-1]
                assert args == (task_factory._zmq_url,)
                assert kwargs == {}

                assert isinstance(client, zmq.Socket)

            assert setsockopt_mock.call_count == 1
            args, _ = setsockopt_mock.call_args_list[-1]
            assert len(args) == 2
            assert args[0] == zmq.LINGER
            assert args[1] == 0

            assert close_mock.call_count == 1
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_connect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
            )
            zmq_context = task_factory._zmq_context

            with task_factory.create_client() as client:
                recv_json_mock = mocker.patch.object(client, 'recv_json')
                send_json_mock = mocker.patch.object(client, 'send_json')
                recv_json_mock.side_effect = [ZMQAgain, None]

                meta: Dict[str, Any] = {}
                with pytest.raises(AsyncMessageError) as ame:
                    task_factory.connect(111111, client, meta)
                assert str(ame.value) == 'no response'
                assert meta.get('response_length', None) == 0
                assert meta.get('action', None) == 'topic:INCOMING.MSG'
                assert meta.get('direction', None) == '<->'

                assert send_json_mock.call_count == 1
                args, kwargs = send_json_mock.call_args_list[-1]
                assert args == ({
                    'action': 'CONN',
                    'client': 111111,
                    'context': {
                        'url': task_factory.endpoint,
                        'connection': 'mq.example.io(1414)',
                        'queue_manager': 'QM01',
                        'channel': 'TCP.IN',
                        'username': 'mq_username',
                        'password': 'mq_password',
                        'key_file': 'mq_username',
                        'cert_label': 'mq_username',
                        'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                        'message_wait': None,
                        'heartbeat_interval': None,
                        'header_type': None,
                    },
                },)
                assert kwargs == {}
                assert recv_json_mock.call_count == 2
                _, kwargs = recv_json_mock.call_args_list[-1]
                assert kwargs.get('flags', None) == zmq.NOBLOCK

                meta = {}
                message = {'success': False, 'message': 'unknown error yo'}
                recv_json_mock.side_effect = [message]

                with pytest.raises(AsyncMessageError) as ame:
                    task_factory.connect(222222, client, meta)
                assert str(ame.value) == 'unknown error yo'

                assert send_json_mock.call_count == 2
                assert recv_json_mock.call_count == 3
                assert meta.get('response_length', None) == 0
                assert meta.get('action', None) == 'topic:INCOMING.MSG'

                meta = {}
                message = {'success': True, 'message': 'hello there', 'worker': 'aaaa-bbbb-cccc-dddd'}
                recv_json_mock.side_effect = [message]

                task_factory.connect(333333, client, meta)

                assert send_json_mock.call_count == 3
                assert recv_json_mock.call_count == 4
                assert meta.get('response_length', None) == 0
                assert meta.get('action', None) == 'topic:INCOMING.MSG'
                assert task_factory._worker.get(333333, None) == 'aaaa-bbbb-cccc-dddd'

                zmq_context.destroy()
                meta = {}
                message = {'success': True, 'message': 'hello there', 'worker': 'aaaa-bbbb-cccc-dddd'}
                recv_json_mock.side_effect = [message]

            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN&HeaderType=RFH2',
            )
            zmq_context = task_factory._zmq_context

            with task_factory.create_client() as client:
                recv_json_mock = mocker.patch.object(client, 'recv_json')
                send_json_mock = mocker.patch.object(client, 'send_json')

                task_factory.connect(444444, client, meta)

                assert send_json_mock.call_count == 1
                assert recv_json_mock.call_count == 1
                args, kwargs = send_json_mock.call_args_list[-1]
                assert args == ({
                    'action': 'CONN',
                    'client': 444444,
                    'context': {
                        'url': task_factory.endpoint,
                        'connection': 'mq.example.io(1414)',
                        'queue_manager': 'QM01',
                        'channel': 'TCP.IN',
                        'username': 'mq_username',
                        'password': 'mq_password',
                        'key_file': 'mq_username',
                        'cert_label': 'mq_username',
                        'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                        'message_wait': None,
                        'heartbeat_interval': None,
                        'header_type': 'rfh2',
                    },
                },)
                assert kwargs == {}
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_get(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        parent.grizzly.state.variables.update({'mq-client-var': 'none', 'mq-client-metadata': 'none'})

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        recv_json_mock = noop_zmq.get_mock('recv_json')
        send_json_mock = noop_zmq.get_mock('send_json')

        recv_json_mock.side_effect = [ZMQAgain, None]

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                payload_variable='mq-client-var',
            )
            zmq_context = task_factory._zmq_context

            task = task_factory()

            messages: List[Any] = [{'success': True, 'message': 'hello there', 'worker': 'dddd-eeee-ffff-9999'}, ZMQAgain, None]
            recv_json_mock.side_effect = messages

            task(parent)

            assert parent.user._context['variables'].get('mq-client-var', None) is None
            assert parent.user._context['variables'].get('mq-client-metadata', None) is None
            assert task_factory._worker.get(id(parent.user), None) == 'dddd-eeee-ffff-9999'
            assert send_json_mock.call_count == 2
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == ({
                'action': 'GET',
                'worker': 'dddd-eeee-ffff-9999',
                'client': id(parent.user),
                'context': {
                    'url': 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    'connection': 'mq.example.io(1414)',
                    'queue_manager': 'QM01',
                    'channel': 'TCP.IN',
                    'username': 'mq_username',
                    'password': 'mq_password',
                    'key_file': 'mq_username',
                    'cert_label': 'mq_username',
                    'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                    'message_wait': None,
                    'heartbeat_interval': None,
                    'header_type': None,
                    'endpoint': 'topic:INCOMING.MSG',
                },
                'payload': None,
            },)
            assert kwargs == {}
            assert recv_json_mock.call_count == 3

            assert fire_spy.call_count == 1
            _, kwargs = fire_spy.call_args_list[-1]  # get
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == parent.user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, AsyncMessageError)
            assert str(exception) == 'no response'

            messages = [{'success': False, 'message': 'memory corruption'}]
            recv_json_mock.side_effect = messages

            task_factory.endpoint = 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN&MaxMessageSize=13337'
            task_factory.create_context()

            task = task_factory()

            task(parent)

            assert parent.user._context['variables'].get('mq-client-var', None) is None
            assert parent.user._context['variables'].get('mq-client-metadata', None) is None
            assert send_json_mock.call_count == 3
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == ({
                'action': 'GET',
                'worker': 'dddd-eeee-ffff-9999',
                'client': id(parent.user),
                'context': {
                    'url': 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    'connection': 'mq.example.io(1414)',
                    'queue_manager': 'QM01',
                    'channel': 'TCP.IN',
                    'username': 'mq_username',
                    'password': 'mq_password',
                    'key_file': 'mq_username',
                    'cert_label': 'mq_username',
                    'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                    'message_wait': None,
                    'heartbeat_interval': None,
                    'header_type': None,
                    'endpoint': 'topic:INCOMING.MSG, max_message_size:13337',
                },
                'payload': None,
            },)
            assert kwargs == {}
            assert recv_json_mock.call_count == 4

            assert fire_spy.call_count == 2
            _, kwargs = fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == parent.user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, AsyncMessageError)
            assert str(exception) == 'memory corruption'

            messages = [{'success': True, 'payload': None}]
            recv_json_mock.side_effect = messages

            task(parent)

            assert parent.user._context['variables'].get('mq-client-var', None) is None
            assert parent.user._context['variables'].get('mq-client-metadata', None) is None
            assert send_json_mock.call_count == 4
            assert recv_json_mock.call_count == 5

            assert fire_spy.call_count == 3
            _, kwargs = fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == 0
            assert kwargs.get('context', None) == parent.user._context
            exception = kwargs.get('exception', None)
            assert isinstance(exception, RuntimeError)
            assert str(exception) == 'response did not contain any payload'

            messages = [{'success': True, 'payload': '{"hello": "world", "foo": "bar"}', 'metadata': {'x-foo-bar': 'test'}}]
            recv_json_mock.side_effect = messages
            task_factory.name = 'mq-get-example'
            task_factory.metadata_variable = 'mq-client-metadata'

            task(parent)

            assert parent.user._context['variables'].get('mq-client-var', None) == '{"hello": "world", "foo": "bar"}'
            assert parent.user._context['variables'].get('mq-client-metadata', None) == '{"x-foo-bar": "test"}'
            assert send_json_mock.call_count == 5
            assert recv_json_mock.call_count == 6

            assert fire_spy.call_count == 4
            _, kwargs = fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} mq-get-example'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == len(messages[0].get('payload', ''))
            assert kwargs.get('context', None) == parent.user._context
            assert kwargs.get('exception', RuntimeError) is None

            async_message_request_mock = mocker.patch('grizzly.tasks.clients.messagequeue.async_message_request', side_effect=[AsyncMessageError('oooh nooo')])
            parent.user._scenario.failure_exception = RestartScenario

            log_error_mock = mocker.patch.object(parent.stats, 'log_error')
            mocker.patch.object(parent, 'on_start', return_value=None)
            mocker.patch.object(parent, 'wait', side_effect=[RuntimeError])
            parent.user.environment.catch_exceptions = False

            parent.tasks.clear()
            parent._task_queue.clear()
            parent._task_queue.append(task)
            parent.task_count = 1

            with pytest.raises(RuntimeError):
                with caplog.at_level(logging.INFO):
                    parent.run()

            async_message_request_mock.assert_called_once()
            log_error_mock.assert_called_once_with(None)

            assert 'restarting scenario' in '\n'.join(caplog.messages)

        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_put(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture, tmp_path_factory: TempPathFactory) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')

        parent = grizzly_fixture()

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')
        recv_json_mock = noop_zmq.get_mock('recv_json')
        send_json_mock = noop_zmq.get_mock('send_json')

        zmq_context: Optional[zmq.Context] = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario

            with pytest.raises(ValueError) as ve:
                task_factory = MessageQueueClientTask(
                    RequestDirection.TO,
                    'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    source=None,
                    destination=None,
                )
            assert str(ve.value) == 'MessageQueueClientTask: source must be set for direction TO'

            source = 'tests/source.json'

            with pytest.raises(ValueError) as ve:
                task_factory = MessageQueueClientTask(
                    RequestDirection.TO,
                    'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    source=source,
                    destination='destination-source.json',
                )
            assert str(ve.value) == 'MessageQueueClientTask: destination is not allowed'

            task_factory = MessageQueueClientTask(
                RequestDirection.TO,
                'mqs://mq_username:mq_password@mq.example.io/queue:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                source=source,
                destination=None,
            )
            zmq_context = task_factory._zmq_context
            messages: List[Any] = [{'success': True, 'message': 'hello there', 'worker': 'dddd-eeee-ffff-9999'}, {'success': True, 'payload': source}]
            recv_json_mock.side_effect = messages

            task = task_factory()

            task(parent)

            assert task_factory._worker.get(id(parent.user), None) == 'dddd-eeee-ffff-9999'

            assert recv_json_mock.call_count == 2
            assert send_json_mock.call_count == 2
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == ({
                'action': 'PUT',
                'worker': 'dddd-eeee-ffff-9999',
                'client': id(parent.user),
                'context': {
                    'url': 'mqs://mq_username:mq_password@mq.example.io/queue:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    'connection': 'mq.example.io(1414)',
                    'queue_manager': 'QM01',
                    'channel': 'TCP.IN',
                    'username': 'mq_username',
                    'password': 'mq_password',
                    'key_file': 'mq_username',
                    'cert_label': 'mq_username',
                    'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                    'message_wait': None,
                    'heartbeat_interval': None,
                    'header_type': None,
                    'endpoint': 'queue:INCOMING.MSG',
                },
                'payload': source,
            },)
            assert kwargs == {}

            assert fire_spy.call_count == 1
            _, kwargs = fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} MessageQueue->queue:INCOMING.MSG'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == len(source)
            assert kwargs.get('context', None) == parent.user._context
            assert kwargs.get('exception', RuntimeError) is None

            test_context = Path(task_factory._context_root)
            (test_context / 'requests' / 'tests').mkdir(exist_ok=True)
            source_file = test_context / 'requests' / 'tests' / 'source.json'
            source_file.write_text('''{
    "hello": "world!"
}''')
            recv_json_mock.side_effect = [{'success': True, 'payload': source_file.read_text()}]
            task_factory.name = 'mq-example-put'

            task(parent)

            assert recv_json_mock.call_count == 3
            assert send_json_mock.call_count == 3
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == ({
                'action': 'PUT',
                'worker': 'dddd-eeee-ffff-9999',
                'client': id(parent.user),
                'context': {
                    'url': 'mqs://mq_username:mq_password@mq.example.io/queue:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    'connection': 'mq.example.io(1414)',
                    'queue_manager': 'QM01',
                    'channel': 'TCP.IN',
                    'username': 'mq_username',
                    'password': 'mq_password',
                    'key_file': 'mq_username',
                    'cert_label': 'mq_username',
                    'ssl_cipher': 'ECDHE_RSA_AES_256_GCM_SHA384',
                    'message_wait': None,
                    'heartbeat_interval': None,
                    'header_type': None,
                    'endpoint': 'queue:INCOMING.MSG',
                },
                'payload': source_file.read_text(),
            },)
            assert kwargs == {}

            assert fire_spy.call_count == 2
            _, kwargs = fire_spy.call_args_list[-1]
            assert kwargs.get('request_type', None) == 'CLTSK'
            assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} mq-example-put'
            assert kwargs.get('response_time', None) >= 0.0
            assert kwargs.get('response_length', None) == len(source_file.read_text())
            assert kwargs.get('context', None) == parent.user._context
            assert kwargs.get('exception', RuntimeError) is None
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

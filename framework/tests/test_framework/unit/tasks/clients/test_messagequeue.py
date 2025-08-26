"""Unit tests of grizzly.tasks.clients.messagequeue."""

from __future__ import annotations

import logging
import subprocess
import sys
from contextlib import suppress
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import zmq.green as zmq
from async_messaged import AsyncMessageError
from grizzly.exceptions import RestartScenario
from grizzly.scenarios import IteratorScenario
from grizzly.tasks.clients import MessageQueueClientTask
from grizzly.types import RequestDirection, StrDict, pymqi
from zmq import sugar as ztypes
from zmq.error import Again as ZMQAgain

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture, NoopZmqFixture


class TestMessageQueueClientTaskNoPymqi:
    @pytest.mark.timeout(40)
    def test_no_pymqi_dependencies(self) -> None:
        env = environ.copy()
        with suppress(KeyError):
            del env['LD_LIBRARY_PATH']

        env['PYTHONPATH'] = '.:framework/src:common/src'

        with pytest.raises(subprocess.CalledProcessError) as e:
            subprocess.check_output(
                [
                    sys.executable,
                    '-c',
                    (
                        'from grizzly.types import RequestDirection; import grizzly.tasks.clients.messagequeue as mq; '
                        'print(f"{mq.pymqi.__name__=}"); mq.MessageQueueClientTask(RequestDirection.FROM, "mqs://localhost:1");'
                    ),
                ],
                env=env,
                stderr=subprocess.STDOUT,
            )
        assert e.value.returncode == 1
        output = e.value.output.decode()
        assert "mq.pymqi.__name__='grizzly_common.dummy_pymqi'" in output
        assert 'NotImplementedError: MessageQueueClientTask could not import pymqi, have you installed IBM MQ dependencies and set environment variable LD_LIBRARY_PATH?' in output


@pytest.mark.skipif(pymqi.__name__ == 'grizzly_common.dummy_pymqi', reason='needs native IBM MQ libraries')
class TestMessageQueueClientTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')

        create_client_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_client', return_value=None)
        create_context_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_context')

        zmq_context: ztypes.Context | None = None
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

            with pytest.raises(NotImplementedError, match='MessageQueueClientTask has not implemented support for step text'):
                MessageQueueClientTask(RequestDirection.FROM, 'mqs://localhost:1', 'messagequeue-request', text='foobar')
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_create_context(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')
        create_client_mocked = mocker.patch('grizzly.tasks.clients.messagequeue.MessageQueueClientTask.create_client', return_value=None)

        zmq_context: ztypes.Context | None = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                (
                    'mqs://mq_username:mq_password@mq.example.com:1415/queue:INCOMING.MESSAGES?QueueManager=QM01&Channel=IN.CHAN'
                    '&wait=133&heartbeat=432&KeyFile=/tmp/mq_keys&SslCipher=NUL&CertLabel=something'
                ),
            )
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
                'key_file': '/tmp/mq_keys',  # noqa: S108
                'cert_label': 'something',
                'ssl_cipher': 'NUL',
                'message_wait': 133,
                'heartbeat_interval': 432,
                'header_type': None,
            }

            task_factory.endpoint = 'https://mq.example.com'
            with pytest.raises(AssertionError, match='MessageQueueClientTask: "https" is not a supported scheme for endpoint'):
                task_factory.create_context()

            task_factory.endpoint = 'mqs:///'
            with pytest.raises(AssertionError, match='MessageQueueClientTask: hostname not specified in "mqs:///"'):
                task_factory.create_context()

            task_factory.endpoint = 'mq://mq.example.io'
            with pytest.raises(AssertionError, match='MessageQueueClientTask: no valid path component found in "mq://mq.example.io"'):
                task_factory.create_context()

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG'
            with pytest.raises(
                AssertionError,
                match='MessageQueueClientTask: QueueManager and Channel must be specified in the query string of "mqs://mq.example.io/topic:INCOMING.MSG"',
            ):
                task_factory.create_context()

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG?Channel=TCP.IN'
            with pytest.raises(AssertionError, match='MessageQueueClientTask: QueueManager must be specified in the query string'):
                task_factory.create_context()

            task_factory.endpoint = 'mqs://mq.example.io/topic:INCOMING.MSG?QueueManager=QM01'
            with pytest.raises(AssertionError, match='MessageQueueClientTask: Channel must be specified in the query string'):
                task_factory.create_context()

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
                'key_file': '/tmp/mq_keys',  # noqa: S108
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
        zmq_context: ztypes.Context | None = None

        parent = grizzly_fixture()
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
            )
            zmq_context = task_factory._zmq_context

            with task_factory.create_client(parent) as client:
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

    def test_connect(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture, noop_zmq: NoopZmqFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.tasks.clients.messagequeue')
        mocker.patch('async_messaged.utils.uuid4', return_value='foobar')
        zmq_context: ztypes.Context | None = None

        parent = grizzly_fixture()

        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
            )
            zmq_context = task_factory._zmq_context

            with task_factory.create_client(parent) as client:
                recv_json_mock = mocker.patch.object(client, 'recv_json')
                send_json_mock = mocker.patch.object(client, 'send_json')
                recv_json_mock.side_effect = [ZMQAgain, None]

                meta: StrDict = {}
                with pytest.raises(AsyncMessageError, match='no response'):
                    task_factory.connect(111111, client, meta)
                assert meta.get('response_length') == 0
                assert meta.get('action') == 'topic:INCOMING.MSG'
                assert meta.get('direction') == '<->'

                send_json_mock.assert_called_once_with(
                    {
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
                        'request_id': 'foobar',
                    },
                )
                assert recv_json_mock.call_count == 2
                _, kwargs = recv_json_mock.call_args_list[-1]
                assert kwargs.get('flags', None) == zmq.NOBLOCK

                meta = {}
                message = {'success': False, 'message': 'unknown error yo'}
                recv_json_mock.side_effect = [message]

                with pytest.raises(AsyncMessageError, match='unknown error yo'):
                    task_factory.connect(222222, client, meta)

                assert send_json_mock.call_count == 2
                assert recv_json_mock.call_count == 3
                assert meta.get('response_length') == 0
                assert meta.get('action') == 'topic:INCOMING.MSG'

                meta = {}
                message = {'success': True, 'message': 'hello there', 'worker': 'aaaa-bbbb-cccc-dddd'}
                recv_json_mock.side_effect = [message]

                task_factory.connect(333333, client, meta)

                assert send_json_mock.call_count == 3
                assert recv_json_mock.call_count == 4
                assert meta.get('response_length') == 0
                assert meta.get('action') == 'topic:INCOMING.MSG'
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

            with task_factory.create_client(parent) as client:
                recv_json_mock = mocker.patch.object(client, 'recv_json')
                send_json_mock = mocker.patch.object(client, 'send_json')

                task_factory.connect(444444, client, meta)

                recv_json_mock.assert_called_once()
                send_json_mock.assert_called_once_with(
                    {
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
                        'request_id': 'foobar',
                    },
                )
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_request_from(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
        noop_zmq('grizzly.tasks.clients.messagequeue')
        mocker.patch('async_messaged.utils.uuid4', return_value='foobar')

        parent = grizzly_fixture(scenario_type=IteratorScenario)

        assert isinstance(parent, IteratorScenario)

        parent.grizzly.scenario.variables.update({'mq-client-var': 'none', 'mq-client-metadata': 'none'})
        parent.user.variables.update({'mq-client-var': 'none', 'mq-client-metadata': 'none'})

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        recv_json_mock = noop_zmq.get_mock('recv_json')
        send_json_mock = noop_zmq.get_mock('send_json')

        recv_json_mock.side_effect = [ZMQAgain, None]

        zmq_context: ztypes.Context | None = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario
            task_factory = MessageQueueClientTask(
                RequestDirection.FROM,
                'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                payload_variable='mq-client-var',
            )
            zmq_context = task_factory._zmq_context

            task = task_factory()

            messages: list[Any] = [{'success': True, 'message': 'hello there', 'worker': 'dddd-eeee-ffff-9999'}, ZMQAgain, None]
            recv_json_mock.side_effect = messages

            task(parent)

            assert parent.user.variables.get('mq-client-var', None) == 'none'
            assert parent.user.variables.get('mq-client-metadata', None) == 'none'
            assert task_factory._worker.get(id(parent.user), None) == 'dddd-eeee-ffff-9999'
            assert send_json_mock.call_count == 2
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == (
                {
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
                    'request_id': 'foobar',
                },
            )
            assert kwargs == {}
            send_json_mock.reset_mock()
            assert recv_json_mock.call_count == 3

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG',
                response_time=ANY(int),
                response_length=0,
                context=parent.user._context,
                exception=ANY(AsyncMessageError, message='no response'),
            )
            fire_spy.reset_mock()

            messages = [{'success': False, 'message': 'memory corruption'}]
            recv_json_mock.side_effect = messages

            task_factory.endpoint = 'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN&MaxMessageSize=13337'
            task_factory.create_context()

            task = task_factory()

            task(parent)

            assert parent.user.variables.get('mq-client-var', None) == 'none'
            assert parent.user.variables.get('mq-client-metadata', None) == 'none'
            send_json_mock.assert_called_once_with(
                {
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
                    'request_id': 'foobar',
                },
            )
            send_json_mock.reset_mock()
            assert recv_json_mock.call_count == 4

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG',
                response_time=ANY(int),
                response_length=0,
                context=parent.user._context,
                exception=ANY(AsyncMessageError, message='memory corruption'),
            )
            fire_spy.reset_mock()

            messages = [{'success': True, 'payload': None}]
            recv_json_mock.side_effect = messages

            task(parent)

            assert parent.user.variables.get('mq-client-var', None) == 'none'
            assert parent.user.variables.get('mq-client-metadata', None) == 'none'
            assert send_json_mock.call_count == 1
            send_json_mock.reset_mock()
            assert recv_json_mock.call_count == 5

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} MessageQueue<-topic:INCOMING.MSG',
                response_time=ANY(int),
                response_length=0,
                context=parent.user._context,
                exception=ANY(RuntimeError, message='response did not contain any payload'),
            )
            fire_spy.reset_mock()

            messages = [{'success': True, 'payload': '{"hello": "world", "foo": "bar"}', 'metadata': {'x-foo-bar': 'test'}}]
            recv_json_mock.side_effect = messages
            task_factory.name = 'mq-get-example'
            task_factory.metadata_variable = 'mq-client-metadata'

            task(parent)

            assert parent.user.variables.get('mq-client-var', None) == '{"hello": "world", "foo": "bar"}'
            assert parent.user.variables.get('mq-client-metadata', None) == '{"x-foo-bar": "test"}'
            assert send_json_mock.call_count == 1
            send_json_mock.reset_mock()
            assert recv_json_mock.call_count == 6

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} mq-get-example',
                response_time=ANY(int),
                response_length=32,
                context=parent.user._context,
                exception=None,
            )
            fire_spy.reset_mock()

            async_message_request_mock = mocker.patch('grizzly.tasks.clients.messagequeue.async_message_request', side_effect=[AsyncMessageError('oooh nooo')])
            parent.user._scenario.failure_handling.update({None: RestartScenario})

            log_error_mock = mocker.patch.object(parent.stats, 'log_error')
            mocker.patch.object(parent, 'on_start', return_value=None)
            mocker.patch.object(parent, 'wait', side_effect=[RuntimeError])
            parent.user.environment.catch_exceptions = False

            parent.tasks.clear()
            parent._task_queue.clear()
            parent._task_queue.append(task)
            parent.__class__.task_count = 1

            with pytest.raises(RuntimeError), caplog.at_level(logging.INFO):
                parent.run()

            async_message_request_mock.assert_called_once()
            log_error_mock.assert_called_once_with(None)

            assert 'restarting scenario' in '\n'.join(caplog.messages)

        finally:
            if zmq_context is not None:
                zmq_context.destroy()

    def test_request_to(self, mocker: MockerFixture, noop_zmq: NoopZmqFixture, grizzly_fixture: GrizzlyFixture) -> None:
        noop_zmq('grizzly.tasks.clients.messagequeue')
        mocker.patch('async_messaged.utils.uuid4', return_value='foobar')

        parent = grizzly_fixture()

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')
        recv_json_mock = noop_zmq.get_mock('recv_json')
        send_json_mock = noop_zmq.get_mock('send_json')

        zmq_context: ztypes.Context | None = None
        try:
            MessageQueueClientTask.__scenario__ = grizzly_fixture.grizzly.scenario

            with pytest.raises(AssertionError, match='MessageQueueClientTask: source must be set for direction TO'):
                task_factory = MessageQueueClientTask(
                    RequestDirection.TO,
                    'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    source=None,
                    destination=None,
                )

            source = 'tests/source.json'

            with pytest.raises(AssertionError, match='MessageQueueClientTask: destination is not allowed'):
                task_factory = MessageQueueClientTask(
                    RequestDirection.TO,
                    'mqs://mq_username:mq_password@mq.example.io/topic:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                    source=source,
                    destination='destination-source.json',
                )

            task_factory = MessageQueueClientTask(
                RequestDirection.TO,
                'mqs://mq_username:mq_password@mq.example.io/queue:INCOMING.MSG?QueueManager=QM01&Channel=TCP.IN',
                source=source,
                destination=None,
            )
            zmq_context = task_factory._zmq_context
            messages: list[Any] = [{'success': True, 'message': 'hello there', 'worker': 'dddd-eeee-ffff-9999'}, {'success': True, 'payload': source}]
            recv_json_mock.side_effect = messages

            task = task_factory()

            task(parent)

            assert task_factory._worker.get(id(parent.user), None) == 'dddd-eeee-ffff-9999'

            assert recv_json_mock.call_count == 2
            assert send_json_mock.call_count == 2
            args, kwargs = send_json_mock.call_args_list[-1]
            assert args == (
                {
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
                    'request_id': 'foobar',
                },
            )
            assert kwargs == {}
            send_json_mock.reset_mock()

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} MessageQueue->queue:INCOMING.MSG',
                response_time=ANY(int),
                response_length=len(source.encode()),
                context=parent.user._context,
                exception=None,
            )
            fire_spy.reset_mock()

            test_context = Path(task_factory._context_root)
            (test_context / 'requests' / 'tests').mkdir(exist_ok=True)
            source_file = test_context / 'requests' / 'tests' / 'source.json'
            source_file.write_text("""{
    "hello": "world!"
}""")
            recv_json_mock.side_effect = [{'success': True, 'payload': source_file.read_text()}]
            task_factory.name = 'mq-example-put'

            task(parent)

            assert recv_json_mock.call_count == 3
            send_json_mock.assert_called_once_with(
                {
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
                    'request_id': 'foobar',
                },
            )
            send_json_mock.reset_mock()

            fire_spy.assert_called_once_with(
                request_type='CLTSK',
                name=f'{parent.user._scenario.identifier} mq-example-put',
                response_time=ANY(int),
                response_length=len(source_file.read_text().encode()),
                context=parent.user._context,
                exception=None,
            )
        finally:
            if zmq_context is not None:
                zmq_context.destroy()

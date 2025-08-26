"""Unit tests of grizzly.protocols."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from http.cookiejar import Cookie, CookieJar
from os import utime
from typing import TYPE_CHECKING

import pytest
import zmq.green as zmq
from grizzly.utils.protocols import (
    async_message_request_wrapper,
    http_populate_cookiejar,
    mq_client_logs,
    zmq_disconnect,
)

from test_framework.helpers import SOME

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture
    from _pytest.tmpdir import TempPathFactory
    from async_messaged import AsyncMessageRequest

    from test_framework.fixtures import BehaveFixture, GrizzlyFixture, MockerFixture


def test_check_mq_client_logs(behave_fixture: BehaveFixture, tmp_path_factory: TempPathFactory, mocker: MockerFixture, caplog: LogCaptureFixture) -> None:  # noqa: PLR0915
    context = behave_fixture.context
    test_context = tmp_path_factory.mktemp('test_context')

    amq_error_dir = test_context / 'IBM' / 'MQ' / 'data' / 'errors'
    amq_error_dir.mkdir(parents=True, exist_ok=True)

    test_logger = logging.getLogger('test_grizzly_print_stats')

    mocker.patch('grizzly.utils.Path.expanduser', return_value=amq_error_dir)
    mocker.patch('grizzly.locust.stats_logger', test_logger)

    # context.started not set
    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 0

    # no error files
    context.started = datetime.now().astimezone()
    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 0

    # one AMQERR*.LOG file, previous run
    amqerr_log_file_1 = amq_error_dir / 'AMQERR01.LOG'
    amqerr_log_file_1.write_text("""10/13/22 06:13:07 - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time(2022-10-13T06:13:07.215Z)
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")

    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 0

    entry_date_1 = (datetime.now() + timedelta(hours=1)).astimezone(tz=timezone.utc)

    # one AMQERR*.LOG file, one entry
    with amqerr_log_file_1.open('a') as fd:
        fd.write(f"""{entry_date_1.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_1.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally.

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")

    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 6

    assert caplog.messages[0] == 'AMQ error log entries:'
    assert caplog.messages[1].strip() == 'Timestamp (UTC)      Message'
    assert caplog.messages[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert (
        caplog.messages[2]
        == caplog.messages[4]
        == ('--------------------|---------------------------------------------------------------------------------------------------------------------------------------------')
    )
    assert caplog.messages[5] == ''

    caplog.clear()

    # two AMQERR files, one with no data, one FDC file, old
    old_date = entry_date_1 - timedelta(hours=2)

    amqerr_log_file_2 = amq_error_dir / 'AMQERR99.LOG'
    amqerr_log_file_2.touch()

    amqerr_fdc_file_1 = amq_error_dir / 'AMQ6150.0.FDC'
    amqerr_fdc_file_1.touch()
    utime(amqerr_fdc_file_1, (old_date.timestamp(), old_date.timestamp()))

    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 6

    assert caplog.messages[0] == 'AMQ error log entries:'
    assert caplog.messages[1].strip() == 'Timestamp (UTC)      Message'
    assert caplog.messages[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert (
        caplog.messages[2]
        == caplog.messages[4]
        == ('--------------------|---------------------------------------------------------------------------------------------------------------------------------------------')
    )
    assert caplog.messages[5] == ''

    caplog.clear()

    # two AMQERR files, both with valid data. three FDC files, one old
    entry_date_2 = entry_date_1 + timedelta(minutes=23)
    amqerr_log_file_2.write_text(f"""{entry_date_2.strftime('%m/%d/%y %H:%M:%S')} - Process(6437.52) User(mqm) Program(amqrmppa)
                    Host(mq.example.io) Installation(Installation1)
                    VRMF(9.2.1.0) QMgr(QM1)
                    Time({entry_date_2.strftime('%Y-%m-%dT%H:%M:%S.000Z')})
                    CommentInsert1(CLIENT.CONN)
                    CommentInsert2(1111)
                    CommentInsert3(1.2.3.4)

AMQ1234E: dude, what did you do?!

EXPLANATION:
----- amqccisa.c : 10957 ------------------------------------------------------
""")
    amqerr_fdc_file_2 = amq_error_dir / 'AMQ1234.1.FDC'
    amqerr_fdc_file_2.touch()
    utime(amqerr_fdc_file_2, (entry_date_2.timestamp(), entry_date_2.timestamp()))

    amqerr_fdc_file_3 = amq_error_dir / 'AMQ4321.9.FDC'
    amqerr_fdc_file_3.touch()
    entry_date_3 = entry_date_2 + timedelta(minutes=73)
    utime(amqerr_fdc_file_3, (entry_date_3.timestamp(), entry_date_3.timestamp()))

    with caplog.at_level(logging.INFO):
        mq_client_logs(context)

    assert len(caplog.messages) == 14

    assert caplog.messages.index('AMQ error log entries:') == 0
    assert caplog.messages.index('AMQ FDC files:') == 7

    amqerr_log_entries = caplog.messages[0:6]
    assert len(amqerr_log_entries) == 6

    assert amqerr_log_entries[1].strip() == 'Timestamp (UTC)      Message'
    assert (
        amqerr_log_entries[2]
        == amqerr_log_entries[5]
        == ('--------------------|---------------------------------------------------------------------------------------------------------------------------------------------')
    )
    assert amqerr_log_entries[3].strip() == f"{entry_date_1.strftime('%Y-%m-%d %H:%M:%S')}  AMQ9999E: Channel 'CLIENT.CONN' to host '1.2.3.4' ended abnormally."
    assert amqerr_log_entries[4].strip() == f'{entry_date_2.strftime("%Y-%m-%d %H:%M:%S")}  AMQ1234E: dude, what did you do?!'

    amqerr_fdc_files = caplog.messages[7:-1]
    assert len(amqerr_fdc_files) == 6

    assert amqerr_fdc_files[1].strip() == 'Timestamp (UTC)      File'
    assert (
        amqerr_fdc_files[2]
        == amqerr_fdc_files[5]
        == ('--------------------|---------------------------------------------------------------------------------------------------------------------------------------------')
    )

    assert amqerr_fdc_files[3].strip() == f'{entry_date_2.strftime("%Y-%m-%d %H:%M:%S")}  {amqerr_fdc_file_2}'
    assert amqerr_fdc_files[4].strip() == f'{entry_date_3.strftime("%Y-%m-%d %H:%M:%S")}  {amqerr_fdc_file_3}'


def test_async_message_request_wrapper(grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
    parent = grizzly_fixture()

    async_message_request_mock = mocker.patch('grizzly.utils.protocols.async_message_request', return_value=None)
    client_mock = mocker.MagicMock()

    # nothing to render
    request: AsyncMessageRequest = {
        'context': {
            'endpoint': 'hello world',
        },
    }

    async_message_request_wrapper(parent, client_mock, request)

    request.update({'client': id(parent.user)})

    async_message_request_mock.assert_called_once_with(client_mock, request)
    async_message_request_mock.reset_mock()

    del request['client']

    # template to render, variable not set
    request = {
        'context': {
            'endpoint': 'hello {{ world }}!',
        },
    }

    async_message_request_wrapper(parent, client_mock, request)

    async_message_request_mock.assert_called_once_with(client_mock, {'context': {'endpoint': 'hello {{ world }}!'}, 'client': id(parent.user)})
    async_message_request_mock.reset_mock()

    # template to render, variable set
    parent.user.set_variable('world', 'foobar')

    async_message_request_wrapper(parent, client_mock, request)

    async_message_request_mock.assert_called_once_with(client_mock, {'context': {'endpoint': 'hello foobar!'}, 'client': id(parent.user)})


def test_http_populate_cookiejar() -> None:
    class CookieMonster:
        cookiejar: CookieJar

        def __init__(self) -> None:
            self.cookiejar = CookieJar()

    monster = CookieMonster()

    # add cookie that should be discarded
    monster.cookiejar.set_cookie(
        Cookie(
            version=0,
            name='leftovers',
            value='crumbles',
            port=None,
            port_specified=False,
            domain='example.com',
            domain_specified=True,
            domain_initial_dot=False,
            path='/',
            path_specified=True,
            secure=False,
            expires=None,
            discard=False,
            comment=None,
            comment_url=None,
            rest={},
        ),
    )

    assert len(monster.cookiejar) == 1

    cookies = {'foo': 'bar', 'bar': 'foo', 'hello': 'world'}

    http_populate_cookiejar(monster, cookies, url='https://example.net')

    assert len(monster.cookiejar) == len(cookies)

    for cookie in monster.cookiejar:
        assert cookie == SOME(
            Cookie,
            version=0,
            port=None,
            port_specified=False,
            domain='example.net',
            domain_specified=True,
            domain_initial_dot=False,
            path='/',
            path_specified=True,
            secure=True,
            expires=None,
            discard=False,
            comment=None,
            comment_url=None,
            _rest={},
        )

    # order cannot be guaranteed
    for name, value in cookies.items():
        found = False

        for cookie in monster.cookiejar:
            if cookie.name == name and cookie.value == value:
                found = True
                break

        if not found:
            pytest.fail(f'cookie {name}={value} not found')


def test_zmq_disconnect(mocker: MockerFixture) -> None:
    socket = mocker.MagicMock(spec=zmq.Socket)

    zmq_disconnect(socket, destroy_context=False)

    socket.close.assert_called_once_with(linger=0)
    socket.context.destroy.assert_not_called()
    socket.reset_mock()

    zmq_disconnect(socket, destroy_context=True)

    socket.close.assert_called_once_with(linger=0)
    socket.context.destroy.assert_called_once_with(linger=0)
    socket.reset_mock()

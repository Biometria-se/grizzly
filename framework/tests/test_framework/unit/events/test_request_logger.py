"""Unit tests of grizzly.users.base.request_logger."""

from __future__ import annotations

from contextlib import suppress
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from grizzly.events import GrizzlyEventHandlerClass, RequestLogger
from grizzly.tasks import RequestTask
from grizzly.types import RequestMethod
from grizzly.users import GrizzlyUser

from test_framework.helpers import rm_rf

if TYPE_CHECKING:  # pragma: no cover
    from collections.abc import Callable

    from test_framework.fixtures import GrizzlyFixture


@pytest.fixture
def get_log_files() -> Callable[[], list[Path]]:
    def wrapped() -> list[Path]:
        logs_root = Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs'
        log_dir = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir is not None:
            logs_root = logs_root / log_dir

        return list(logs_root.glob('*.log'))

    return wrapped


class TestRequestLogger:
    @pytest.mark.parametrize('log_prefix', [False, True])
    def test___init__(self, grizzly_fixture: GrizzlyFixture, *, log_prefix: bool) -> None:
        log_root = Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs'
        try:
            if log_prefix:
                environ['GRIZZLY_LOG_DIR'] = 'asdf'
                log_root = log_root / 'asdf'

            assert not log_root.exists()
            parent = grizzly_fixture()

            assert len(parent.user.events.request._handlers) == 2
            assert any(
                h.__class__ is RequestLogger and isinstance(h, GrizzlyEventHandlerClass) and isinstance(h.user, GrizzlyUser) and h.user is parent.user
                for h in parent.user.events.request._handlers
            )
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_LOG_DIR']

            rm_rf(log_root)

    def test__remove_secrets_attribute(self) -> None:
        assert RequestLogger._remove_secrets_attribute(
            {
                'test': 'visible',
                'access_token': 'hidden',
                'Authorization': 'hidden',
                'authorization': 'hidden',
                'Content-Type': 'application/json',
            },
        ) == {
            'test': 'visible',
            'access_token': '*** REMOVED ***',
            'Authorization': '*** REMOVED ***',
            'authorization': '*** REMOVED ***',
            'Content-Type': 'application/json',
        }

        assert RequestLogger._remove_secrets_attribute({'contents': 'test value'}) == {'contents': 'test value'}
        assert RequestLogger._remove_secrets_attribute(None) is None
        assert RequestLogger._remove_secrets_attribute(True) is True  # noqa: FBT003
        assert RequestLogger._remove_secrets_attribute('hello world') == 'hello world'
        assert RequestLogger._remove_secrets_attribute('SharedAccessKey=foobar') == 'SharedAccessKey=*** REMOVED ***'
        assert RequestLogger._remove_secrets_attribute('hello;SharedAccessKey=foobar;world') == 'hello;SharedAccessKey=*** REMOVED ***;world'

    @pytest.mark.usefixtures('get_log_files')
    def test___call__(self, grizzly_fixture: GrizzlyFixture, get_log_files: Callable[[], list[Path]]) -> None:
        parent = grizzly_fixture()
        parent.user.host = 'mq://mq.example.org?QueueManager=QMGR01&Channel=SYS.CONN'

        event = RequestLogger(parent.user)

        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='MSG.INCOMING')

        # pre sanity check
        assert get_log_files() == []

        # no exception, and do not log all requests
        event('test-request', (None, '{}'), request)

        assert get_log_files() == []

        event('[test-request!', (None, '{}'), request, Exception('error message'))

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]

        assert log_file.stem.startswith('test-request')

        log_file_contents = log_file.read_text()

        try:
            assert log_file_contents.count('<empty>') == 3
            assert log_file_contents.count('{}') == 1
            assert '-> POST mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:' in log_file_contents
            assert '<- mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN status=ERROR:' in log_file_contents
            assert 'Exception: error message' in log_file_contents
        finally:
            log_file.unlink()

        parent.user._context['log_all_requests'] = True
        request.method = RequestMethod.PUT
        request.source = 'execute command foobar'
        request.metadata = {
            'application': 'helloworld',
        }

        event(
            'custom-user-call',
            (
                {
                    'x-bus-message': 'yes',
                    'sent-by': 'grizzly',
                },
                '<?xml encoding="UTF-8" version="1.0"?><test>value</test>',
            ),
            request,
        )

        log_files = get_log_files()
        assert len(log_files) == 1
        log_file = log_files[-1]

        log_file_contents = log_file.read_text()

        try:
            # check response section
            assert (
                """<- mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN status=OK:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>"""
                in log_file_contents
            )

            # check request section
            assert (
                """-> PUT mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
{
  "application": "helloworld"
}

payload:
execute command foobar"""
                in log_file_contents
            )
        finally:
            log_file.unlink()

        parent.user._context['log_all_requests'] = False
        request.method = RequestMethod.GET

        event(
            'custom-user-call',
            (
                {
                    'x-bus-message': 'yes',
                    'sent-by': 'grizzly',
                },
                '<?xml encoding="UTF-8" version="1.0"?><test>value</test>',
            ),
            request,
            Exception('error message'),
            locust_request_meta={
                'response_time': 133.7,
            },
        )

        log_files = get_log_files()
        assert len(log_files) == 1
        log_file = log_files[-1]

        log_file_contents = log_file.read_text()

        try:
            # check response section
            assert (
                """<- mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN (133.70 ms) status=ERROR:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>"""
                in log_file_contents
            )

            # check request section
            assert (
                """-> GET mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
{
  "application": "helloworld"
}

payload:
execute command foobar"""
                in log_file_contents
            )
        finally:
            log_file.unlink()

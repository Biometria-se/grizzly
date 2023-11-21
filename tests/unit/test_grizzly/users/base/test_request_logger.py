"""Unit tests of grizzly.users.base.request_logger."""
from __future__ import annotations

from contextlib import suppress
from os import environ
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Generator, List, Type

import pytest
from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager

from grizzly.tasks import RequestTask
from grizzly.types import GrizzlyResponseContextManager, RequestMethod
from grizzly.users.base import GrizzlyUser, HttpRequests, RequestLogger

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.capture import CaptureFixture
    from _pytest.fixtures import SubRequest

    from tests.fixtures import BehaveFixture, GrizzlyFixture, ResponseContextManagerFixture


@pytest.fixture(params=[False, True])
def request_logger(request: SubRequest, grizzly_fixture: GrizzlyFixture) -> Generator[RequestLogger, None, None]:
    try:
        if request.param:
            environ['GRIZZLY_LOG_DIR'] = 'foobar'

        grizzly_fixture.grizzly.scenarios.create(grizzly_fixture.behave.create_scenario('test'))

        cls = type('FakeRequestLogger', (GrizzlyUser, RequestLogger), {'host': 'dummy://test', '__scenario__': grizzly_fixture.grizzly.scenario, '_tasks': []})

        yield cls(grizzly_fixture.behave.locust.environment)
    finally:
        with suppress(KeyError):
            del environ['GRIZZLY_LOG_DIR']


@pytest.fixture()
def get_log_files() -> Callable[[], List[Path]]:
    def wrapped() -> List[Path]:
        logs_root = Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs'
        log_dir = environ.get('GRIZZLY_LOG_DIR', None)
        if log_dir is not None:
            logs_root = logs_root / log_dir

        return list(logs_root.glob('*.log'))


    return wrapped


class TestRequestLogger:
    @pytest.mark.parametrize('log_prefix', [False, True])
    def test___init__(self, behave_fixture: BehaveFixture, *, log_prefix: bool) -> None:
        try:
            log_root = Path(environ['GRIZZLY_CONTEXT_ROOT']) / 'logs'
            if log_prefix:
                environ['GRIZZLY_LOG_DIR'] = 'asdf'
                log_root = log_root / 'asdf'

            assert not log_root.exists()

            behave_fixture.grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

            fake_user_type = type('FakeRequestLogger', (GrizzlyUser, RequestLogger, HttpRequests), {
                'host': 'https://test.example.org',
                '__scenario__': behave_fixture.grizzly.scenario,
            })

            user = fake_user_type(behave_fixture.locust.environment)

            assert log_root.is_dir()

            assert not user.context().get('log_all_requests', True)
            assert len(user.response_event._handlers) == 1

            RequestLogger.host = 'dummy://test'
            user = RequestLogger(behave_fixture.locust.environment)

            assert len(user.response_event._handlers) == 1
            assert user.client is None
        finally:
            with suppress(KeyError):
                del environ['GRIZZLY_LOG_DIR']

    @pytest.mark.usefixtures('request_logger')
    def test_normalize(self, request_logger: RequestLogger) -> None:
        assert request_logger._normalize('test') == 'test'
        assert request_logger._normalize('Hello World!') == 'Hello-World'
        assert request_logger._normalize('[does]this-look* <strange>!') == 'doesthis-look-strange'

    @pytest.mark.usefixtures('request_logger')
    def test__remove_secrets_attribute(self, request_logger: RequestLogger) -> None:
        assert request_logger._remove_secrets_attribute({
            'test': 'visible',
            'access_token': 'hidden',
            'Authorization': 'hidden',
            'authorization': 'hidden',
            'Content-Type': 'application/json',
        }) == {
            'test': 'visible',
            'access_token': '*** REMOVED ***',
            'Authorization': '*** REMOVED ***',
            'authorization': '*** REMOVED ***',
            'Content-Type': 'application/json',
        }

        assert request_logger._remove_secrets_attribute({'contents': 'test value'}) == {'contents': 'test value'}
        assert request_logger._remove_secrets_attribute(None) is None
        assert request_logger._remove_secrets_attribute(True) is True  # noqa: FBT003
        assert request_logger._remove_secrets_attribute('hello world') == 'hello world'

    @pytest.mark.usefixtures('request_logger', 'get_log_files')
    @pytest.mark.parametrize('cls_rcm', [ResponseContextManager, FastResponseContextManager])
    def test_request_logger_http(  # noqa: PLR0915
        self,
        grizzly_fixture: GrizzlyFixture,
        request_logger: RequestLogger,
        get_log_files: Callable[[], List[Path]],
        cls_rcm: Type[GrizzlyResponseContextManager],
        response_context_manager_fixture: ResponseContextManagerFixture,
        capsys: CaptureFixture,
    ) -> None:
        parent = grizzly_fixture()
        request_logger.host = 'https://test.example.org'
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        response = response_context_manager_fixture(cls_rcm, 200, name=request.name)  # type: ignore[arg-type]

        assert get_log_files() == []

        # response status code == 200, don't do anything
        request_logger.request_logger('test-request', response, request, parent.user)
        assert get_log_files() == []

        # response status code == 401, but is added as an allowed response code, don't do anything
        response = response_context_manager_fixture(cls_rcm, 401, name=request.name)  # type: ignore[arg-type]
        request.response.add_status_code(401)

        response_context = request.response
        setattr(request, 'response', None)  # noqa: B010
        request_logger.request_logger('test-request', response, request, parent.user)

        setattr(request, 'response', response_context)  # noqa: B010

        request_logger.request_logger('test-request', response, request, parent.user)

        assert get_log_files() == []

        # log request, name not set, byte body
        request_logger._context['log_all_requests'] = True
        request.response.status_codes = [200]
        response = response_context_manager_fixture(cls_rcm, 401, name=request.name)  # type: ignore[arg-type]
        del response.request_meta['response_time']

        request_logger.request_logger('none-test', response, request, parent.user)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert log_file.stem.startswith('none-test')

        log_file_contents = log_file.read_text()

        try:
            assert '-> POST:' in log_file_contents
            assert '<- status=401' in log_file_contents
            assert log_file_contents.count('<empty>') == 4
            assert log_file_contents.count('metadata:') == 2
            assert log_file_contents.count('payload:') == 2
            assert log_file_contents.count('] -> ') == 1
            assert log_file_contents.count('] <- ') == 1
            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0
        finally:
            log_file.unlink()
        capsys.readouterr()

        # log failed request (400 - bad request) with locust meta data
        request_logger._context['log_all_requests'] = False
        request.method = RequestMethod.GET
        response = response_context_manager_fixture(
            cls_rcm,
            status_code=400,
            request_method='GET',
            request_headers={},
            response_body={"test": "contents"},
            url='https://test.example.org/api/v1/test',
            name=request.name,  # type: ignore[arg-type]
        )
        request.name = 'test-log-file'
        response.request_meta = {
            'response_time': 200,
        }

        request_logger.request_logger(request.name, response, request, parent.user)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert log_file.stem.startswith('test-log-file')

        log_file_contents = log_file.read_text()

        try:
            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0
            assert 'GET https://test.example.org/api/v1/test' in log_file_contents
            assert '<- https://test.example.org/api/v1/test (200.00 ms) status=400' in log_file_contents
            assert """{
  "test": "contents"
}""" in log_file_contents
        finally:
            log_file.unlink()
        capsys.readouterr()

        # log failed request (400 - bad request) with locust meta data
        request.name = 'test-log-file2'
        request.method = RequestMethod.PUT
        response = response_context_manager_fixture(
            cls_rcm,
            status_code=400,
            response_body='',
            response_headers={
                'x-cookie': 'asdfasdfasdf',
            },
            request_method='PUT',
            request_body='test body str',
            request_headers={
                'Content-Type': 'application/json',
                'Content-Length': '1337',
            },
            url='https://test.example.org/api/v1/test',
            name=request.name,  # type: ignore[arg-type]
        )
        response.request_meta = {
            'response_time': 137.2111,
        }

        request_logger.request_logger(request.name, response, request, parent.user)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert log_file.stem.startswith('test-log-file2')

        log_file_contents = log_file.read_text()

        try:
            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0

            assert '<- https://test.example.org/api/v1/test (137.21 ms) status=400' in log_file_contents
            assert 'PUT https://test.example.org/api/v1/test' in log_file_contents
            assert """payload:
test body str""" in log_file_contents
            assert """metadata:
{
  "Content-Type": "application/json",
  "Content-Length": "1337"
}""".lower() in log_file_contents.lower()
            assert """metadata:
{
  "x-cookie": "asdfasdfasdf"
}""" in log_file_contents
        finally:
            log_file.unlink()

    @pytest.mark.usefixtures('request_logger', 'get_log_files')
    def test_handler_custom(self, grizzly_fixture: GrizzlyFixture, request_logger: RequestLogger, get_log_files: Callable[[], List[Path]]) -> None:
        parent = grizzly_fixture()
        parent.user.host = 'mq://mq.example.org?QueueManager=QMGR01&Channel=SYS.CONN'
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='MSG.INCOMING')

        # pre sanity check
        assert get_log_files() == []

        # no exception, and do not log all requests
        request_logger.request_logger('test-request', (None, '{}'), request, parent.user)

        assert get_log_files() == []

        request_logger.request_logger('[test-request!', (None, '{}'), request, parent.user, Exception('error message'))

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]

        assert log_file.stem.startswith('test-request')

        log_file_contents = log_file.read_text()

        try:
            assert log_file_contents.count('<empty>') == 3
            assert log_file_contents.count('{}') == 1
            assert '-> POST mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:' in log_file_contents
            assert '<- status=ERROR:' in log_file_contents
            assert 'Exception: error message' in log_file_contents
        finally:
            log_file.unlink()

        request_logger._context['log_all_requests'] = True
        request.method = RequestMethod.PUT

        request_logger.request_logger(
            'custom-user-call',
            ({
                'x-bus-message': 'yes',
                'sent-by': 'grizzly',
            }, '<?xml encoding="UTF-8" version="1.0"?><test>value</test>'),
            request,
            parent.user,
        )

        log_files = get_log_files()
        assert len(log_files) == 1
        log_file = log_files[-1]

        log_file_contents = log_file.read_text()

        try:
            # check response section
            assert """<- status=OK:
metadata:
<empty>

payload:
<empty>""" in log_file_contents

            # check request section
            assert """-> PUT mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>""" in log_file_contents
        finally:
            log_file.unlink()

        request_logger._context['log_all_requests'] = False
        request.method = RequestMethod.GET

        request_logger.request_logger(
            'custom-user-call',
            ({
                'x-bus-message': 'yes',
                'sent-by': 'grizzly',
            }, '<?xml encoding="UTF-8" version="1.0"?><test>value</test>'),
            request,
            parent.user,
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
            assert """<- (133.70 ms) status=ERROR:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>

Exception: error message""" in log_file_contents

            # check request section
            assert """-> GET mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
<empty>

payload:
<empty>""" in log_file_contents
        finally:
            log_file.unlink()

import shutil

from os import environ, path, listdir, remove
from typing import Generator, List, Callable, Type

import pytest

from _pytest.tmpdir import TempPathFactory
from _pytest.capture import CaptureFixture
from locust.clients import ResponseContextManager
from locust.contrib.fasthttp import ResponseContextManager as FastResponseContextManager

from grizzly.users.base import RequestLogger, HttpRequests
from grizzly.types import RequestMethod, GrizzlyResponseContextManager
from grizzly.tasks import RequestTask

from ....fixtures import LocustFixture, ResponseContextManagerFixture


@pytest.mark.usefixtures('locust_fixture')
@pytest.fixture
def request_logger(locust_fixture: LocustFixture, tmp_path_factory: TempPathFactory) -> Generator[RequestLogger, None, None]:
    test_context = tmp_path_factory.mktemp('test_context') / 'requests'
    test_context_root = path.dirname(str(test_context))
    environ['GRIZZLY_CONTEXT_ROOT'] = test_context_root

    try:
        RequestLogger.host = 'https://example.org'
        yield RequestLogger(locust_fixture.env)
    finally:
        shutil.rmtree(test_context_root)

        try:
            del environ['GRIZZLY_CONTEXT_ROOT']
        except KeyError:
            pass


@pytest.fixture
def get_log_files() -> Callable[[], List[str]]:
    def wrapped() -> List[str]:
        logs_root = path.join(environ['GRIZZLY_CONTEXT_ROOT'], 'logs')
        log_files = [
            path.join(logs_root, f)
            for f in listdir(logs_root)
            if path.isfile(path.join(logs_root, f)) and f.endswith('.log')
        ]

        return log_files

    return wrapped


class TestRequestLogger:
    def test___init__(self, locust_fixture: LocustFixture) -> None:
        assert not path.isdir(path.join(environ['GRIZZLY_CONTEXT_ROOT'], 'logs'))

        fake_user_type = type('FakeRequestLogger', (RequestLogger, HttpRequests,), {
            'host': 'https://test.example.org',
        })

        user = fake_user_type(locust_fixture.env)

        assert path.isdir(path.join(environ['GRIZZLY_CONTEXT_ROOT'], 'logs'))
        assert not user._context.get('log_all_requests', None)
        assert len(user.response_event._handlers) == 1

        RequestLogger.host = 'mq://example.org'
        user = RequestLogger(locust_fixture.env)

        assert len(user.response_event._handlers) == 1
        assert user.client is None

    @pytest.mark.usefixtures('request_logger')
    def test__normalize(self, request_logger: RequestLogger) -> None:
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
        assert request_logger._remove_secrets_attribute(True) is True
        assert request_logger._remove_secrets_attribute('hello world') == 'hello world'

    @pytest.mark.usefixtures('request_logger')
    def test_request(self, request_logger: RequestLogger) -> None:
        with pytest.raises(NotImplementedError):
            request_logger.request(RequestTask(RequestMethod.GET, 'test', endpoint='/hello'))

    @pytest.mark.usefixtures('request_logger', 'get_log_files')
    @pytest.mark.parametrize('cls_rcm', [ResponseContextManager, FastResponseContextManager])
    def test_request_logger_http(
        self,
        request_logger: RequestLogger,
        get_log_files: Callable[[], List[str]],
        cls_rcm: Type[GrizzlyResponseContextManager],
        response_context_manager_fixture: ResponseContextManagerFixture,
        capsys: CaptureFixture,
    ) -> None:
        request_logger.host = 'https://test.example.org'
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='/api/test')
        response = response_context_manager_fixture(cls_rcm, 200, name=request.name)  # type: ignore

        assert get_log_files() == []

        # response status code == 200, don't do anything
        request_logger.request_logger('test-request', response, request, request_logger)
        assert get_log_files() == []

        # response status code == 401, but is added as an allowed response code, don't do anything
        response = response_context_manager_fixture(cls_rcm, 401, name=request.name)  # type: ignore
        request.response.add_status_code(401)

        response_context = request.response
        setattr(request, 'response', None)
        request_logger.request_logger('test-request', response, request, request_logger)

        setattr(request, 'response', response_context)

        request_logger.request_logger('test-request', response, request, request_logger)

        assert get_log_files() == []

        # log request, name not set, byte body
        request_logger._context['log_all_requests'] = True
        request.response.status_codes = [200]
        response = response_context_manager_fixture(cls_rcm, 401, name=request.name)  # type: ignore
        del response.request_meta['response_time']

        request_logger.request_logger('none-test', response, request, request_logger)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert path.basename(log_file).startswith('none-test')

        with open(log_file) as fd:
            log_file_contents = fd.read()

            print(log_file_contents)

            assert '-> POST:' in log_file_contents
            assert '<- status=401' in log_file_contents
            assert log_file_contents.count('<empty>') == 4
            assert log_file_contents.count('metadata:') == 2
            assert log_file_contents.count('payload:') == 2
            assert log_file_contents.count('] -> ') == 1
            assert log_file_contents.count('] <- ') == 1
            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0

        remove(log_file)
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
            name=request.name,  # type: ignore
        )
        request.name = 'test-log-file'
        response.request_meta = {
            'response_time': 200,
        }

        request_logger.request_logger(request.name, response, request, request_logger)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert path.basename(log_file).startswith('test-log-file')

        with open(log_file) as fd:
            log_file_contents = fd.read()

            print(log_file_contents)

            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0
            assert 'GET https://test.example.org/api/v1/test' in log_file_contents
            assert '<- https://test.example.org/api/v1/test (200.00 ms) status=400' in log_file_contents
            assert '''{
  "test": "contents"
}''' in log_file_contents

        remove(log_file)
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
            name=request.name,  # type: ignore
        )
        response.request_meta = {
            'response_time': 137.2111,
        }

        request_logger.request_logger(request.name, response, request, request_logger)

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]
        assert path.basename(log_file).startswith('test-log-file2')

        with open(log_file) as fd:
            log_file_contents = fd.read()

            print(log_file_contents)

            assert log_file_contents.count('None') == 0
            assert log_file_contents.count('[]') == 0

            assert '<- https://test.example.org/api/v1/test (137.21 ms) status=400' in log_file_contents
            assert 'PUT https://test.example.org/api/v1/test' in log_file_contents
            assert '''payload:
test body str''' in log_file_contents
            assert '''metadata:
{
  "Content-Type": "application/json",
  "Content-Length": "1337"
}'''.lower() in log_file_contents.lower()
            assert '''metadata:
{
  "x-cookie": "asdfasdfasdf"
}''' in log_file_contents

        remove(log_file)

    @pytest.mark.usefixtures('request_logger', 'get_log_files')
    def test_handler_custom(self, request_logger: RequestLogger, get_log_files: Callable[[], List[str]]) -> None:
        request_logger.host = 'mq://mq.example.org?QueueManager=QMGR01&Channel=SYS.CONN'
        request = RequestTask(RequestMethod.POST, name='test-request', endpoint='MSG.INCOMING')

        # pre sanity check
        assert get_log_files() == []

        # no exception, and do not log all requests
        request_logger.request_logger('test-request', (None, '{}'), request, request_logger)

        assert get_log_files() == []

        request_logger.request_logger('[test-request!', (None, '{}'), request, request_logger, Exception('error message'))

        log_files = get_log_files()

        assert len(log_files) == 1
        log_file = log_files[-1]

        assert path.basename(log_file).startswith('test-request.')

        with open(log_file) as fd:
            log_file_contents = fd.read()

            assert log_file_contents.count('<empty>') == 3
            assert log_file_contents.count('{}') == 1
            assert '-> POST mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:' in log_file_contents
            assert '<- status=ERROR:' in log_file_contents
            assert 'Exception: error message' in log_file_contents

        remove(log_file)

        request_logger._context['log_all_requests'] = True
        request.method = RequestMethod.PUT

        request_logger.request_logger(
            'custom-user-call',
            ({
                'x-bus-message': 'yes',
                'sent-by': 'grizzly',
            }, '<?xml encoding="UTF-8" version="1.0"?><test>value</test>'),
            request,
            request_logger,
        )

        log_files = get_log_files()
        assert len(log_files) == 1
        log_file = log_files[-1]

        with open(log_file) as fd:
            log_file_contents = fd.read()

            # check response section
            assert '''<- status=OK:
metadata:
<empty>

payload:
<empty>''' in log_file_contents

            # check request section
            assert '''-> PUT mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>''' in log_file_contents

        remove(log_file)

        request_logger._context['log_all_requests'] = False
        request.method = RequestMethod.GET

        request_logger.request_logger(
            'custom-user-call',
            ({
                'x-bus-message': 'yes',
                'sent-by': 'grizzly',
            }, '<?xml encoding="UTF-8" version="1.0"?><test>value</test>'),
            request,
            request_logger,
            Exception('error message'),
            locust_request_meta={
                'response_time': 133.7,
            }
        )

        log_files = get_log_files()
        assert len(log_files) == 1
        log_file = log_files[-1]

        with open(log_file) as fd:
            log_file_contents = fd.read()

            # check response section
            assert '''<- (133.70 ms) status=ERROR:
metadata:
{
  "x-bus-message": "yes",
  "sent-by": "grizzly"
}

payload:
<?xml encoding="UTF-8" version="1.0"?><test>value</test>

Exception: error message''' in log_file_contents

            # check request section
            assert '''-> GET mq://mq.example.org/MSG.INCOMING?QueueManager=QMGR01&Channel=SYS.CONN:
metadata:
<empty>

payload:
<empty>''' in log_file_contents

        remove(log_file)

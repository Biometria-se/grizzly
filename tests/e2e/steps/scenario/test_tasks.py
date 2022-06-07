from json import dumps as jsondumps
from typing import cast
from textwrap import dedent

from behave.runner import Context
from behave.model import Feature
from grizzly.context import GrizzlyContext

from ....fixtures import BehaveContextFixture


def test_e2e_step_task_request_text_with_name_to_endpoint(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 5, f'{len(tasks)} != 5'

        request = tasks[0]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.POST, f'{request.method} != RequestMethod.POST'
        assert request.name == 'test-post', f'{request.name} != test-post'
        assert request.endpoint == '/api/test', f'{request.endpoint} != /api/test'
        assert isinstance(request.template, Template), 'request.template is not a Template'
        assert request.source is not None, 'request.source is None'
        assert jsonloads(request.source) == {'test': 'post'}, f"{request.source} != {'test': 'post'}"
        assert request.response.content_type == TransformerContentType.UNDEFINED, f'{request.response.content_type} != TransformerContentType.UNDEFINED'

        request = tasks[1]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.PUT
        assert request.name == 'test-put'
        assert request.endpoint == '/api/test'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'put'}
        assert request.response.content_type == TransformerContentType.JSON

        request = tasks[2]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.GET
        assert request.name == 'test-get'
        assert request.endpoint == '/api/test'
        assert request.template is None
        assert request.source is None
        assert request.response.content_type == TransformerContentType.UNDEFINED

        request = tasks[3]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.SEND
        assert request.name == 'test-send'
        assert request.endpoint == 'queue:receive-queue'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'send'}
        assert request.response.content_type == TransformerContentType.UNDEFINED

        request = tasks[4]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.RECEIVE
        assert request.name == 'test-receive'
        assert request.endpoint == 'queue:receive-queue'
        assert request.template is None
        assert request.source is None
        assert request.response.content_type == TransformerContentType.XML

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_requests)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            dedent('''Then post request with name "test-post" to endpoint "/api/test"
                """
                {
                    "test": "post"
                }
                """
            '''),
            dedent('''Then put request with name "test-put" to endpoint "/api/test | content_type=json"
                """
                {
                    "test": "put"
                }
                """
            '''),
            'Then get request with name "test-get" from endpoint "/api/test"',
            dedent('''Then send request with name "test-send" to endpoint "queue:receive-queue"
                """
                {
                    "test": "send"
                }
                """
            '''),
            'Then receive request with name "test-receive" from endpoint "queue:receive-queue | content_type=xml"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_request_file_with_name_endpoint(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 3, f'{len(tasks)} != 3'

        request = tasks[0]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.SEND, f'{request.method} != RequestMethod.SEND'
        assert request.name == 'test-send', f'{request.name} != test-send'
        assert request.endpoint == 'queue:receive-queue', f'{request.endpoint} != queue:receive-queue'
        assert isinstance(request.template, Template), 'request.template is not a Template'
        assert request.source is not None, 'request.source is None'
        assert jsonloads(request.source) == {'test': 'request-send'}, f"{request.source} != {'test': 'request-send'}"
        assert request.response.content_type == TransformerContentType.XML, f'{request.response.content_type} != TransformerContentType.XML'
        assert len(request.get_templates()) == 0

        request = tasks[1]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.POST
        assert request.name == 'test-post'
        assert request.endpoint == '/api/test'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'request-{{ post }}'}
        assert request.response.content_type == TransformerContentType.JSON
        assert len(request.get_templates()) == 1

        request = tasks[2]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.PUT
        assert request.name == 'test-put-{{ foo }}'
        assert request.endpoint == '/api/{{ bar }}'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'request-put-{{ foobar }}'}
        assert request.response.content_type == TransformerContentType.UNDEFINED
        assert len(request.get_templates()) == 3

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_requests)

    request_files = behave_context_fixture.root / 'features' / 'requests' / 'test'
    request_files.mkdir(exist_ok=True)

    (request_files / 'request-send.j2.json').write_text(jsondumps({'test': 'request-send'}))
    (request_files / 'request-post.j2.json').write_text(jsondumps({'test': 'request-{{ post }}'}))
    (request_files / 'request-put.j2.json').write_text(jsondumps({'test': 'request-put-{{ foobar }}'}))

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then send request "test/request-send.j2.json" with name "test-send" to endpoint "queue:receive-queue | content_type=xml"',
            'Then post request "test/request-post.j2.json" with name "test-post" to endpoint "/api/test | content_type=json"',
            'Then put request "test/request-put.j2.json" with name "test-put-{{ foo }}" to endpoint "/api/{{ bar }}"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_request_file_with_name(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 2, f'{len(tasks)} != 2'

        for index, request in enumerate(tasks, start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.POST
            assert request.name == f'test-post-{index}'
            assert request.endpoint == '/api/test'
            assert isinstance(request.template, Template)
            assert request.source is not None
            assert jsonloads(request.source) == {'test': f'request-{{{{ post_{index} }}}}-{index}'}
            assert request.response.content_type == TransformerContentType.JSON
            assert len(request.get_templates()) == 1

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_requests)

    request_files = behave_context_fixture.root / 'features' / 'requests' / 'test'
    request_files.mkdir(exist_ok=True)

    (request_files / 'request-post-1.j2.json').write_text(jsondumps({'test': 'request-{{ post_1 }}-1'}))
    (request_files / 'request-post-2.j2.json').write_text(jsondumps({'test': 'request-{{ post_2 }}-2'}))

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then post request "test/request-post-1.j2.json" with name "test-post-1" to endpoint "/api/test | content_type=json"',
            'Then post request "test/request-post-2.j2.json" with name "test-post-2"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_request_text_with_name(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 4, f'{len(tasks)} != 4'

        for index, request in enumerate(tasks[:2], start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.POST
            assert request.name == f'test-post-{index}'
            assert request.endpoint == '/api/test'
            assert isinstance(request.template, Template)
            assert request.source is not None
            assert jsonloads(request.source) == {'value': f'test-post-{index}'}
            assert request.response.content_type == TransformerContentType.JSON
            assert len(request.get_templates()) == 0

        for index, request in enumerate(tasks[2:], start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.GET
            assert request.name == f'test-get-{index}'
            assert request.endpoint == '/api/test'
            assert request.template is None
            assert request.source is None
            assert request.response.content_type == TransformerContentType.XML
            assert len(request.get_templates()) == 0

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_requests)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            dedent('''Then post request with name "test-post-1" to endpoint "/api/test | content_type=json"
                """
                {
                    "value": "test-post-1"
                }
                """
            '''),
            dedent('''Then post request with name "test-post-2"
                """
                {
                    "value": "test-post-2"
                }
                """
            '''),
            'Then get request with name "test-get-1" from endpoint "/api/test | content_type=\"application/xml\""',
            'Then get request with name "test-get-2"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_wait_seconds(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_task_wait(context: Context) -> None:
        from grizzly.tasks import WaitTask
        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, WaitTask)
        assert task.time == 13.37

        task = tasks[1]
        assert isinstance(task, WaitTask)
        assert task.time == 0.123

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_task_wait)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then wait for "13.37" seconds',
            'Then wait for "0.123" seconds',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_log_message(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_task_wait(context: Context) -> None:
        from grizzly.tasks import LogMessage
        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, LogMessage)
        assert task.message == 'hello world!'
        assert len(task.get_templates()) == 0

        task = tasks[1]
        assert isinstance(task, LogMessage)
        assert task.message == 'foobar={{ foobar }}'
        assert len(task.get_templates()) == 1

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_task_wait)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then log message "hello world!"',
            'Then log message "foobar={{ foobar }}"',
        ],
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_transform(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_transform(context: Context) -> None:
        from grizzly.tasks import TransformerTask
        from grizzly_extras.transformer import TransformerContentType, JsonTransformer

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, TransformerTask)
        assert task.content == '{{ document }}'
        assert task.variable == 'document_id'
        assert task.expression == '$.documents.id'
        assert task.content_type == TransformerContentType.JSON
        assert issubclass(task._transformer, JsonTransformer)
        assert task.get_templates() == ['{{ document }}']
        assert callable(task._parser)

        task = tasks[1]
        assert isinstance(task, TransformerTask)
        assert task.content == '{{ document }}'
        assert task.variable == 'document_title'
        assert task.expression == '$.documents.title'
        assert task.content_type == TransformerContentType.JSON
        assert issubclass(task._transformer, JsonTransformer)
        assert task.get_templates() == ['{{ document }}']
        assert callable(task._parser)

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_transform)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "document_id" is "None"',
            'And value for variable "document_title" is "None"',
            'And value for variable "document" is "{\"document\": {\"id\": \"DOCUMENT_8843-1\", \"title\": \"TPM Report 2021\"}}"',
            'Then parse "{{ document }}" as "json" and save value of "$.documents.id" in variable "document_id"',
            'Then parse "{{ document }}" as "json" and save value of "$.documents.title" in variable "document_title"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_client_get_endpoint(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import HttpClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, HttpClientTask)
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == 'https://www.example.org/example.json'
        assert task.name == 'https-get'
        assert task.variable == 'example_openapi', f'{task.variable} != example_openapi'
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        assert task.get_templates() == []

        task = tasks[1]
        assert isinstance(task, HttpClientTask)
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == '{{ endpoint }}'
        assert task.name == 'http-get'
        assert task.variable == 'endpoint_result'
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        assert task.get_templates() == ['{{ endpoint }}']

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_client_task)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "example_openapi" is "None"',
            'And value for variable "endpoint_result" is "None"',
            'Then get "https://www.example.org/example.json" with name "https-get" and save response in "example_openapi"',
            'Then get "http://{{ endpoint }}" with name "http-get" and save response in "endpoint_result"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_client_put_endpoint_file_destination(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import BlobStorageClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bs-put'
        assert task.variable is None
        assert task.source == 'test-file.json'
        assert task.destination == 'uploaded-test-file.json'
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'http'
        assert task.account_name == 'my-unsecure-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=http;AccountName=my-unsecure-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

        task = tasks[1]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bss://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bss-put'
        assert task.variable is None
        assert task.source == 'test-files.json'
        assert task.destination == 'uploaded-test-files.json'
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_client_task)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then put "test-file.json" to "bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container" with name "bs-put" as "uploaded-test-file.json"',
            'Then put "test-files.json" to "bss://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "bss-put" as "uploaded-test-files.json"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_client_put_endpoint_file(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import BlobStorageClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bs-put'
        assert task.variable is None
        assert task.source == 'test-file.json'
        assert task.destination is None
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'http'
        assert task.account_name == 'my-unsecure-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=http;AccountName=my-unsecure-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

        task = tasks[1]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bss://my-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bss-put'
        assert task.variable is None
        assert task.source == 'test-files.json'
        assert task.destination is None
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_client_task)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then put "test-file.json" to "bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container" with name "bs-put"',
            'Then put "test-files.json" to "bss://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "bss-put"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_date(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_date_task(context: Context) -> None:
        from grizzly.tasks import DateTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 3

        task = tasks[0]
        assert isinstance(task, DateTask)
        assert task.variable == 'date1'
        assert task.value == '2022-01-17 12:21:37'
        assert task.arguments == {
            'timezone': 'UTC',
            'format': '%Y-%m-%dT%H:%M:%S.%f',
            'offset': '1D',
        }
        assert task.get_templates() == []

        task = tasks[1]
        assert isinstance(task, DateTask)
        assert task.variable == 'date2'
        assert task.value == '{{ AtomicDate.test }}'
        assert task.arguments == {
            'offset': '-1D',
        }
        assert task.get_templates() == ['{{ AtomicDate.test }}'], f'{str(task.get_templates())}'

        task = tasks[2]
        assert isinstance(task, DateTask)
        assert task.variable == 'date3'
        assert task.value == '{{ datetime.now() }}'
        assert task.arguments == {
            'offset': '1Y',
            'timezone': '{{ timezone }}',
        }
        assert sorted(task.get_templates()) == sorted([
            '{{ datetime.now() }}',
            '{{ timezone }}',
        ]), str(task.get_templates())

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_date_task)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'And value for variable "date1" is "none"',
            'And value for variable "date2" is "none"',
            'And value for variable "date3" is "none"',
            'And value for variable "AtomicDate.test" is "now"',
            'Then parse date "2022-01-17 12:21:37 | timezone=UTC, format="%Y-%m-%dT%H:%M:%S.%f", offset=1D" and save in variable "date1"',
            'Then parse date "{{ AtomicDate.test }} | offset=-1D" and save in variable "date2"',
            'Then parse date "{{ datetime.now() }} | offset=1Y, timezone=\"{{ timezone }}\"" and save in variable "date3"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_async_group(behave_context_fixture: BehaveContextFixture) -> None:
    def validate_async_group(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.types import RequestMethod
        from grizzly.tasks import AsyncRequestGroupTask, RequestTask
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.async_group is None

        tasks = grizzly.scenario.tasks
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, AsyncRequestGroupTask)
        assert sorted(task.get_templates()) == sorted([
            'async-group-{{ index }}',
            'async-group-{{ index }}:test-post-1',
            'async-group-{{ index }}:test-get-1',
        ]), str(task.get_templates())
        assert task.name == 'async-group-{{ index }}'
        assert len(task.requests) == 2

        request = task.requests[0]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.POST
        assert request.name == 'async-group-{{ index }}:test-post-1'
        assert request.endpoint == '/api/test'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'value': 'i have good news!'}
        assert request.get_templates() == ['async-group-{{ index }}:test-post-1']

        request = task.requests[1]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.GET
        assert request.name == 'async-group-{{ index }}:test-get-1'
        assert request.endpoint == '/api/test'
        assert request.template is None
        assert request.source is None
        assert request.get_templates() == ['async-group-{{ index }}:test-get-1']

        task = tasks[1]
        assert isinstance(task, RequestTask)
        assert task.method == RequestMethod.GET
        assert task.name == 'test-get-2'
        assert task.endpoint == '/api/test'
        assert task.source is None
        assert task.template is None

        raise SystemExit(0)

    behave_context_fixture.add_validator(validate_async_group)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Given an async request group with name "async-group-{{ index }}"',
            dedent('''Then post request with name "test-post-1" to endpoint "/api/test"
                """
                {
                    "value": "i have good news!"
                }
                """
            '''),
            'Then get request with name "test-get-1" from endpoint "/api/test"',
            'And close async request group',
            'Then get request with name "test-get-2" from endpoint "/api/test"',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_timer_start_and_stop(behave_context_fixture: BehaveContextFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        timer_1 = stats.get('001 timer-1', 'TIMR')
        timer_2 = stats.get('001 timer-2', 'TIMR')

        assert timer_1.num_requests == 1, f'{timer_1.num_requests=} != 1'
        assert timer_1.total_response_time > 3900 and timer_1.total_response_time < 4100, f'timer_1.total_response_time != 3900<{timer_1.total_response_time}<4100'

        assert timer_2.num_requests == 1, f'{timer_2.num_requests=} != 1'
        assert timer_2.total_response_time > 2900 and timer_2.total_response_time < 3100, f'timer_2.total_response_time != 2900<{timer_2.total_response_time}<3100'

    behave_context_fixture.add_after_feature(after_feature)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Then log message "before-timer-1"',
            'Then start timer with name "timer-1"',
            'Then wait for "1.0" seconds',
            'Then log message "before-timer-2"',
            'Then start timer with name "timer-2"',
            'Then wait for "3.0" seconds',
            'Then stop timer with name "timer-1"',
            'Then stop timer with name "timer-2"',
        ],
    )

    rc, output = behave_context_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert result.index('before-timer-1') < result.index('before-timer-2')


def test_e2e_step_task_request_wait(behave_context_fixture: BehaveContextFixture) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestWaitTask
        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks.pop()  # remove dummy

        task = grizzly.scenario.tasks.pop()

        assert isinstance(task, RequestWaitTask), f'{type(task)} is not expected RequestWaitTask'
        assert task.min_time == 1.4
        assert task.max_time is None

        task = grizzly.scenario.tasks.pop()

        assert isinstance(task, RequestWaitTask), f'{type(task)} is not expected RequestWaitTask'
        assert task.min_time == 15
        assert task.max_time is None

        task = grizzly.scenario.tasks.pop()

        assert isinstance(task, RequestWaitTask), f'{type(task)} is not expected RequestWaitTask'
        assert task.min_time == 1.4
        assert task.max_time == 1.7

        task = grizzly.scenario.tasks.pop()

        assert isinstance(task, RequestWaitTask), f'{type(task)} is not expected RequestWaitTask'
        assert task.min_time == 15
        assert task.max_time == 18

        raise SystemExit(0)

    behave_context_fixture.add_validator(validator)

    feature_file = behave_context_fixture.test_steps(
        scenario=[
            'Given wait "15..18" seconds between tasks',
            'And wait "1.4..1.7" seconds between tasks',
            'Given wait "15" seconds between tasks',
            'And wait "1.4" seconds between tasks',
        ]
    )

    rc, _ = behave_context_fixture.execute(feature_file)

    assert rc == 0

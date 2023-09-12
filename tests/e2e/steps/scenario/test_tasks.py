from json import dumps as jsondumps, loads as jsonloads
from typing import cast, List, Dict
from textwrap import dedent
from pathlib import Path

from grizzly.context import GrizzlyContext
from grizzly.types.behave import Context, Feature

from tests.fixtures import End2EndFixture


def test_e2e_step_task_request_text_with_name_to_endpoint(e2e_fixture: End2EndFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 4, f'{len(tasks)} != 5'

        request = tasks[0]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.POST, f'{request.method} != RequestMethod.POST'
        assert request.name == 'test-post', f'{request.name} != test-post'
        assert request.endpoint == '/api/echo', f'{request.endpoint} != /api/echo'
        assert isinstance(request.template, Template), 'request.template is not a Template'
        assert request.source is not None, 'request.source is None'
        assert jsonloads(request.source) == {'test': 'post'}, f"{request.source} != {'test': 'post'}"
        assert request.response.content_type == TransformerContentType.UNDEFINED, f'{request.response.content_type} != TransformerContentType.UNDEFINED'

        request = tasks[1]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.PUT
        assert request.name == 'test-put'
        assert request.endpoint == '/api/echo'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'put'}
        assert request.response.content_type == TransformerContentType.JSON

        request = tasks[2]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.GET
        assert request.name == 'test-get'
        assert request.endpoint == '/api/echo'
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

    e2e_fixture.add_validator(validate_requests)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            dedent('''Then post request with name "test-post" to endpoint "/api/echo"
                """
                {
                    "test": "post"
                }
                """
            '''),
            dedent('''Then put request with name "test-put" to endpoint "/api/echo | content_type=json"
                """
                {
                    "test": "put"
                }
                """
            '''),
            'Then get request with name "test-get" from endpoint "/api/echo"',
            dedent('''Then send request with name "test-send" to endpoint "queue:receive-queue"
                """
                {
                    "test": "send"
                }
                """
            '''),
        ],
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 1

    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed' in result
    expected_parts = ['SEND 001 test-send: ', 'NotImplementedError(\'SEND is not implemented for RestApiUser_001\')']
    if e2e_fixture._distributed:
        expected_parts.insert(1, '"')
        expected_parts.append('"')

    expected = ''.join(expected_parts)

    assert expected in result


def test_e2e_step_task_request_file_with_name_endpoint(e2e_fixture: End2EndFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()

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
        assert request.endpoint == '/api/echo'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'request-{{ post }}'}
        assert request.response.content_type == TransformerContentType.JSON
        assert len(request.get_templates()) == 1

        request = tasks[2]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.PUT
        assert request.name == 'test-put-{{ foo }}'
        assert request.endpoint == '/api/echo?foo={{ bar }}'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'test': 'request-put-{{ foobar }}'}
        assert request.response.content_type == TransformerContentType.UNDEFINED
        assert len(request.get_templates()) == 3

    e2e_fixture.add_validator(validate_requests)

    request_files = e2e_fixture.root / 'features' / 'requests' / 'test'
    request_files.mkdir(exist_ok=True)

    (request_files / 'request-send.j2.json').write_text(jsondumps({'test': 'request-send'}))
    (request_files / 'request-post.j2.json').write_text(jsondumps({'test': 'request-{{ post }}'}))
    (request_files / 'request-put.j2.json').write_text(jsondumps({'test': 'request-put-{{ foobar }}'}))

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "foo" is "bar"',
            'And value for variable "bar" is "foo"',
            'And value for variable "post" is "get"',
            'And value for variable "foobar" is "barfoo"',
            'Then send request "test/request-send.j2.json" with name "test-send" to endpoint "queue:receive-queue | content_type=xml"',
            'Then post request "test/request-post.j2.json" with name "test-post" to endpoint "/api/echo | content_type=json"',
            'Then put request "test/request-put.j2.json" with name "test-put-{{ foo }}" to endpoint "/api/echo?foo={{ bar }}"',
        ],
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 1
    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed' in result
    expected_parts = ['SEND 001 test-send: ', 'NotImplementedError(\'SEND is not implemented for RestApiUser_001\')']
    if e2e_fixture._distributed:
        expected_parts.insert(1, '"')
        expected_parts.append('"')

    expected = ''.join(expected_parts)

    assert expected in result


def test_e2e_step_task_request_file_with_name(e2e_fixture: End2EndFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 2, f'{len(tasks)} != 2'

        for index, request in enumerate(tasks, start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.POST
            assert request.name == f'test-post-{index}'
            assert request.endpoint == '/api/echo'
            assert isinstance(request.template, Template)
            assert request.source is not None
            assert jsonloads(request.source) == {'test': f'request-{{{{ post_{index} }}}}-{index}'}
            assert request.response.content_type == TransformerContentType.JSON
            assert len(request.get_templates()) == 1

    e2e_fixture.add_validator(validate_requests)

    request_files = e2e_fixture.root / 'features' / 'requests' / 'test'
    request_files.mkdir(exist_ok=True)

    (request_files / 'request-post-1.j2.json').write_text(jsondumps({'test': 'request-{{ post_1 }}-1'}))
    (request_files / 'request-post-2.j2.json').write_text(jsondumps({'test': 'request-{{ post_2 }}-2'}))

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "post_1" is "hello world"',
            'And value for variable "post_2" is "foobar"',
            'Then post request "test/request-post-1.j2.json" with name "test-post-1" to endpoint "/api/echo | content_type=json"',
            'Then post request "test/request-post-2.j2.json" with name "test-post-2"',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_request_text_with_name(e2e_fixture: End2EndFixture) -> None:
    def validate_requests(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.tasks import RequestTask
        from grizzly.types import RequestMethod
        from grizzly_extras.transformer import TransformerContentType
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()

        tasks.pop()  # remove dummy task added by fixture

        assert len(tasks) == 4, f'{len(tasks)} != 4'

        for index, request in enumerate(tasks[:2], start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.POST
            assert request.name == f'test-post-{index}'
            assert request.endpoint == '/api/echo'
            assert isinstance(request.template, Template)
            assert request.source is not None
            assert jsonloads(request.source) == {'value': f'test-post-{index}'}
            assert request.response.content_type == TransformerContentType.JSON
            assert len(request.get_templates()) == 0

        for index, request in enumerate(tasks[2:], start=1):
            assert isinstance(request, RequestTask)
            assert request.method == RequestMethod.GET
            assert request.name == f'test-get-{index}'
            assert request.endpoint == '/api/echo'
            assert request.template is None
            assert request.source is None
            assert request.response.content_type == TransformerContentType.XML
            assert len(request.get_templates()) == 0

    e2e_fixture.add_validator(validate_requests)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            dedent('''Then post request with name "test-post-1" to endpoint "/api/echo | content_type=json"
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
            'Then get request with name "test-get-1" from endpoint "/api/echo | content_type=\"application/xml\""',
            'Then get request with name "test-get-2"',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_wait_seconds(e2e_fixture: End2EndFixture) -> None:
    def validate_task_wait(context: Context) -> None:
        from grizzly.tasks import WaitTask
        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 3

        task = tasks[0]
        assert isinstance(task, WaitTask)
        assert task.time_expression == '13.37'

        task = tasks[1]
        assert isinstance(task, WaitTask)
        assert task.time_expression == '0.123'

        task = tasks[2]
        assert isinstance(task, WaitTask)
        assert task.time_expression == '{{ wait_time }}'

    e2e_fixture.add_validator(validate_task_wait)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And ask for value of variable "wait_time"',
            'Then wait for "13.37" seconds',
            'Then wait for "0.123" seconds',
            'Then wait for "{{ wait_time }}" seconds',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file, testdata={'wait_time': '126'})

    assert rc == 0


def test_e2e_step_task_log_message(e2e_fixture: End2EndFixture) -> None:
    def validate_task_wait(context: Context) -> None:
        from grizzly.tasks import LogMessageTask
        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, LogMessageTask)
        assert task.message == 'hello world!'
        assert len(task.get_templates()) == 0

        task = tasks[1]
        assert isinstance(task, LogMessageTask)
        assert task.message == 'foobar={{ foobar }}'
        assert len(task.get_templates()) == 1

    e2e_fixture.add_validator(validate_task_wait)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "foobar" is "hello world!"',
            'Then log message "hello world!"',
            'Then log message "foobar={{ foobar }}"',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_transform(e2e_fixture: End2EndFixture) -> None:
    def validate_transform(context: Context) -> None:
        from grizzly.tasks import TransformerTask
        from grizzly_extras.transformer import TransformerContentType, JsonTransformer

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 4

        task = tasks[0]
        assert isinstance(task, TransformerTask)
        assert task.content == '{{ document }}'
        assert task.variable == 'document_id'
        assert task.expression == '$.document.id'
        assert task.content_type == TransformerContentType.JSON
        assert issubclass(task._transformer, JsonTransformer)
        assert task.get_templates() == ['{{ document }}']
        assert callable(task._parser)

        task = tasks[1]
        assert isinstance(task, TransformerTask)
        assert task.content == '{{ document }}'
        assert task.variable == 'document_title'
        assert task.expression == '$.document.title'
        assert task.content_type == TransformerContentType.JSON
        assert issubclass(task._transformer, JsonTransformer)
        assert task.get_templates() == ['{{ document }}']
        assert callable(task._parser)

    e2e_fixture.add_validator(validate_transform)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "document_id" is "None"',
            'And value for variable "document_title" is "None"',
            'And value for variable "document" is "{\"document\": {\"id\": \"DOCUMENT_8843-1\", \"title\": \"TPM Report 2021\"}}"',
            'Then parse "{{ document }}" as "json" and save value of "$.document.id" in variable "document_id"',
            'Then parse "{{ document }}" as "json" and save value of "$.document.title" in variable "document_title"',
            'Then log message "document_id={{ document_id }}"',
            'Then log message "document_title={{ document_title }}"',
        ]
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_client_get_endpoint(e2e_fixture: End2EndFixture) -> None:
    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import HttpClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        e2e_fixture_host = data['e2e_fixture.host']

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 3

        task = tasks[0]
        assert isinstance(task, HttpClientTask)
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == f'http://{e2e_fixture_host}/api/echo?foo=bar'
        assert task.name == 'https-get'
        assert task.payload_variable == 'example_openapi', f'{task.payload_variable} != example_openapi'
        assert task.metadata_variable is None
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        actual_templates = task.get_templates()
        assert actual_templates == ['{{ example_openapi }}'], f"{actual_templates} != ['{{{{ example_openapi }}}}']"

        task = tasks[1]
        assert isinstance(task, HttpClientTask)
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == '{{ endpoint }}/api/echo?bar=foo', f'{task.endpoint=}'
        assert task.name == 'http-get'
        assert task.payload_variable == 'endpoint_payload'
        assert task.metadata_variable == 'endpoint_metadata'
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        actual_templates = task.get_templates()
        assert (
            sorted(actual_templates) == sorted(['{{ endpoint }}/api/echo?bar=foo', '{{ endpoint_payload }} {{ endpoint_metadata }}'])
        ), f"{actual_templates} != ['{{{{ endpoint }}}}', '{{{{ endpoint_payload }}}} {{{{ endpoint_metadata}}}}']"

    table: List[Dict[str, str]] = [{
        'e2e_fixture.host': e2e_fixture.host,
    }]

    e2e_fixture.add_validator(validate_client_task, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "example_openapi" is "none"',
            'And value for variable "endpoint_payload" is "none"',
            'And value for variable "endpoint_metadata" is "none"',
            f'And value for variable "endpoint" is "http://{e2e_fixture.host}"',
            f'Then get "http://{e2e_fixture.host}/api/echo?foo=bar" with name "https-get" and save response payload in "example_openapi"',
            'Then get "http://{{ endpoint }}/api/echo?bar=foo" with name "http-get" and save response payload in "endpoint_payload" and metadata in "endpoint_metadata"',
            'Then log message "example_openapi={{ example_openapi }}, endpoint_payload={{ endpoint_payload }}, endpoint_metadata={{ endpoint_metadata }}"',
        ]
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_client_get_endpoint_until(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master

        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)
        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('CLTSK', '001 http-get', 2, 1,),
            ('UNTL', '001 http-get, w=0.11s, r=3, em=1', 1, 0,),
            ('CLTSK', '001 http-env-get', 1, 1,),
            ('UNTL', '001 https-env-get, w=0.1s, r=1, em=1', 1, 1,),
            ('CLTSK', '001 https-get', 2, 1,),
            ('UNTL', '001 https-get, w=0.12s r=1, em=1', 1, 0,),
        ]

        for method, name, expected_num_requests, expected_num_failures in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    def validate_client_task_until(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly_extras.transformer import TransformerContentType
        from grizzly.tasks.clients import HttpClientTask
        from grizzly.tasks import UntilRequestTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        e2e_fixture_host = data['e2e_fixture.host']

        tasks = grizzly.scenario.tasks()
        tasks.pop()  # remove dummy task added by `test_steps`

        assert len(tasks) == 3

        parent_task = tasks[0]
        assert isinstance(parent_task, UntilRequestTask)
        assert parent_task.retries == 3
        assert parent_task.wait == 0.11, f'{parent_task.wait=}'
        assert parent_task.expected_matches == 1
        task = parent_task.request
        assert isinstance(task, HttpClientTask)
        assert task.arguments == {}
        assert task.content_type == TransformerContentType.JSON
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == f'http://{e2e_fixture_host}/api/until/id?nth=2&wrong=bar&right=foo&as_array=True', f'{task.name=}, {task.endpoint=}'
        assert task.name == 'https-get'
        assert task.payload_variable is None
        assert task.metadata_variable is None
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        assert task.get_templates() == []

        parent_task = tasks[1]
        assert isinstance(parent_task, UntilRequestTask)
        assert parent_task.retries == 3
        assert parent_task.wait == 0.12
        assert parent_task.expected_matches == 1
        task = parent_task.request
        assert isinstance(task, HttpClientTask)
        assert task.arguments == {}
        assert task.content_type == TransformerContentType.JSON
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == '{{ endpoint }}/api/until/success?nth=2&wrong=false&right=true&as_array=True', f'{task.name=}, {task.endpoint=}'
        assert task.name == 'http-get'
        assert task.payload_variable is None
        assert task.metadata_variable is None
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        assert task.get_templates() == ['{{ endpoint }}/api/until/success?nth=2&wrong=false&right=true&as_array=True'], f'{task.name=}, {task.get_templates()=}'

        parent_task = tasks[2]
        assert isinstance(parent_task, UntilRequestTask)
        assert parent_task.retries == 1
        assert parent_task.wait == 0.1
        assert parent_task.expected_matches == 1
        task = parent_task.request
        assert isinstance(task, HttpClientTask)
        assert task.arguments == {'verify': False}
        assert task.content_type == TransformerContentType.JSON
        assert task.direction == RequestDirection.FROM
        assert task.endpoint == f'http://{e2e_fixture_host}/api/until/hello?nth=2&wrong=foobar&right=world&as_array=True', f'{task.name=}, {task.endpoint=}'
        assert task.name == 'https-env-get'
        assert task.payload_variable is None
        assert task.metadata_variable is None
        assert task.source is None
        assert task.destination is None
        assert task._short_name == 'Http'
        assert task.get_templates() == []

    table: List[Dict[str, str]] = [{
        'e2e_fixture.host': e2e_fixture.host,
    }]

    e2e_fixture.add_validator(validate_client_task_until, table=table)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'And value for variable "endpoint" is "http://{e2e_fixture.host}"',
            (
                f'Then get "http://{e2e_fixture.host}/api/until/id?nth=2&wrong=bar&right=foo&as_array=True | '
                'content_type=json" with name "https-get" until "$.`this`[?id="foo"] | wait=0.11"'
            ),
            (
                'Then get "http://{{ endpoint }}/api/until/success?nth=2&wrong=false&right=true&as_array=True | '
                'content_type=json" with name "http-get" until "$.`this`[?success="true"] | wait=0.12"'
            ),
            (
                'Then get "https://$conf::test.host$/api/until/hello?nth=2&wrong=foobar&right=world&as_array=True | content_type=json, verify=False" with name "https-env-get" '
                'until "$.`this`[?hello=\"world\"] | retries=1, expected_matches=1, wait=0.1"'
            ),
        ]
    )

    env_conf: Dict[str, Dict[str, Dict[str, str]]] = {
        'configuration': {
            'test': {
                'host': f'http://{e2e_fixture.host}',
            }
        }
    }

    rc, output = e2e_fixture.execute(feature_file, env_conf=env_conf)

    result = ''.join(output)

    assert rc == 1
    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed' in result


def test_e2e_step_task_client_put_endpoint_file_destination(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master

        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)
        errors = grizzly.state.locust.environment.stats.errors

        assert len(errors) == 2

        for error in errors.values():
            assert isinstance(error.error, FileNotFoundError)

    e2e_fixture.add_after_feature(after_feature)

    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import BlobStorageClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bs-put'
        assert task.payload_variable is None
        assert task.metadata_variable is None
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
        assert task.payload_variable is None
        assert task.metadata_variable is None
        assert task.source == 'test-files.json'
        assert task.destination == 'uploaded-test-files.json'
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

    e2e_fixture.add_validator(validate_client_task)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then put "test-file.json" to "bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container" with name "bs-put" as "uploaded-test-file.json"',
            'Then put "test-files.json" to "bss://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "bss-put" as "uploaded-test-files.json"',
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 1
    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed' in result


def test_e2e_step_task_client_put_endpoint_file(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master

        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)
        errors = grizzly.state.locust.environment.stats.errors

        assert len(errors) == 2

        for error in errors.values():
            assert isinstance(error.error, FileNotFoundError)

    e2e_fixture.add_after_feature(after_feature)

    def validate_client_task(context: Context) -> None:
        from grizzly.types import RequestDirection
        from grizzly.tasks.clients import BlobStorageClientTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 2

        task = tasks[0]
        assert isinstance(task, BlobStorageClientTask)
        assert task.direction == RequestDirection.TO
        assert task.endpoint == 'bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container'
        assert task.name == 'bs-put'
        assert task.payload_variable is None
        assert task.metadata_variable is None
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
        assert task.payload_variable is None
        assert task.metadata_variable is None
        assert task.source == 'test-files.json'
        assert task.destination is None
        assert task._short_name == 'BlobStorage'
        assert task.get_templates() == []
        assert task._endpoints_protocol == 'https'
        assert task.account_name == 'my-storage'
        assert task.account_key == 'aaaabbb='
        assert task.container == 'my-container'
        assert task.connection_string == 'DefaultEndpointsProtocol=https;AccountName=my-storage;AccountKey=aaaabbb=;EndpointSuffix=core.windows.net'

    e2e_fixture.add_validator(validate_client_task)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then put "test-file.json" to "bs://my-unsecure-storage?AccountKey=aaaabbb=&Container=my-container" with name "bs-put"',
            'Then put "test-files.json" to "bss://my-storage?AccountKey=aaaabbb=&Container=my-container" with name "bss-put"',
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)

    result = ''.join(output)

    assert rc == 1
    assert 'HOOK-ERROR in after_feature: RuntimeError: locust test failed' in result


def test_e2e_step_task_date(e2e_fixture: End2EndFixture) -> None:
    def validate_date_task(context: Context) -> None:
        from grizzly.tasks import DateTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        tasks = grizzly.scenario.tasks()
        tasks.pop()

        assert len(tasks) == 4

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

    e2e_fixture.add_validator(validate_date_task)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "timezone" is "UTC"',
            'And value for variable "date1" is "none"',
            'And value for variable "date2" is "none"',
            'And value for variable "date3" is "none"',
            'And value for variable "AtomicDate.test" is "now"',
            'Then parse date "2022-01-17 12:21:37 | timezone=UTC, format=\'%Y-%m-%dT%H:%M:%S.%f\', offset=1D" and save in variable "date1"',
            'Then parse date "{{ AtomicDate.test }} | offset=-1D" and save in variable "date2"',
            'Then parse date "{{ datetime.now() }} | offset=1Y, timezone=\'{{ timezone }}\'" and save in variable "date3"',
            'Then log message "date1={{ date1 }}, date2={{ date2 }}, date3={{ date3 }}"',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_async_group(e2e_fixture: End2EndFixture) -> None:
    def validate_async_group(context: Context) -> None:
        from json import loads as jsonloads
        from grizzly.types import RequestMethod
        from grizzly.tasks import AsyncRequestGroupTask, RequestTask
        from jinja2 import Template

        grizzly = cast(GrizzlyContext, context.grizzly)

        assert grizzly.scenario.tasks.tmp.async_group is None

        tasks = grizzly.scenario.tasks()
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
        assert len(task.tasks) == 2

        request = task.tasks[0]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.POST
        assert request.name == 'async-group-{{ index }}:test-post-1'
        assert request.endpoint == '/api/echo'
        assert isinstance(request.template, Template)
        assert request.source is not None
        assert jsonloads(request.source) == {'value': 'i have good news!'}
        assert request.get_templates() == ['async-group-{{ index }}:test-post-1']

        request = task.tasks[1]
        assert isinstance(request, RequestTask)
        assert request.method == RequestMethod.GET
        assert request.name == 'async-group-{{ index }}:test-get-1'
        assert request.endpoint == '/api/echo?foo=bar'
        assert request.template is None
        assert request.source is None
        assert request.get_templates() == ['async-group-{{ index }}:test-get-1']

        task = tasks[1]
        assert isinstance(task, RequestTask)
        assert task.method == RequestMethod.GET
        assert task.name == 'test-get-2'
        assert task.endpoint == '/api/echo?bar=foo'
        assert task.source is None
        assert task.template is None

    e2e_fixture.add_validator(validate_async_group)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "index" is "13"',
            'Given an async request group with name "async-group-{{ index }}"',
            dedent('''Then post request with name "test-post-1" to endpoint "/api/echo"
                """
                {
                    "value": "i have good news!"
                }
                """
            '''),
            'Then get request with name "test-get-1" from endpoint "/api/echo?foo=bar"',
            'And close async request group',
            'Then get request with name "test-get-2" from endpoint "/api/echo?bar=foo"',
        ]
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_timer_start_and_stop(e2e_fixture: End2EndFixture) -> None:
    def after_feature(context: Context, feature: Feature) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        timer_1 = stats.get('001 timer-1', 'TIMR')
        timer_2 = stats.get('001 timer-2', 'TIMR')

        assert timer_1.num_requests == 1, f'{timer_1.num_requests=} != 1'
        assert timer_1.total_response_time > 3900 and timer_1.total_response_time < 4100, f'timer_1.total_response_time != 3900<{timer_1.total_response_time}<4100'

        assert timer_2.num_requests == 1, f'{timer_2.num_requests=} != 1'
        assert timer_2.total_response_time > 2900 and timer_2.total_response_time < 3100, f'timer_2.total_response_time != 2900<{timer_2.total_response_time}<3100'

    e2e_fixture.add_after_feature(after_feature)

    feature_file = e2e_fixture.test_steps(
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

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert result.index('before-timer-1') < result.index('before-timer-2')


def test_e2e_step_task_request_wait(e2e_fixture: End2EndFixture) -> None:
    def validate_request_wait(context: Context) -> None:
        from grizzly.tasks import TaskWaitTask
        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks().pop()  # remove dummy

        assert len(grizzly.scenario.tasks()) == 4

        task = grizzly.scenario.tasks()[0]

        assert isinstance(task, TaskWaitTask), f'{type(task)} is not expected TaskWaitTask'
        assert task.min_time == 15
        assert task.max_time == 18

        task = grizzly.scenario.tasks()[1]

        assert isinstance(task, TaskWaitTask), f'{type(task)} is not expected TaskWaitTask'
        assert task.min_time == 1.4
        assert task.max_time == 1.7

        task = grizzly.scenario.tasks()[2]

        assert isinstance(task, TaskWaitTask), f'{type(task)} is not expected TaskWaitTask'
        assert task.min_time == 15
        assert task.max_time is None

        task = grizzly.scenario.tasks()[3]

        assert isinstance(task, TaskWaitTask), f'{type(task)} is not expected TaskWaitTask'
        assert task.min_time == 1.4
        assert task.max_time is None

    e2e_fixture.add_validator(validate_request_wait)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Given wait "15..18" seconds between tasks',
            'And wait "1.4..1.7" seconds between tasks',
            'Given wait "15" seconds between tasks',
            'And wait "1.4" seconds between tasks',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_task_conditional(e2e_fixture: End2EndFixture) -> None:
    def validate_task_conditional(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks().pop()  # remove dummy task

        assert len(grizzly.scenario.tasks()) == 2

    def after_feature(context: Context, feature: Feature) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        stats = grizzly.state.locust.environment.stats

        conditional = stats.get('001 conditional-1: False (1)', 'COND')

        assert conditional.num_requests == 1, f'{conditional.num_requests} != 1'
        assert conditional.total_content_length == 1, f'{conditional.total_content_length} != 1'

        conditional = stats.get('001 conditional-2: True (1)', 'COND')

        assert conditional.num_requests == 1, f'{conditional.num_requests} != 1'
        assert conditional.total_content_length == 1, f'{conditional.total_content_length} != 1'

    e2e_fixture.add_validator(validate_task_conditional)
    e2e_fixture.add_after_feature(after_feature)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And ask for value of variable "value_1"',
            'And ask for value of variable "value_2"',
            'When condition "{{ value_1 | int > 5 }}" with name "conditional-1" is true, execute these tasks',
            'Then log message "{{ value_1 }} was greater than 5"',
            'But if condition is false, execute these tasks',
            'Then log message "{{ value_1 }} was less than or equal to 5"',
            'Then end condition',
            'When condition "{{ value_2 | int > 5 }}" with name "conditional-2" is true, execute these tasks',
            'Then log message "{{ value_2 }} was greater than 5"',
            'But if condition is false, execute these tasks',
            'Then log message "{{ value_2 }} was less than or equal to 5"',
            'Then end condition',
        ],
    )

    rc, output = e2e_fixture.execute(feature_file, testdata={'value_1': '1', 'value_2': '6'})

    assert rc == 0
    result = ''.join(output)

    assert '1 was greater than 5' not in result
    assert '1 was less than or equal to 5' in result
    assert '6 was greater than 5' in result
    assert '6 was less than or equal to 5' not in result


def test_e2e_step_task_loop(e2e_fixture: End2EndFixture) -> None:
    def validate_task_loop(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks().pop()  # remove dummy task

        assert len(grizzly.scenario.tasks()) == 1

    def after_feature(context: Context, feature: Feature) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        stats = grizzly.state.locust.environment.stats
        loop = stats.get('001 loop-1 (1)', 'LOOP')

        assert loop.num_requests == 1, f'{loop.num_requests} != 1'

    e2e_fixture.add_validator(validate_task_loop)
    e2e_fixture.add_after_feature(after_feature)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "loop_value" is "none"',
            'And value for variable "loop_values" is "[\"foo\", \"bar\", \"hello\", \"world\"]"',
            'Then loop "{{ loop_values }}" as variable "loop_value" with name "loop-1"',
            'Then log message "loop_value={{ loop_value }}"',
            'Then end loop',
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert 'loop_value=foo' in result
    assert 'loop_value=bar' in result
    assert 'loop_value=hello' in result
    assert 'loop_value=world' in result


def test_e2e_step_task_keystore(e2e_fixture: End2EndFixture) -> None:
    def validate_keystore_task(context: Context) -> None:
        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks().pop()  # remove dummy task

        assert len(grizzly.scenario.tasks()) == 4

    e2e_fixture.add_validator(validate_keystore_task)

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "foobar" is "none"',
            'And value for variable "barfoo" is "none"',
            'Then set "foobar" in keystore with value "[\'hello\', \'world\']"',
            'Then get "foobar" from keystore and save in variable "foobar"',
            'Then get "barfoo" from keystore and save in variable "barfoo", with default value "{\'hello\': \'world\'}"',
            'Then log message "foobar={{ foobar }}, barfoo={{ barfoo }}"',
        ]
    )

    rc, output = e2e_fixture.execute(feature_file)

    assert rc == 0

    result = ''.join(output)

    assert "foobar=['hello', 'world'], barfoo={'hello': 'world'}" in result

    persistent_file = e2e_fixture.root / 'features' / 'persistent' / f'{Path(feature_file).stem}.json'
    assert persistent_file.exists()
    assert jsonloads(persistent_file.read_text()) == {
        'grizzly::keystore': {
            'foobar': ['hello', 'world'],
            'barfoo': {'hello': 'world'},
        },
    }

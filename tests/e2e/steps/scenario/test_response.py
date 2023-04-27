from typing import cast, Dict, List
from itertools import product

from grizzly.types.behave import Context
from grizzly.context import GrizzlyContext
from grizzly.types import ResponseTarget
from grizzly.types.behave import Feature

from tests.fixtures import End2EndFixture


def test_e2e_step_response_save_matches(e2e_fixture: End2EndFixture) -> None:
    targets = [target for target in ResponseTarget]

    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import SaveHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks().pop()  # latest task is a dummy task

        rows = list(context.table)
        assert len(grizzly.scenario.orphan_templates) == len(rows)

        for row in rows:
            data = row.as_dict()
            handler_type = data['target']
            index = int(data['index'])
            attr_name = data['attr_name']

            request = grizzly.scenario.tasks()[index]

            assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'

            handlers = getattr(request.response.handlers, handler_type)

            assert f'{{{{ expression_{index} }}}}' in grizzly.scenario.orphan_templates, f'{{{{ expression_{index} }}}} not in {grizzly.scenario.orphan_templates}'
            assert grizzly.state.variables.get(f'expression_{index}', None) == f'$.`this`.{attr_name}', f'variable expression_{index} is not $.`this`.{attr_name}'
            assert len(handlers) == 1, f'unexpected number of {target} handlers'
            handler = handlers[0]
            assert isinstance(handler, SaveHandlerAction), f'{handler.__class__.__name__} != SaveHandlerAction'
            assert handler.variable == f'tmp_{index}', f'{handler.variable} != tmp_{index}'
            assert handler.expression == f'{{{{ expression_{index} }}}}', f'{handler.expression} != {{{{ expression_{index} }}}}'
            assert handler.match_with == 'foo.*$', f'{handler.match_with} != foo.*$'
            assert handler.expected_matches == 1, f'{handler.expected_matches} != 1'

    table: List[Dict[str, str]] = []
    scenario: List[str] = []

    index = 0
    for target in targets:
        if target == ResponseTarget.METADATA:
            attr_name = 'Foobar'
        else:
            attr_name = 'foobar'

        table.append({'target': target.name.lower(), 'index': str(index), 'attr_name': attr_name})

        scenario += [
            f'Given value for variable "expression_{index}" is "$.`this`.{attr_name}"',
            f'Given value for variable "tmp_{index}" is "none"',
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/echo?foobar=foo | content_type=json"',
            'And metadata "foobar" is "foobar"',
            f'Then save response {target.name.lower()} "{{{{ expression_{index} }}}} | expected_matches=1" that matches "foo.*$" in variable "tmp_{index}"',
            f'Then log message "tmp_{index}={{{{ tmp_{index} }}}}"',
        ]
        index += 2

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=scenario,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_response_save(e2e_fixture: End2EndFixture) -> None:
    targets = [target for target in ResponseTarget]

    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import SaveHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks().pop()  # latest task is a dummy task

        rows = list(context.table)

        for row in rows:
            data = row.as_dict()
            handler_type = data['target']
            index = int(data['index'])
            attr_name = data['attr_name']

            assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

            request = grizzly.scenario.tasks()[index]

            assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'

            handlers = getattr(request.response.handlers, handler_type)

            assert len(handlers) == 1, f'unexpected number of {target} handlers'
            handler = handlers[0]
            assert isinstance(handler, SaveHandlerAction), f'{handler.__class__.__name__} != SaveHandlerAction'
            assert handler.variable == f'tmp_{index}', f'{handler.variable} != tmp_{index}'
            assert handler.expression == f'$.`this`.{attr_name}', f'{handler.expression} != $.`this`.{attr_name}'
            assert handler.match_with == '.*', f'{handler.match_with} != .*'
            assert handler.expected_matches == 1, f'{handler.expected_matches} != 1'

    table: List[Dict[str, str]] = []
    scenario: List[str] = []

    index = 0
    for target in targets:
        if target == ResponseTarget.METADATA:
            attr_name = 'Foobar'
        else:
            attr_name = 'foobar'
        table.append({'target': target.name.lower(), 'index': str(index), 'attr_name': attr_name})

        scenario += [
            f'Given value for variable "tmp_{index}" is "none"',
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/echo?foobar=foo | content_type=json"',
            'And metadata "foobar" is "foobar"',
            f'Then save response {target.name.lower()} "$.`this`.{attr_name} | expected_matches=1" in variable "tmp_{index}"',
            f'Then log message "tmp_{index}={{{{ tmp_{index} }}}}"',
        ]

        index += 2

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=scenario,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


def test_e2e_step_response_validate(e2e_fixture: End2EndFixture) -> None:
    parameterize = list(product(ResponseTarget, ['is', 'is not']))

    def after_feature(context: Context, feature: Feature) -> None:
        from grizzly.locust import on_master

        if on_master(context):
            return

        grizzly = cast(GrizzlyContext, context.grizzly)
        stats = grizzly.state.locust.environment.stats

        expectations = [
            ('GET', '001 metadata-handler', 2, 1,),
            ('GET', '001 payload-handler', 2, 1,),
        ]

        for method, name, expected_num_requests, expected_num_failures in expectations:
            stat = stats.get(name, method)
            assert stat.num_failures == expected_num_failures, f'{stat.method}:{stat.name}.num_failures: {stat.num_failures} != {expected_num_failures}'
            assert stat.num_requests == expected_num_requests, f'{stat.method}:{stat.name}.num_requests: {stat.num_requests} != {expected_num_requests}'

    e2e_fixture.add_after_feature(after_feature)

    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import ValidationHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks().pop()  # latest task is a dummy task

        rows = list(context.table)

        for index, row in enumerate(rows):
            data = row.as_dict()
            handler_type = data['target']
            textual_condition = data['condition']

            assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

            assert grizzly.scenario.failure_exception is None

            request = grizzly.scenario.tasks()[index]

            assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'

            handlers = getattr(request.response.handlers, handler_type)

            assert len(handlers) == 1, f'unexpected number of {target} handlers'
            handler = handlers[0]
            assert isinstance(handler, ValidationHandlerAction), f'{handler.__class__.__name__} != ValidationHandlerAction'
            if textual_condition == 'is not':
                assert not handler.condition
            else:
                assert handler.condition

            assert handler.expression == '$.hello.world', f'{handler.expression} != $.hello.world'
            assert handler.match_with == 'foo[bar]?', f'{handler.match_with} != foo[bar]?'
            assert handler.expected_matches == 1, f'{handler.expected_matches} != 1'

    table: List[Dict[str, str]] = []
    scenario: List[str] = []

    for target, condition in parameterize:
        table.append({
            'target': target.name.lower(),
            'condition': condition,
        })

        scenario += [
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/echo | content_type=json"',
            f'When response {target.name.lower()} "$.hello.world" {condition} "foo[bar]?" fail request',
        ]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=scenario,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 1


def test_e2e_step_allow_status_codes(e2e_fixture: End2EndFixture) -> None:
    status_codes = ['200,301', '-200,500']

    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask

        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks().pop()  # latest task is a dummy task

        rows = list(context.table)

        for index, row in enumerate(rows):
            data = row.as_dict()
            status_codes = [int(status_code.strip()) for status_code in data['status_codes'].split(',') if status_code.strip() != '-200']

            request = grizzly.scenario.tasks()[index]

            assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
            assert request.response.status_codes == status_codes

        assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

    table: List[Dict[str, str]] = []
    scenario: List[str] = []

    for status_code in status_codes:
        table.append({'status_codes': status_code})
        _, s = status_code.split(',', 1)
        scenario += [
            f'Then get request with name "test-allow-status-codes" from endpoint "/api/statuscode/{s}"',
            f'And allow response status codes "{status_code}"',
        ]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=scenario,
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0


def test_e2e_step_allow_status_codes_table(e2e_fixture: End2EndFixture) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks().pop()

        request = grizzly.scenario.tasks()[-1]
        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.name == 'test-get-2', f'{request.name} != test-get-2'
        assert request.response.status_codes == [404], f'{str(request.response.status_codes)} != [404]'

        request = grizzly.scenario.tasks()[-2]
        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.name == 'test-get-1', f'{request.name} != test-get-1'
        assert request.response.status_codes == [200, 302], f'{str(request.response.status_codes)} != [200, 302]'

    e2e_fixture.add_validator(
        validator,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then get request with name "test-get-1" from endpoint "/api/statuscode/302"',
            'Then get request with name "test-get-2" from endpoint "/api/statuscode/404"',
            '''And allow response status codes
      | status   |
      | 200, 302 |
      | -200,404 |
''',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0


def test_e2e_step_response_content_type(e2e_fixture: End2EndFixture) -> None:
    content_types = [
        'json', 'application/json',
        'xml', 'application/xml',
        'plain', 'text/plain',
    ]

    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly_extras.transformer import TransformerContentType

        grizzly = cast(GrizzlyContext, context.grizzly)
        grizzly.scenario.tasks.pop()

        rows = list(context.table)

        for index, row in enumerate(rows):
            data = row.as_dict()

            request = grizzly.scenario.tasks()[index]
            assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
            assert request.name == f'test-get-{index}', f'{request.name} != test-get-{index}'
            assert request.response.content_type == TransformerContentType.from_string(data['content_type'])

    table: List[Dict[str, str]] = []
    scenario: List[str] = []

    for index, content_type in enumerate(content_types):
        table.append({'content_type': content_type})

        scenario += [
            f'Then get request with name "test-get-{index}" from endpoint "/api/echo?foo=bar"',
            f'And set response content type to "{content_type}"',
        ]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=scenario,
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0

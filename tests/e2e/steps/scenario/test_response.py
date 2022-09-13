from typing import cast, Dict, List
from itertools import product

import pytest

from behave.runner import Context
from grizzly.context import GrizzlyContext
from grizzly.types import ResponseTarget

from ....fixtures import End2EndFixture


@pytest.mark.parametrize('target', [target for target in ResponseTarget])
def test_e2e_step_response_save_matches(e2e_fixture: End2EndFixture, target: ResponseTarget) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import SaveHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        handler_type = data['target']

        grizzly.scenario.tasks.pop()  # latest task is a dummy task

        assert len(grizzly.scenario.orphan_templates) == 1, 'unexpected number of orphan templates'
        assert grizzly.scenario.orphan_templates[0] == '{{ expression }}', f'{grizzly.scenario.orphan_templates[0]} != {{ expression }}'

        request = grizzly.scenario.tasks[-2]

        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'

        handlers = getattr(request.response.handlers, handler_type)

        assert len(handlers) == 1, f'unexpected number of {target} handlers'
        handler = handlers[0]
        assert isinstance(handler, SaveHandlerAction), f'{handler.__class__.__name__} != SaveHandlerAction'
        assert handler.variable == 'tmp', f'{handler.variable} != tmp'
        assert handler.expression == '{{ expression }}', f'{handler.expression} != {{{{ expression }}}}'
        assert handler.match_with == 'foo[bar]?', f'{handler.match_with} != foo[bar]?'
        assert handler.expected_matches == 10, f'{handler.expected_matches} != 10'

    table: List[Dict[str, str]] = [{
        'target': target.name.lower(),
    }]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "tmp" is "none"',
            'And value for variable "expression" is "$.hello.world"',
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/test | content_type=json"',
            f'Then save response {target.name.lower()} "{{{{ expression }}}} | expected_matches=10" that matches "foo[bar]?" in variable "tmp"',
            'Then log message "tmp={{ tmp }}"',
        ],
        identifier=target.name,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('target', [target for target in ResponseTarget])
def test_e2e_step_response_save(e2e_fixture: End2EndFixture, target: ResponseTarget) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import SaveHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        handler_type = data['target']

        grizzly.scenario.tasks.pop()  # latest task is a dummy task

        assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

        request = grizzly.scenario.tasks[-2]

        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'

        handlers = getattr(request.response.handlers, handler_type)

        assert len(handlers) == 1, f'unexpected number of {target} handlers'
        handler = handlers[0]
        assert isinstance(handler, SaveHandlerAction), f'{handler.__class__.__name__} != SaveHandlerAction'
        assert handler.variable == 'foobar', f'{handler.variable} != foobar'
        assert handler.expression == '$.hello.world', f'{handler.expression} != $.hello.world'
        assert handler.match_with == '.*', f'{handler.match_with} != .*'
        assert handler.expected_matches == 1, f'{handler.expected_matches} != 1'

    table: List[Dict[str, str]] = [{
        'target': target.name.lower(),
    }]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'And value for variable "foobar" is "none"',
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/test | content_type=json"',
            f'Then save response {target.name.lower()} "$.hello.world" in variable "foobar"',
            'Then log message "foobar={{ foobar }}"',
        ],
        identifier=target.name,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('target,condition', list(product(ResponseTarget, ['is', 'is not'])))
def test_e2e_step_response_validate(e2e_fixture: End2EndFixture, target: ResponseTarget, condition: str) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly.users.base.response_handler import ValidationHandlerAction

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        handler_type = data['target']
        textual_condition = data['condition']

        grizzly.scenario.tasks.pop()  # latest task is a dummy task

        assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

        assert grizzly.scenario.failure_exception is None

        request = grizzly.scenario.tasks[-1]

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

    table: List[Dict[str, str]] = [{
        'target': target.name.lower(),
        'condition': condition,
    }]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            f'Then get request with name "{target.name.lower()}-handler" from endpoint "/api/test | content_type=json"',
            f'When response {target.name.lower()} "$.hello.world" {condition} "foo[bar]?" fail request',
        ],
        identifier=target.name,
    )

    rc, _ = e2e_fixture.execute(feature_file)

    assert rc == 0


@pytest.mark.parametrize('status_codes', [
    '200,302',
    '-200,404',
])
def test_e2e_step_allow_status_codes(e2e_fixture: End2EndFixture, status_codes: str) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        data = list(context.table)[0].as_dict()
        status_codes = [int(status_code.strip()) for status_code in data['status_codes'].split(',') if status_code.strip() != '-200']

        grizzly.scenario.tasks.pop()  # latest task is a dummy task

        request = grizzly.scenario.tasks[-1]

        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.response.status_codes == status_codes
        assert len(grizzly.scenario.orphan_templates) == 0, 'unexpected number of orphan templates'

    table: List[Dict[str, str]] = [{
        'status_codes': status_codes,
    }]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then get request with name "test-allow-status-codes" from endpoint "/api/test"',
            f'And allow response status codes "{status_codes}"',
        ],
        identifier=status_codes,
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0


def test_e2e_step_allow_status_codes_table(e2e_fixture: End2EndFixture) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask

        grizzly = cast(GrizzlyContext, context.grizzly)

        grizzly.scenario.tasks.pop()

        request = grizzly.scenario.tasks[-1]
        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.name == 'test-get-2', f'{request.name} != test-get-2'
        assert request.response.status_codes == [404], f'{str(request.response.status_codes)} != [404]'

        request = grizzly.scenario.tasks[-2]
        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.name == 'test-get-1', f'{request.name} != test-get-1'
        assert request.response.status_codes == [200, 302], f'{str(request.response.status_codes)} != [200, 302]'

    e2e_fixture.add_validator(
        validator,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then get request with name "test-get-1" from endpoint "/api/test"',
            'Then get request with name "test-get-2" from endpoint "/api/test"',
            '''And allow response status codes
      | status   |
      | 200, 302 |
      | -200,404 |
''',
        ],
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0


@pytest.mark.parametrize('content_type', [
    'json', 'application/json',
    'xml', 'application/xml',
    'plain', 'text/plain',
])
def test_e2e_step_response_content_type(e2e_fixture: End2EndFixture, content_type: str) -> None:
    def validator(context: Context) -> None:
        from grizzly.tasks import RequestTask
        from grizzly_extras.transformer import TransformerContentType

        grizzly = cast(GrizzlyContext, context.grizzly)
        data = list(context.table)[0].as_dict()

        request = grizzly.scenario.tasks[-2]
        assert isinstance(request, RequestTask), f'{request.__class__.__name__} != RequestTask'
        assert request.name == 'test-get-1', f'{request.name} != test-get-1'
        assert request.response.content_type == TransformerContentType.from_string(data['content_type'])

    table: List[Dict[str, str]] = [{
        'content_type': content_type,
    }]

    e2e_fixture.add_validator(
        validator,
        table=table,
    )

    feature_file = e2e_fixture.test_steps(
        scenario=[
            'Then get request with name "test-get-1" from endpoint "/api/test"',
            f'And set response content type to "{content_type}"',
        ],
        identifier=content_type,
    )

    rc, _ = e2e_fixture.execute(feature_file)
    assert rc == 0

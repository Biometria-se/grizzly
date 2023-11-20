"""Unit tests of grizzly.steps.scenario.tasks.until."""
from __future__ import annotations

from typing import TYPE_CHECKING, List, cast

import pytest

from grizzly.context import GrizzlyContext
from grizzly.steps import step_task_client_get_endpoint_until, step_task_request_with_name_endpoint_until
from grizzly.tasks import UntilRequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestMethod
from grizzly.types.behave import Row, Table
from grizzly_extras.transformer import TransformerContentType

if TYPE_CHECKING:  # pragma: no cover
    from tests.fixtures import BehaveFixture


def test_step_task_request_with_name_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    assert len(grizzly.scenario.tasks()) == 0

    with pytest.raises(AssertionError, match='this step is only valid for request methods with direction FROM'):
        step_task_request_with_name_endpoint_until(behave, RequestMethod.POST, 'test', '/api/test', '$.`this`[?status="ready"]')

    behave.text = 'foo bar'
    with pytest.raises(AssertionError, match='this step does not have support for step text'):
        step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')

    behave.text = None

    with pytest.raises(ValueError, match='content type must be specified for request'):
        step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test', '$.`this`[?status="ready"]')

    step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/test | content_type=json', '$.`this`[?status="ready"]')

    assert len(grizzly.scenario.tasks()) == 1

    rows: List[Row] = []
    rows.append(Row(['endpoint'], ['{{ variable }}']))
    rows.append(Row(['endpoint'], ['foo']))
    rows.append(Row(['endpoint'], ['bar']))
    behave.table = Table(['endpoint'], rows=rows)

    step_task_request_with_name_endpoint_until(behave, RequestMethod.GET, 'test', '/api/{{ endpoint }} | content_type=json', '$.`this`[?status="{{ endpoint }}"]')

    assert len(grizzly.scenario.tasks()) == 4
    tasks = cast(List[UntilRequestTask], grizzly.scenario.tasks())

    templates: List[str] = []

    assert tasks[-1].request.endpoint == '/api/bar'
    assert tasks[-1].condition == '$.`this`[?status="bar"]'
    templates += tasks[-1].get_templates()
    assert tasks[-2].request.endpoint == '/api/foo'
    assert tasks[-2].condition == '$.`this`[?status="foo"]'
    templates += tasks[-2].get_templates()
    assert tasks[-3].request.endpoint == '/api/{{ variable }}'
    assert tasks[-3].condition == '$.`this`[?status="{{ variable }}"]'
    templates += tasks[-3].get_templates()

    assert len(templates) == 2
    assert sorted(templates) == sorted([
        '$.`this`[?status="{{ variable }}"]',
        '/api/{{ variable }}',
    ])


def test_step_task_client_get_endpoint_until(behave_fixture: BehaveFixture) -> None:
    behave = behave_fixture.context
    grizzly = cast(GrizzlyContext, behave.grizzly)
    grizzly.scenarios.create(behave_fixture.create_scenario('test scenario'))

    with pytest.raises(AssertionError, match='could not find scheme in "api.example.io/api/test"'):
        step_task_client_get_endpoint_until(behave, 'api.example.io/api/test', 'step-name', '$.`this`[?status=true]')

    with pytest.raises(AssertionError, match='no client task registered for foo'):
        step_task_client_get_endpoint_until(behave, 'foo://api.example.io/api/test', 'step-name', '$.`this`[?status=true]')

    with pytest.raises(ValueError, match='content type must be specified for request'):
        step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test', 'step-name', '$.`this`[?status=true]')

    with pytest.raises(ValueError, match='unsupported arguments foo, bar'):
        step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test | content_type=json', 'step-name', '$.`this`[?success=true] | foo=bar, bar=foo')

    step_task_client_get_endpoint_until(behave, 'https://api.example.io/api/test | content_type=json', 'step-name', '$.`this`[?success=true]')

    assert len(grizzly.scenario.tasks()) == 1
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, UntilRequestTask)
    assert isinstance(task.request, HttpClientTask)
    assert task.request.content_type == TransformerContentType.JSON
    assert task.request.endpoint == 'https://api.example.io/api/test'

    grizzly.state.configuration['test.host'] = 'https://api.example.com'

    step_task_client_get_endpoint_until(behave, 'https://$conf::test.host$/api/test | content_type=json', 'step-name', '$.`this`[success=false]')

    assert len(grizzly.scenario.tasks()) == 2
    task = grizzly.scenario.tasks()[-1]
    assert isinstance(task, UntilRequestTask)
    assert isinstance(task.request, HttpClientTask)
    assert task.request.content_type == TransformerContentType.JSON
    assert task.request.endpoint == 'https://api.example.com/api/test'

    behave.text = '1=1'
    with pytest.raises(NotImplementedError, match='HttpClientTask has not implemented support for step text'):
        step_task_client_get_endpoint_until(behave, 'https://$conf::test.host$/api/test | content_type=json', 'step-name', '$.`this`[success=false]')

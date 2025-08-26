"""Unit tests for grizzly.tasks.transformer."""

from __future__ import annotations

from contextlib import suppress
from json import dumps as jsondumps
from typing import TYPE_CHECKING

import pytest
from grizzly.exceptions import RestartScenario
from grizzly.tasks import TransformerTask
from grizzly_common.transformer import TransformerContentType, TransformerError, transformer

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from test_framework.fixtures import GrizzlyFixture, MockerFixture


class TestTransformerTask:
    def test_task(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:  # noqa: PLR0915
        grizzly = grizzly_fixture.grizzly

        parent = grizzly_fixture()

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        with pytest.raises(AssertionError, match='test_variable has not been initialized'):
            TransformerTask(
                variable='test_variable',
                expression='$.',
                content_type=TransformerContentType.JSON,
                content='',
            )

        grizzly.scenario.variables.update({'test_variable': 'none'})
        parent.user.variables.update({'test_variable': 'none'})

        json_transformer = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]
        try:
            with pytest.raises(AssertionError, match='could not find a transformer for JSON'):
                TransformerTask(
                    variable='test_variable',
                    expression='$.',
                    content_type=TransformerContentType.JSON,
                    content='',
                )
        finally:
            transformer.available.update({TransformerContentType.JSON: json_transformer})

        task_factory = TransformerTask(
            variable='test_variable',
            expression='$.result.value',
            content_type=TransformerContentType.JSON,
            content='',
        )

        assert task_factory.__template_attributes__ == {'content', 'expression'}

        task = task_factory()

        assert callable(task)

        task(parent)

        fire_spy.assert_called_once_with(
            request_type='TRNSF',
            name=f'{parent.user._scenario.identifier} Transformer=>test_variable',
            response_time=ANY(int),
            response_length=0,
            context=parent.user._context,
            exception=ANY(TransformerError, message='failed to transform input as JSON'),
        )
        fire_spy.reset_mock()

        task_factory = TransformerTask(
            variable='test_variable',
            expression='$.result.value',
            content_type=TransformerContentType.JSON,
            content=jsondumps(
                {
                    'result': {
                        'value': 'hello world!',
                    },
                },
            ),
        )

        task = task_factory()

        assert callable(task)

        assert parent.user.variables.get('test_variable', None) == 'none'

        task(parent)

        assert parent.user.variables.get('test_variable', None) == 'hello world!'

        parent.user.set_variable('payload_url', 'none')
        parent.user._scenario.variables.update({'payload_url': 'none'})
        content = jsondumps(
            {
                'entityType': 'contract',
                'entityConcreteType': 'contract',
                'entityId': 'C000001',
                'entityVersion': 1,
                'entityStatus': 'Created',
                'entityStatusChangedAtUtc': '2021-03-10T07:03:00Z',
                'format': 'json',
                'payloads': [
                    {
                        'url': 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile',
                        'expiresAtUtc': '2021-03-20T09:13:26.000000Z',
                    },
                ],
            },
        )
        task_factory = TransformerTask(
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content=content,
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user.variables.get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        parent.user.variables.update(
            {
                'payload_url': None,
                'payload': content,
            },
        )

        task_factory = TransformerTask(
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content='{{ payload }}',
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user.variables.get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        task_factory = TransformerTask(
            variable='test_variable',
            expression='$.result.name',
            content_type=TransformerContentType.JSON,
            content=jsondumps(
                {
                    'result': {
                        'value': 'hello world!',
                    },
                },
            ),
        )
        task = task_factory()
        parent.user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(RestartScenario):
            task(parent)

        fire_spy.assert_called_once_with(
            request_type='TRNSF',
            name=f'{parent.user._scenario.identifier} Transformer=>test_variable',
            response_time=ANY(int),
            response_length=37,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='"$.result.name" returned 0 matches'),
        )
        fire_spy.reset_mock()

        # allow 0 matches with `| min_matches=0`
        task_factory = TransformerTask(
            variable='test_variable',
            expression='$.result.name | min_matches=0',
            content_type=TransformerContentType.JSON,
            content=jsondumps(
                {
                    'result': {
                        'value': 'hello world!',
                    },
                },
            ),
        )
        task = task_factory()
        parent.user._scenario.failure_handling.update({None: RestartScenario})
        task(parent)

        fire_spy.assert_not_called()

        with suppress(KeyError):
            del parent.user._scenario.failure_handling[None]

        task_factory = TransformerTask(
            variable='test_variable',
            expression='$.result[?value="hello world!"]',
            content_type=TransformerContentType.JSON,
            content=jsondumps(
                {
                    'result': [
                        {'value': 'hello world!'},
                        {'value': 'hello world!'},
                        {'value': 'hello world!'},
                    ],
                },
            ),
        )
        task = task_factory()
        task(parent)

        assert (
            parent.user.variables['test_variable']
            == """{"value": "hello world!"}
{"value": "hello world!"}
{"value": "hello world!"}"""
        )

        parent.user._scenario.variables.update({'test_bool': 'none'})
        parent.user.variables.update({'test_bool': 'none'})
        task_factory = TransformerTask(
            variable='test_bool',
            expression='$.success',
            content_type=TransformerContentType.JSON,
            content=jsondumps(
                {
                    'success': True,
                },
            ),
        )
        assert task_factory.min_matches == 1
        task = task_factory()
        task(parent)

        assert parent.user.variables['test_bool']

        task_factory = TransformerTask(
            variable='test_variable',
            expression='//actor[@id="9"] | min_matches=-1',
            content_type=TransformerContentType.XML,
            content="""<root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
  <actors>
    <actor id="7">Christian Bale</actor>
    <actor id="8">Liam Neeson</actor>
    <actor id="9">Michael Caine</actor>
  </actors>
  <foo:singers>
    <foo:singer id="10">Tom Waits</foo:singer>
    <foo:singer id="11">B.B. King</foo:singer>
    <foo:singer id="12">Ray Charles</foo:singer>
  </foo:singers>
</root>""",
        )

        assert task_factory.expression == '//actor[@id="9"]'
        assert task_factory.min_matches == -1

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user.variables['test_variable'] == '<actor id="9">Michael Caine</actor>'

        parent.user._scenario.variables.update({'child_elem': 'none'})
        parent.user.set_variable('child_elem', 'none')

        task_factory = TransformerTask(
            variable='child_elem',
            expression='/root/actors/child::*',
            content_type=TransformerContentType.XML,
            content="""<root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
  <actors>
    <actor id="7">Christian Bale</actor>
    <actor id="8">Liam Neeson</actor>
    <actor id="9">Michael Caine</actor>
  </actors>
  <foo:singers>
    <foo:singer id="10">Tom Waits</foo:singer>
    <foo:singer id="11">B.B. King</foo:singer>
    <foo:singer id="12">Ray Charles</foo:singer>
  </foo:singers>
</root>""",
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert (
            parent.user.variables['child_elem']
            == """<actor id="7">Christian Bale</actor>
<actor id="8">Liam Neeson</actor>
<actor id="9">Michael Caine</actor>"""
        )

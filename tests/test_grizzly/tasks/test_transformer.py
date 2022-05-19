from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture

from grizzly_extras.transformer import transformer, TransformerContentType
from grizzly.tasks import TransformerTask
from grizzly.exceptions import RestartScenario, TransformerLocustError

from ...fixtures import GrizzlyFixture


class TestTransformerTask:
    def test(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        scenario_context = grizzly_fixture.request_task.request.scenario

        fire_spy = mocker.spy(scenario.user.environment.events.request, 'fire')

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
                scenario=scenario_context,
            )
        assert 'test_variable has not been initialized' in str(ve)

        grizzly.state.variables.update({'test_variable': 'none'})

        json_transformer = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
                scenario=scenario_context,
            )
        assert 'could not find a transformer for JSON' in str(ve)

        transformer.available.update({TransformerContentType.JSON: json_transformer})

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
                scenario=scenario_context,
            )
        assert '$. is not a valid expression for JSON' in str(ve)

        task_factory = TransformerTask(
            grizzly, variable='test_variable', expression='$.result.value', content_type=TransformerContentType.JSON, content='',
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        task(scenario)

        assert fire_spy.call_count == 1
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TRNSF'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} Transformer=>test_variable'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == scenario.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, TransformerLocustError)
        assert str(exception) == 'failed to transform JSON'

        task_factory = TransformerTask(
            grizzly,
            variable='test_variable',
            expression='$.result.value',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': {
                    'value': 'hello world!',
                },
            }),
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        assert scenario.user._context['variables'].get('test_variable', None) is None

        task(scenario)

        assert scenario.user._context['variables'].get('test_variable', None) == 'hello world!'

        grizzly.state.variables.update({'payload_url': 'none'})
        content = jsondumps({
            "entityType": "contract",
            "entityConcreteType": "contract",
            "entityId": "C000001",
            "entityVersion": 1,
            "entityStatus": "Created",
            "entityStatusChangedAtUtc": "2021-03-10T07:03:00Z",
            "format": "json",
            "payloads": [{
                "url": "https://mystorageaccount.blob.core.windows.net/mycontainer/myfile",
                "expiresAtUtc": "2021-03-20T09:13:26.000000Z",
            }]
        })
        task_factory = TransformerTask(
            grizzly,
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content=content,
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        task(scenario)

        assert scenario.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        scenario.user._context['variables'].update({
            'payload_url': None,
            'payload': content,
        })

        task_factory = TransformerTask(
            grizzly,
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content='{{ payload }}',
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        task(scenario)

        assert scenario.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        assert fire_spy.call_count == 1

        task_factory = TransformerTask(
            grizzly,
            variable='test_variable',
            expression='$.result.name',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': {
                    'value': 'hello world!',
                },
            }),
            scenario=scenario_context,
        )
        task = task_factory()
        scenario_context.failure_exception = RestartScenario
        with pytest.raises(RestartScenario):
            task(scenario)

        assert fire_spy.call_count == 2
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TRNSF'
        assert kwargs.get('name', None) == f'{scenario.user._scenario.identifier} Transformer=>test_variable'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 37
        assert kwargs.get('context', None) == scenario.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"$.result.name" returned 0 matches'

        scenario_context.failure_exception = None

        task_factory = TransformerTask(
            grizzly,
            variable='test_variable',
            expression='$.result[?value="hello world!"]',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': [
                    {'value': 'hello world!'},
                    {'value': 'hello world!'},
                    {'value': 'hello world!'},
                ],
            }),
            scenario=scenario_context,
        )
        task = task_factory()
        task(scenario)

        assert scenario.user._context['variables']['test_variable'] == '''{"value": "hello world!"}
{"value": "hello world!"}
{"value": "hello world!"}'''

        task_factory = TransformerTask(
            grizzly,
            variable='test_variable',
            expression='//actor[@id="9"]',
            content_type=TransformerContentType.XML,
            content='''<root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
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
</root>''',
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        task(scenario)

        assert scenario.user._context['variables']['test_variable'] == '<actor id="9">Michael Caine</actor>'

        grizzly.state.variables['child_elem'] = 'none'

        task_factory = TransformerTask(
            grizzly,
            variable='child_elem',
            expression='/root/actors/child::*',
            content_type=TransformerContentType.XML,
            content='''<root xmlns:foo="http://www.foo.org/" xmlns:bar="http://www.bar.org">
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
</root>''',
            scenario=scenario_context,
        )

        task = task_factory()

        assert callable(task)

        task(scenario)

        assert scenario.user._context['variables']['child_elem'] == '''<actor id="7">Christian Bale</actor>
<actor id="8">Liam Neeson</actor>
<actor id="9">Michael Caine</actor>'''

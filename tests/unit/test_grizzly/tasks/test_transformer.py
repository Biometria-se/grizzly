from json import dumps as jsondumps

import pytest

from pytest_mock import MockerFixture

from grizzly_extras.transformer import transformer, TransformerContentType
from grizzly.tasks import TransformerTask
from grizzly.exceptions import RestartScenario, TransformerLocustError

from tests.fixtures import GrizzlyFixture


class TestTransformerTask:
    def test(self, grizzly_fixture: GrizzlyFixture, mocker: MockerFixture) -> None:
        grizzly = grizzly_fixture.grizzly

        parent = grizzly_fixture()

        fire_spy = mocker.spy(parent.user.environment.events.request, 'fire')

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert 'test_variable has not been initialized' in str(ve)

        grizzly.state.variables.update({'test_variable': 'none'})

        json_transformer = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert 'could not find a transformer for JSON' in str(ve)

        transformer.available.update({TransformerContentType.JSON: json_transformer})

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                grizzly, variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert '$. is not a valid expression for JSON' in str(ve)

        task_factory = TransformerTask(
            grizzly, variable='test_variable', expression='$.result.value', content_type=TransformerContentType.JSON, content='',
        )

        assert task_factory.__template_attributes__ == {'content'}

        task = task_factory()

        assert callable(task)

        task(parent)

        assert fire_spy.call_count == 1
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TRNSF'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} Transformer=>test_variable'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 0
        assert kwargs.get('context', None) == parent.user._context
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
        )

        task = task_factory()

        assert callable(task)

        assert parent.user._context['variables'].get('test_variable', None) is None

        task(parent)

        assert parent.user._context['variables'].get('test_variable', None) == 'hello world!'

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
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        parent.user._context['variables'].update({
            'payload_url': None,
            'payload': content,
        })

        task_factory = TransformerTask(
            grizzly,
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content='{{ payload }}',
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

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
        )
        task = task_factory()
        parent.user._scenario.failure_exception = RestartScenario
        with pytest.raises(RestartScenario):
            task(parent)

        assert fire_spy.call_count == 2
        _, kwargs = fire_spy.call_args_list[-1]

        assert kwargs.get('request_type', None) == 'TRNSF'
        assert kwargs.get('name', None) == f'{parent.user._scenario.identifier} Transformer=>test_variable'
        assert kwargs.get('response_time', None) >= 0.0
        assert kwargs.get('response_length', None) == 37
        assert kwargs.get('context', None) == parent.user._context
        exception = kwargs.get('exception', None)
        assert isinstance(exception, RuntimeError)
        assert str(exception) == '"$.result.name" returned 0 matches'

        parent.user._scenario.failure_exception = None

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
        )
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['test_variable'] == '''{"value": "hello world!"}
{"value": "hello world!"}
{"value": "hello world!"}'''

        task_factory = TransformerTask(
            grizzly,
            variable='test_variable',
            expression='$.success',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'success': True,
            }),
        )
        task = task_factory()
        task(parent)

        assert parent.user._context['variables']['test_variable'] == 'True'

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
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user._context['variables']['test_variable'] == '<actor id="9">Michael Caine</actor>'

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
        )

        task = task_factory()

        assert callable(task)

        task(parent)

        assert parent.user._context['variables']['child_elem'] == '''<actor id="7">Christian Bale</actor>
<actor id="8">Liam Neeson</actor>
<actor id="9">Michael Caine</actor>'''

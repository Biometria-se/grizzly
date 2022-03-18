from typing import cast
from json import dumps as jsondumps

import pytest

from grizzly_extras.transformer import transformer, TransformerContentType
from grizzly.context import GrizzlyContext
from grizzly.tasks import TransformerTask
from grizzly.exceptions import TransformerLocustError

from ..fixtures import GrizzlyFixture


class TestTransformerTask:
    def test(self, grizzly_fixture: GrizzlyFixture) -> None:
        behave = grizzly_fixture.behave
        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert 'test_variable has not been initialized' in str(ve)

        grizzly = cast(GrizzlyContext, behave.grizzly)
        grizzly.state.variables.update({'test_variable': 'none'})

        json_transformer = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert 'could not find a transformer for JSON' in str(ve)

        transformer.available.update({TransformerContentType.JSON: json_transformer})

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert '$. is not a valid expression for JSON' in str(ve)

        task = TransformerTask(
            variable='test_variable', expression='$.result.value', content_type=TransformerContentType.JSON, content='',
        )

        implementation = task.implementation()

        assert callable(implementation)

        _, _, scenario = grizzly_fixture()

        assert scenario is not None

        with pytest.raises(TransformerLocustError) as tle:
            implementation(scenario)
        assert 'failed to transform JSON' in str(tle)

        task = TransformerTask(
            variable='test_variable',
            expression='$.result.value',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': {
                    'value': 'hello world!',
                },
            })
        )

        implementation = task.implementation()

        assert callable(implementation)

        assert scenario.user._context['variables'].get('test_variable', None) is None

        implementation(scenario)

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
        task = TransformerTask(
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content=content,
        )

        implementation = task.implementation()

        assert callable(implementation)

        implementation(scenario)

        assert scenario.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        scenario.user._context['variables']['payload_url'] = None
        scenario.user._context['variables']['payload'] = content

        task = TransformerTask(
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content='{{ payload }}',
        )

        implementation = task.implementation()

        assert callable(implementation)

        implementation(scenario)

        assert scenario.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        task = TransformerTask(
            variable='test_variable',
            expression='$.result.name',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': {
                    'value': 'hello world!',
                },
            })
        )
        implementation = task.implementation()
        with pytest.raises(RuntimeError) as re:
            implementation(scenario)
        assert 'TransformerTask: "$.result.name" returned 0 matches' in str(re)

        task = TransformerTask(
            variable='test_variable',
            expression='$.result[?value="hello world!"]',
            content_type=TransformerContentType.JSON,
            content=jsondumps({
                'result': [
                    {'value': 'hello world!'},
                    {'value': 'hello world!'},
                    {'value': 'hello world!'},
                ],
            })
        )
        implementation = task.implementation()
        with pytest.raises(RuntimeError) as re:
            implementation(scenario)
        assert 'TransformerTask: "$.result[?value="hello world!"]" returned 3 matches' in str(re)

        task = TransformerTask(
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

        implementation = task.implementation()

        assert callable(implementation)

        implementation(scenario)

        assert scenario.user._context['variables']['test_variable'] == '<actor id="9">Michael Caine</actor>'

from typing import Callable, cast
from json import dumps as jsondumps

import pytest

from pytest_mock import mocker, MockerFixture  # pylint: disable=unused-import
from behave.runner import Context

from grizzly_extras.transformer import transformer, TransformerContentType
from grizzly.context import GrizzlyContext
from grizzly.task import TransformerTask
from grizzly.exceptions import TransformerLocustError

from ..fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestTransformerTask:
    @pytest.mark.usefixtures('behave_context', 'grizzly_context')
    def test(self, behave_context: Context, grizzly_context: Callable) -> None:
        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=TransformerContentType.JSON, content='',
            )
        assert 'test_variable has not been initialized' in str(ve)

        grizzly = cast(GrizzlyContext, behave_context.grizzly)
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

        _, _, tasks, _ = grizzly_context()

        with pytest.raises(TransformerLocustError) as re:
            implementation(tasks)
        assert 'failed to transform JSON' in str(re)

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

        assert tasks.user._context['variables'].get('test_variable', None) is None

        implementation(tasks)

        assert tasks.user._context['variables'].get('test_variable', None) == 'hello world!'

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

        implementation(tasks)

        assert tasks.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'

        tasks.user._context['variables']['payload_url'] = None
        tasks.user._context['variables']['payload'] = content

        task = TransformerTask(
            variable='payload_url',
            expression='$.payloads[0].url',
            content_type=TransformerContentType.JSON,
            content='{{ payload }}',
        )

        implementation = task.implementation()

        assert callable(implementation)

        implementation(tasks)

        assert tasks.user._context['variables'].get('payload_url', None) == 'https://mystorageaccount.blob.core.windows.net/mycontainer/myfile'


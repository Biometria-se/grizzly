import logging

from typing import Any, Tuple, Optional, Dict, Callable, cast
from json import dumps as jsondumps

import pytest

from pytest_mock import mocker  # pylint: disable=unused-import
from pytest_mock.plugin import MockerFixture
from _pytest.logging import LogCaptureFixture
from locust.clients import ResponseContextManager
from locust.user.users import User
from behave.runner import Context
from grizzly.context import GrizzlyContext

from grizzly.task import (
    RequestTask,
    SleepTask,
    PrintTask,
    TransformerTask,
    RequestTaskHandlers,
    RequestTaskResponse,
)

from grizzly.types import ResponseContentType, RequestMethod

from grizzly_extras.transformer import transformer

from .fixtures import grizzly_context, request_task, behave_context, locust_environment  # pylint: disable=unused-import

class TestRequestTaskHandlers:
    def tests(self) -> None:
        handlers = RequestTaskHandlers()

        assert hasattr(handlers, 'metadata')
        assert hasattr(handlers, 'payload')

        assert len(handlers.metadata) == 0
        assert len(handlers.payload) == 0

        def handler(input: Tuple[ResponseContentType, Any], user: User, manager: Optional[ResponseContextManager]) -> None:
            pass

        handlers.add_metadata(handler)
        handlers.add_payload(handler)

        assert len(handlers.metadata) == 1
        assert len(handlers.payload) == 1


class TestRequestTaskResponse:
    def test(self) -> None:
        response_task = RequestTaskResponse()
        assert response_task.content_type == ResponseContentType.GUESS

        assert isinstance(response_task.handlers, RequestTaskHandlers)

        assert 200 in response_task.status_codes

        response_task.add_status_code(-200)
        assert 200 not in response_task.status_codes

        response_task.add_status_code(200)
        response_task.add_status_code(302)
        assert [200, 302] == response_task.status_codes

        response_task.add_status_code(200)
        assert [200, 302] == response_task.status_codes

        response_task.add_status_code(-302)
        response_task.add_status_code(400)
        assert [200, 400] == response_task.status_codes


class TestRequestTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, grizzly_context: Callable, mocker: MockerFixture) -> None:
        task = RequestTask(RequestMethod.from_string('POST'), 'test-name', '/api/test')

        assert task.method == RequestMethod.POST
        assert task.name == 'test-name'
        assert task.endpoint == '/api/test'

        assert not hasattr(task, 'scenario')

        assert task.template is None
        assert task.source is None

        implementation = task.implementation()
        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        mocker.patch.object(tasks.user, 'request', noop)
        request_spy = mocker.spy(tasks.user, 'request')

        implementation(tasks)

        assert request_spy.call_count == 1
        args, _ = request_spy.call_args_list[0]
        assert args[0] is task


class TestSleepTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, mocker: MockerFixture, grizzly_context: Callable) -> None:
        task = SleepTask(sleep=1.0)

        assert task.sleep == 1.0
        implementation = task.implementation()

        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        def noop(*args: Tuple[Any, ...], **kwargs: Dict[str, Any]) -> Any:
            pass

        import grizzly.task
        mocker.patch.object(grizzly.task, 'gsleep', noop)
        gsleep_spy = mocker.spy(grizzly.task, 'gsleep')

        implementation(tasks)

        assert gsleep_spy.call_count == 1
        args, _ = gsleep_spy.call_args_list[0]
        assert args[0] == task.sleep


class TestPrintTask:
    @pytest.mark.usefixtures('grizzly_context')
    def test(self, mocker: MockerFixture, grizzly_context: Callable, caplog: LogCaptureFixture) -> None:
        task = PrintTask(message='hello world!')
        assert task.message == 'hello world!'

        implementation = task.implementation()

        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        with caplog.at_level(logging.INFO):
            implementation(tasks)
        assert 'hello world!' in caplog.text
        caplog.clear()

        task = PrintTask(message='variable={{ variable }}')
        assert task.message == 'variable={{ variable }}'

        implementation = task.implementation()

        assert callable(implementation)

        tasks.user._context['variables']['variable'] = 'hello world!'

        with caplog.at_level(logging.INFO):
            implementation(tasks)
        assert 'variable=hello world!' in caplog.text
        caplog.clear()


class TestTransformerTask:
    @pytest.mark.usefixtures('behave_context', 'grizzly_context')
    def test(self, behave_context: Context, grizzly_context: Callable) -> None:
        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=ResponseContentType.JSON, content='',
            )
        assert 'test_variable has not been initialized' in str(ve)

        grizzly = cast(GrizzlyContext, behave_context.grizzly)
        grizzly.state.variables.update({'test_variable': 'none'})

        json_transformer = transformer.available[ResponseContentType.JSON]
        del transformer.available[ResponseContentType.JSON]

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=ResponseContentType.JSON, content='',
            )
        assert 'could not find a transformer for JSON' in str(ve)

        transformer.available.update({ResponseContentType.JSON: json_transformer})

        with pytest.raises(ValueError) as ve:
            TransformerTask(
                variable='test_variable', expression='$.', content_type=ResponseContentType.JSON, content='',
            )
        assert '$. is not a valid expression for JSON' in str(ve)

        task = TransformerTask(
            variable='test_variable', expression='$.result.value', content_type=ResponseContentType.JSON, content='',
        )

        implementation = task.implementation()

        assert callable(implementation)

        _, _, tasks, _ = grizzly_context()

        with pytest.raises(RuntimeError) as re:
            implementation(tasks)
        assert 'failed to transform JSON' in str(re)

        task = TransformerTask(
            variable='test_variable',
            expression='$.result.value',
            content_type=ResponseContentType.JSON,
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

        task = TransformerTask(
            variable='test_variable',
            expression='//actor[@id="9"]',
            content_type=ResponseContentType.XML,
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

        implementation(tasks)

        assert tasks.user._context['variables']['test_variable'] == '<actor id="9">Michael Caine</actor>'




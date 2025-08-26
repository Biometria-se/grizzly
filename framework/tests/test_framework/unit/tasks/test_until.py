"""Unit tests for grizzly.tasks.until."""

from __future__ import annotations

from contextlib import suppress
from json import dumps as jsondumps
from logging import ERROR
from typing import TYPE_CHECKING, Any

import pytest
from grizzly.exceptions import RestartScenario
from grizzly.tasks import GrizzlyMetaRequestTask, RequestTask, UntilRequestTask
from grizzly.tasks.clients import HttpClientTask
from grizzly.types import RequestDirection, RequestMethod, StrDict
from grizzly.types.locust import StopUser
from grizzly_common.transformer import TransformerContentType, TransformerError, transformer

from test_framework.helpers import ANY

if TYPE_CHECKING:  # pragma: no cover
    from _pytest.logging import LogCaptureFixture

    from test_framework.fixtures import GrizzlyFixture, MockerFixture


parameterize = (
    'meta_request_task_type,meta_args,meta_kwargs',
    [
        (RequestTask, (RequestMethod.GET,), {'name': 'test-request', 'endpoint': '/api/test | content_type=json'}),
        (HttpClientTask, (RequestDirection.FROM, 'https://example.io/test | content_type=json', 'test-request'), {}),
    ],
)


class TestUntilRequestTask:
    def test___init__(self, grizzly_fixture: GrizzlyFixture) -> None:
        grizzly_fixture()

        request = RequestTask(RequestMethod.GET, name='test', endpoint='/api/test')

        with pytest.raises(AssertionError, match='content type must be specified for request'):
            UntilRequestTask(request, '$.`this`[?status="ready"]')

        request.response.content_type = request.content_type = TransformerContentType.JSON

        with pytest.raises(AssertionError, match='unsupported arguments foo, bar'):
            UntilRequestTask(request, '$.`this`[?status="ready"] | foo=bar, bar=foo')

        task = UntilRequestTask(request, '$.`this`[?status="ready"]')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 1.0
        assert task.retries == 3
        assert task.__template_attributes__ == {'condition', 'request'}

        task = UntilRequestTask(request, '$.`this`[?status="ready"] | wait=100, retries=10')

        assert task.condition == '$.`this`[?status="ready"]'
        assert task.wait == 100
        assert task.retries == 10

        with pytest.raises(AssertionError, match='wait argument cannot be less than 0.1 seconds'):
            UntilRequestTask(request, '$.`this`[?status="ready"] | wait=0.0, retries=10')

        with pytest.raises(AssertionError, match='retries argument cannot be less than 1'):
            UntilRequestTask(request, '$.`this`[?status="ready"] | wait=0.1, retries=0')

    @pytest.mark.parametrize(*parameterize)
    def test___call__(  # noqa: PLR0915
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        caplog: LogCaptureFixture,
        meta_request_task_type: type[GrizzlyMetaRequestTask],
        meta_args: tuple[Any, ...],
        meta_kwargs: StrDict,
    ) -> None:
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        if meta_request_task_type == HttpClientTask:
            meta_request_task_type.__scenario__ = grizzly.scenario  # type: ignore[attr-defined]

        meta_request_task = meta_request_task_type(*meta_args, **meta_kwargs)

        def create_response(status: str) -> str:
            return jsondumps(
                {
                    'status': status,
                },
            )

        request_spy = mocker.patch.object(
            meta_request_task,
            'execute',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
                (None, create_response('ready')),
                (None, create_response('ready')),
            ],
        )

        time_spy = mocker.patch(
            'grizzly.tasks.until.perf_counter',
            side_effect=[
                0.0,
                153.5,
                0.0,
                12.25,
                0.0,
                12.25,
                0.0,
                1.5,
                0.0,
                0.8,
                0.0,
                0.8,
                0.0,
                0.555,
                0.0,
                0.666,
                0.0,
                0.666,
                0.0,
                0.111,
            ],
        )

        fire_spy = mocker.patch.object(
            parent.user.environment.events.request,
            'fire',
        )

        gsleep_spy = mocker.patch('grizzly.tasks.until.gsleep', autospec=True)

        task_factory = UntilRequestTask(meta_request_task, '/status[text()="ready"]')
        task = task_factory()

        with pytest.raises(RuntimeError, match='is not a valid expression for JSON'):
            task(parent)

        jsontransformer_orig = transformer.available[TransformerContentType.JSON]
        del transformer.available[TransformerContentType.JSON]

        task_factory = UntilRequestTask(meta_request_task, '$.`this`[?status="ready"] | wait=100, retries=10')

        with pytest.raises(TypeError, match='could not find a transformer for JSON'):
            task_factory()

        transformer.available[TransformerContentType.JSON] = jsontransformer_orig

        task_factory = UntilRequestTask(meta_request_task, "$.`this`[?status='ready'] | wait=100, retries=10")
        task = task_factory()

        with caplog.at_level(ERROR):
            task(parent)

        assert len(caplog.messages) == 0
        assert time_spy.call_count == 2
        assert request_spy.call_count == 3
        assert gsleep_spy.call_count == 3
        call_args_list: list[tuple[float]] = []
        for args_list in gsleep_spy.call_args_list:
            args, _ = args_list
            call_args_list.append(args)
        assert call_args_list == [(100.0,), (100.0,), (100.0,)]

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=100.0s, r=10, em=1',
            response_time=153500,
            response_length=61,
            context=parent.user._context,
            exception=None,
        )
        fire_spy.reset_mock()

        # -->
        parent.grizzly.scenario.variables.update({'wait': 100.0, 'retries': 10})
        parent.user.variables.update({'wait': 100.0, 'retries': 10})
        task_factory = UntilRequestTask(meta_request_task, "$.`this`[?status='ready'] | wait='{{ wait }}', retries='{{ retries }}'")
        task = task_factory()

        with caplog.at_level(ERROR):
            task(parent)

        assert len(caplog.messages) == 0
        assert time_spy.call_count == 4
        assert request_spy.call_count == 4
        assert gsleep_spy.call_count == 4
        call_args_list = []
        for args_list in gsleep_spy.call_args_list:
            args, _ = args_list
            call_args_list.append(args)
        assert call_args_list == [(100.0,), (100.0,), (100.0,), (100.0,)]

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=100.0s, r=10, em=1',
            response_time=12250,
            response_length=19,
            context=parent.user._context,
            exception=None,
        )
        fire_spy.reset_mock()

        # <--
        request_spy = mocker.patch.object(
            meta_request_task,
            'execute',
            side_effect=[
                (None, create_response('working')),
                (None, create_response('working')),
            ],
        )

        parent.user._scenario.failure_handling.update({None: StopUser})
        task_factory = UntilRequestTask(meta_request_task, '$.`this`[?status="ready"] | wait=10, retries=2')
        task = task_factory()

        with pytest.raises(StopUser), caplog.at_level(ERROR):
            task(parent)

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=10.0s, r=2, em=1',
            response_time=12250,
            response_length=42,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='found 0 matching values for $.`this`[?status="ready"] in payload'),
        )
        fire_spy.reset_mock()

        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == (
            f'{parent.user._scenario.identifier} test-request, w=10.0s, r=2, em=1: endpoint={meta_request_task.endpoint}, number_of_matches=0, '
            f'condition=\'$.`this`[?status="ready"]\', retry=2, response_time=12250 payload=\n{jsondumps({"status": "working"}, indent=2)}'
        )

        caplog.clear()

        request_spy = mocker.patch.object(
            meta_request_task,
            'execute',
            side_effect=[
                RuntimeError('foo bar'),
                (None, create_response('working')),
                (None, create_response('working')),
            ],
        )

        parent.user._scenario.failure_handling.update({None: RestartScenario})
        with pytest.raises(RestartScenario), caplog.at_level(ERROR):
            task(parent)

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=10.0s, r=2, em=1',
            response_time=1500,
            response_length=21,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='foo bar'),
        )
        fire_spy.reset_mock()

        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == (f'{parent.user._scenario.identifier} test-request, w=10.0s, r=2, em=1: retry=0, endpoint={meta_request_task.endpoint}')

        caplog.clear()

        request_spy = mocker.patch.object(
            meta_request_task,
            'execute',
            side_effect=[
                (None, create_response('working')),
                RuntimeError('foo bar'),
                (None, create_response('working')),
                (None, create_response('ready')),
            ],
        )

        task_factory = UntilRequestTask(meta_request_task, '$.`this`[?status="ready"] | wait=4, retries=4')
        task = task_factory()

        with caplog.at_level(ERROR):
            task(parent)

        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == (f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=1: retry=1, endpoint={meta_request_task.endpoint}')

        caplog.clear()

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=1',
            response_time=800,
            response_length=61,
            context=parent.user._context,
            exception=None,
        )
        fire_spy.reset_mock()

        return_value_object = {
            'list': [
                {
                    'count': 18,
                    'value': 'first',
                },
                {
                    'count': 18,
                    'value': 'wildcard',
                },
                {
                    'count': 19,
                    'value': 'second',
                },
                {
                    'count': 20,
                    'value': 'third',
                },
                {
                    'count': 21,
                    'value': 'fourth',
                },
                {
                    'count': 22,
                    'value': 'fifth',
                },
            ],
        }

        request_spy = mocker.patch.object(
            meta_request_task,
            'execute',
            return_value=(None, jsondumps(return_value_object)),
        )

        task_factory = UntilRequestTask(meta_request_task, '$.list[?(@.count > 19)] | wait=4, expected_matches=3, retries=4')
        assert task_factory.expected_matches == 3
        assert task_factory.wait == 4
        assert task_factory.retries == 4

        task = task_factory()

        with caplog.at_level(ERROR):
            task(parent)

        assert len(caplog.messages) == 0

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=3',
            response_time=800,
            response_length=213,
            context=parent.user._context,
            exception=None,
        )
        fire_spy.reset_mock()

        task_factory = UntilRequestTask(meta_request_task, '$.list[?(@.count > 19)] | wait=4, expected_matches=4, retries=4')
        assert task_factory.expected_matches == 4
        assert task_factory.wait == 4
        assert task_factory.retries == 4

        task = task_factory()
        with pytest.raises(RestartScenario), caplog.at_level(ERROR):
            task(parent)

        assert len(caplog.messages) == 1
        assert caplog.messages[-1] == (
            f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=4: endpoint={meta_request_task.endpoint}, number_of_matches=3, '
            f"condition='$.list[?(@.count > 19)]', retry=4, response_time=555 payload=\n{jsondumps(return_value_object, indent=2)}"
        )

        caplog.clear()

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=4',
            response_time=555,
            response_length=852,
            context=parent.user._context,
            exception=ANY(RuntimeError, message='found 3 matching values for $.list[?(@.count > 19)] in payload'),
        )
        fire_spy.reset_mock()

        task_factory = UntilRequestTask(meta_request_task, '$.list[?(@.count == 18)] | wait=4, expected_matches=2, retries=4')
        assert task_factory.expected_matches == 2
        assert task_factory.wait == 4
        assert task_factory.retries == 4

        task = task_factory()
        task(parent)

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=4, em=2',
            response_time=666,
            response_length=213,
            context=parent.user._context,
            exception=None,
        )
        fire_spy.reset_mock()

        task_factory = UntilRequestTask(meta_request_task, '$.list[?(@.count == 18)] | wait=4, expected_matches=1, retries=1')
        task = task_factory()
        request_spy.return_value = ({}, None)

        with suppress(KeyError):
            del parent.user._scenario.failure_handling[None]

        task(parent)

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=1, em=1',
            response_time=666,
            response_length=0,
            context=parent.user._context,
            exception=ANY(TransformerError),
        )
        fire_spy.reset_mock()

        mocker.patch.object(parent.logger, 'error', side_effect=[RuntimeError, None])
        request_spy.return_value = ({}, None)

        task(parent)

        fire_spy.assert_called_once_with(
            request_type='UNTL',
            name=f'{parent.user._scenario.identifier} test-request, w=4.0s, r=1, em=1',
            response_time=111,
            response_length=0,
            context=parent.user._context,
            exception=ANY(RuntimeError),
        )
        fire_spy.reset_mock()

    @pytest.mark.parametrize(*parameterize)
    def test_on_start(
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        meta_request_task_type: type[GrizzlyMetaRequestTask],
        meta_args: tuple,
        meta_kwargs: StrDict,
    ) -> None:
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        if meta_request_task_type == HttpClientTask:
            meta_request_task_type.__scenario__ = grizzly.scenario  # type: ignore[attr-defined]

        meta_request_task = meta_request_task_type(*meta_args, **meta_kwargs)

        on_start_spy = mocker.spy(meta_request_task, 'on_start')

        task_factory = UntilRequestTask(meta_request_task, "$.`this`[?status='ready'] | wait=100, retries=10")
        task = task_factory()

        task.on_start(parent)

        on_start_spy.assert_called_once_with(parent)

    @pytest.mark.parametrize(*parameterize)
    def test_on_stop(
        self,
        grizzly_fixture: GrizzlyFixture,
        mocker: MockerFixture,
        meta_request_task_type: type[GrizzlyMetaRequestTask],
        meta_args: tuple,
        meta_kwargs: StrDict,
    ) -> None:
        parent = grizzly_fixture()

        grizzly = grizzly_fixture.grizzly

        if meta_request_task_type == HttpClientTask:
            meta_request_task_type.__scenario__ = grizzly.scenario  # type: ignore[attr-defined]

        meta_request_task = meta_request_task_type(*meta_args, **meta_kwargs)

        on_stop_spy = mocker.spy(meta_request_task, 'on_stop')

        task_factory = UntilRequestTask(meta_request_task, "$.`this`[?status='ready'] | wait=100, retries=10")
        task = task_factory()

        task.on_stop(parent)

        on_stop_spy.assert_called_once_with(parent)
